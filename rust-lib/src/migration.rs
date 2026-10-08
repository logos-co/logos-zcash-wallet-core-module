//! The ZIP 318 migration from Orchard to Ironwood, driven from the wallet thread.
//!
//! The plan the approver reviews is the plan committed: it is kept in memory under an
//! id and a digest, never re-planned at signing. After signing, the drive loop proves,
//! broadcasts on schedule over Tor and records outcomes; what needs the password again
//! (an expired transfer, a re-plan) is reported as needing approval.

use rand::SeedableRng;
use rusqlite::Connection;
use serde_json::{json, Value};
use zcash_client_backend::data_api::{Account as _, AccountSource, WalletCommitmentTrees, WalletRead};
use zcash_client_backend::util::SystemClock;
use zcash_client_sqlite::pool_migration::orchard_ironwood::PoolMigrations;
use zcash_client_sqlite::AccountUuid;
use zcash_keys::keys::UnifiedSpendingKey;
use zcash_pool_migration::engine::{
    commit_preparation, plan_migration, prove_preparation, prove_transfer, MigrationPlan, MigrationTxKind,
    PoolMigrationRead, PoolMigrationWrite, ProveOutcome,
};
use zcash_pool_migration::satisfiability::{advance_migration, AdvanceConfig, DuenessTargets, ReorgSettleDepth, ReplanThreshold};
use zcash_pool_migration::state::{AdvanceStep, StepKind};
use zcash_pool_migration::wallet::{WalletMigration, WalletMigrationProver};
use zcash_protocol::consensus::{BlockHeight, NetworkUpgrade, Parameters};

use crate::keys::Phrase;
use crate::network::ZNetwork;
use crate::wallet::Db;

/// Ten blocks of settling before a displacement counts as permanent, as zcash-devtool uses.
const REORG_SETTLE_DEPTH: ReorgSettleDepth = ReorgSettleDepth::new(10);
/// Our estimate of NU7's mainnet height until ZIP 259 fixes it on 2026-10-20.
const MAINNET_NU7_ESTIMATE: u32 = 3_542_100;
/// No part of a run may broadcast within this many blocks plus two hours of NU7.
const NU7_GUARD_BLOCKS: u32 = 576;

pub type Store<'c> = PoolMigrations<&'c mut Connection, ZNetwork, SystemClock>;

fn rng() -> rand_chacha::ChaCha20Rng {
    rand_chacha::ChaCha20Rng::from_rng(&mut rand::rng())
}

pub fn store(conn: &mut Connection, params: ZNetwork, account: AccountUuid) -> Result<Store<'_>, String> {
    PoolMigrations::for_account(params, SystemClock, conn, account).map_err(|e| e.to_string())
}

fn ufvk(db: &Db, account: AccountUuid) -> Result<zcash_keys::keys::UnifiedFullViewingKey, String> {
    let acct = db.get_account(account).map_err(|e| e.to_string())?.ok_or("account missing")?;
    acct.ufvk().cloned().ok_or_else(|| "the account has no full viewing key".into())
}

/// The NU7 height a run must stay clear of, real or estimated.
fn nu7_height(params: &ZNetwork) -> Option<u32> {
    params.activation_height(NetworkUpgrade::Nu7).map(u32::from).or(match params {
        ZNetwork::Main => Some(MAINNET_NU7_ESTIMATE),
        ZNetwork::Test | ZNetwork::Regtest => None,
    })
}

/// Blocks in two hours, at the spacing in force at `h`.
fn two_hours_of_blocks(params: &ZNetwork, h: u32) -> u32 {
    match params.activation_height(NetworkUpgrade::Nu7) {
        Some(nu7) if h >= u32::from(nu7) => 2 * 3600 / 25,
        _ => 2 * 3600 / 75,
    }
}

pub fn plan(db: &Db, conn: &mut Connection, params: ZNetwork, account: AccountUuid) -> Result<MigrationPlan, String> {
    let store = store(conn, params, account)?;
    let migration = WalletMigration::new(db, account, ufvk(db, account)?, store);
    plan_migration(&params, &migration, &mut rng()).map_err(|e| e.to_string())
}

/// What the approver reviews. The digest binds approval to exactly this plan.
pub fn preview(params: &ZNetwork, plan: &MigrationPlan, tip: u32) -> Value {
    let crossing: Vec<u64> = plan.crossing_values().iter().map(|z| z.into_u64()).collect();
    let schedule: Vec<Value> = plan
        .schedule()
        .iter()
        .map(|s| json!({"broadcastHeight": u32::from(s.broadcast_height()), "expiryHeight": u32::from(s.expiry_height())}))
        .collect();
    let last = plan.schedule().iter().map(|s| u32::from(s.broadcast_height())).max().unwrap_or(tip);
    let nu7 = nu7_height(params);
    let crosses_nu7 = nu7.is_some_and(|h| tip < h && last + NU7_GUARD_BLOCKS + two_hours_of_blocks(params, last) >= h);
    let mut v = json!({
        "network": params.name(),
        "amountsMadePublic": crossing,
        "migrating": crossing.iter().sum::<u64>(),
        "fundingNotes": plan.funding_notes().len(),
        "preparationTransactions": plan.preparation().transaction_count(),
        "preparationLayers": plan.preparation().layer_count(),
        "transfers": plan.schedule().len(),
        "schedule": schedule,
        "firstBroadcast": plan.schedule().iter().map(|s| u32::from(s.broadcast_height())).min(),
        "lastBroadcast": last,
        "chainTip": tip,
        "crossesNu7": crosses_nu7,
        "nu7Height": nu7,
    });
    let digest = blake2b_simd::Params::new().hash_length(32).personal(b"LogosZcashMigPrv").hash(v.to_string().as_bytes());
    v["digest"] = json!(hex::encode(digest.as_bytes()));
    v
}

/// Signs every transaction of the run with the account's Orchard spend authority, which
/// lives for this call only, and stores the run. Refused across NU7 (ZIP 318 has no rule
/// for it; parts signed for NU6.3 would die at activation).
pub fn commit(
    db: &Db,
    conn: &mut Connection,
    params: ZNetwork,
    account: AccountUuid,
    plan: &MigrationPlan,
    preview: &Value,
    phrase: &Phrase,
) -> Result<Value, String> {
    if preview["crossesNu7"].as_bool() == Some(true) {
        return Err("this run would still be broadcasting when NU7 activates; start it after the upgrade".into());
    }
    let acct = db.get_account(account).map_err(|e| e.to_string())?.ok_or("account missing")?;
    let index = match acct.source() {
        AccountSource::Derived { derivation, .. } => derivation.account_index(),
        _ => return Err("this account was not derived from the wallet's seed".into()),
    };
    let usk = UnifiedSpendingKey::from_seed(&params, secrecy::ExposeSecret::expose_secret(&phrase.seed()), index)
        .map_err(|e| format!("{e:?}"))?;
    let target = db.chain_height().map_err(|e| e.to_string())?.ok_or("the wallet is not synced")? + 1;
    let store = store(conn, params, account)?;
    let mut migration = WalletMigration::new(db, account, ufvk(db, account)?, store);
    let state = commit_preparation(&params, target, &mut migration, usk.orchard(), plan, &mut rng(), ReplanThreshold::DEFAULT)
        .map_err(|e| e.to_string())?;
    Ok(json!({"status": state.status().as_ref(), "transactions": state.transactions().len()}))
}

/// One step of the drive loop's outcome, for status and events.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Drive {
    None,
    Proved(usize),
    Broadcast { txid: String, accepted: bool, detail: String },
    NeedsApproval(&'static str),
    Reevaluate,
    Waiting(Option<u32>),
    Complete,
}

/// Runs one step of the ZIP 318 drive loop. `broadcast` submits raw bytes and returns
/// whether the node accepted them, and its answer.
/// At most this many proofs per drive, so wallet reads are not kept waiting.
const PROOFS_PER_DRIVE: usize = 2;

pub fn drive(
    db: &mut Db,
    conn: &mut Connection,
    params: ZNetwork,
    account: AccountUuid,
    broadcast: &mut dyn FnMut(Vec<u8>) -> Result<(bool, String), String>,
) -> Result<Drive, String> {
    let fvk = ufvk(db, account)?.orchard().cloned().ok_or("the account has no Orchard viewing key")?;
    let scanned = db.block_fully_scanned().map_err(|e| e.to_string())?.ok_or("the wallet has not scanned yet")?.block_height();
    let tip = db.chain_height().map_err(|e| e.to_string())?.ok_or("the wallet is not synced")?;
    let targets = DuenessTargets::new(scanned + 1, tip + 1);
    let prep_anchor = checkpoint_at_or_below(db, scanned)?.unwrap_or(scanned);
    let config = AdvanceConfig::new(REORG_SETTLE_DEPTH);
    let mut rng = rng();

    let (advance, mut state) = {
        let mut store = store(conn, params, account)?;
        let Some(mut state) = store.get_migration().map_err(|e| e.to_string())? else { return Ok(Drive::None) };
        let advance = advance_migration(&mut store, &mut state, targets, &config, &mut rng).map_err(|e| e.to_string())?;
        (advance, state)
    };
    match advance.step() {
        AdvanceStep::Prove { transactions } => {
            let mut proved = 0;
            // A proof takes seconds on the wallet thread; the rest wait for the next drive.
            for target in transactions.into_iter().take(PROOFS_PER_DRIVE) {
                let id = target.id();
                let outcome = {
                    let mut prover = WalletMigrationProver::new(&mut *db, rng.clone(), account, fvk.clone());
                    match target.kind() {
                        MigrationTxKind::Transfer { .. } => prove_transfer(&params, &mut prover, &mut state, id, scanned, &mut rng),
                        MigrationTxKind::Preparation { .. } => prove_preparation(&mut prover, &mut state, id, prep_anchor),
                    }
                    .map_err(|e| format!("proving {}: {e}", u32::from(id)))?
                };
                let mut store = store(conn, params, account)?;
                match outcome {
                    ProveOutcome::Proved(p) => {
                        store.store_proved_transaction(&mut state, p).map_err(|e| e.to_string())?;
                        proved += 1;
                    }
                    ProveOutcome::NotYetProvable | ProveOutcome::MarkedUnsatisfiable { .. } => {
                        store.replace_migration(&state).map_err(|e| e.to_string())?;
                    }
                }
            }
            Ok(Drive::Proved(proved))
        }
        AdvanceStep::Broadcast { id } => {
            let mut store = store(conn, params, account)?;
            let tx = store.take_transaction_for_broadcast(&mut rng, &state, *id).map_err(|e| e.to_string())?;
            let txid = tx.txid().to_string();
            let mut raw = vec![];
            tx.write(&mut raw).map_err(|e| e.to_string())?;
            let (accepted, detail) = broadcast(raw).unwrap_or_else(|e| (false, e));
            if accepted {
                state.mark_broadcast(*id);
            } else {
                state.report_broadcast_failure(*id, tip);
            }
            store.replace_migration(&state).map_err(|e| e.to_string())?;
            Ok(Drive::Broadcast { txid, accepted, detail })
        }
        AdvanceStep::Rebuild { .. } => Ok(Drive::NeedsApproval("an expired transfer must be signed again")),
        AdvanceStep::Replan => Ok(Drive::NeedsApproval("too much of the run can no longer mine; plan it again")),
        AdvanceStep::Reevaluate => Ok(Drive::Reevaluate),
        AdvanceStep::Waiting => Ok(Drive::Waiting(advance.next().map(|(h, _)| u32::from(h)))),
        AdvanceStep::Complete => Ok(Drive::Complete),
    }
}

/// The highest Orchard checkpoint at or below `height`. Scanning checkpoints a block only at its
/// last note commitment (and on the ZIP 318 grid), so a block without shielded outputs has none.
fn checkpoint_at_or_below(db: &mut Db, height: BlockHeight) -> Result<Option<BlockHeight>, String> {
    use shardtree::{error::ShardTreeError, store::ShardStore};
    db.with_orchard_tree_mut::<_, _, ShardTreeError<_>>(|tree| {
        let mut best = None;
        // The store binds the limit as an SQL integer; ~100 recent plus the durable grid ones exist.
        tree.store()
            .for_each_checkpoint(100_000, |id, _| {
                if *id <= height {
                    best = Some(*id);
                }
                Ok(())
            })
            .map_err(ShardTreeError::Storage)?;
        Ok(best)
    })
    .map_err(|e| e.to_string())
}

/// Status for the app: the run's summary and what it waits for.
pub fn status(conn: &mut Connection, params: ZNetwork, account: AccountUuid) -> Result<Value, String> {
    let store = store(conn, params, account)?;
    let list = store.list_migrations().map_err(|e| e.to_string())?;
    let Some(last) = list.last() else { return Ok(json!({"ok": true, "active": false})) };
    Ok(json!({
        "ok": true,
        "active": !last.status().is_terminal(),
        "id": last.id().expose_uuid().to_string(),
        "status": last.status().as_ref(),
        "committedHeight": last.committed_height().map(u32::from),
        "totalInput": last.total_input().into_u64(),
        "migratable": last.total_migratable().into_u64(),
        "transactions": last.transaction_count(),
        "mined": last.mined_count(),
        "inFlight": last.in_flight_count(),
        "unsatisfiable": last.unsatisfiable_count(),
        "migrated": last.value_migrated().into_u64(),
    }))
}

pub fn cancel(conn: &mut Connection, params: ZNetwork, account: AccountUuid) -> Result<Value, String> {
    let mut store = store(conn, params, account)?;
    let out = store.cancel_migration().map_err(|e| e.to_string())?;
    Ok(json!({"inFlight": out.in_flight().len(), "mined": out.mined().len(), "released": out.released().len()}))
}

/// The step kinds, for logs.
pub fn step_name(k: StepKind) -> &'static str {
    match k {
        StepKind::Prove => "prove",
        StepKind::Broadcast => "broadcast",
        StepKind::Rebuild => "rebuild",
        StepKind::Replan => "replan",
        StepKind::Reevaluate => "reevaluate",
        StepKind::Waiting => "waiting",
        StepKind::Complete => "complete",
    }
}
