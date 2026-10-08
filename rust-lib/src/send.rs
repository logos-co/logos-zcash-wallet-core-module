//! Sends: a proposal is built without keys; approval decrypts the seed, proves and
//! signs, and the seed is dropped right after.

use std::collections::BTreeMap;
use std::path::Path;
use std::time::Instant;

use serde::Deserialize;
use serde_json::{json, Value};
use zcash_address::ZcashAddress;
use zcash_client_backend::data_api::wallet::input_selection::{GreedyInputSelector, SpendPolicy};
use zcash_client_backend::data_api::wallet::{
    create_proposed_transactions, propose_transfer, ConfirmationsPolicy, SpendingKeys,
};
use zcash_client_backend::data_api::{Account as _, AccountSource, WalletRead};
use zcash_client_backend::fees::standard::SingleOutputChangeStrategy;
use zcash_client_backend::fees::{DustOutputPolicy, StandardFeeRule};
use zcash_client_backend::proposal::Proposal;
use zcash_client_backend::util::SystemClock;
use zcash_client_backend::wallet::OvkPolicy;
use zcash_client_sqlite::{AccountUuid, ReceivedNoteId};
use zcash_keys::keys::UnifiedSpendingKey;
use zcash_proofs::prover::LocalTxProver;
use zcash_protocol::consensus::{BlockHeight, NetworkUpgrade, Parameters};
use zcash_protocol::memo::MemoBytes;
use zcash_protocol::value::Zatoshis;
use zcash_protocol::{PoolType, ShieldedPool, TxId};
use zip321::{Payment, TransactionRequest};

use crate::keys::Phrase;
use crate::network::ZNetwork;
use crate::wallet::Db;

/// A preview older than this is refused at approval.
pub const PREVIEW_TTL_SECS: u64 = 120;
/// Expiry after NU7 (ZIP 203, ZIP 218); 40 blocks before it.
const EXPIRY_AFTER_NU7: u32 = 120;
const EXPIRY_BEFORE_NU7: u32 = 40;

const SPEND_HASH: &str = "8270785a1a0d0bc77196f000ee6d221c9c9894f55307bd9357c3f0105d31ca63991ab91324160d8f53e2bbd3c2633a6eb8bdf5205d822e7f3f73edac51b2b70c";
const OUTPUT_HASH: &str = "657e3d38dbb5cb5e7dd2970e8b03d69b4787dd907285b5a7f0790dcc8072f60bf593b32cc2d1c030e00ff5ae64bf84c5c3beb84ddc841d48264b4a171744d028";

pub type WalletProposal = Proposal<StandardFeeRule, ReceivedNoteId>;

#[derive(Deserialize, Clone)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RecipientInput {
    pub address: String,
    /// Zatoshis.
    pub amount: u64,
    #[serde(default)]
    pub memo: Option<String>,
}

#[derive(Deserialize, Clone)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SendInput {
    #[serde(default)]
    pub recipients: Vec<RecipientInput>,
    /// A ZIP 321 payment request, instead of recipients.
    #[serde(default)]
    pub uri: Option<String>,
    /// Consent to spend from more than one pool, which makes amounts public (ZIP 315).
    #[serde(default)]
    pub allow_mixed_pools: bool,
}

pub enum AnyProposal {
    Send(WalletProposal),
    Shield(Proposal<StandardFeeRule, std::convert::Infallible>),
}

impl AnyProposal {
    pub fn min_target_height(&self) -> BlockHeight {
        match self {
            AnyProposal::Send(p) => p.min_target_height().into(),
            AnyProposal::Shield(p) => p.min_target_height().into(),
        }
    }

    pub fn preview(&self, params: &ZNetwork, expiry: BlockHeight) -> Value {
        match self {
            AnyProposal::Send(p) => preview(params, p, expiry),
            AnyProposal::Shield(p) => {
                let mut v = preview(params, p, expiry);
                v["shielding"] = json!(true);
                v
            }
        }
    }
}

pub struct Prepared {
    pub created: Instant,
    /// Each proposal with its expiry; several only when shielding several addresses.
    pub items: Vec<(AnyProposal, BlockHeight)>,
    pub preview: Value,
}

impl Prepared {
    /// Several transactions go out minutes apart, so their timing does not tie the
    /// addresses together.
    pub fn spaced(&self) -> bool {
        self.items.len() > 1
    }
}

/// One shielding proposal per transparent address that holds enough to shield.
pub fn propose_shield_all(db: &mut Db, params: ZNetwork, account: AccountUuid) -> Result<Vec<AnyProposal>, String> {
    use zcash_keys::encoding::AddressCodec;
    let tip = db.chain_height().map_err(|e| e.to_string())?.ok_or("the wallet is not synced")?;
    let balances = db
        .get_transparent_balances(account, (tip + 1).into(), ConfirmationsPolicy::default())
        .map_err(|e| e.to_string())?;
    let mut addrs: Vec<String> = balances
        .into_iter()
        .filter(|(_, (_, b))| b.spendable_value().into_u64() >= SHIELDING_THRESHOLD)
        .map(|(a, _)| a.encode(&params))
        .collect();
    addrs.sort();
    if addrs.is_empty() {
        return Err("no transparent address holds enough to shield".into());
    }
    addrs.iter().map(|a| propose_shield(db, params, account, a)).collect()
}

/// Transparent funds are offered for shielding above this amount (0.001 ZEC).
pub const SHIELDING_THRESHOLD: u64 = 100_000;

/// Proposes shielding everything at one transparent address, never several in one
/// transaction, which would link them (ZIP 315).
pub fn propose_shield(db: &mut Db, params: ZNetwork, account: AccountUuid, address: &str) -> Result<AnyProposal, String> {
    let taddr = match zcash_keys::address::Address::decode(&params, address.trim()) {
        Some(zcash_keys::address::Address::Transparent(t)) => t,
        _ => return Err("not a transparent address of this network".into()),
    };
    let selector = GreedyInputSelector::new();
    let change = SingleOutputChangeStrategy::new(StandardFeeRule::Zip317, None, ShieldedPool::Ironwood, DustOutputPolicy::default());
    let threshold = Zatoshis::from_u64(SHIELDING_THRESHOLD).expect("constant");
    zcash_client_backend::data_api::wallet::propose_shielding::<_, _, _, _, std::convert::Infallible>(
        db,
        &params,
        &selector,
        &change,
        threshold,
        &[taddr],
        account,
        ConfirmationsPolicy::default(),
        zcash_client_backend::data_api::CoinbaseFilter::AllTransparentOutputs,
        None,
    )
    .map(AnyProposal::Shield)
    .map_err(|e| e.to_string())
}

fn pool_name(p: PoolType) -> &'static str {
    match p {
        PoolType::Transparent => "transparent",
        PoolType::Shielded(ShieldedPool::Sapling) => "sapling",
        PoolType::Shielded(ShieldedPool::Orchard) => "orchard",
        PoolType::Shielded(ShieldedPool::Ironwood) => "ironwood",
    }
}

pub fn request(params: ZNetwork, input: &SendInput) -> Result<TransactionRequest, String> {
    if let Some(uri) = &input.uri {
        if !input.recipients.is_empty() {
            return Err("give recipients or a payment URI, not both".into());
        }
        return TransactionRequest::from_uri(uri).map_err(|e| format!("payment URI: {e:?}"));
    }
    if input.recipients.is_empty() {
        return Err("no recipients".into());
    }
    let mut payments = Vec::with_capacity(input.recipients.len());
    for (i, r) in input.recipients.iter().enumerate() {
        let addr = ZcashAddress::try_from_encoded(r.address.trim()).map_err(|e| format!("recipient {i}: {e}"))?;
        if zcash_keys::address::Address::decode(&params, r.address.trim()).is_none() {
            return Err(format!("recipient {i} is for another network"));
        }
        let amount = Zatoshis::from_u64(r.amount).map_err(|_| format!("recipient {i}: bad amount"))?;
        if amount == Zatoshis::ZERO {
            return Err(format!("recipient {i}: the amount is zero"));
        }
        let memo = match r.memo.as_deref().filter(|m| !m.is_empty()) {
            Some(m) => Some(MemoBytes::from_bytes(m.as_bytes()).map_err(|_| format!("recipient {i}: a memo holds at most 512 bytes"))?),
            None => None,
        };
        let p = Payment::new(addr, Some(amount), memo, None, None, vec![])
            .map_err(|_| format!("recipient {i}: memos go only to shielded addresses"))?;
        payments.push(p);
    }
    TransactionRequest::new(payments).map_err(|e| format!("{e:?}"))
}

/// The expiry for a transaction targeting `target`: 120 blocks after NU7, 40 before it,
/// never past the block before activation; within 3 blocks of NU7 the send waits.
pub fn expiry_for(params: &ZNetwork, target: BlockHeight) -> Result<BlockHeight, String> {
    match params.activation_height(NetworkUpgrade::Nu7) {
        Some(nu7) if target >= nu7 => Ok(target + EXPIRY_AFTER_NU7),
        Some(nu7) if u32::from(nu7) - u32::from(target) <= 3 => {
            Err("a network upgrade activates within 3 blocks; send after it".into())
        }
        Some(nu7) => Ok((target + EXPIRY_BEFORE_NU7).min(nu7 - 1)),
        None => Ok(target + EXPIRY_BEFORE_NU7),
    }
}

/// Proposes from one shielded pool at a time, Ironwood first. Spending from several
/// pools at once needs the caller's consent, because it reveals amounts.
pub fn propose(db: &mut Db, params: ZNetwork, account: AccountUuid, input: &SendInput) -> Result<WalletProposal, String> {
    let req = request(params, input)?;
    let selector = GreedyInputSelector::new();
    let change = SingleOutputChangeStrategy::new(StandardFeeRule::Zip317, None, ShieldedPool::Ironwood, DustOutputPolicy::default());
    let mut last_err = String::from("no spendable shielded funds");
    for pools in [vec![ShieldedPool::Ironwood], vec![ShieldedPool::Sapling], vec![ShieldedPool::Orchard]] {
        let policy = SpendPolicy::shielded_pools(pools);
        match propose_transfer::<_, _, _, _, std::convert::Infallible>(
            db, &params, account, &selector, &change, req.clone(), ConfirmationsPolicy::default(), &policy, None, None,
        ) {
            Ok(p) => return Ok(p),
            Err(e) => last_err = e.to_string(),
        }
    }
    if !input.allow_mixed_pools {
        return Err(format!("needs_mixed_pools: no single pool can pay this ({last_err})"));
    }
    let policy = SpendPolicy::shielded_pools([ShieldedPool::Ironwood, ShieldedPool::Sapling, ShieldedPool::Orchard]);
    propose_transfer::<_, _, _, _, std::convert::Infallible>(
        db, &params, account, &selector, &change, req, ConfirmationsPolicy::default(), &policy, None, None,
    )
    .map_err(|e| e.to_string())
}

/// What the approver reviews: recipients, fee, pools, and the amount made public.
pub fn preview<N>(params: &ZNetwork, p: &Proposal<StandardFeeRule, N>, expiry: BlockHeight) -> Value {
    let mut recipients = vec![];
    let mut fee = 0u64;
    let mut ins: BTreeMap<&str, u64> = BTreeMap::new();
    let mut outs: BTreeMap<&str, u64> = BTreeMap::new();
    let mut change = vec![];
    for step in p.steps().iter() {
        fee += step.balance().fee_required().into_u64();
        for (i, pay) in step.transaction_request().payments() {
            let pool = step.payment_pools().get(i).copied().unwrap_or(PoolType::Transparent);
            let amount = pay.amount().map_or(0, |a| a.into_u64());
            *outs.entry(pool_name(pool)).or_default() += amount;
            recipients.push(json!({
                "address": pay.recipient_address().encode(),
                "amount": amount,
                "pool": pool_name(pool),
                "memo": pay.memo().and_then(|m| String::from_utf8(m.as_slice().iter().copied().take_while(|b| *b != 0).collect()).ok()),
            }));
        }
        if let Some(si) = step.shielded_inputs() {
            for n in si.notes().iter() {
                let pool = pool_name(PoolType::Shielded(n.note().pool()));
                *ins.entry(pool).or_default() += n.note().value().into_u64();
            }
        }
        for c in step.balance().proposed_change() {
            let pool = pool_name(c.output_pool());
            *outs.entry(pool).or_default() += c.value().into_u64();
            change.push(json!({"pool": pool, "amount": c.value().into_u64()}));
        }
    }
    let leaving: u64 = ["sapling", "orchard", "ironwood"]
        .iter()
        .map(|pool| ins.get(pool).copied().unwrap_or(0).saturating_sub(outs.get(pool).copied().unwrap_or(0)))
        .sum();
    json!({
        "recipients": recipients,
        "fee": fee,
        "change": change,
        "spentFrom": ins,
        "steps": p.steps().len(),
        "targetHeight": u32::from(p.min_target_height()),
        "expiryHeight": u32::from(expiry),
        "amountMadePublic": leaving.saturating_sub(fee),
        "network": params.name(),
        "ttlSecs": PREVIEW_TTL_SECS,
    })
}

/// Checks the parameter files against zcash_proofs' hashes before loading them,
/// since a mismatch inside the crate panics.
pub fn load_prover(dir: &Path) -> Result<LocalTxProver, String> {
    let (spend, output) = (dir.join("sapling-spend.params"), dir.join("sapling-output.params"));
    for (path, want) in [(&spend, SPEND_HASH), (&output, OUTPUT_HASH)] {
        let bytes = std::fs::read(path).map_err(|e| format!("Sapling parameters missing at {}: {e}", path.display()))?;
        let got = blake2b_simd::Params::new().hash_length(64).hash(&bytes);
        if hex::encode(got.as_bytes()) != want {
            return Err(format!("{} does not match zcash_proofs' hash", path.display()));
        }
    }
    Ok(LocalTxProver::new(&spend, &output))
}

/// Proves and signs with keys derived from `phrase`, which the caller drops after.
pub fn sign(
    db: &mut Db,
    params: ZNetwork,
    account: AccountUuid,
    prepared: &Prepared,
    phrase: &Phrase,
    prover: &LocalTxProver,
) -> Result<Vec<(TxId, Vec<u8>)>, String> {
    if prepared.created.elapsed().as_secs() > PREVIEW_TTL_SECS {
        return Err("the preview expired; prepare the send again".into());
    }
    let acct = db.get_account(account).map_err(|e| e.to_string())?.ok_or("account missing")?;
    let index = match acct.source() {
        AccountSource::Derived { derivation, .. } => derivation.account_index(),
        _ => return Err("this account was not derived from the wallet's seed".into()),
    };
    let usk = UnifiedSpendingKey::from_seed(&params, phrase.seed().expose_secret(), index).map_err(|e| format!("{e:?}"))?;
    let mut rng = crate::wallet::new_rng();
    let keys = SpendingKeys::new(usk);
    let mut txids = vec![];
    for (proposal, expiry) in &prepared.items {
        let made = match proposal {
            AnyProposal::Send(p) => create_proposed_transactions::<_, _, std::convert::Infallible, _, std::convert::Infallible, _>(
                db, &params, &SystemClock, &mut rng, prover, prover, &keys, OvkPolicy::Sender, p, Some(*expiry),
            )
            .map_err(|e| e.to_string())?,
            AnyProposal::Shield(p) => create_proposed_transactions::<_, _, std::convert::Infallible, _, std::convert::Infallible, _>(
                db, &params, &SystemClock, &mut rng, prover, prover, &keys, OvkPolicy::Sender, p, Some(*expiry),
            )
            .map_err(|e| e.to_string())?,
        };
        txids.extend(made);
    }
    let mut out = vec![];
    for txid in txids {
        let tx = db.get_transaction(txid).map_err(|e| e.to_string())?.ok_or("built transaction missing")?;
        let mut raw = vec![];
        tx.write(&mut raw).map_err(|e| e.to_string())?;
        out.push((txid, raw));
    }
    Ok(out)
}

use secrecy::ExposeSecret;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expiry_rules_around_testnet_nu7() {
        let p = ZNetwork::Test;
        let nu7 = p.activation_height(NetworkUpgrade::Nu7).unwrap();
        assert_eq!(expiry_for(&p, nu7 + 10).unwrap(), nu7 + 10 + 120);
        assert_eq!(expiry_for(&p, nu7 - 100).unwrap(), nu7 - 100 + 40);
        assert_eq!(expiry_for(&p, nu7 - 20).unwrap(), nu7 - 1);
        assert!(expiry_for(&p, nu7 - 2).is_err());
    }

    #[test]
    fn requests() {
        let t = ZNetwork::Test;
        let ok = SendInput {
            recipients: vec![RecipientInput { address: "tmFW7wTNyz1p12KFJizXKM4MRh243MYEj11".into(), amount: 1000, memo: None }],
            uri: None,
            allow_mixed_pools: false,
        };
        assert!(request(t, &ok).is_ok());
        let mut memo = ok.clone();
        memo.recipients[0].memo = Some("hi".into());
        assert!(request(t, &memo).unwrap_err().contains("shielded"));
        assert!(request(ZNetwork::Main, &ok).unwrap_err().contains("another network"));
        let mut zero = ok.clone();
        zero.recipients[0].amount = 0;
        assert!(request(t, &zero).is_err());
    }
}
