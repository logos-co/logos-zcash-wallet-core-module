//! Logos glue for `zcash_wallet_core_module` (rust-first, `concurrency: "multi"`).
//!
//! Only `zcash_wallet_backend` may call. Structured values cross as JSON strings:
//! `{ ok, ... }` or `{ ok: false, error }`. Secrets arrive as call arguments and
//! leave as return values, never as events.

use std::sync::{Arc, OnceLock};

use logos_rust_sdk::{AboutToUnload, LogosCaller, Shutdown};
use serde_json::{json, Value};
use zeroize::Zeroizing;

use crate::engine::{Engine, Event, Sink};
use crate::gate::{Caller, Callers};

pub trait ZcashWalletCoreModule: Send + Sync + 'static {
    /// `{ ok, version, crates }`.
    fn version(&self) -> String;
    /// Starts `create_wallet`, `restore_wallet`, `open_wallet`, `close_wallet`,
    /// `change_password`, `propose` (`{ send }`), `propose_shielding` (`{ address }`),
    /// `sign_and_send` (`{ proposalId, password }`), `plan_migration`, `sign_migration`
    /// (`{ planId, digest, password }`), `pause_migration`, `resume_migration` or
    /// `cancel_migration`. `params` is a JSON object. `{ ok, jobId, receipt }`.
    fn start_job(&self, kind: String, params: String) -> String;
    /// `{ ok, jobId, kind, state: queued|running|done|failed|cancelled, error }`.
    fn job_status(&self, job_id: String, receipt: String) -> String;
    /// `{ ok, result }` once done.
    fn job_result(&self, job_id: String, receipt: String) -> String;
    fn ack_job(&self, job_id: String, receipt: String) -> bool;
    fn cancel_job(&self, job_id: String, receipt: String) -> bool;
    /// `{ ok, wallets: [{ name, network, accountUuid, birthdayHeight, createdAt }] }`.
    fn list_wallets(&self, network: String) -> String;
    /// `{ ok, open, name?, network?, accountUuid?, birthdayHeight? }`.
    fn wallet_status(&self) -> String;
    /// `{ ok, sync: { state, tip, fullyScanned, blocksFetched, blocksScanned, ... } }`.
    fn sync_status(&self) -> String;
    /// Spendable, pending and total per pool, in zatoshis.
    fn balances(&self, account: String) -> String;
    /// `{ ok, unified, transparent }`: the current shielded-only Unified Address and
    /// the current transparent address.
    fn addresses(&self, account: String) -> String;
    /// A new diversified shielded address.
    fn new_address(&self, account: String) -> String;
    /// `{ ok, page, pageSize, rows: [{ txid, kind: received|sent|shielded|migration, height,
    /// pending, expired, expiryHeight, time, delta, fee, pools, memos, to, amountMadePublic }] }`.
    fn history(&self, account: String, page: i64) -> String;
    /// The Orchard-to-Ironwood run, if any: status, counts, ZEC migrated, paused, and
    /// whether it needs approval again.
    fn migration_status(&self) -> String;
    /// The recovery phrase, once, after checking the password.
    fn reveal_seed(&self, password: String) -> String;
    /// The account's Unified Full Viewing Key, after checking the password.
    fn export_viewing_key(&self, account: String, password: String) -> String;

    fn on_context_ready(&self, _ctx: &RustModuleContext) {}
}

pub trait ZcashWalletCoreModuleEvents {
    /// Opened or closed; payload is wallet_status()'s shape.
    fn wallet_state_changed(&self, payload: String);
    /// sync_status()'s `sync` object, at most once a second.
    fn sync_progress(&self, payload: String);
    /// balances()'s shape, when it changes.
    fn balance_changed(&self, payload: String);
    /// migration_status()'s shape, or `{ needsApproval }`, when the run moves.
    fn migration_changed(&self, payload: String);
    fn job_finished(&self, job_id: String, state: String);
}

include!(concat!(env!("CARGO_MANIFEST_DIR"), "/generated/provider_gen.rs"));

#[derive(Default)]
pub struct ZcashWalletCoreModuleImpl {
    engine: OnceLock<Arc<Engine>>,
    callers: Mutex<Callers>,
}

fn refused() -> String {
    json!({"ok": false, "error": "not authorized"}).to_string()
}

fn not_ready() -> String {
    json!({"ok": false, "error": "starting"}).to_string()
}

impl ZcashWalletCoreModuleImpl {
    fn admitted(&self) -> bool {
        let caller = match logos_rust_sdk::current_caller() {
            LogosCaller::Module { name, .. } => Caller::Module(name),
            LogosCaller::HostAnchor => Caller::Host,
            LogosCaller::Unknown => Caller::Unknown,
            _ => Caller::Other,
        };
        self.callers.lock().unwrap().admits(&caller)
    }

    /// Runs `f` for an admitted caller once the engine exists.
    fn gated(&self, f: impl FnOnce(&Engine) -> Value) -> String {
        if !self.admitted() {
            return refused();
        }
        match self.engine.get() {
            Some(e) => f(e).to_string(),
            None => not_ready(),
        }
    }
}

impl ZcashWalletCoreModule for ZcashWalletCoreModuleImpl {
    fn version(&self) -> String {
        json!({
            "ok": true,
            "version": env!("CARGO_PKG_VERSION"),
            "crates": {"zcash_client_backend": "0.25.0-pre.1", "zcash_client_sqlite": "0.23.0-pre.1", "zcash_protocol": "0.11.0-pre.0"},
        })
        .to_string()
    }

    fn start_job(&self, kind: String, params: String) -> String {
        let params = Zeroizing::new(params);
        self.gated(|e| e.start_job(&kind, params.to_string()))
    }

    fn job_status(&self, job_id: String, receipt: String) -> String {
        self.gated(|e| e.jobs().status(&job_id, &receipt))
    }

    fn job_result(&self, job_id: String, receipt: String) -> String {
        self.gated(|e| e.jobs().result(&job_id, &receipt))
    }

    fn ack_job(&self, job_id: String, receipt: String) -> bool {
        self.admitted() && self.engine.get().is_some_and(|e| e.jobs().ack(&job_id, &receipt))
    }

    fn cancel_job(&self, job_id: String, receipt: String) -> bool {
        self.admitted() && self.engine.get().is_some_and(|e| e.jobs().cancel(&job_id, &receipt))
    }

    fn list_wallets(&self, network: String) -> String {
        self.gated(|e| e.list_wallets(&network))
    }

    fn wallet_status(&self) -> String {
        self.gated(|e| e.wallet_status())
    }

    fn sync_status(&self) -> String {
        self.gated(|e| e.sync_status())
    }

    fn balances(&self, _account: String) -> String {
        self.gated(|e| e.balances())
    }

    fn addresses(&self, _account: String) -> String {
        self.gated(|e| e.addresses())
    }

    fn new_address(&self, _account: String) -> String {
        self.gated(|e| e.new_address())
    }

    fn migration_status(&self) -> String {
        self.gated(|e| e.migration_status())
    }

    fn history(&self, _account: String, page: i64) -> String {
        self.gated(|e| e.history(page.clamp(0, u32::MAX as i64) as u32))
    }

    fn reveal_seed(&self, password: String) -> String {
        let password = Zeroizing::new(password);
        self.gated(|e| e.reveal_seed(&password))
    }

    fn export_viewing_key(&self, _account: String, password: String) -> String {
        let password = Zeroizing::new(password);
        self.gated(|e| e.export_viewing_key(&password))
    }

    fn on_context_ready(&self, ctx: &RustModuleContext) {
        let dir = std::path::PathBuf::from(&ctx.instance_persistence_path);
        let callers = std::fs::read_to_string(dir.join("callers.json")).ok();
        *self.callers.lock().unwrap() = Callers::from_file(callers.as_deref());
        let sink: Sink = Arc::new(|ev| match ev {
            Event::WalletState(v) => emit_wallet_state_changed(&v.to_string()),
            Event::SyncProgress(v) => emit_sync_progress(&v.to_string()),
            Event::BalanceChanged(v) => emit_balance_changed(&v.to_string()),
            Event::MigrationChanged(v) => emit_migration_changed(&v.to_string()),
            Event::JobFinished { id, state } => emit_job_finished(&id, state),
        });
        // Sapling parameters ship beside the plugin; ZCASH_PARAMS_DIR overrides for development.
        let params = std::env::var_os("ZCASH_PARAMS_DIR").map(std::path::PathBuf::from).or_else(|| {
            let m = std::path::PathBuf::from(&ctx.module_path);
            let parent = m.parent().map(|p| p.to_path_buf());
            [Some(m.clone()), Some(m.join("lib")), parent.clone(), parent.map(|p| p.join("lib"))]
                .into_iter()
                .flatten()
                .find(|d| d.join("sapling-spend.params").is_file())
        });
        let _ = self.engine.set(Engine::new(dir.join("wallets"), sink, None, params));
    }
}

impl AboutToUnload for ZcashWalletCoreModuleImpl {
    /// Stops sync between bounded steps and joins the threads, well inside the
    /// host's grace period.
    fn about_to_unload(&self) -> Shutdown {
        if let Some(e) = self.engine.get() {
            e.shutdown();
        }
        Shutdown::Synchronous
    }
}

#[no_mangle]
pub extern "Rust" fn logos_module_install() {
    logos_install!(ZcashWalletCoreModuleImpl);
}
