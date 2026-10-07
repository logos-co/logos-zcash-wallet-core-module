//! The engine behind the module: a job worker, and the open wallet with its sync thread.
//!
//! The sync thread owns the writer connection. Reads use a second connection, so a
//! scan in progress never delays a balance or an address.

use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use serde::Deserialize;
use serde_json::{json, Value};
use zcash_client_backend::data_api::wallet::ConfirmationsPolicy;
use zcash_client_backend::data_api::{Account as _, WalletRead, WalletWrite};
use zcash_client_sqlite::AccountUuid;
use zcash_keys::keys::UnifiedAddressRequest;
use zeroize::{Zeroize, Zeroizing};

use crate::jobs::{JobState, Jobs};
use crate::keys::Phrase;
use crate::net::socks::ProxyAddr;
use crate::network::ZNetwork;
use crate::sync::cache::grid_floor;
use crate::sync::driver::{Progress, Step, SyncConfig, Syncer, HEAD};
use crate::sync::fetch;
use crate::wallet::{self, Db, Meta, WalletDir};

pub enum Event {
    WalletState(Value),
    SyncProgress(Value),
    BalanceChanged(Value),
    JobFinished { id: String, state: &'static str },
}

pub type Sink = Arc<dyn Fn(Event) + Send + Sync>;

#[derive(Deserialize, Clone)]
pub struct Routes {
    pub proxy: String,
    pub servers: Vec<String>,
}

impl Routes {
    fn config(&self) -> Result<SyncConfig, String> {
        if self.servers.is_empty() {
            return Err("no servers".into());
        }
        if self.servers.iter().any(|s| !s.starts_with("https://")) {
            return Err("servers must be https:// URLs".into());
        }
        Ok(SyncConfig::new(ProxyAddr::parse(&self.proxy)?, self.servers.clone()))
    }
}

/// A job's parameters, parsed and taken out of the caller's string at once.
enum Task {
    Create { network: ZNetwork, name: String, password: Zeroizing<String>, routes: Routes },
    Restore { network: ZNetwork, name: String, password: Zeroizing<String>, phrase: Phrase, birthday: u32, routes: Routes },
    Open { network: ZNetwork, name: String, password: Zeroizing<String>, routes: Routes },
    Close,
    ChangePassword { network: ZNetwork, name: String, old: Zeroizing<String>, new: Zeroizing<String> },
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Params {
    network: Option<String>,
    name: Option<String>,
    password: Option<String>,
    old_password: Option<String>,
    new_password: Option<String>,
    phrase: Option<String>,
    birthday_height: Option<u32>,
    routes: Option<Routes>,
}

impl Drop for Params {
    fn drop(&mut self) {
        for s in [&mut self.password, &mut self.old_password, &mut self.new_password, &mut self.phrase].into_iter().flatten() {
            s.zeroize();
        }
    }
}

fn parse_task(kind: &str, raw: &str) -> Result<Task, String> {
    let mut p: Params = serde_json::from_str(raw).map_err(|e| format!("params: {e}"))?;
    let network = || {
        p.network.as_deref().and_then(ZNetwork::parse).ok_or_else(|| "network must be mainnet or testnet".to_string())
    };
    let name = |p: &Params| -> Result<String, String> {
        let n = p.name.clone().ok_or("name is required")?;
        if wallet::valid_name(&n) { Ok(n) } else { Err("names use letters, digits, - and _ (at most 64)".into()) }
    };
    let take = |s: &mut Option<String>, what: &str| -> Result<Zeroizing<String>, String> {
        s.take().filter(|v| !v.is_empty()).map(Zeroizing::new).ok_or_else(|| format!("{what} is required"))
    };
    Ok(match kind {
        "create_wallet" => Task::Create {
            network: network()?,
            name: name(&p)?,
            password: take(&mut p.password, "password")?,
            routes: p.routes.clone().ok_or("routes are required")?,
        },
        "restore_wallet" => {
            let phrase = Phrase::parse(&take(&mut p.phrase, "phrase")?).map_err(|e| e.to_string())?;
            Task::Restore {
                network: network()?,
                name: name(&p)?,
                password: take(&mut p.password, "password")?,
                phrase,
                birthday: p.birthday_height.ok_or("birthdayHeight is required")?,
                routes: p.routes.clone().ok_or("routes are required")?,
            }
        }
        "open_wallet" => Task::Open {
            network: network()?,
            name: name(&p)?,
            password: take(&mut p.password, "password")?,
            routes: p.routes.clone().ok_or("routes are required")?,
        },
        "close_wallet" => Task::Close,
        "change_password" => Task::ChangePassword {
            network: network()?,
            name: name(&p)?,
            old: take(&mut p.old_password, "oldPassword")?,
            new: take(&mut p.new_password, "newPassword")?,
        },
        other => return Err(format!("unknown job kind {other}")),
    })
}

enum WalletCmd {
    NewAddress(Sender<Result<Value, String>>),
}

struct OpenWallet {
    meta: Meta,
    network: ZNetwork,
    account: AccountUuid,
    dir: WalletDir,
    reader: Mutex<Db>,
    cmd: Sender<WalletCmd>,
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
    progress: Arc<Mutex<Progress>>,
}

impl OpenWallet {
    fn shut(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

struct Queue {
    items: Mutex<VecDeque<(String, Task)>>,
    ready: Condvar,
    closed: AtomicBool,
}

pub struct Engine {
    root: PathBuf,
    rt: Arc<tokio::runtime::Runtime>,
    jobs: Arc<Mutex<Jobs>>,
    queue: Arc<Queue>,
    open: Arc<Mutex<Option<OpenWallet>>>,
    sink: Sink,
    worker: Mutex<Option<JoinHandle<()>>>,
    /// age's scrypt work factor; only tests lower it.
    work_factor: Option<u8>,
}

impl Engine {
    pub fn new(root: PathBuf, sink: Sink, work_factor: Option<u8>) -> Arc<Self> {
        let rt = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .thread_name("zcash-net")
                .enable_all()
                .build()
                .expect("tokio runtime"),
        );
        let engine = Arc::new(Self {
            root,
            rt,
            jobs: Arc::default(),
            queue: Arc::new(Queue { items: Mutex::default(), ready: Condvar::new(), closed: AtomicBool::new(false) }),
            open: Arc::default(),
            sink,
            worker: Mutex::new(None),
            work_factor,
        });
        let e = Arc::downgrade(&engine);
        let q = engine.queue.clone();
        let handle = std::thread::Builder::new()
            .name("zcash-jobs".into())
            .spawn(move || loop {
                let next = {
                    let mut items = q.items.lock().unwrap();
                    loop {
                        if q.closed.load(Ordering::SeqCst) {
                            return;
                        }
                        if let Some(n) = items.pop_front() {
                            break n;
                        }
                        items = q.ready.wait(items).unwrap();
                    }
                };
                let Some(engine) = e.upgrade() else { return };
                engine.run(next.0, next.1);
            })
            .expect("job thread");
        *engine.worker.lock().unwrap() = Some(handle);
        engine
    }

    pub fn start_job(&self, kind: &str, params: String) -> Value {
        let params = Zeroizing::new(params);
        let task = match parse_task(kind, &params) {
            Ok(t) => t,
            Err(e) => return json!({"ok": false, "error": e}),
        };
        let (id, receipt, _) = self.jobs.lock().unwrap().add(kind);
        self.queue.items.lock().unwrap().push_back((id.clone(), task));
        self.queue.ready.notify_one();
        json!({"ok": true, "jobId": id, "receipt": receipt})
    }

    pub fn jobs(&self) -> std::sync::MutexGuard<'_, Jobs> {
        self.jobs.lock().unwrap()
    }

    fn run(&self, id: String, task: Task) {
        if !self.jobs.lock().unwrap().begin(&id) {
            return;
        }
        let outcome = match task {
            Task::Create { network, name, password, routes } => self.create(network, &name, &password, None, None, &routes),
            Task::Restore { network, name, password, phrase, birthday, routes } => {
                self.create(network, &name, &password, Some(phrase), Some(birthday), &routes)
            }
            Task::Open { network, name, password, routes } => self.open_wallet(network, &name, &password, &routes),
            Task::Close => Ok(self.close()),
            Task::ChangePassword { network, name, old, new } => WalletDir::new(&self.root, network, &name)
                .change_password(&old, &new, self.work_factor)
                .map(|_| json!({"name": name}))
                .map_err(|e| e.to_string()),
        };
        if let Some(state) = self.jobs.lock().unwrap().finish(&id, outcome) {
            (self.sink)(Event::JobFinished { id, state: state.as_str() });
        }
        let _ = JobState::Done;
    }

    /// Creates a wallet; with a phrase, restores one from `birthday`.
    fn create(
        &self,
        network: ZNetwork,
        name: &str,
        password: &str,
        phrase: Option<Phrase>,
        birthday: Option<u32>,
        routes: &Routes,
    ) -> Result<Value, String> {
        let cfg = routes.config()?;
        let dir = WalletDir::new(&self.root, network, name);
        if dir.exists() {
            return Err(format!("a wallet named {name} already exists"));
        }
        let server = cfg.servers[0].clone();
        let (tip, _) = self
            .rt
            .block_on(fetch::tip_and_info(&server, &cfg.proxy, crate::net::socks::Isolation::fresh()))
            .map_err(|e| e.to_string())?;
        // A new wallet starts at the grid height at least HEAD blocks below the tip, so
        // the tree state it asks for is one every new wallet of the same hour asks for.
        let start = match birthday {
            Some(h) => grid_floor(h.min(tip)),
            None => grid_floor(tip.saturating_sub(HEAD)),
        };
        let below = self.rt.block_on(fetch::tree_state(&server, &cfg.proxy, start - 1)).map_err(|e| e.to_string())?;
        let state = below.to_chain_state().map_err(|e| e.to_string())?;
        let phrase = phrase.unwrap_or_else(Phrase::generate);
        let meta = dir.create(network, name, password, &phrase, state, self.work_factor).map_err(|e| e.to_string())?;
        Ok(json!({"name": meta.name, "network": meta.network, "birthdayHeight": meta.birthday_height, "accountUuid": meta.account_uuid}))
    }

    fn open_wallet(&self, network: ZNetwork, name: &str, password: &str, routes: &Routes) -> Result<Value, String> {
        let cfg = routes.config()?;
        let dir = WalletDir::new(&self.root, network, name);
        if !dir.exists() {
            return Err(format!("no wallet named {name} on {}", network.name()));
        }
        self.close();
        let meta = dir.meta().map_err(|e| e.to_string())?;
        let account = dir.account().map_err(|e| e.to_string())?;
        let key = dir.unlock_db_key(password).map_err(|e| e.to_string())?;
        let writer = dir.open_db(network, &key).map_err(|e| e.to_string())?;
        let reader = dir.open_db(network, &key).map_err(|e| e.to_string())?;
        let cache = Arc::new(dir.open_cache(&key).map_err(|e| e.to_string())?);
        drop(key);

        let stop = Arc::new(AtomicBool::new(false));
        let progress = Arc::new(Mutex::new(Progress::default()));
        let (cmd_tx, cmd_rx) = channel();
        let syncer = Syncer::new(network, cfg, self.rt.handle().clone(), cache, meta.birthday_height);
        let thread = {
            let (stop, progress, sink) = (stop.clone(), progress.clone(), self.sink.clone());
            std::thread::Builder::new()
                .name("zcash-wallet".into())
                .spawn(move || wallet_loop(writer, syncer, cmd_rx, stop, progress, sink, account))
                .map_err(|e| e.to_string())?
        };
        let result = json!({"name": meta.name, "network": meta.network, "birthdayHeight": meta.birthday_height, "accountUuid": meta.account_uuid});
        *self.open.lock().unwrap() = Some(OpenWallet {
            meta,
            network,
            account,
            dir,
            reader: Mutex::new(reader),
            cmd: cmd_tx,
            stop,
            thread: Some(thread),
            progress,
        });
        (self.sink)(Event::WalletState(self.wallet_status()));
        Ok(result)
    }

    /// Stops sync and drops the database keys. Returns the wallet that was open.
    pub fn close(&self) -> Value {
        let taken = self.open.lock().unwrap().take();
        match taken {
            Some(mut w) => {
                w.shut();
                (self.sink)(Event::WalletState(json!({"ok": true, "open": false})));
                json!({"closed": w.meta.name})
            }
            None => json!({"closed": null}),
        }
    }

    /// Stops everything; used when the module unloads.
    pub fn shutdown(&self) {
        self.close();
        self.queue.closed.store(true, Ordering::SeqCst);
        self.queue.ready.notify_all();
        if let Some(h) = self.worker.lock().unwrap().take() {
            let _ = h.join();
        }
    }

    pub fn list_wallets(&self, network: &str) -> Value {
        let Some(n) = ZNetwork::parse(network) else { return json!({"ok": false, "error": "unknown network"}) };
        match wallet::list(&self.root, n) {
            Ok(list) => json!({"ok": true, "wallets": list}),
            Err(e) => json!({"ok": false, "error": e.to_string()}),
        }
    }

    pub fn wallet_status(&self) -> Value {
        match self.open.lock().unwrap().as_ref() {
            Some(w) => json!({"ok": true, "open": true, "name": w.meta.name, "network": w.network.name(),
                              "accountUuid": w.meta.account_uuid, "birthdayHeight": w.meta.birthday_height}),
            None => json!({"ok": true, "open": false}),
        }
    }

    pub fn sync_status(&self) -> Value {
        match self.open.lock().unwrap().as_ref() {
            Some(w) => json!({"ok": true, "sync": *w.progress.lock().unwrap()}),
            None => json!({"ok": false, "error": "no wallet is open"}),
        }
    }

    fn with_open<T>(&self, f: impl FnOnce(&OpenWallet) -> Result<T, String>) -> Result<T, String> {
        let guard = self.open.lock().unwrap();
        let w = guard.as_ref().ok_or("no wallet is open")?;
        f(w)
    }

    pub fn balances(&self) -> Value {
        let r = self.with_open(|w| {
            let db = w.reader.lock().unwrap();
            balances_json(&db, w.account)
        });
        r.unwrap_or_else(|e| json!({"ok": false, "error": e}))
    }

    pub fn addresses(&self) -> Value {
        let r = self.with_open(|w| {
            let db = w.reader.lock().unwrap();
            let ua = db
                .get_last_generated_address_matching(w.account, UnifiedAddressRequest::SHIELDED)
                .map_err(|e| e.to_string())?;
            let transparent = first_external_transparent(&db, w.account)?;
            Ok(json!({"ok": true, "unified": ua.map(|a| a.encode(&w.network)), "transparent": transparent}))
        });
        r.unwrap_or_else(|e| json!({"ok": false, "error": e}))
    }

    pub fn new_address(&self) -> Value {
        let rx = self.with_open(|w| {
            let (tx, rx) = channel();
            w.cmd.send(WalletCmd::NewAddress(tx)).map_err(|_| "wallet is closing".to_string())?;
            Ok(rx)
        });
        match rx {
            Ok(rx) => rx
                .recv_timeout(Duration::from_secs(15))
                .map_err(|_| "the wallet did not answer".to_string())
                .and_then(|r| r)
                .unwrap_or_else(|e| json!({"ok": false, "error": e})),
            Err(e) => json!({"ok": false, "error": e}),
        }
    }

    pub fn reveal_seed(&self, password: &str) -> Value {
        let r = self.with_open(|w| {
            let phrase = w.dir.unseal_phrase(password).map_err(|e| e.to_string())?;
            Ok(json!({"ok": true, "phrase": phrase.as_str()}))
        });
        r.unwrap_or_else(|e| json!({"ok": false, "error": e}))
    }

    pub fn export_viewing_key(&self, password: &str) -> Value {
        let r = self.with_open(|w| {
            w.dir.unlock_db_key(password).map_err(|e| e.to_string())?;
            let db = w.reader.lock().unwrap();
            let account = db.get_account(w.account).map_err(|e| e.to_string())?.ok_or("account missing")?;
            let ufvk = account.ufvk().ok_or("this account has no full viewing key")?;
            let encoded = ufvk.encode(&w.network).map_err(|e| format!("{e:?}"))?;
            Ok(json!({"ok": true, "ufvk": encoded}))
        });
        r.unwrap_or_else(|e| json!({"ok": false, "error": e}))
    }
}

fn zat(v: zcash_protocol::value::Zatoshis) -> u64 {
    v.into_u64()
}

fn pool_json(b: &zcash_client_backend::data_api::Balance) -> Value {
    json!({
        "spendable": zat(b.spendable_value()),
        "pendingChange": zat(b.change_pending_confirmation()),
        "pendingSpendability": zat(b.value_pending_spendability()),
        "total": zat(b.total()),
    })
}

fn balances_json(db: &Db, account: AccountUuid) -> Result<Value, String> {
    let Some(summary) = db.get_wallet_summary(ConfirmationsPolicy::default()).map_err(|e| e.to_string())? else {
        return Ok(json!({"ok": true, "ready": false}));
    };
    let Some(b) = summary.account_balances().get(&account) else {
        return Ok(json!({"ok": true, "ready": false}));
    };
    let shielded_spendable = zat(b.sapling_balance().spendable_value()) + zat(b.orchard_balance().spendable_value()) + zat(b.ironwood_balance().spendable_value());
    let shielded_total = zat(b.sapling_balance().total()) + zat(b.orchard_balance().total()) + zat(b.ironwood_balance().total());
    Ok(json!({
        "ok": true,
        "ready": true,
        "chainTip": u32::from(summary.chain_tip_height()),
        "fullyScanned": u32::from(summary.fully_scanned_height()),
        "pools": {
            "ironwood": pool_json(b.ironwood_balance()),
            "sapling": pool_json(b.sapling_balance()),
            "orchard": pool_json(b.orchard_balance()),
            "transparent": pool_json(&b.unshielded_balance()),
        },
        "shielded": {"spendable": shielded_spendable, "total": shielded_total},
        "total": zat(b.total()),
    }))
}

/// The lowest-index external transparent address. Rotation after a payment comes
/// with the Receive tab work.
fn first_external_transparent(db: &Db, account: AccountUuid) -> Result<Option<String>, String> {
    use zcash_keys::encoding::AddressCodec;
    let receivers = db.get_transparent_receivers(account, false, false).map_err(|e| e.to_string())?;
    let mut best: Option<(u32, String)> = None;
    for (addr, meta) in receivers {
        let idx = meta.address_index().map(|i| i.index()).unwrap_or(u32::MAX);
        if best.as_ref().is_none_or(|(b, _)| idx < *b) {
            best = Some((idx, addr.encode(db.params())));
        }
    }
    Ok(best.map(|(_, a)| a))
}

fn wallet_loop(
    mut db: Db,
    mut syncer: Syncer,
    cmd_rx: Receiver<WalletCmd>,
    stop: Arc<AtomicBool>,
    progress: Arc<Mutex<Progress>>,
    sink: Sink,
    account: AccountUuid,
) {
    let mut backoff = Duration::from_millis(500);
    let mut last_emit = Instant::now() - Duration::from_secs(60);
    let mut last_balance: Option<Value> = None;
    let mut last_balance_check = Instant::now() - Duration::from_secs(60);
    while !stop.load(Ordering::SeqCst) {
        while let Ok(cmd) = cmd_rx.try_recv() {
            match cmd {
                WalletCmd::NewAddress(reply) => {
                    let r = db
                        .get_next_available_address(account, UnifiedAddressRequest::SHIELDED)
                        .map_err(|e| e.to_string())
                        .and_then(|o| o.ok_or_else(|| "account missing".to_string()))
                        .map(|(ua, _)| json!({"ok": true, "unified": ua.encode(db.params())}));
                    let _ = reply.send(r);
                }
            }
        }
        match syncer.step(&mut db) {
            Ok(Step::Worked) => backoff = Duration::from_millis(500),
            Ok(Step::Waiting | Step::Synced) => {
                let _ = syncer.wait(&mut db, Duration::from_millis(500));
            }
            Err(e) => {
                syncer.progress.last_error = Some(e.to_string());
                let until = Instant::now() + backoff;
                while Instant::now() < until && !stop.load(Ordering::SeqCst) {
                    std::thread::sleep(Duration::from_millis(100));
                }
                backoff = (backoff * 2).min(Duration::from_secs(60));
            }
        }
        *progress.lock().unwrap() = syncer.progress.clone();
        if last_emit.elapsed() >= Duration::from_secs(1) {
            sink(Event::SyncProgress(json!(syncer.progress)));
            last_emit = Instant::now();
        }
        if last_balance_check.elapsed() >= Duration::from_secs(2) {
            last_balance_check = Instant::now();
            if let Ok(b) = balances_json(&db, account) {
                if last_balance.as_ref() != Some(&b) {
                    sink(Event::BalanceChanged(b.clone()));
                    last_balance = Some(b);
                }
            }
        }
    }
}
