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
use crate::wallet::{self, borrow_db, Db, Meta, WalletDir};

pub enum Event {
    WalletState(Value),
    SyncProgress(Value),
    BalanceChanged(Value),
    MigrationChanged(Value),
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
    Propose { input: crate::send::SendInput },
    ProposeShielding { address: String },
    SignAndSend { proposal: String, password: Zeroizing<String> },
    PlanMigration,
    SignMigration { plan: String, digest: String, password: Zeroizing<String> },
    MigrationControl(&'static str),
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
    send: Option<crate::send::SendInput>,
    proposal_id: Option<String>,
    address: Option<String>,
    plan_id: Option<String>,
    digest: Option<String>,
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
        "propose" => Task::Propose { input: p.send.clone().ok_or("send is required")? },
        "plan_migration" => Task::PlanMigration,
        "sign_migration" => Task::SignMigration {
            plan: p.plan_id.clone().ok_or("planId is required")?,
            digest: p.digest.clone().ok_or("digest is required")?,
            password: take(&mut p.password, "password")?,
        },
        "pause_migration" => Task::MigrationControl("pause"),
        "resume_migration" => Task::MigrationControl("resume"),
        "cancel_migration" => Task::MigrationControl("cancel"),
        "propose_shielding" => Task::ProposeShielding { address: p.address.clone().ok_or("address is required")? },
        "sign_and_send" => Task::SignAndSend {
            proposal: p.proposal_id.clone().ok_or("proposalId is required")?,
            password: take(&mut p.password, "password")?,
        },
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
    Propose(crate::send::SendInput, Sender<Result<Value, String>>),
    ProposeShielding(String, Sender<Result<Value, String>>),
    PlanMigration(Sender<Result<Value, String>>),
    SignMigration(String, String, Phrase, Sender<Result<Value, String>>),
    MigrationControl(&'static str, Sender<Result<Value, String>>),
    MigrationStatus(Sender<Result<Value, String>>),
    Sign(String, Phrase, Arc<zcash_proofs::prover::LocalTxProver>, Sender<Result<(Vec<(String, Vec<u8>)>, bool), String>>),
}

struct OpenWallet {
    meta: Meta,
    network: ZNetwork,
    account: AccountUuid,
    dir: WalletDir,
    reader: Mutex<rusqlite::Connection>,
    cmd: Sender<WalletCmd>,
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
    progress: Arc<Mutex<Progress>>,
    routes: Routes,
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
    params_dir: Option<PathBuf>,
    prover: Mutex<Option<Arc<zcash_proofs::prover::LocalTxProver>>>,
}

impl Engine {
    pub fn new(root: PathBuf, sink: Sink, work_factor: Option<u8>, params_dir: Option<PathBuf>) -> Arc<Self> {
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
            params_dir,
            prover: Mutex::new(None),
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
            Task::Propose { input } => self.propose(input),
            Task::ProposeShielding { address } => self.ask(|tx| WalletCmd::ProposeShielding(address, tx), Duration::from_secs(60)),
            Task::PlanMigration => self.ask(WalletCmd::PlanMigration, Duration::from_secs(120)),
            Task::SignMigration { plan, digest, password } => self.sign_migration(plan, digest, &password),
            Task::MigrationControl(what) => self.ask(|tx| WalletCmd::MigrationControl(what, tx), Duration::from_secs(60)),
            Task::SignAndSend { proposal, password } => self.sign_and_send(&proposal, &password),
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
        // A restore starts from a bundled checkpoint, so no server learns the birthday.
        // A new wallet starts at the grid height at least HEAD blocks below the tip:
        // every new wallet of that hour asks for the same tree state.
        let state = match birthday {
            Some(h) => crate::checkpoints::restore_point(network, h)?,
            None => {
                let server = cfg.servers[0].clone();
                let (tip, _) = self
                    .rt
                    .block_on(fetch::tip_and_info(&server, &cfg.proxy, crate::net::socks::Isolation::fresh()))
                    .map_err(|e| e.to_string())?;
                let start = grid_floor(tip.saturating_sub(HEAD));
                let below = self.rt.block_on(fetch::tree_state(&server, &cfg.proxy, start - 1)).map_err(|e| e.to_string())?;
                below.to_chain_state().map_err(|e| e.to_string())?
            }
        };
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
        let reader = dir.open_conn(&key).map_err(|e| e.to_string())?;
        let cache = Arc::new(dir.open_cache(&key).map_err(|e| e.to_string())?);
        let mig_conn = dir.open_conn(&key).map_err(|e| e.to_string())?;
        drop(key);

        let stop = Arc::new(AtomicBool::new(false));
        let progress = Arc::new(Mutex::new(Progress::default()));
        let (cmd_tx, cmd_rx) = channel();
        let net = (self.rt.handle().clone(), cfg.clone());
        let paused_flag = dir.path.join("migration-paused");
        let syncer = Syncer::new(network, cfg, self.rt.handle().clone(), cache, meta.birthday_height);
        let thread = {
            let (stop, progress, sink) = (stop.clone(), progress.clone(), self.sink.clone());
            std::thread::Builder::new()
                .name("zcash-wallet".into())
                .spawn(move || wallet_loop(writer, syncer, cmd_rx, stop, progress, sink, account, mig_conn, net, paused_flag))
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
            routes: routes.clone(),
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

    fn ask<T>(&self, make: impl FnOnce(Sender<Result<T, String>>) -> WalletCmd, wait: Duration) -> Result<T, String> {
        let rx = self.with_open(|w| {
            let (tx, rx) = channel();
            w.cmd.send(make(tx)).map_err(|_| "wallet is closing".to_string())?;
            Ok(rx)
        })?;
        rx.recv_timeout(wait).map_err(|_| "the wallet did not answer".to_string())?
    }

    fn propose(&self, input: crate::send::SendInput) -> Result<Value, String> {
        self.ask(|tx| WalletCmd::Propose(input, tx), Duration::from_secs(60))
    }

    fn prover(&self) -> Result<Arc<zcash_proofs::prover::LocalTxProver>, String> {
        let mut g = self.prover.lock().unwrap();
        if let Some(p) = g.as_ref() {
            return Ok(p.clone());
        }
        let dir = self.params_dir.clone().ok_or("no Sapling parameter directory is configured")?;
        let p = Arc::new(crate::send::load_prover(&dir)?);
        *g = Some(p.clone());
        Ok(p)
    }

    /// Decrypts the seed, has the wallet thread prove and sign, then broadcasts each
    /// transaction on a fresh circuit, trying the servers in order.
    fn sign_and_send(&self, proposal: &str, password: &str) -> Result<Value, String> {
        let (dir, routes) = self.with_open(|w| Ok((WalletDir { path: w.dir.path.clone() }, w.routes.clone())))?;
        let phrase = dir.unseal_phrase(password).map_err(|e| e.to_string())?;
        let prover = self.prover()?;
        let (built, spaced) = self.ask(|tx| WalletCmd::Sign(proposal.to_string(), phrase, prover, tx), Duration::from_secs(300))?;
        let cfg = routes.config()?;
        let mut sent = vec![];
        let mut built: Vec<(String, Vec<u8>)> = built;
        if spaced {
            // The first goes now; the rest follow 2-10 minutes apart, each on its own circuit.
            let later: Vec<(String, Vec<u8>)> = built.split_off(1);
            for (txid, _) in &later {
                sent.push(json!({"txid": txid, "accepted": null, "scheduled": true}));
            }
            let (servers, proxy) = (cfg.servers.clone(), cfg.proxy.clone());
            self.rt.spawn(async move {
                for (txid, raw) in later {
                    let wait = rand::RngExt::random_range(&mut rand::rng(), 120..600);
                    tokio::time::sleep(Duration::from_secs(wait)).await;
                    let server = crate::sync::enhance::pick(&servers).to_string();
                    let r = fetch::send_transaction(&server, &proxy, raw).await;
                    tracing::info!(target: "zcash", "scheduled shielding {txid}: {r:?}");
                }
            });
        }
        for (txid, raw) in built {
            let mut outcome = json!({"txid": txid, "accepted": false});
            for server in &cfg.servers {
                match self.rt.block_on(fetch::send_transaction(server, &cfg.proxy, raw.clone())) {
                    Ok((0, _)) => {
                        outcome = json!({"txid": txid, "accepted": true, "server": server});
                        // Servers in the route table are one per operator: the next one checks.
                        if let Some(other) = cfg.servers.iter().find(|s| *s != server).cloned() {
                            let (proxy, raw, id) = (cfg.proxy.clone(), raw.clone(), txid_bytes(&txid));
                            self.rt.spawn(async move {
                                let note = fetch::confirm_elsewhere(&other, &proxy, id, raw).await;
                                tracing::info!(target: "zcash", "{note}");
                            });
                        }
                        break;
                    }
                    Ok((code, msg)) => outcome = json!({"txid": txid, "accepted": false, "server": server, "error": format!("{code}: {msg}")}),
                    Err(e) => outcome = json!({"txid": txid, "accepted": false, "server": server, "error": e.to_string()}),
                }
            }
            sent.push(outcome);
        }
        Ok(json!({"transactions": sent}))
    }

    /// Decrypts the phrase and has the wallet thread commit exactly the plan reviewed.
    fn sign_migration(&self, plan: String, digest: String, password: &str) -> Result<Value, String> {
        let dir = self.with_open(|w| Ok(WalletDir { path: w.dir.path.clone() }))?;
        let phrase = dir.unseal_phrase(password).map_err(|e| e.to_string())?;
        self.ask(|tx| WalletCmd::SignMigration(plan, digest, phrase, tx), Duration::from_secs(600))
    }

    pub fn migration_status(&self) -> Value {
        self.ask(WalletCmd::MigrationStatus, Duration::from_secs(15)).unwrap_or_else(|e| json!({"ok": false, "error": e}))
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
            let conn = w.reader.lock().unwrap();
            balances_json(&borrow_db(&conn, w.network), w.account)
        });
        r.unwrap_or_else(|e| json!({"ok": false, "error": e}))
    }

    pub fn addresses(&self) -> Value {
        let r = self.with_open(|w| {
            let conn = w.reader.lock().unwrap();
            let db = borrow_db(&conn, w.network);
            let ua = db
                .get_last_generated_address_matching(w.account, UnifiedAddressRequest::SHIELDED)
                .map_err(|e| e.to_string())?;
            let transparent = first_external_transparent(&db, w.account)?;
            Ok((ua.map(|a| a.encode(&w.network)), transparent))
        });
        match r {
            // The account's first address may carry a transparent receiver; the
            // shielded-only one is made on first use.
            Ok((None, transparent)) => {
                let made = self.new_address();
                json!({"ok": made["ok"], "unified": made["unified"], "transparent": transparent, "error": made.get("error")})
            }
            Ok((unified, transparent)) => json!({"ok": true, "unified": unified, "transparent": transparent}),
            Err(e) => json!({"ok": false, "error": e}),
        }
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

    /// Newest first. Each row: txid, kind, height or pending, delta, fee, pools,
    /// memos, the amount that crossed pools publicly, and expiry for unmined sends.
    pub fn history(&self, page: u32) -> Value {
        let r = self.with_open(|w| {
            let conn = w.reader.lock().unwrap();
            crate::history::page(&conn, w.network, w.account, page)
        });
        r.unwrap_or_else(|e| json!({"ok": false, "error": e}))
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
            let conn = w.reader.lock().unwrap();
            let db = borrow_db(&conn, w.network);
            let account = db.get_account(w.account).map_err(|e| e.to_string())?.ok_or("account missing")?;
            let ufvk = account.ufvk().ok_or("this account has no full viewing key")?;
            let encoded = ufvk.encode(&w.network).map_err(|e| format!("{e:?}"))?;
            Ok(json!({"ok": true, "ufvk": encoded}))
        });
        r.unwrap_or_else(|e| json!({"ok": false, "error": e}))
    }
}

/// A txid's internal byte order from its display form.
fn txid_bytes(display: &str) -> Vec<u8> {
    let mut b = hex::decode(display).unwrap_or_default();
    b.reverse();
    b
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

fn balances_json<C: std::borrow::Borrow<rusqlite::Connection>>(db: &wallet::WalletDbOf<C>, account: AccountUuid) -> Result<Value, String> {
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
        "transparentAddresses": transparent_funds(db, account, summary.chain_tip_height())?,
        "shieldingThreshold": crate::send::SHIELDING_THRESHOLD,
        "total": zat(b.total()),
    }))
}

/// Transparent funds per address, for a Shield action each (ZIP 315: never linked).
fn transparent_funds<C: std::borrow::Borrow<rusqlite::Connection>>(db: &wallet::WalletDbOf<C>, account: AccountUuid, tip: zcash_protocol::consensus::BlockHeight) -> Result<Value, String> {
    use zcash_keys::encoding::AddressCodec;
    let balances = db
        .get_transparent_balances(account, (tip + 1).into(), ConfirmationsPolicy::default())
        .map_err(|e| e.to_string())?;
    let mut rows: Vec<Value> = balances
        .into_iter()
        .filter(|(_, (_, b))| b.total().into_u64() > 0)
        .map(|(addr, (_, b))| json!({"address": addr.encode(db.params()), "spendable": zat(b.spendable_value()), "total": zat(b.total())}))
        .collect();
    rows.sort_by(|a, b| a["address"].as_str().cmp(&b["address"].as_str()));
    Ok(json!(rows))
}

/// The lowest-index external transparent address. Rotation after a payment comes
/// with the Receive tab work.
fn first_external_transparent<C: std::borrow::Borrow<rusqlite::Connection>>(db: &wallet::WalletDbOf<C>, account: AccountUuid) -> Result<Option<String>, String> {
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

/// Files a proposal under a new id and returns its preview.
fn keep(
    prepared: &mut std::collections::HashMap<String, crate::send::Prepared>,
    next: &mut u64,
    params: &ZNetwork,
    proposals: Vec<crate::send::AnyProposal>,
) -> Result<Value, String> {
    let mut items = vec![];
    let mut previews = vec![];
    for p in proposals {
        let expiry = crate::send::expiry_for(params, p.min_target_height())?;
        previews.push(p.preview(params, expiry));
        items.push((p, expiry));
    }
    let preview = if previews.len() == 1 {
        previews.pop().expect("one")
    } else {
        let fee: u64 = previews.iter().filter_map(|v| v["fee"].as_u64()).sum();
        json!({"shielding": true, "batch": previews, "fee": fee, "transactions": items.len(),
               "spacing": "each transaction goes out on its own circuit, 2 to 10 minutes after the previous one",
               "ttlSecs": crate::send::PREVIEW_TTL_SECS})
    };
    *next += 1;
    let id = format!("p{next}");
    prepared.insert(id.clone(), crate::send::Prepared { created: Instant::now(), items, preview: preview.clone() });
    Ok(json!({"proposalId": id, "preview": preview}))
}

#[allow(clippy::too_many_arguments)]
fn wallet_loop(
    mut db: Db,
    mut syncer: Syncer,
    cmd_rx: Receiver<WalletCmd>,
    stop: Arc<AtomicBool>,
    progress: Arc<Mutex<Progress>>,
    sink: Sink,
    account: AccountUuid,
    mut mig_conn: rusqlite::Connection,
    net: (tokio::runtime::Handle, SyncConfig),
    paused_flag: PathBuf,
) {
    let mut plans: std::collections::HashMap<String, (zcash_pool_migration::engine::MigrationPlan, Value, Instant)> = Default::default();
    let mut last_drive = Instant::now() - Duration::from_secs(60);
    let mut blocker: Option<&'static str> = None;
    let mut backoff = Duration::from_millis(500);
    let mut last_emit = Instant::now() - Duration::from_secs(60);
    let mut last_balance: Option<Value> = None;
    let mut last_balance_check = Instant::now() - Duration::from_secs(60);
    let mut prepared: std::collections::HashMap<String, crate::send::Prepared> = Default::default();
    let mut next_proposal = 0u64;
    let params = *db.params();
    while !stop.load(Ordering::SeqCst) {
        prepared.retain(|_, p| p.created.elapsed().as_secs() <= crate::send::PREVIEW_TTL_SECS);
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
                WalletCmd::Propose(input, reply) => {
                    let r = crate::send::propose(&mut db, params, account, &input)
                        .map(crate::send::AnyProposal::Send)
                        .and_then(|p| keep(&mut prepared, &mut next_proposal, &params, vec![p]));
                    let _ = reply.send(r);
                }
                WalletCmd::ProposeShielding(address, reply) => {
                    // An empty address shields every address above the threshold, one transaction each.
                    let r = if address.trim().is_empty() {
                        crate::send::propose_shield_all(&mut db, params, account)
                    } else {
                        crate::send::propose_shield(&mut db, params, account, &address).map(|p| vec![p])
                    }
                    .and_then(|ps| keep(&mut prepared, &mut next_proposal, &params, ps));
                    let _ = reply.send(r);
                }
                WalletCmd::PlanMigration(reply) => {
                    let tip = db.chain_height().ok().flatten().map_or(0, u32::from);
                    let r = crate::migration::plan(&db, &mut mig_conn, params, account).map(|plan| {
                        let preview = crate::migration::preview(&params, &plan, tip);
                        let id = format!("m{}", plans.len() + 1);
                        plans.insert(id.clone(), (plan, preview.clone(), Instant::now()));
                        json!({"planId": id, "preview": preview})
                    });
                    let _ = reply.send(r);
                }
                WalletCmd::SignMigration(id, digest, phrase, reply) => {
                    let r = match plans.remove(&id) {
                        None => Err("unknown or expired plan".to_string()),
                        Some((_, preview, _)) if preview["digest"].as_str() != Some(digest.as_str()) => {
                            Err("the plan changed since it was reviewed".to_string())
                        }
                        Some((plan, preview, _)) => crate::migration::commit(&db, &mut mig_conn, params, account, &plan, &preview, &phrase),
                    };
                    drop(phrase);
                    if r.is_ok() {
                        sink(Event::MigrationChanged(crate::migration::status(&mut mig_conn, params, account).unwrap_or_default()));
                    }
                    let _ = reply.send(r);
                }
                WalletCmd::MigrationControl(what, reply) => {
                    let r = match what {
                        "pause" => std::fs::write(&paused_flag, b"paused").map(|_| json!({"paused": true})).map_err(|e| e.to_string()),
                        "resume" => {
                            let _ = std::fs::remove_file(&paused_flag);
                            blocker = None;
                            Ok(json!({"paused": false}))
                        }
                        _ => crate::migration::cancel(&mut mig_conn, params, account),
                    };
                    sink(Event::MigrationChanged(crate::migration::status(&mut mig_conn, params, account).unwrap_or_default()));
                    let _ = reply.send(r);
                }
                WalletCmd::MigrationStatus(reply) => {
                    let r = crate::migration::status(&mut mig_conn, params, account).map(|mut v| {
                        v["paused"] = json!(paused_flag.exists());
                        v["needsApproval"] = json!(blocker);
                        v
                    });
                    let _ = reply.send(r);
                }
                WalletCmd::Sign(id, phrase, prover, reply) => {
                    let r = match prepared.remove(&id) {
                        None => Err("unknown or expired proposal".to_string()),
                        Some(p) => crate::send::sign(&mut db, params, account, &p, &phrase, &prover)
                            .map(|v| (v.into_iter().map(|(txid, raw)| (txid.to_string(), raw)).collect(), p.spaced())),
                    };
                    drop(phrase);
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
        // Plans expire with their anchor bucket: 144 blocks is about an hour after NU7.
        plans.retain(|_, (_, _, t)| t.elapsed() < Duration::from_secs(3600));
        // The migration moves only on a synced wallet, at most every 30 s, between sync steps.
        if syncer.progress.state == "synced" && !paused_flag.exists() && last_drive.elapsed() >= Duration::from_secs(30) {
            last_drive = Instant::now();
            let (rt, cfg) = (&net.0, &net.1);
            // Broadcast through a server other than the one syncing, when there is one.
            let server = cfg.servers.get(1).unwrap_or(&cfg.servers[0]).clone();
            let mut send = |raw: Vec<u8>| -> Result<(bool, String), String> {
                rt.block_on(fetch::send_transaction(&server, &cfg.proxy, raw))
                    .map(|(code, msg)| (code == 0, msg))
                    .map_err(|e| e.to_string())
            };
            match crate::migration::drive(&mut db, &mut mig_conn, params, account, &mut send) {
                Ok(crate::migration::Drive::None) | Ok(crate::migration::Drive::Waiting(_)) => {}
                Ok(crate::migration::Drive::NeedsApproval(why)) => {
                    if blocker != Some(why) {
                        blocker = Some(why);
                        sink(Event::MigrationChanged(json!({"needsApproval": why})));
                    }
                }
                Ok(other) => {
                    blocker = None;
                    let mut v = crate::migration::status(&mut mig_conn, params, account).unwrap_or_default();
                    v["lastStep"] = json!(format!("{other:?}"));
                    sink(Event::MigrationChanged(v));
                }
                Err(e) => syncer.progress.last_error = Some(format!("migration: {e}")),
            }
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
