//! The sync driver. It runs on the wallet thread, one bounded step at a time, so
//! commands and cancellation are handled between steps.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::mpsc::{channel, Receiver, RecvTimeoutError, Sender};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use futures_util::TryStreamExt;
use serde::Serialize;
use zcash_client_backend::data_api::chain::{error::Error as ChainError, scan_cached_blocks, ChainState, CommitmentTreeRoot};
use zcash_client_backend::data_api::scanning::ScanPriority;
use zcash_client_backend::data_api::wallet::decrypt_and_store_transaction;
use zcash_client_backend::data_api::{
    TransactionDataRequest, WalletCommitmentTrees, WalletRead, WalletWrite, IRONWOOD_SHARD_HEIGHT, ORCHARD_SHARD_HEIGHT,
    SAPLING_SHARD_HEIGHT,
};
use zcash_client_backend::proto::compact_formats::CompactBlock;
use zcash_client_backend::proto::service::{GetSubtreeRootsArg, LightdInfo, ShieldedProtocol};
use zcash_keys::encoding::AddressCodec;
use zcash_primitives::merkle_tree::HashSer;
use zcash_primitives::transaction::Transaction;
use zcash_protocol::consensus::BlockHeight;
use zcash_transparent::address::TransparentAddress;

use super::cache::{chunk_first, grid_floor, BlockCache, GRID};
use super::crosscheck::{self, CrossCheck, Verdict};
use super::enhance::{self, Fetched};
use super::fetch::{self, Chunk};
use super::frontier::{self, FrontierError};
use crate::net::client::{connect, NetError};
use crate::net::ipc::LOCAL_NODE_URL;
use crate::net::socks::{Isolation, ProxyAddr};
use crate::network::ZNetwork;
use crate::wallet::Db;

/// Blocks within this distance of the tip stay cached for reorgs.
pub const HEAD: u32 = 100;

/// Reading from the local node, a server checks it this often, and sooner after a mismatch.
const CROSS_CHECK_EVERY: Duration = Duration::from_secs(120);
const RECHECK_AFTER: Duration = Duration::from_secs(20);

#[derive(Debug, Clone)]
pub struct SyncConfig {
    pub proxy: ProxyAddr,
    /// Chunks go to these servers in turn; the first also answers tip queries.
    pub servers: Vec<String>,
    /// Where transactions go out; the sync servers unless the routes say otherwise.
    pub broadcast: Vec<String>,
    pub parallel: usize,
    /// Cached chunks not yet scanned, plus downloads in flight, stay under this.
    pub ahead: usize,
    /// Shielded outputs per scan call, which bounds how long one step takes.
    pub work_cap: usize,
    pub tip_poll: Duration,
}

impl SyncConfig {
    pub fn new(proxy: ProxyAddr, servers: Vec<String>) -> Self {
        let broadcast = servers.clone();
        Self { proxy, servers, broadcast, parallel: 2, ahead: 24, work_cap: 2_000, tip_poll: Duration::from_secs(30) }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SyncError {
    #[error(transparent)]
    Net(#[from] NetError),
    #[error("block cache: {0}")]
    Cache(#[from] super::cache::CacheError),
    #[error("wallet database: {0}")]
    Db(String),
    #[error("server {server} is on chain {got}, not {want}")]
    WrongChain { server: String, got: String, want: String },
    #[error("server {server} reports branch {got}; this wallet expects {want}; an update may be required")]
    UnknownBranch { server: String, got: String, want: String },
    #[error("server {server}: {detail}")]
    Misbehaving { server: String, detail: String },
}

#[derive(Debug, Clone, Default, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Progress {
    pub state: String,
    pub tip: Option<u32>,
    pub birthday: u32,
    pub fully_scanned: Option<u32>,
    pub blocks_fetched: u64,
    pub blocks_scanned: u64,
    pub outputs_scanned: u64,
    pub downloads_in_flight: usize,
    pub details_pending: usize,
    /// Seconds to finish at the measured pace; none until there is a pace to measure.
    pub eta_secs: Option<u64>,
    pub blocks_left: Option<u64>,
    pub chunks_cached: usize,
    pub last_error: Option<String>,
    /// Reading from the local node: how it compared with a server last time.
    pub cross_check: Option<CrossCheck>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Step {
    /// Did work; call again.
    Worked,
    /// Waiting on the network.
    Waiting,
    /// Everything up to the known tip is scanned.
    Synced,
}

enum Msg {
    Chunk(u32, Result<Chunk, NetError>),
    /// A cached chunk's missing blocks: chunk start, first height, result.
    Tail(u32, u32, Result<Vec<CompactBlock>, NetError>),
    Tip(Result<(u32, LightdInfo), NetError>),
    Enhanced(TransactionDataRequest, Result<Fetched, NetError>),
    /// Local node only: an address's transactions up to a height.
    Received(TransparentAddress, u32, Result<Vec<(Transaction, Option<BlockHeight>)>, NetError>),
    CrossCheck(Result<CrossCheck, NetError>),
}

/// What a sync error came from. The next success of the same source clears it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ErrorSource {
    Tip,
    Chunk(u32),
    Tail(u32),
    Details,
    Transparent,
    /// A chain that changed under the scan.
    Scan,
    /// The engine's: a failed step, and the migration.
    Step,
    Migration,
    /// The local node against a server.
    CrossCheck,
}

/// Errors by source, so the one shown is the newest that still stands.
#[derive(Default)]
struct Errors {
    by_source: HashMap<ErrorSource, (u64, String)>,
    seq: u64,
}

impl Errors {
    fn set(&mut self, source: ErrorSource, e: String) {
        self.seq += 1;
        self.by_source.insert(source, (self.seq, e));
    }

    fn clear(&mut self, source: ErrorSource) {
        self.by_source.remove(&source);
    }

    fn get(&self, source: ErrorSource) -> Option<&str> {
        self.by_source.get(&source).map(|(_, e)| e.as_str())
    }

    /// Once synced, downloads and scans that failed on the way are moot.
    fn clear_moot(&mut self) {
        self.by_source.retain(|s, _| !matches!(s, ErrorSource::Chunk(_) | ErrorSource::Tail(_) | ErrorSource::Scan));
    }

    fn latest(&self) -> Option<String> {
        self.by_source.values().max_by_key(|(seq, _)| *seq).map(|(_, e)| e.clone())
    }
}

pub struct Syncer {
    params: ZNetwork,
    cfg: SyncConfig,
    rt: tokio::runtime::Handle,
    cache: Arc<BlockCache>,
    tx: Sender<Msg>,
    rx: Receiver<Msg>,
    in_flight: BTreeSet<u32>,
    /// Failed downloads wait before the next try: failures so far and the time to retry.
    retry: HashMap<u32, (u32, Instant)>,
    tails_in_flight: BTreeSet<u32>,
    tip_in_flight: bool,
    last_tip_poll: Option<Instant>,
    tip: Option<u32>,
    /// Subtree roots stored this session per pool (Sapling, Orchard, Ironwood), and the shards
    /// a window last needed when they were fetched.
    roots: Option<[u64; 3]>,
    roots_asked: [u64; 3],
    next_server: usize,
    /// The chain state after the last scan call, to continue mid-chunk cheaply.
    last_end: Option<ChainState>,
    enh_in_flight: HashSet<TransactionDataRequest>,
    last_enh: Option<Instant>,
    /// Local node only: the height each transparent address is looked up to, lookups in
    /// flight, and a pause after failures.
    t_checked: HashMap<TransparentAddress, u32>,
    t_in_flight: HashSet<TransparentAddress>,
    t_failures: u32,
    t_pause: Option<Instant>,
    /// When scanning began this session, and how many blocks since, for the ETA.
    pace: Option<(Instant, u64)>,
    /// Local node only: the next cross-check, the server it goes to, and a first mismatch.
    check_due: Option<Instant>,
    check_in_flight: bool,
    next_checker: usize,
    differed: bool,
    errors: Errors,
    pub progress: Progress,
}

fn db_err<E: std::fmt::Display>(e: E) -> SyncError {
    SyncError::Db(e.to_string())
}

/// 1, 2, 4 ... 60 s after consecutive failures, plus up to a quarter more at random, so
/// a failing server is not hammered with fresh connections (and Tor circuits).
fn retry_delay(failures: u32) -> Duration {
    let base = Duration::from_secs(1u64 << failures.saturating_sub(1).min(6)).min(Duration::from_secs(60));
    base + Duration::from_millis(rand::RngExt::random_range(&mut rand::rng(), 0..=base.as_millis() as u64 / 4))
}

impl Syncer {
    pub fn new(params: ZNetwork, cfg: SyncConfig, rt: tokio::runtime::Handle, cache: Arc<BlockCache>, birthday: u32) -> Self {
        let (tx, rx) = channel();
        Self {
            params,
            cfg,
            rt,
            cache,
            tx,
            rx,
            in_flight: BTreeSet::new(),
            retry: HashMap::new(),
            tails_in_flight: BTreeSet::new(),
            tip_in_flight: false,
            last_tip_poll: None,
            tip: None,
            roots: None,
            roots_asked: [0; 3],
            next_server: 0,
            last_end: None,
            enh_in_flight: HashSet::new(),
            last_enh: None,
            t_checked: HashMap::new(),
            t_in_flight: HashSet::new(),
            t_failures: 0,
            t_pause: None,
            pace: None,
            check_due: None,
            check_in_flight: false,
            next_checker: 0,
            differed: false,
            errors: Errors::default(),
            progress: Progress { state: "starting".into(), birthday, ..Default::default() },
        }
    }

    /// Records an error that `last_error` shows until its source next succeeds.
    pub fn fail(&mut self, source: ErrorSource, e: impl std::fmt::Display) {
        self.errors.set(source, e.to_string());
        self.progress.last_error = self.errors.latest();
    }

    pub fn succeed(&mut self, source: ErrorSource) {
        self.errors.clear(source);
        self.progress.last_error = self.errors.latest();
    }

    /// The error `source` last recorded, while it stands.
    pub fn error(&self, source: ErrorSource) -> Option<&str> {
        self.errors.get(source)
    }

    fn server_for(&mut self) -> String {
        let s = self.cfg.servers[self.next_server % self.cfg.servers.len()].clone();
        self.next_server += 1;
        s
    }

    /// Waits up to `timeout` for a download to land, so an idle loop does not spin.
    pub fn wait(&mut self, db: &mut Db, timeout: Duration) -> Result<(), SyncError> {
        match self.rx.recv_timeout(timeout) {
            Ok(msg) => self.handle(db, msg),
            Err(RecvTimeoutError::Timeout | RecvTimeoutError::Disconnected) => Ok(()),
        }
    }

    pub fn step(&mut self, db: &mut Db) -> Result<Step, SyncError> {
        while let Ok(msg) = self.rx.try_recv() {
            self.handle(db, msg)?;
        }
        self.poll_tip_if_due();
        let Some(tip) = self.tip else {
            self.progress.state = "connecting".into();
            return Ok(Step::Waiting);
        };
        self.cross_check_if_due();
        if self.roots.is_none() {
            self.roots = Some(self.load_subtree_roots(db, [0; 3])?);
        }
        self.plan_downloads(db, tip)?;
        let worked = self.scan_once(db, tip)?;
        self.progress.fully_scanned = db.block_fully_scanned().map_err(db_err)?.map(|m| u32::from(m.block_height()));
        self.update_eta(db, tip)?;
        self.progress.downloads_in_flight = self.in_flight.len() + self.tails_in_flight.len();
        if worked {
            self.progress.state = "scanning".into();
            return Ok(Step::Worked);
        }
        self.plan_enhancements(db, tip)?;
        let synced = self.progress.fully_scanned.is_some_and(|h| h >= tip) && self.in_flight.is_empty();
        if synced {
            self.errors.clear_moot();
            self.progress.last_error = self.errors.latest();
        }
        self.progress.state = if synced { "synced" } else { "downloading" }.into();
        Ok(if synced { Step::Synced } else { Step::Waiting })
    }

    fn poll_tip_if_due(&mut self) {
        let due = self.last_tip_poll.is_none_or(|t| t.elapsed() >= self.cfg.tip_poll);
        if !due || self.tip_in_flight {
            return;
        }
        self.tip_in_flight = true;
        self.last_tip_poll = Some(Instant::now());
        let (server, proxy, tx) = (self.cfg.servers[0].clone(), self.cfg.proxy.clone(), self.tx.clone());
        self.rt.spawn(async move {
            let _ = tx.send(Msg::Tip(fetch::tip_and_info(&server, &proxy, Isolation::fresh()).await));
        });
    }

    /// Reading from the local node, the servers that send transactions take turns checking it.
    fn cross_check_if_due(&mut self) {
        let checkers: Vec<String> = self.cfg.broadcast.iter().filter(|s| *s != LOCAL_NODE_URL).cloned().collect();
        if !self.local() || checkers.is_empty() || self.check_in_flight || self.check_due.is_some_and(|t| Instant::now() < t) {
            return;
        }
        let server = checkers[self.next_checker % checkers.len()].clone();
        self.next_checker += 1;
        self.check_in_flight = true;
        self.check_due = Some(Instant::now() + CROSS_CHECK_EVERY);
        let (proxy, tx, net) = (self.cfg.proxy.clone(), self.tx.clone(), self.params);
        self.rt.spawn(async move {
            let _ = tx.send(Msg::CrossCheck(crosscheck::check(net, &server, &proxy).await));
        });
    }

    fn handle(&mut self, db: &mut Db, msg: Msg) -> Result<(), SyncError> {
        match msg {
            Msg::Tip(r) => {
                self.tip_in_flight = false;
                match r {
                    Ok((tip, info)) => {
                        // Under the tip's source too, so it stays shown while every poll gives it.
                        if let Err(e) = self.check_server(&self.cfg.servers[0].clone(), &info, tip) {
                            self.fail(ErrorSource::Tip, &e);
                            return Err(e);
                        }
                        if self.tip.is_none_or(|t| t != tip) {
                            db.update_chain_tip(BlockHeight::from(tip)).map_err(db_err)?;
                            self.tip = Some(tip);
                        }
                        self.progress.tip = Some(tip);
                        self.succeed(ErrorSource::Tip);
                    }
                    Err(e) => {
                        tracing::warn!(target: "zcash", "tip poll: {e}");
                        self.fail(ErrorSource::Tip, e);
                    }
                }
            }
            Msg::Chunk(start, r) => {
                self.in_flight.remove(&start);
                match r {
                    Ok(chunk) => {
                        self.retry.remove(&start);
                        self.succeed(ErrorSource::Chunk(start));
                        self.store_chunk(chunk)?
                    }
                    Err(e) => {
                        let failures = self.retry.get(&start).map_or(0, |r| r.0) + 1;
                        tracing::warn!(target: "zcash", "chunk {start}: {e} (failure {failures})");
                        self.retry.insert(start, (failures, Instant::now() + retry_delay(failures)));
                        self.fail(ErrorSource::Chunk(start), e);
                    }
                }
            }
            Msg::Enhanced(req, r) => {
                self.enh_in_flight.remove(&req);
                match r {
                    Ok(fetched) => {
                        self.succeed(ErrorSource::Details);
                        self.apply_enhancement(db, req, fetched)?
                    }
                    Err(e) => self.fail(ErrorSource::Details, e),
                }
            }
            Msg::Received(address, upto, r) => {
                self.t_in_flight.remove(&address);
                match r {
                    Ok(txs) => {
                        for (tx, height) in &txs {
                            decrypt_and_store_transaction(&self.params, db, tx, *height).map_err(db_err)?;
                        }
                        self.t_checked.insert(address, upto);
                        self.t_failures = 0;
                        self.succeed(ErrorSource::Transparent);
                    }
                    Err(e) => {
                        self.t_failures += 1;
                        tracing::warn!(target: "zcash", "transparent lookup: {e} (failure {})", self.t_failures);
                        self.t_pause = Some(Instant::now() + retry_delay(self.t_failures));
                        self.fail(ErrorSource::Transparent, e);
                    }
                }
            }
            Msg::CrossCheck(r) => {
                self.check_in_flight = false;
                match r {
                    // Once may be a block arriving between the reads; twice running is a fork.
                    Ok(c) if c.verdict == Verdict::Differs && !self.differed => {
                        tracing::warn!(target: "zcash", "cross-check: {} differs at {}; checking again", c.server, c.node_tip.min(c.server_tip));
                        self.differed = true;
                        self.check_due = Some(Instant::now() + RECHECK_AFTER);
                    }
                    Ok(c) => {
                        self.differed = c.verdict == Verdict::Differs;
                        if self.differed {
                            let at = c.node_tip.min(c.server_tip);
                            self.fail(ErrorSource::CrossCheck, format!("your node and {} have different blocks at {at}: one of them is on a false chain", c.server));
                        } else {
                            self.succeed(ErrorSource::CrossCheck);
                        }
                        self.progress.cross_check = Some(c);
                    }
                    Err(e) => tracing::warn!(target: "zcash", "cross-check: {e}"),
                }
            }
            Msg::Tail(start, first, r) => {
                self.tails_in_flight.remove(&start);
                match r {
                    Ok(blocks) => {
                        self.retry.remove(&start);
                        self.succeed(ErrorSource::Tail(start));
                        self.progress.blocks_fetched += blocks.len() as u64;
                        self.cache.insert_blocks(&blocks)?;
                    }
                    Err(e) => {
                        let failures = self.retry.get(&start).map_or(0, |r| r.0) + 1;
                        tracing::warn!(target: "zcash", "blocks from {first}: {e} (failure {failures})");
                        self.retry.insert(start, (failures, Instant::now() + retry_delay(failures)));
                        self.fail(ErrorSource::Tail(start), e);
                    }
                }
            }
        }
        Ok(())
    }

    fn check_server(&self, server: &str, info: &LightdInfo, tip: u32) -> Result<(), SyncError> {
        if !self.params.accepts_lightd_chain(&info.chain_name) {
            let want = self.params.lightd_chain_name();
            return Err(SyncError::WrongChain { server: server.into(), got: info.chain_name.clone(), want: want.into() });
        }
        if !self.params.accepts_branch(&info.consensus_branch_id, tip) {
            let want = self.params.branch_id_hex(BlockHeight::from(tip + 1));
            return Err(SyncError::UnknownBranch { server: server.into(), got: info.consensus_branch_id.clone(), want });
        }
        Ok(())
    }

    /// Checks a chunk before it enters the cache: the blocks must continue the
    /// tree state below them, and must lead to the tree state of the chunk above
    /// if that one is already here.
    fn store_chunk(&mut self, chunk: Chunk) -> Result<(), SyncError> {
        let misbehaving = |detail: String| SyncError::Misbehaving { server: chunk.server.clone(), detail };
        let below = chunk.below.to_chain_state().map_err(|e| misbehaving(e.to_string()))?;
        if chunk.blocks.len() as u32 != chunk.last + 1 - chunk_first(chunk.start) {
            return Err(misbehaving(format!("returned {} blocks for {}..={}", chunk.blocks.len(), chunk.start, chunk.last)));
        }
        let end = frontier::advance(&below, &chunk.blocks).map_err(|e| misbehaving(e.to_string()))?;
        if let Some(above) = self.cache.tree_state(chunk.last)? {
            let above = above.to_chain_state().map_err(|e| misbehaving(e.to_string()))?;
            if above.block_hash() != end.block_hash()
                || above.final_sapling_tree() != end.final_sapling_tree()
                || above.final_orchard_tree() != end.final_orchard_tree()
                || above.final_ironwood_tree() != end.final_ironwood_tree()
            {
                return Err(misbehaving(format!("chunk {} does not lead to the tree state at {}", chunk.start, chunk.last)));
            }
        }
        self.progress.blocks_fetched += chunk.blocks.len() as u64;
        self.cache.insert_blocks(&chunk.blocks)?;
        self.cache.put_tree_state(&chunk.below, &chunk.server)?;
        self.cache.mark_chunk(chunk.start, chunk.last + 1, &chunk.server)?;
        Ok(())
    }

    /// Loads the subtree roots completed since `from`, per pool, and returns how many each now has.
    fn load_subtree_roots(&mut self, db: &mut Db, from: [u64; 3]) -> Result<[u64; 3], SyncError> {
        let (server, proxy) = (self.cfg.servers[0].clone(), self.cfg.proxy.clone());
        let fetch = |p: ShieldedProtocol, start: u64| {
            let (server, proxy) = (server.clone(), proxy.clone());
            async move {
                let mut c = connect(&server, &proxy, Isolation::fresh()).await?;
                let arg = GetSubtreeRootsArg { start_index: start as u32, shielded_protocol: p as i32, max_entries: 0 };
                let roots: Vec<_> = c
                    .get_subtree_roots(arg)
                    .await
                    .map_err(|status| NetError::Status { server: server.clone(), status })?
                    .into_inner()
                    .try_collect()
                    .await
                    .map_err(|status| NetError::Status { server: server.clone(), status })?;
                Ok::<_, NetError>(roots)
            }
        };
        fn parse<H: HashSer>(roots: Vec<zcash_client_backend::proto::service::SubtreeRoot>) -> Result<Vec<CommitmentTreeRoot<H>>, String> {
            roots
                .into_iter()
                .map(|r| {
                    let h = H::read(&r.root_hash[..]).map_err(|e| e.to_string())?;
                    Ok(CommitmentTreeRoot::from_parts(BlockHeight::from_u32(r.completing_block_height as u32), h))
                })
                .collect()
        }
        let bad = |d: String| SyncError::Misbehaving { server: server.clone(), detail: d };
        let [s, o, i] = from;
        let sapling = parse::<sapling::Node>(self.rt.block_on(fetch(ShieldedProtocol::Sapling, s))?).map_err(bad)?;
        let orchard = parse::<orchard::tree::MerkleHashOrchard>(self.rt.block_on(fetch(ShieldedProtocol::Orchard, o))?).map_err(bad)?;
        let ironwood = parse::<orchard::tree::MerkleHashOrchard>(self.rt.block_on(fetch(ShieldedProtocol::Ironwood, i))?).map_err(bad)?;
        db.put_sapling_subtree_roots(s, &sapling).map_err(db_err)?;
        db.put_orchard_subtree_roots(o, &orchard).map_err(db_err)?;
        db.put_ironwood_subtree_roots(i, &ironwood).map_err(db_err)?;
        Ok([s + sapling.len() as u64, o + orchard.len() as u64, i + ironwood.len() as u64])
    }

    /// The wallet database refuses a gap between shards. A window that starts in a shard past
    /// the roots loaded so far first loads those completed since, as when the chain grew during
    /// the session (a local node syncing). Once per need: a source without them fails the scan.
    fn ensure_roots(&mut self, db: &mut Db, state: &ChainState) -> Result<(), SyncError> {
        let need = first_shards(state);
        let have = self.roots.unwrap_or_default();
        if !roots_missing(need, have, self.roots_asked) {
            return Ok(());
        }
        // Recorded after the fetch, so a failed one is retried on the next step.
        self.roots = Some(self.load_subtree_roots(db, have)?);
        self.roots_asked = need;
        Ok(())
    }

    /// Chunks still needed, from the tip down, each with the first height it lacks a scan for.
    fn needed_chunks(&self, db: &Db, tip: u32) -> Result<Vec<(u32, u32)>, SyncError> {
        let mut starts: BTreeMap<u32, u32> = BTreeMap::new();
        for r in db.suggest_scan_ranges().map_err(db_err)? {
            if matches!(r.priority(), ScanPriority::Scanned | ScanPriority::Ignored) {
                continue;
            }
            let (s, e) = (u32::from(r.block_range().start), u32::from(r.block_range().end).min(tip + 1));
            let mut c = grid_floor(s);
            while c < e {
                let from = s.max(chunk_first(c));
                starts.entry(c).and_modify(|f| *f = (*f).min(from)).or_insert(from);
                c += GRID;
            }
        }
        Ok(starts.into_iter().rev().collect())
    }

    fn plan_downloads(&mut self, db: &Db, tip: u32) -> Result<(), SyncError> {
        let tip_chunk = grid_floor(tip);
        let needed = self.needed_chunks(db, tip)?;
        // A cached chunk grows from its first missing block: the tip chunk up to the tip, and a
        // chunk the tip has moved past (or a wallet reopened later) up to its grid line.
        for &(start, from) in &needed {
            if self.tails_in_flight.len() >= self.cfg.parallel {
                break;
            }
            if self.tails_in_flight.contains(&start) || self.cache.chunk_end(start)?.is_none() {
                continue;
            }
            if self.retry.get(&start).is_some_and(|r| Instant::now() < r.1) {
                continue;
            }
            let last = if start == tip_chunk { tip } else { start + GRID - 1 };
            let have = self.cache.contiguous_end(from)?;
            if have <= last {
                self.tails_in_flight.insert(start);
                let (server, proxy, tx) = (self.server_for(), self.cfg.proxy.clone(), self.tx.clone());
                self.rt.spawn(async move {
                    let _ = tx.send(Msg::Tail(start, have, fetch::fetch_blocks(&server, &proxy, have, last).await));
                });
            }
        }
        let mut cached = 0;
        for &(start, _) in &needed {
            if self.cache.chunk_end(start)?.is_some() {
                cached += 1;
            }
        }
        self.progress.chunks_cached = cached;
        for (start, _) in needed {
            if self.in_flight.len() >= self.cfg.parallel || cached + self.in_flight.len() >= self.cfg.ahead {
                break;
            }
            if self.in_flight.contains(&start) || self.cache.chunk_end(start)?.is_some() {
                continue;
            }
            if self.retry.get(&start).is_some_and(|r| Instant::now() < r.1) {
                continue;
            }
            let last = if start == tip_chunk { tip } else { start + GRID - 1 };
            self.in_flight.insert(start);
            let (server, proxy, tx) = (self.server_for(), self.cfg.proxy.clone(), self.tx.clone());
            self.rt.spawn(async move {
                let _ = tx.send(Msg::Chunk(start, fetch::fetch_chunk(&server, &proxy, start, last).await));
            });
        }
        Ok(())
    }

    /// Blocks still to scan, and the time left at this session's pace.
    fn update_eta(&mut self, db: &Db, tip: u32) -> Result<(), SyncError> {
        let left: u64 = db
            .suggest_scan_ranges()
            .map_err(db_err)?
            .iter()
            .filter(|r| !matches!(r.priority(), ScanPriority::Scanned | ScanPriority::Ignored))
            .map(|r| (u32::from(r.block_range().end).min(tip + 1)).saturating_sub(u32::from(r.block_range().start)) as u64)
            .sum();
        self.progress.blocks_left = Some(left);
        let (start, scanned_then) = *self.pace.get_or_insert((Instant::now(), self.progress.blocks_scanned));
        let done = self.progress.blocks_scanned.saturating_sub(scanned_then);
        let secs = start.elapsed().as_secs_f64();
        self.progress.eta_secs = (done > 0 && secs > 5.0).then(|| (left as f64 * secs / done as f64).round() as u64);
        if left == 0 {
            self.progress.eta_secs = Some(0);
        }
        Ok(())
    }

    /// Sends out what the wallet asks to learn, a few at a time.
    fn local(&self) -> bool {
        self.cfg.servers.first().is_some_and(|s| s == LOCAL_NODE_URL)
    }

    fn plan_enhancements(&mut self, db: &Db, tip: u32) -> Result<(), SyncError> {
        // The delays only keep a server from tying requests together; the local node is ours.
        let local = self.local();
        let (every, at_once) = if local { (Duration::from_secs(1), 16) } else { (Duration::from_secs(10), 4) };
        if self.last_enh.is_some_and(|t| t.elapsed() < every) {
            return Ok(());
        }
        self.last_enh = Some(Instant::now());
        if local {
            self.plan_receipts(db, tip, at_once)?;
        }
        let requests = db.transaction_data_requests().map_err(db_err)?;
        self.progress.details_pending = requests.len();
        let now = SystemTime::now();
        for req in requests {
            if self.enh_in_flight.len() >= at_once {
                break;
            }
            if self.enh_in_flight.contains(&req) || (!local && enhance::not_before(&req).is_some_and(|t| t > now)) {
                continue;
            }
            self.enh_in_flight.insert(req.clone());
            let server = enhance::pick(&self.cfg.servers).to_string();
            let (proxy, tx, params) = (self.cfg.proxy.clone(), self.tx.clone(), self.params);
            self.rt.spawn(async move {
                if !local {
                    tokio::time::sleep(enhance::random_delay()).await;
                }
                let r = enhance::fetch(params, &server, &proxy, &req, tip).await;
                let _ = tx.send(Msg::Enhanced(req, r));
            });
        }
        Ok(())
    }

    /// The local node's compact blocks carry no transparent data (Zebra 7.0.0-rc.0 ignores
    /// poolTypes), so funds to the wallet's transparent addresses are looked up there by address.
    /// At a remote server those lookups would tie the addresses together; this node is ours.
    fn plan_receipts(&mut self, db: &Db, tip: u32, at_once: usize) -> Result<(), SyncError> {
        if self.t_pause.is_some_and(|t| Instant::now() < t) {
            return Ok(());
        }
        let from = self.progress.birthday.max(1);
        for account in db.get_account_ids().map_err(db_err)? {
            for address in db.get_transparent_receivers(account, true, true).map_err(db_err)?.into_keys() {
                if self.t_in_flight.len() >= at_once {
                    return Ok(());
                }
                let checked = self.t_checked.get(&address).copied().unwrap_or(from - 1);
                if checked >= tip || !self.t_in_flight.insert(address) {
                    continue;
                }
                let (server, proxy, tx, params) = (self.cfg.servers[0].clone(), self.cfg.proxy.clone(), self.tx.clone(), self.params);
                self.rt.spawn(async move {
                    let r = enhance::received(params, &server, &proxy, address.encode(&params), checked + 1, tip, tip).await;
                    let _ = tx.send(Msg::Received(address, tip, r));
                });
            }
        }
        Ok(())
    }

    fn apply_enhancement(&mut self, db: &mut Db, req: TransactionDataRequest, fetched: Fetched) -> Result<(), SyncError> {
        match fetched {
            Fetched::Status(txid, status) => db.set_transaction_status(txid, status).map_err(db_err)?,
            Fetched::Txs(txs) => {
                for (tx, height) in &txs {
                    decrypt_and_store_transaction(&self.params, db, tx, *height).map_err(db_err)?;
                }
                if let TransactionDataRequest::TransactionsInvolvingAddress(r) = req {
                    let as_of = r.block_range_end().map(|e| e - 1).unwrap_or(BlockHeight::from(self.tip.unwrap_or(0)));
                    db.notify_address_checked(r, as_of).map_err(db_err)?;
                }
            }
        }
        Ok(())
    }

    /// The chain state just below `start`, from the grid tree state and cached blocks.
    fn state_before(&self, start: u32) -> Result<Option<ChainState>, SyncError> {
        if let Some(s) = &self.last_end {
            if u32::from(s.block_height()) + 1 == start {
                return Ok(Some(s.clone()));
            }
        }
        let g = chunk_first(grid_floor(start));
        let Some(ts) = self.cache.tree_state(g - 1)? else { return Ok(None) };
        let base = ts.to_chain_state().map_err(|e| SyncError::Misbehaving { server: "cache".into(), detail: e.to_string() })?;
        let mut blocks = Vec::with_capacity((start - g) as usize);
        for h in g..start {
            match self.cache.block(h)? {
                Some(b) => blocks.push(b),
                None => return Ok(None),
            }
        }
        frontier::advance(&base, &blocks)
            .map(Some)
            .map_err(|e: FrontierError| SyncError::Misbehaving { server: "cache".into(), detail: e.to_string() })
    }

    /// One scan call over cached blocks, in the wallet's priority order. Returns
    /// whether anything was scanned.
    fn scan_once(&mut self, db: &mut Db, tip: u32) -> Result<bool, SyncError> {
        for r in db.suggest_scan_ranges().map_err(db_err)? {
            if matches!(r.priority(), ScanPriority::Scanned | ScanPriority::Ignored) {
                continue;
            }
            let (rs, re) = (u32::from(r.block_range().start), u32::from(r.block_range().end).min(tip + 1));
            // Highest cached window first: downloads arrive from the tip down.
            let mut c = grid_floor(re.saturating_sub(1));
            loop {
                let ws = rs.max(c);
                if ws < re && self.cache.chunk_end(c)?.is_some() {
                    let avail = self.cache.contiguous_end(ws)?.min(re).min(c + GRID);
                    if avail > ws {
                        if let Some(state) = self.state_before(ws)? {
                            self.ensure_roots(db, &state)?;
                            self.scan_window(db, ws, avail, state)?;
                            self.prune_chunk(db, c, tip)?;
                            return Ok(true);
                        }
                    }
                }
                if c <= rs || c < GRID {
                    break;
                }
                c -= GRID;
            }
        }
        Ok(false)
    }

    /// Drops a chunk's blocks once nothing in it is left to scan, outside the head.
    fn prune_chunk(&mut self, db: &Db, start: u32, tip: u32) -> Result<(), SyncError> {
        let end = start + GRID;
        if end + HEAD > tip {
            return Ok(());
        }
        let open = db.suggest_scan_ranges().map_err(db_err)?.into_iter().any(|r| {
            !matches!(r.priority(), ScanPriority::Scanned | ScanPriority::Ignored)
                && u32::from(r.block_range().start) < end
                && u32::from(r.block_range().end) > start
        });
        if !open {
            self.cache.forget_chunk(start)?;
        }
        Ok(())
    }

    fn scan_window(&mut self, db: &mut Db, start: u32, end: u32, state: ChainState) -> Result<(), SyncError> {
        // Bound the call by work, not by block count.
        let mut blocks = Vec::new();
        let mut outputs = 0usize;
        for h in start..end {
            let Some(b) = self.cache.block(h)? else { break };
            outputs += b.vtx.iter().map(|t| t.outputs.len() + t.actions.len() + t.ironwood_actions.len()).sum::<usize>();
            blocks.push(b);
            if outputs >= self.cfg.work_cap {
                break;
            }
        }
        let limit = blocks.len();
        match scan_cached_blocks(&self.params, self.cache.as_ref(), db, BlockHeight::from(start), &state, limit) {
            Ok(_) => {
                self.last_end = frontier::advance(&state, &blocks).ok();
                self.succeed(ErrorSource::Scan);
                self.progress.blocks_scanned += limit as u64;
                self.progress.outputs_scanned += outputs as u64;
                Ok(())
            }
            Err(ChainError::Scan(e)) if e.is_continuity_error() => {
                let at = u32::from(e.at_height());
                tracing::warn!(target: "zcash", "chain changed at {at}; rewinding");
                let target = BlockHeight::from(at.saturating_sub(10));
                let rewound = db.truncate_to_height(target).map_err(db_err)?;
                self.cache.truncate_from(u32::from(rewound) + 1)?;
                self.last_end = None;
                self.fail(ErrorSource::Scan, format!("chain changed at {at}; rewound to {}", u32::from(rewound)));
                Ok(())
            }
            Err(e) => Err(SyncError::Db(e.to_string())),
        }
    }
}

/// Per pool, the shard the next note commitment after `state` falls in.
fn first_shards(state: &ChainState) -> [u64; 3] {
    [
        state.final_sapling_tree().tree_size() >> SAPLING_SHARD_HEIGHT,
        state.final_orchard_tree().tree_size() >> ORCHARD_SHARD_HEIGHT,
        state.final_ironwood_tree().tree_size() >> IRONWOOD_SHARD_HEIGHT,
    ]
}

/// Whether a window starting in shards `need` lacks roots: some pool needs a shard past the
/// `have` roots stored, and past what was already asked for.
fn roots_missing(need: [u64; 3], have: [u64; 3], asked: [u64; 3]) -> bool {
    let past = |a: [u64; 3], b: [u64; 3]| a.iter().zip(b).any(|(x, y)| *x > y);
    past(need, have) && past(need, asked)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roots_are_loaded_only_past_a_gap() {
        // Shard 0 of Ironwood loaded, a window in shard 2: shard 1's root is missing.
        assert!(roots_missing([3, 2, 2], [3, 2, 1], [0; 3]));
        // Adjacent to the roots, or inside them: no gap.
        assert!(!roots_missing([3, 2, 1], [3, 2, 1], [0; 3]));
        assert!(!roots_missing([0, 0, 0], [3, 2, 1], [0; 3]));
        // Already asked for that need; only a later shard asks again.
        assert!(!roots_missing([3, 2, 2], [3, 2, 1], [3, 2, 2]));
        assert!(roots_missing([3, 2, 3], [3, 2, 1], [3, 2, 2]));
    }

    #[test]
    fn the_first_shard_of_an_empty_tree_is_zero() {
        let state = ChainState::empty(BlockHeight::from(1_000_000), zcash_primitives::block::BlockHash([0; 32]));
        assert_eq!(first_shards(&state), [0, 0, 0]);
    }

    #[test]
    fn an_error_stands_until_its_source_succeeds() {
        let mut e = Errors::default();
        e.set(ErrorSource::Tip, "tip poll failed".into());
        e.set(ErrorSource::Chunk(1000), "chunk 1000 failed".into());
        assert_eq!(e.latest().as_deref(), Some("chunk 1000 failed"));
        e.clear(ErrorSource::Chunk(2000));
        assert_eq!(e.latest().as_deref(), Some("chunk 1000 failed"), "another chunk's success");
        e.clear(ErrorSource::Chunk(1000));
        assert_eq!(e.latest().as_deref(), Some("tip poll failed"));
        e.set(ErrorSource::Tip, "tip poll failed again".into());
        assert_eq!(e.get(ErrorSource::Tip), Some("tip poll failed again"));
        e.clear(ErrorSource::Tip);
        assert_eq!(e.latest(), None);
    }

    #[test]
    fn synced_clears_only_moot_errors() {
        let mut e = Errors::default();
        e.set(ErrorSource::Migration, "migration: no server".into());
        e.set(ErrorSource::Chunk(0), "chunk".into());
        e.set(ErrorSource::Tail(0), "tail".into());
        e.set(ErrorSource::Scan, "chain changed".into());
        e.clear_moot();
        assert_eq!(e.latest().as_deref(), Some("migration: no server"));
    }

    #[test]
    fn retries_back_off_to_a_minute() {
        let within = |d: Duration, lo: u64| d >= Duration::from_secs(lo) && d <= Duration::from_secs(lo) + Duration::from_secs(lo) / 4;
        assert!(within(retry_delay(1), 1));
        assert!(within(retry_delay(3), 4));
        assert!(within(retry_delay(40), 60));
    }
}
