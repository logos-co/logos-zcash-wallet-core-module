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
use zcash_client_backend::data_api::{TransactionDataRequest, WalletCommitmentTrees, WalletRead, WalletWrite};
use zcash_client_backend::proto::compact_formats::CompactBlock;
use zcash_client_backend::proto::service::{GetSubtreeRootsArg, LightdInfo, ShieldedProtocol};
use zcash_primitives::merkle_tree::HashSer;
use zcash_protocol::consensus::BlockHeight;

use super::cache::{chunk_first, grid_floor, BlockCache, GRID};
use super::enhance::{self, Fetched};
use super::fetch::{self, Chunk};
use super::frontier::{self, FrontierError};
use crate::net::client::{connect, NetError};
use crate::net::socks::{Isolation, ProxyAddr};
use crate::network::ZNetwork;
use crate::wallet::Db;

/// Blocks within this distance of the tip stay cached for reorgs.
pub const HEAD: u32 = 100;

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
    roots_loaded: bool,
    next_server: usize,
    /// The chain state after the last scan call, to continue mid-chunk cheaply.
    last_end: Option<ChainState>,
    enh_in_flight: HashSet<TransactionDataRequest>,
    last_enh: Option<Instant>,
    /// When scanning began this session, and how many blocks since, for the ETA.
    pace: Option<(Instant, u64)>,
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
            roots_loaded: false,
            next_server: 0,
            last_end: None,
            enh_in_flight: HashSet::new(),
            last_enh: None,
            pace: None,
            progress: Progress { state: "starting".into(), birthday, ..Default::default() },
        }
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
        if !self.roots_loaded {
            self.load_subtree_roots(db)?;
            self.roots_loaded = true;
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

    fn handle(&mut self, db: &mut Db, msg: Msg) -> Result<(), SyncError> {
        match msg {
            Msg::Tip(r) => {
                self.tip_in_flight = false;
                match r {
                    Ok((tip, info)) => {
                        self.check_server(&self.cfg.servers[0].clone(), &info, tip)?;
                        if self.tip.is_none_or(|t| t != tip) {
                            db.update_chain_tip(BlockHeight::from(tip)).map_err(db_err)?;
                            self.tip = Some(tip);
                        }
                        self.progress.tip = Some(tip);
                    }
                    Err(e) => {
                        tracing::warn!(target: "zcash", "tip poll: {e}");
                        self.progress.last_error = Some(e.to_string());
                    }
                }
            }
            Msg::Chunk(start, r) => {
                self.in_flight.remove(&start);
                match r {
                    Ok(chunk) => {
                        self.retry.remove(&start);
                        self.store_chunk(chunk)?
                    }
                    Err(e) => {
                        let failures = self.retry.get(&start).map_or(0, |r| r.0) + 1;
                        tracing::warn!(target: "zcash", "chunk {start}: {e} (failure {failures})");
                        self.retry.insert(start, (failures, Instant::now() + retry_delay(failures)));
                        self.progress.last_error = Some(e.to_string());
                    }
                }
            }
            Msg::Enhanced(req, r) => {
                self.enh_in_flight.remove(&req);
                match r {
                    Ok(fetched) => self.apply_enhancement(db, req, fetched)?,
                    Err(e) => self.progress.last_error = Some(e.to_string()),
                }
            }
            Msg::Tail(start, first, r) => {
                self.tails_in_flight.remove(&start);
                match r {
                    Ok(blocks) => {
                        self.retry.remove(&start);
                        self.progress.blocks_fetched += blocks.len() as u64;
                        self.cache.insert_blocks(&blocks)?;
                    }
                    Err(e) => {
                        let failures = self.retry.get(&start).map_or(0, |r| r.0) + 1;
                        tracing::warn!(target: "zcash", "blocks from {first}: {e} (failure {failures})");
                        self.retry.insert(start, (failures, Instant::now() + retry_delay(failures)));
                        self.progress.last_error = Some(e.to_string());
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

    fn load_subtree_roots(&mut self, db: &mut Db) -> Result<(), SyncError> {
        let (server, proxy) = (self.cfg.servers[0].clone(), self.cfg.proxy.clone());
        let fetch = |p: ShieldedProtocol| {
            let (server, proxy) = (server.clone(), proxy.clone());
            async move {
                let mut c = connect(&server, &proxy, Isolation::fresh()).await?;
                let arg = GetSubtreeRootsArg { start_index: 0, shielded_protocol: p as i32, max_entries: 0 };
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
        let sapling = parse::<sapling::Node>(self.rt.block_on(fetch(ShieldedProtocol::Sapling))?).map_err(bad)?;
        let orchard = parse::<orchard::tree::MerkleHashOrchard>(self.rt.block_on(fetch(ShieldedProtocol::Orchard))?).map_err(bad)?;
        let ironwood = parse::<orchard::tree::MerkleHashOrchard>(self.rt.block_on(fetch(ShieldedProtocol::Ironwood))?).map_err(bad)?;
        db.put_sapling_subtree_roots(0, &sapling).map_err(db_err)?;
        db.put_orchard_subtree_roots(0, &orchard).map_err(db_err)?;
        db.put_ironwood_subtree_roots(0, &ironwood).map_err(db_err)?;
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
    fn plan_enhancements(&mut self, db: &Db, tip: u32) -> Result<(), SyncError> {
        if self.last_enh.is_some_and(|t| t.elapsed() < Duration::from_secs(10)) {
            return Ok(());
        }
        self.last_enh = Some(Instant::now());
        let requests = db.transaction_data_requests().map_err(db_err)?;
        self.progress.details_pending = requests.len();
        let now = SystemTime::now();
        for req in requests {
            if self.enh_in_flight.len() >= 4 {
                break;
            }
            if self.enh_in_flight.contains(&req) || enhance::not_before(&req).is_some_and(|t| t > now) {
                continue;
            }
            self.enh_in_flight.insert(req.clone());
            let server = enhance::pick(&self.cfg.servers).to_string();
            let (proxy, tx, params) = (self.cfg.proxy.clone(), self.tx.clone(), self.params);
            self.rt.spawn(async move {
                tokio::time::sleep(enhance::random_delay()).await;
                let r = enhance::fetch(params, &server, &proxy, &req, tip).await;
                let _ = tx.send(Msg::Enhanced(req, r));
            });
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
                self.progress.last_error = Some(format!("chain changed at {at}; rewound to {}", u32::from(rewound)));
                Ok(())
            }
            Err(e) => Err(SyncError::Db(e.to_string())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retries_back_off_to_a_minute() {
        let within = |d: Duration, lo: u64| d >= Duration::from_secs(lo) && d <= Duration::from_secs(lo) + Duration::from_secs(lo) / 4;
        assert!(within(retry_delay(1), 1));
        assert!(within(retry_delay(3), 4));
        assert!(within(retry_delay(40), 60));
    }
}
