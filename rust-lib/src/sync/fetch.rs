//! Downloads on the grid. Each chunk travels on its own Tor circuit.

use std::time::Duration;

use futures_util::TryStreamExt;
use zcash_client_backend::proto::compact_formats::CompactBlock;
use zcash_client_backend::proto::service::{BlockId, BlockRange, ChainSpec, Empty, LightdInfo, PoolType, TreeState};

use crate::net::client::{connect, Client, NetError};
use crate::net::socks::{Isolation, ProxyAddr};

pub const ALL_POOLS: [PoolType; 4] = [PoolType::Transparent, PoolType::Sapling, PoolType::Orchard, PoolType::Ironwood];

pub(super) fn status(server: &str) -> impl Fn(tonic::Status) -> NetError + '_ {
    move |status| NetError::Status { server: server.into(), status }
}

/// Each call of a tip poll gives up after this: polls are 30 s apart, and a stalled one would
/// hold up the next.
pub const TIP_CALL: Duration = Duration::from_secs(60);

/// `msg` as a call with a grpc-timeout: tonic's own channels enforce it, and the Tor channel
/// hands it to tor_module as the request's timeout.
fn within<T>(msg: T, budget: Duration) -> tonic::Request<T> {
    let mut req = tonic::Request::new(msg);
    req.set_timeout(budget);
    req
}

/// Blocks per GetBlockRange call. Late 2022's spam averaged up to ~100 KB a block, and Tor gives
/// a circuit ~25 KB/s, so a whole 1,000-block chunk in one call outran any budget.
pub const BLOCKS_PER_CALL: u32 = 50;
/// A call that runs out of time is made again in halves, down to this many blocks.
const FEWEST_PER_CALL: u32 = 6;
/// Each call gives up after this, tor_module's longest request.
pub const BLOCKS_CALL: Duration = Duration::from_secs(600);
/// A range gives up after this many failed calls in a row.
pub const RETRIES: u32 = 3;

/// Blocks first..=last on one circuit. A call that times out or breaks off is made again on a
/// fresh one from `reconnect`, in halves if it ran out of time; the blocks already here stay.
pub(crate) async fn block_range<F, Fut>(c: &mut Client, mut reconnect: F, server: &str, first: u32, last: u32) -> Result<Vec<CompactBlock>, NetError>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<Client, NetError>>,
{
    let mut blocks = Vec::new();
    let (mut from, mut per_call, mut failures) = (first, BLOCKS_PER_CALL, 0);
    while from <= last {
        let to = last.min(from + (per_call - 1));
        let range = BlockRange {
            start: Some(BlockId { height: from as u64, hash: vec![] }),
            end: Some(BlockId { height: to as u64, hash: vec![] }),
            pool_types: ALL_POOLS.iter().map(|p| *p as i32).collect(),
        };
        let part = match c.get_block_range(within(range, BLOCKS_CALL)).await {
            Ok(stream) => stream.into_inner().try_collect::<Vec<CompactBlock>>().await,
            Err(s) => Err(s),
        };
        match part {
            Ok(part) => {
                blocks.extend(part);
                from = to + 1;
                failures = 0;
            }
            Err(s) if transient(&s) && failures < RETRIES => {
                failures += 1;
                if out_of_time(&s) && per_call > FEWEST_PER_CALL {
                    per_call /= 2;
                }
                tracing::warn!(target: "zcash", "blocks {from}..={to}: {}; again on a fresh circuit", s.message());
                *c = reconnect().await?;
            }
            Err(s) => return Err(status(server)(s)),
        }
    }
    Ok(blocks)
}

/// tonic's own timeout, the server's deadline, or tor_module's.
fn out_of_time(s: &tonic::Status) -> bool {
    matches!(s.code(), tonic::Code::Cancelled | tonic::Code::DeadlineExceeded)
        || (s.code() == tonic::Code::Unavailable && s.message().starts_with("no answer within"))
}

/// A call lost on the way, not an answer from the server.
fn transient(s: &tonic::Status) -> bool {
    out_of_time(s) || s.code() == tonic::Code::Unavailable
}

/// A grid chunk: the tree state just below `start`, and blocks start..=last.
pub struct Chunk {
    pub start: u32,
    pub last: u32,
    pub server: String,
    pub below: TreeState,
    pub blocks: Vec<CompactBlock>,
}

pub async fn fetch_chunk(server: &str, proxy: &ProxyAddr, start: u32, last: u32) -> Result<Chunk, NetError> {
    let first = super::cache::chunk_first(start);
    let mut c = connect(server, proxy, Isolation::fresh()).await?;
    let blocks = block_range(&mut c, || connect(server, proxy, Isolation::fresh()), server, first, last).await?;
    // Genesis holds no notes and lightwalletd cannot serve its tree state, so below block 1
    // is the empty tree under the hash block 1 points to. The blocks are checked against it.
    let below = if first == 1 {
        let mut hash = blocks.first().map(|b| b.prev_hash.clone()).unwrap_or_default();
        hash.reverse();
        TreeState { height: 0, hash: hex::encode(hash), ..Default::default() }
    } else {
        c.get_tree_state(BlockId { height: (first - 1) as u64, hash: vec![] }).await.map_err(status(server))?.into_inner()
    };
    Ok(Chunk { start, last, server: server.into(), below, blocks })
}

/// Blocks first..=last on a fresh circuit, for the growing tip chunk.
pub async fn fetch_blocks(server: &str, proxy: &ProxyAddr, first: u32, last: u32) -> Result<Vec<CompactBlock>, NetError> {
    let mut c = connect(server, proxy, Isolation::fresh()).await?;
    block_range(&mut c, || connect(server, proxy, Isolation::fresh()), server, first, last).await
}

pub async fn tip_and_info(server: &str, proxy: &ProxyAddr, iso: Isolation) -> Result<(u32, LightdInfo), NetError> {
    let mut c = connect(server, proxy, iso).await?;
    let info = c.get_lightd_info(within(Empty {}, TIP_CALL)).await.map_err(status(server))?.into_inner();
    let tip = c.get_latest_block(within(ChainSpec {}, TIP_CALL)).await.map_err(status(server))?.into_inner().height as u32;
    Ok((tip, info))
}

pub async fn tree_state(server: &str, proxy: &ProxyAddr, height: u32) -> Result<TreeState, NetError> {
    let mut c = connect(server, proxy, Isolation::fresh()).await?;
    Ok(c.get_tree_state(BlockId { height: height as u64, hash: vec![] }).await.map_err(status(server))?.into_inner())
}

/// Broadcasts on a fresh circuit. Returns lightwalletd's error code and message;
/// code 0 means the node accepted it.
pub async fn send_transaction(server: &str, proxy: &ProxyAddr, raw: Vec<u8>) -> Result<(i32, String), NetError> {
    let mut c = connect(server, proxy, Isolation::fresh()).await?;
    let r = c
        .send_transaction(zcash_client_backend::proto::service::RawTransaction { data: raw, height: 0 })
        .await
        .map_err(status(server))?
        .into_inner();
    Ok((r.error_code, r.error_message))
}

/// Whether `server` knows the transaction, mined or in its mempool, asked on a fresh circuit.
pub async fn knows_transaction(server: &str, proxy: &ProxyAddr, txid: &[u8]) -> Result<bool, NetError> {
    let mut c = connect(server, proxy, Isolation::fresh()).await?;
    let filter = zcash_client_backend::proto::service::TxFilter { block: None, index: 0, hash: txid.to_vec() };
    match c.get_transaction(filter).await {
        Ok(_) => Ok(true),
        Err(s) if s.code() == tonic::Code::NotFound => Ok(false),
        Err(s) => Err(NetError::Status { server: server.into(), status: s }),
    }
}

/// After one operator accepted a transaction, the other must know it within two minutes,
/// or it is sent there too. Returns what happened, for the log.
pub async fn confirm_elsewhere(other: &str, proxy: &ProxyAddr, txid: Vec<u8>, raw: Vec<u8>) -> String {
    for _ in 0..6 {
        tokio::time::sleep(std::time::Duration::from_secs(20)).await;
        if let Ok(true) = knows_transaction(other, proxy, &txid).await {
            return format!("{other} saw the transaction");
        }
    }
    match send_transaction(other, proxy, raw).await {
        Ok((0, _)) => format!("{other} had not seen it; sent there too"),
        Ok((code, msg)) => format!("{other} refused it: {code} {msg}"),
        Err(e) => format!("{other} unreachable: {e}"),
    }
}
