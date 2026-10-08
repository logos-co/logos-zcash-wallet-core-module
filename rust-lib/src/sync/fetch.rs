//! Downloads on the grid. Each chunk travels on its own Tor circuit.

use futures_util::TryStreamExt;
use zcash_client_backend::proto::compact_formats::CompactBlock;
use zcash_client_backend::proto::service::{BlockId, BlockRange, ChainSpec, Empty, LightdInfo, PoolType, TreeState};

use crate::net::client::{connect, NetError};
use crate::net::socks::{Isolation, ProxyAddr};

pub const ALL_POOLS: [PoolType; 4] = [PoolType::Transparent, PoolType::Sapling, PoolType::Orchard, PoolType::Ironwood];

fn status(server: &str) -> impl Fn(tonic::Status) -> NetError + '_ {
    move |status| NetError::Status { server: server.into(), status }
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
    let range = BlockRange {
        start: Some(BlockId { height: first as u64, hash: vec![] }),
        end: Some(BlockId { height: last as u64, hash: vec![] }),
        pool_types: ALL_POOLS.iter().map(|p| *p as i32).collect(),
    };
    let blocks: Vec<CompactBlock> =
        c.get_block_range(range).await.map_err(status(server))?.into_inner().try_collect().await.map_err(status(server))?;
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
    let range = BlockRange {
        start: Some(BlockId { height: first as u64, hash: vec![] }),
        end: Some(BlockId { height: last as u64, hash: vec![] }),
        pool_types: ALL_POOLS.iter().map(|p| *p as i32).collect(),
    };
    c.get_block_range(range).await.map_err(status(server))?.into_inner().try_collect().await.map_err(status(server))
}

pub async fn tip_and_info(server: &str, proxy: &ProxyAddr, iso: Isolation) -> Result<(u32, LightdInfo), NetError> {
    let mut c = connect(server, proxy, iso).await?;
    let info = c.get_lightd_info(Empty {}).await.map_err(status(server))?.into_inner();
    let tip = c.get_latest_block(ChainSpec {}).await.map_err(status(server))?.into_inner().height as u32;
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
