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
    let mut c = connect(server, proxy, Isolation::fresh()).await?;
    let below = c
        .get_tree_state(BlockId { height: (start - 1) as u64, hash: vec![] })
        .await
        .map_err(status(server))?
        .into_inner();
    let range = BlockRange {
        start: Some(BlockId { height: start as u64, hash: vec![] }),
        end: Some(BlockId { height: last as u64, hash: vec![] }),
        pool_types: ALL_POOLS.iter().map(|p| *p as i32).collect(),
    };
    let blocks: Vec<CompactBlock> =
        c.get_block_range(range).await.map_err(status(server))?.into_inner().try_collect().await.map_err(status(server))?;
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
