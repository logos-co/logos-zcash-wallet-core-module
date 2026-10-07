//! Moves a chain state forward through cached blocks, so a scan can start at any
//! height while tree states are only ever fetched at grid heights.

use zcash_client_backend::data_api::chain::ChainState;
use zcash_client_backend::proto::compact_formats::CompactBlock;
use zcash_primitives::block::BlockHash;
use zcash_protocol::consensus::BlockHeight;

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum FrontierError {
    #[error("block {0} does not follow the chain state")]
    Discontinuity(u32),
    #[error("block {0} carries a commitment that does not decode")]
    BadCommitment(u32),
    #[error("block {height}: {pool} tree size {computed} differs from the block's {reported}")]
    SizeMismatch { height: u32, pool: &'static str, computed: u64, reported: u64 },
    #[error("a note commitment tree is full")]
    TreeFull,
}

fn bytes32(v: &[u8], h: u32) -> Result<[u8; 32], FrontierError> {
    v.try_into().map_err(|_| FrontierError::BadCommitment(h))
}

/// Appends every note commitment in `blocks` (consecutive, starting right after
/// `state`) and returns the state after the last block. Each block's reported
/// tree sizes must match the computed ones.
pub fn advance(state: &ChainState, blocks: &[CompactBlock]) -> Result<ChainState, FrontierError> {
    let mut sapling = state.final_sapling_tree().clone();
    let mut orchard = state.final_orchard_tree().clone();
    let mut ironwood = state.final_ironwood_tree().clone();
    let mut height = u32::from(state.block_height());
    let mut hash = state.block_hash();

    for b in blocks {
        let h = b.height as u32;
        if h != height + 1 || b.prev_hash.as_slice() != hash.0.as_slice() {
            return Err(FrontierError::Discontinuity(h));
        }
        for tx in &b.vtx {
            for out in &tx.outputs {
                let cmu = Option::from(sapling::note::ExtractedNoteCommitment::from_bytes(&bytes32(&out.cmu, h)?))
                    .ok_or(FrontierError::BadCommitment(h))?;
                if !sapling.append(sapling::Node::from_cmu(&cmu)) {
                    return Err(FrontierError::TreeFull);
                }
            }
            for act in &tx.actions {
                let cmx = Option::from(orchard::note::ExtractedNoteCommitment::from_bytes(&bytes32(&act.cmx, h)?))
                    .ok_or(FrontierError::BadCommitment(h))?;
                if !orchard.append(orchard::tree::MerkleHashOrchard::from_cmx(&cmx)) {
                    return Err(FrontierError::TreeFull);
                }
            }
            for act in &tx.ironwood_actions {
                let cmx = Option::from(orchard::note::ExtractedNoteCommitment::from_bytes(&bytes32(&act.cmx, h)?))
                    .ok_or(FrontierError::BadCommitment(h))?;
                if !ironwood.append(orchard::tree::MerkleHashOrchard::from_cmx(&cmx)) {
                    return Err(FrontierError::TreeFull);
                }
            }
        }
        if let Some(meta) = &b.chain_metadata {
            for (pool, computed, reported) in [
                ("Sapling", sapling.tree_size(), meta.sapling_commitment_tree_size as u64),
                ("Orchard", orchard.tree_size(), meta.orchard_commitment_tree_size as u64),
                ("Ironwood", ironwood.tree_size(), meta.ironwood_commitment_tree_size as u64),
            ] {
                if computed != reported {
                    return Err(FrontierError::SizeMismatch { height: h, pool, computed, reported });
                }
            }
        }
        height = h;
        hash = BlockHash::try_from_slice(&b.hash).ok_or(FrontierError::Discontinuity(h))?;
    }
    Ok(ChainState::new(BlockHeight::from(height), hash, sapling, orchard, ironwood))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::net::{client::connect, socks::{Isolation, ProxyAddr}};
    use futures_util::TryStreamExt;
    use zcash_client_backend::proto::service::{BlockId, BlockRange, PoolType};

    /// Fetches the tree state at a grid height, advances it through real testnet
    /// blocks, and compares the result with the server's tree state.
    #[tokio::test]
    #[ignore]
    async fn advance_matches_server_tree_state() {
        let proxy = ProxyAddr::parse(&std::env::var("ZCASH_TEST_TOR").unwrap()).unwrap();
        let mut c = connect("https://testnet.zec.rocks:443", &proxy, Isolation::fresh()).await.unwrap();
        let (from, to) = (4_470_999u64, 4_471_350u64);
        let start = c.get_tree_state(BlockId { height: from, hash: vec![] }).await.unwrap().into_inner();
        let end = c.get_tree_state(BlockId { height: to, hash: vec![] }).await.unwrap().into_inner();
        let pools = [PoolType::Transparent, PoolType::Sapling, PoolType::Orchard, PoolType::Ironwood];
        let range = BlockRange {
            start: Some(BlockId { height: from + 1, hash: vec![] }),
            end: Some(BlockId { height: to, hash: vec![] }),
            pool_types: pools.iter().map(|p| *p as i32).collect(),
        };
        let blocks: Vec<_> = c.get_block_range(range).await.unwrap().into_inner().try_collect().await.unwrap();
        let actions: usize = blocks.iter().flat_map(|b| &b.vtx).map(|t| t.actions.len() + t.ironwood_actions.len() + t.outputs.len()).sum();
        let advanced = advance(&start.to_chain_state().unwrap(), &blocks).unwrap();
        let expected = end.to_chain_state().unwrap();
        println!("{} blocks, {} shielded outputs", blocks.len(), actions);
        assert_eq!(advanced.block_hash(), expected.block_hash());
        assert_eq!(advanced.final_sapling_tree(), expected.final_sapling_tree());
        assert_eq!(advanced.final_orchard_tree(), expected.final_orchard_tree());
        assert_eq!(advanced.final_ironwood_tree(), expected.final_ironwood_tree());
    }
}
