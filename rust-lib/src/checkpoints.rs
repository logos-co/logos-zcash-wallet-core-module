//! Restore checkpoints shipped in the package, so restoring never tells a server
//! the wallet's birthday. Each is the tree state just below a grid height.

use serde::{Deserialize, Serialize};
use zcash_client_backend::data_api::chain::ChainState;
use zcash_client_backend::proto::service::TreeState;
use zcash_primitives::block::BlockHash;
use zcash_protocol::consensus::{NetworkUpgrade, Parameters};

use crate::network::ZNetwork;

/// Checkpoints sit every SPACING blocks; a restore scans at most this many extra.
pub const SPACING: u32 = 10_000;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Checkpoint {
    /// The first block a wallet restored from here may hold funds in.
    pub start: u32,
    pub hash: String,
    pub time: u32,
    pub sapling: String,
    pub orchard: String,
    pub ironwood: String,
}

impl Checkpoint {
    pub fn from_tree_state(ts: &TreeState) -> Result<Self, String> {
        ts.to_chain_state().map_err(|e| e.to_string())?;
        Ok(Self {
            start: ts.height as u32 + 1,
            hash: ts.hash.clone(),
            time: ts.time,
            sapling: ts.sapling_tree.clone(),
            orchard: ts.orchard_tree.clone(),
            ironwood: ts.ironwood_tree.clone(),
        })
    }

    pub fn tree_state(&self, network: ZNetwork) -> TreeState {
        TreeState {
            network: network.lightd_chain_name().into(),
            height: (self.start - 1) as u64,
            hash: self.hash.clone(),
            time: self.time,
            sapling_tree: self.sapling.clone(),
            orchard_tree: self.orchard.clone(),
            ironwood_tree: self.ironwood.clone(),
        }
    }
}

const MAINNET: &str = include_str!("../checkpoints/mainnet.json");
const TESTNET: &str = include_str!("../checkpoints/testnet.json");

pub fn all(network: ZNetwork) -> Vec<Checkpoint> {
    let raw = match network {
        ZNetwork::Main => MAINNET,
        ZNetwork::Test => TESTNET,
        ZNetwork::Regtest => "[]",
    };
    serde_json::from_str(raw).unwrap_or_default()
}

/// The chain state to restore from for a wallet first used at `birthday`: the last
/// checkpoint at or below it, or Sapling activation with empty trees.
pub fn restore_point(network: ZNetwork, birthday: u32) -> Result<ChainState, String> {
    let best = all(network).into_iter().filter(|c| c.start <= birthday).max_by_key(|c| c.start);
    match best {
        Some(c) => c.tree_state(network).to_chain_state().map_err(|e| e.to_string()),
        None => {
            let sapling = network.activation_height(NetworkUpgrade::Sapling).ok_or("no Sapling activation")?;
            Ok(ChainState::empty(sapling - 1, BlockHash([0; 32])))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bundled_checkpoints_parse_and_are_on_the_grid() {
        for net in [ZNetwork::Main, ZNetwork::Test] {
            let all = all(net);
            assert!(!all.is_empty(), "{} has no checkpoints", net.name());
            for c in &all {
                assert_eq!(c.start % SPACING, 0, "checkpoint {} is off the grid", c.start);
                c.tree_state(net).to_chain_state().unwrap();
            }
            let last = all.last().unwrap().start;
            assert_eq!(u32::from(restore_point(net, last + 5).unwrap().block_height()) + 1, last);
        }
        let early = restore_point(ZNetwork::Test, 1).unwrap();
        assert!(early.final_sapling_tree().tree_size() == 0);
    }
}
