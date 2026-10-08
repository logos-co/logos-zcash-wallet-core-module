//! Reading from the local node, a server checks it: at the lower of their two tips, both must
//! have the same block. A node fed a false chain fails this; one that is only behind does not.

use serde::Serialize;
use zcash_client_backend::proto::service::{BlockId, ChainSpec};
use zcash_protocol::consensus::{NetworkUpgrade, Parameters};

use super::fetch::status;
use crate::net::client::{connect, NetError};
use crate::net::ipc::LOCAL_NODE_URL;
use crate::net::socks::{Isolation, ProxyAddr};
use crate::network::ZNetwork;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Verdict {
    Agrees,
    /// The same chain, with the node further behind: still syncing, or short of peers.
    NodeBehind,
    /// The same chain, with the server further behind.
    ServerBehind,
    /// Different blocks at the same height: one of the two is on a false chain.
    Differs,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CrossCheck {
    pub verdict: Verdict,
    pub server: String,
    pub node_tip: u32,
    pub server_tip: u32,
    /// Unix seconds.
    pub checked_at: u64,
}

/// How far apart the tips may be: 2 blocks, or 6 at or above NU7, as between operators.
pub fn tolerance(net: ZNetwork, lower: u32) -> u32 {
    match net.activation_height(NetworkUpgrade::Nu7) {
        Some(nu7) if lower >= u32::from(nu7) => 6,
        _ => 2,
    }
}

pub fn judge(net: ZNetwork, node_tip: u32, server_tip: u32, same_block: bool) -> Verdict {
    let slack = tolerance(net, node_tip.min(server_tip));
    if !same_block {
        Verdict::Differs
    } else if node_tip + slack < server_tip {
        Verdict::NodeBehind
    } else if server_tip + slack < node_tip {
        Verdict::ServerBehind
    } else {
        Verdict::Agrees
    }
}

/// Both tips, then both tree states at the lower one, whose block hashes must match.
pub async fn check(net: ZNetwork, server: &str, proxy: &ProxyAddr) -> Result<CrossCheck, NetError> {
    let mut node = connect(LOCAL_NODE_URL, proxy, Isolation::fresh()).await?;
    let mut remote = connect(server, proxy, Isolation::fresh()).await?;
    let node_tip = node.get_latest_block(ChainSpec {}).await.map_err(status(LOCAL_NODE_URL))?.into_inner().height as u32;
    let server_tip = remote.get_latest_block(ChainSpec {}).await.map_err(status(server))?.into_inner().height as u32;
    let lower = node_tip.min(server_tip);
    // lightwalletd serves no tree state below Sapling, where the wallet has nothing to read.
    let sapling = net.activation_height(NetworkUpgrade::Sapling).map_or(1, u32::from).max(1);
    let same_block = lower < sapling || {
        let at = BlockId { height: lower as u64, hash: vec![] };
        let a = node.get_tree_state(at.clone()).await.map_err(status(LOCAL_NODE_URL))?.into_inner().hash;
        let b = remote.get_tree_state(at).await.map_err(status(server))?.into_inner().hash;
        !a.is_empty() && a.eq_ignore_ascii_case(&b)
    };
    let checked_at = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0, |d| d.as_secs());
    Ok(CrossCheck { verdict: judge(net, node_tip, server_tip, same_block), server: server.into(), node_tip, server_tip, checked_at })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_node_is_judged_by_the_block_at_the_lower_tip() {
        let net = ZNetwork::Main;
        assert_eq!(judge(net, 3_000_000, 3_000_000, true), Verdict::Agrees);
        assert_eq!(judge(net, 2_999_998, 3_000_000, true), Verdict::Agrees, "within the tolerance");
        assert_eq!(judge(net, 2_999_997, 3_000_000, true), Verdict::NodeBehind);
        assert_eq!(judge(net, 1_000, 3_000_000, true), Verdict::NodeBehind, "still syncing");
        assert_eq!(judge(net, 3_000_003, 3_000_000, true), Verdict::ServerBehind);
        // A different block wins over any distance: the node may be behind on a fork.
        assert_eq!(judge(net, 3_000_000, 3_000_000, false), Verdict::Differs);
        assert_eq!(judge(net, 2_999_000, 3_000_000, false), Verdict::Differs);
    }

    #[test]
    fn the_tolerance_widens_at_nu7() {
        // Testnet NU7 activated at 4,465,026.
        assert_eq!(tolerance(ZNetwork::Test, 4_465_025), 2);
        assert_eq!(tolerance(ZNetwork::Test, 4_465_026), 6);
        assert_eq!(judge(ZNetwork::Test, 4_480_000, 4_480_006, true), Verdict::Agrees);
        assert_eq!(judge(ZNetwork::Test, 4_480_000, 4_480_007, true), Verdict::NodeBehind);
    }
}
