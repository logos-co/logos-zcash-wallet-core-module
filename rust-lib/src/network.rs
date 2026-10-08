//! Networks the wallet runs on, as consensus parameters.

use zcash_protocol::consensus::{self, BlockHeight, NetworkType, NetworkUpgrade, Parameters};

/// NU7's mainnet height, set here the day ZIP 259 fixes it (due 2026-10-20) if the
/// final crates are late. Branch IDs follow from it.
pub const MAINNET_NU7_OVERRIDE: Option<u32> = None;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ZNetwork {
    Main,
    Test,
}

impl ZNetwork {
    pub fn name(self) -> &'static str {
        match self {
            ZNetwork::Main => "mainnet",
            ZNetwork::Test => "testnet",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "mainnet" | "main" => Some(ZNetwork::Main),
            "testnet" | "test" => Some(ZNetwork::Test),
            _ => None,
        }
    }

    /// The chain name lightwalletd reports in GetLightdInfo.
    pub fn lightd_chain_name(self) -> &'static str {
        match self {
            ZNetwork::Main => "main",
            ZNetwork::Test => "test",
        }
    }

    fn inner(self) -> consensus::Network {
        match self {
            ZNetwork::Main => consensus::Network::MainNetwork,
            ZNetwork::Test => consensus::Network::TestNetwork,
        }
    }

    /// The consensus branch ID at `height`, as lightwalletd prints it.
    pub fn branch_id_hex(self, height: BlockHeight) -> String {
        format!("{:08x}", u32::from(consensus::BranchId::for_height(&self, height)))
    }
}

impl Parameters for ZNetwork {
    fn network_type(&self) -> NetworkType {
        self.inner().network_type()
    }

    fn activation_height(&self, nu: NetworkUpgrade) -> Option<BlockHeight> {
        match (self, nu, MAINNET_NU7_OVERRIDE) {
            (ZNetwork::Main, NetworkUpgrade::Nu7, Some(h)) => Some(BlockHeight::from(h)),
            _ => self.inner().activation_height(nu),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mainnet_nu7_is_unset_until_zip_259_fixes_it() {
        // The pre-release crates carry no mainnet height; sends near it are handled by
        // expiry_for, and the release gate checks this before any mainnet release.
        assert_eq!(MAINNET_NU7_OVERRIDE, None);
        assert_eq!(ZNetwork::Main.activation_height(NetworkUpgrade::Nu7), None);
    }

    #[test]
    fn testnet_branch_after_nu7() {
        // Testnet NU7 activated at 4,465,026 (ZIP 259); the server reported 77190ad9 above it.
        assert_eq!(ZNetwork::Test.branch_id_hex(BlockHeight::from(4_476_424)), "77190ad9");
        assert_ne!(ZNetwork::Test.branch_id_hex(BlockHeight::from(4_465_025)), "77190ad9");
    }
}
