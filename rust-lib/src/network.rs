//! Networks the wallet runs on, as consensus parameters.

use std::sync::OnceLock;

use serde::Deserialize;
use zcash_protocol::consensus::{self, BlockHeight, NetworkType, NetworkUpgrade, Parameters};
use zcash_protocol::local_consensus::LocalNetwork;

/// NU7's mainnet height, set here the day ZIP 259 fixes it (due 2026-10-20) if the
/// final crates are late. Branch IDs follow from it.
pub const MAINNET_NU7_OVERRIDE: Option<u32> = None;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ZNetwork {
    Main,
    Test,
    /// A local test chain; its upgrade heights come from `regtest.json`. Test harnesses only.
    Regtest,
}

/// Upgrade heights for regtest, as zebrad's `[network.testnet_parameters.activation_heights]`.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct RegtestHeights {
    pub overwinter: Option<u32>,
    pub sapling: Option<u32>,
    pub blossom: Option<u32>,
    pub heartwood: Option<u32>,
    pub canopy: Option<u32>,
    pub nu5: Option<u32>,
    pub nu6: Option<u32>,
    pub nu6_1: Option<u32>,
    pub nu6_2: Option<u32>,
    pub nu6_3: Option<u32>,
    pub nu7: Option<u32>,
}

static REGTEST: OnceLock<LocalNetwork> = OnceLock::new();

/// Sets the regtest upgrade heights once per process; later calls are ignored.
pub fn configure_regtest(h: &RegtestHeights) {
    let b = |v: Option<u32>| v.map(BlockHeight::from);
    let _ = REGTEST.set(LocalNetwork {
        overwinter: b(h.overwinter),
        sapling: b(h.sapling),
        blossom: b(h.blossom),
        heartwood: b(h.heartwood),
        canopy: b(h.canopy),
        nu5: b(h.nu5),
        nu6: b(h.nu6),
        nu6_1: b(h.nu6_1),
        nu6_2: b(h.nu6_2),
        nu6_3: b(h.nu6_3),
        nu7: b(h.nu7),
    });
}

pub fn regtest_configured() -> bool {
    REGTEST.get().is_some()
}

impl ZNetwork {
    pub fn name(self) -> &'static str {
        match self {
            ZNetwork::Main => "mainnet",
            ZNetwork::Test => "testnet",
            ZNetwork::Regtest => "regtest",
        }
    }

    /// Regtest only parses once its heights are configured.
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "mainnet" | "main" => Some(ZNetwork::Main),
            "testnet" | "test" => Some(ZNetwork::Test),
            "regtest" if regtest_configured() => Some(ZNetwork::Regtest),
            _ => None,
        }
    }

    /// The chain name lightwalletd reports in GetLightdInfo.
    pub fn lightd_chain_name(self) -> &'static str {
        match self {
            ZNetwork::Main => "main",
            ZNetwork::Test => "test",
            ZNetwork::Regtest => "regtest",
        }
    }

    /// Whether a server's reported chain name fits. Zebra names regtest "test", zcashd "regtest".
    pub fn accepts_lightd_chain(self, name: &str) -> bool {
        name == self.lightd_chain_name() || (self == ZNetwork::Regtest && name == "test")
    }

    /// The consensus branch ID at `height`, as lightwalletd prints it.
    pub fn branch_id_hex(self, height: BlockHeight) -> String {
        format!("{:08x}", u32::from(consensus::BranchId::for_height(&self, height)))
    }
}

impl Parameters for ZNetwork {
    fn network_type(&self) -> NetworkType {
        match self {
            ZNetwork::Main => NetworkType::Main,
            ZNetwork::Test => NetworkType::Test,
            ZNetwork::Regtest => NetworkType::Regtest,
        }
    }

    fn activation_height(&self, nu: NetworkUpgrade) -> Option<BlockHeight> {
        match (self, nu, MAINNET_NU7_OVERRIDE) {
            (ZNetwork::Main, NetworkUpgrade::Nu7, Some(h)) => Some(BlockHeight::from(h)),
            (ZNetwork::Main, _, _) => consensus::Network::MainNetwork.activation_height(nu),
            (ZNetwork::Test, _, _) => consensus::Network::TestNetwork.activation_height(nu),
            (ZNetwork::Regtest, _, _) => REGTEST.get().and_then(|l| l.activation_height(nu)),
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
