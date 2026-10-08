//! Makes a recovery phrase and prints its regtest addresses, offline, so zebrad can
//! mine to a wallet before any server exists.
//!
//! usage: cargo run --no-default-features --example regtest_keys -- <regtest.json> [phrase file]

use zcash_keys::encoding::AddressCodec;
use zcash_keys::keys::{UnifiedAddressRequest, UnifiedSpendingKey};
use zcash_wallet_core::keys::Phrase;
use zcash_wallet_core::network::{configure_regtest, RegtestHeights, ZNetwork};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let heights: RegtestHeights = serde_json::from_str(&std::fs::read_to_string(&args[1]).unwrap()).unwrap();
    configure_regtest(&heights);
    let net = ZNetwork::Regtest;
    let phrase = match args.get(2) {
        Some(f) => Phrase::parse(&std::fs::read_to_string(f).unwrap()).unwrap(),
        None => Phrase::generate(),
    };
    let usk = UnifiedSpendingKey::from_seed(&net, secrecy::ExposeSecret::expose_secret(&phrase.seed()), zip32::AccountId::ZERO).unwrap();
    let ufvk = usk.to_unified_full_viewing_key();
    let (orchard_ua, _) = ufvk
        .default_address(UnifiedAddressRequest::unsafe_custom(
            zcash_keys::keys::ReceiverRequirement::Require,
            zcash_keys::keys::ReceiverRequirement::Omit,
            zcash_keys::keys::ReceiverRequirement::Omit,
        ))
        .unwrap();
    use zcash_transparent::keys::{IncomingViewingKey, NonHardenedChildIndex};
    let taddr = ufvk.transparent().unwrap().derive_external_ivk().unwrap().derive_address(NonHardenedChildIndex::ZERO).unwrap();
    println!("{}", serde_json::json!({
        "phrase": phrase.as_str(),
        "orchardUa": orchard_ua.encode(&net),
        "transparent": taddr.encode(&net),
    }));
}
