//! Live testnet sync over Tor. Run with ZCASH_TEST_TOR=socks5h://127.0.0.1:PORT and --ignored.

use std::sync::Arc;
use std::time::{Duration, Instant};

use zcash_client_backend::data_api::WalletRead;
use zcash_wallet_core::keys::Phrase;
use zcash_wallet_core::net::socks::ProxyAddr;
use zcash_wallet_core::network::ZNetwork;
use zcash_wallet_core::sync::cache::grid_floor;
use zcash_wallet_core::sync::driver::{Step, SyncConfig, Syncer};
use zcash_wallet_core::sync::fetch;
use zcash_wallet_core::wallet::WalletDir;

const SERVER: &str = "https://testnet.zec.rocks:443";

#[test]
#[ignore]
fn new_wallet_syncs_to_tip() {
    let proxy = ProxyAddr::parse(&std::env::var("ZCASH_TEST_TOR").unwrap()).unwrap();
    let chunks: u32 = std::env::var("ZCASH_TEST_CHUNKS").ok().and_then(|v| v.parse().ok()).unwrap_or(5);
    let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(2).enable_all().build().unwrap();
    let (tip, _) = rt.block_on(fetch::tip_and_info(SERVER, &proxy, zcash_wallet_core::net::socks::Isolation::fresh())).unwrap();
    let birthday = grid_floor(tip) - chunks * 1000;
    let below = rt.block_on(fetch::tree_state(SERVER, &proxy, birthday - 1)).unwrap();

    let root = tempfile::tempdir().unwrap();
    let net = ZNetwork::Test;
    let w = WalletDir::new(root.path(), net, "t");
    w.create(net, "t", "pw", &Phrase::generate(), below.to_chain_state().unwrap(), Some(10)).unwrap();
    let key = w.unlock_db_key("pw").unwrap();
    let mut db = w.open_db(net, &key).unwrap();
    let cache = Arc::new(w.open_cache(&key).unwrap());

    let cfg = SyncConfig::new(proxy, vec![SERVER.into()]);
    let mut s = Syncer::new(net, cfg, rt.handle().clone(), cache, birthday);
    let t0 = Instant::now();
    let mut last_print = Instant::now();
    loop {
        let step = s.step(&mut db).unwrap();
        if last_print.elapsed() > Duration::from_secs(5) {
            println!("{:>6.1}s {}", t0.elapsed().as_secs_f64(), serde_json::to_string(&s.progress).unwrap());
            last_print = Instant::now();
        }
        match step {
            Step::Synced => break,
            Step::Worked => {}
            Step::Waiting => s.wait(&mut db, Duration::from_millis(500)).unwrap(),
        }
        assert!(t0.elapsed() < Duration::from_secs(900), "sync took too long");
    }
    let p = &s.progress;
    println!("synced {} blocks ({} outputs) from {} to {} in {:.1}s", p.blocks_scanned, p.outputs_scanned, birthday, tip, t0.elapsed().as_secs_f64());
    assert!(db.block_fully_scanned().unwrap().unwrap().block_height() >= tip.into());
}
