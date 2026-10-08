//! Builds the bundled restore checkpoints: tree states every SPACING blocks, at
//! grid heights, fetched over Tor. Mainnet entries must agree across two operators.
//!
//! usage: ZCASH_TEST_TOR=socks5h://127.0.0.1:PORT cargo run --release --no-default-features \
//!        --example checkpoints -- <mainnet|testnet> <out.json>

use std::collections::BTreeMap;

use futures_util::stream::{self, StreamExt};
use zcash_wallet_core::checkpoints::{Checkpoint, SPACING};
use zcash_wallet_core::net::socks::{Isolation, ProxyAddr};
use zcash_wallet_core::sync::fetch;

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let (network, out) = (args[1].as_str(), &args[2]);
    let proxy = ProxyAddr::parse(&std::env::var("ZCASH_TEST_TOR").expect("ZCASH_TEST_TOR")).unwrap();
    let servers: Vec<&str> = match network {
        "mainnet" => vec!["https://zec.rocks:443", "https://us.zec.stardust.rest:443"],
        "testnet" => vec!["https://testnet.zec.rocks:443"],
        _ => panic!("mainnet or testnet"),
    };
    let (tip, _) = fetch::tip_and_info(servers[0], &proxy, Isolation::fresh()).await.unwrap();
    // Sapling activation: 419,200 on mainnet, 280,000 on testnet.
    // The first checkpoint sits one spacing above Sapling activation, where servers have no tree state.
    let first = if network == "mainnet" { 430_000 } else { 290_000 };
    let last = tip.saturating_sub(1_000) / SPACING * SPACING;
    let heights: Vec<u32> = (first / SPACING..=last / SPACING).map(|k| k * SPACING).filter(|h| *h >= first).collect();
    eprintln!("{network}: {} checkpoints from {} to {}, tip {tip}", heights.len(), heights[0], last);

    let results: Vec<(u32, Result<Checkpoint, String>)> = stream::iter(heights)
        .map(|h| {
            let (proxy, servers) = (proxy.clone(), servers.clone());
            async move {
                let mut states = vec![];
                for s in &servers {
                    let mut last_err = String::new();
                    let mut got = None;
                    for _ in 0..3 {
                        match fetch::tree_state(s, &proxy, h - 1).await {
                            Ok(ts) => {
                                got = Some(ts);
                                break;
                            }
                            Err(e) => last_err = e.to_string(),
                        }
                    }
                    match got {
                        Some(ts) => states.push(ts),
                        None => return (h, Err(format!("{s}: {last_err}"))),
                    }
                }
                let a = &states[0];
                for b in &states[1..] {
                    if (a.hash.as_str(), a.sapling_tree.as_str(), a.orchard_tree.as_str(), a.ironwood_tree.as_str())
                        != (b.hash.as_str(), b.sapling_tree.as_str(), b.orchard_tree.as_str(), b.ironwood_tree.as_str())
                    {
                        return (h, Err("operators disagree".into()));
                    }
                }
                (h, Checkpoint::from_tree_state(a).map_err(|e| e.to_string()))
            }
        })
        .buffer_unordered(4)
        .collect()
        .await;

    let mut ok = BTreeMap::new();
    for (h, r) in results {
        match r {
            Ok(c) => {
                ok.insert(h, c);
            }
            Err(e) => panic!("checkpoint {h}: {e}"),
        }
    }
    let list: Vec<&Checkpoint> = ok.values().collect();
    std::fs::write(out, serde_json::to_string(&list).unwrap()).unwrap();
    eprintln!("wrote {} checkpoints to {out}", list.len());
}
