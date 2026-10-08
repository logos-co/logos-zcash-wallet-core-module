//! The engine against a local regtest chain: zebrad plus two lightwalletd on loopback, with
//! Orchard coinbase before NU6.3, transparent coinbase, then Ironwood coinbase.
//!
//! With a chain from tools/regtest/chain.sh:
//! REGTEST_HEIGHTS=../tools/regtest/regtest.json REGTEST_PHRASE=phrase.txt REGTEST_RPC=http://127.0.0.1:28232 \
//! REGTEST_SERVERS=http://127.0.0.1:29061,http://127.0.0.1:29063 ZCASH_PARAMS_DIR=... \
//!   cargo test --release --no-default-features --test regtest -- --ignored --nocapture
//! REGTEST_STEPS=sync,send,shield,migrate picks steps (default all).

use std::process::Command;
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::{json, Value};
use zcash_keys::keys::{ReceiverRequirement, UnifiedAddressRequest, UnifiedSpendingKey};
use zcash_wallet_core::engine::{Engine, Event, Sink};
use zcash_wallet_core::keys::Phrase;
use zcash_wallet_core::network::{configure_regtest, RegtestHeights, ZNetwork};

const PW: &str = "regtest-pw";

struct Chain {
    engine: Arc<Engine>,
    rpc: String,
    routes: Value,
}

impl Chain {
    fn rpc(&self, method: &str, params: Value) -> Value {
        let body =
            json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).to_string();
        let out = Command::new("curl")
            .args([
                "-s",
                "--max-time",
                "300",
                "-H",
                "content-type: application/json",
                "--data-binary",
                &body,
                &self.rpc,
            ])
            .output()
            .expect("curl");
        let v: Value = serde_json::from_slice(&out.stdout)
            .unwrap_or_else(|_| panic!("rpc {method}: {}", String::from_utf8_lossy(&out.stdout)));
        assert!(v["error"].is_null(), "rpc {method}: {v}");
        v["result"].clone()
    }

    fn tip(&self) -> u32 {
        self.rpc("getblockcount", json!([])).as_u64().unwrap() as u32
    }

    fn mine(&self, n: u32) {
        self.rpc("generate", json!([n]));
    }

    fn job(&self, kind: &str, params: Value) -> Value {
        let r = self.engine.start_job(kind, params.to_string());
        assert_eq!(r["ok"], true, "{kind}: {r}");
        let (id, receipt) = (
            r["jobId"].as_str().unwrap().to_string(),
            r["receipt"].as_str().unwrap().to_string(),
        );
        let t0 = Instant::now();
        loop {
            let s = self.engine.jobs().status(&id, &receipt);
            match s["state"].as_str().unwrap() {
                "done" => return self.engine.jobs().result(&id, &receipt)["result"].clone(),
                "failed" | "cancelled" => panic!("{kind}: {s}"),
                _ => {}
            }
            assert!(t0.elapsed() < Duration::from_secs(600), "{kind} timed out");
            std::thread::sleep(Duration::from_millis(200));
        }
    }

    /// Waits until the wallet has scanned the node's tip.
    fn synced(&self) -> Value {
        let want = self.tip();
        let t0 = Instant::now();
        loop {
            let s = self.engine.sync_status()["sync"].clone();
            if s["state"] == "synced" && s["fullyScanned"].as_u64() >= Some(want as u64) {
                return s;
            }
            assert!(t0.elapsed() < Duration::from_secs(300), "sync stalled: {s}");
            std::thread::sleep(Duration::from_millis(300));
        }
    }

    fn balances(&self) -> Value {
        let b = self.engine.balances();
        assert_eq!(b["ready"], true, "{b}");
        b
    }

    /// Mines one block and waits for the wallet to see it.
    fn confirm(&self) {
        self.mine(1);
        self.synced();
    }
}

fn total(b: &Value, pool: &str) -> u64 {
    b["pools"][pool]["total"].as_u64().unwrap()
}

/// An Orchard-receiver Unified Address for a fresh phrase: Ironwood after NU6.3.
fn fresh_address(net: ZNetwork) -> String {
    let phrase = Phrase::generate();
    let seed = secrecy::ExposeSecret::expose_secret(&phrase.seed()).clone();
    let usk = UnifiedSpendingKey::from_seed(&net, &seed, zip32::AccountId::ZERO).unwrap();
    let req = UnifiedAddressRequest::unsafe_custom(
        ReceiverRequirement::Require,
        ReceiverRequirement::Omit,
        ReceiverRequirement::Omit,
    );
    usk.to_unified_full_viewing_key()
        .default_address(req)
        .unwrap()
        .0
        .encode(&net)
}

#[test]
#[ignore]
fn send_shield_and_migrate_on_regtest() {
    if std::env::var_os("REGTEST_RPC").is_none() {
        eprintln!("skipped: REGTEST_RPC is not set (see tools/regtest/chain.sh)");
        return;
    }
    let env = |k: &str| std::env::var(k).unwrap_or_else(|_| panic!("{k} is not set"));
    let heights: RegtestHeights =
        serde_json::from_str(&std::fs::read_to_string(env("REGTEST_HEIGHTS")).unwrap()).unwrap();
    configure_regtest(&heights);
    let nu6_3 = heights.nu6_3.unwrap_or(u32::MAX);
    let net = ZNetwork::Regtest;
    let phrase = std::fs::read_to_string(env("REGTEST_PHRASE")).unwrap();
    let servers: Vec<String> = env("REGTEST_SERVERS")
        .split(',')
        .map(String::from)
        .collect();
    let _ = tracing_subscriber::fmt().with_max_level(tracing::Level::INFO).with_test_writer().try_init();
    let root = tempfile::tempdir().unwrap();
    let sink: Sink = Arc::new(|ev| {
        if let Event::MigrationChanged(v) = ev {
            println!("event migration_changed {v}");
        }
    });
    let engine = Engine::new(
        root.path().join("wallets"),
        sink,
        Some(10),
        Some(env("ZCASH_PARAMS_DIR").into()),
    );
    let c = Chain {
        engine,
        rpc: env("REGTEST_RPC"),
        routes: json!({"proxy": "direct", "servers": servers}),
    };

    let t0 = Instant::now();
    c.job("restore_wallet", json!({"network": "regtest", "name": "r1", "password": PW, "phrase": phrase.trim(), "birthdayHeight": 1, "routes": c.routes}));
    c.job("open_wallet", json!({"network": "regtest", "name": "r1", "password": PW, "routes": c.routes}));
    let s = c.synced();
    println!(
        "synced to {} in {:.1}s: {s}",
        c.tip(),
        t0.elapsed().as_secs_f64()
    );
    let b = c.balances();
    println!("balances {b}");
    println!("addresses {}", c.engine.addresses());
    assert!(
        b["orchardToMigrate"]["total"].as_u64().unwrap() > 0,
        "no Orchard coinbase found"
    );
    if c.tip() > nu6_3 {
        assert!(total(&b, "ironwood") > 0, "no Ironwood coinbase found");
    }
    if c.tip() > 20 {
        assert!(
            total(&b, "transparent") > 0,
            "no transparent coinbase found"
        );
    }
    assert_eq!(
        b["shielded"]["total"].as_u64().unwrap(),
        total(&b, "ironwood") + total(&b, "sapling")
    );
    let steps = std::env::var("REGTEST_STEPS").unwrap_or("sync,send,shield,migrate".into());
    let step = |name: &str| steps.split(',').any(|s| s == name);
    if !step("send") && !step("shield") && !step("migrate") {
        return;
    }

    // A payment from a single pool, signed at approval and broadcast to the first server.
    if step("send") {
        let to = fresh_address(net);
        let amount = 100_000_000u64;
        let p = c.job(
            "propose",
            json!({"send": {"recipients": [{"address": to, "amount": amount, "memo": "regtest"}]}}),
        );
        println!("send preview {}", p["preview"]);
        let sent = c.job(
            "sign_and_send",
            json!({"proposalId": p["proposalId"], "password": PW}),
        );
        println!("sent {sent}");
        assert_eq!(sent["transactions"][0]["accepted"], true, "{sent}");
        let before = b.clone();
        c.confirm();
        let b = c.balances();
        let fee = p["preview"]["fee"].as_u64().unwrap_or(0);
        println!(
            "after send: ironwood {} -> {} (fee {fee})",
            total(&before, "ironwood"),
            total(&b, "ironwood")
        );
        let history = c.engine.history(0);
        let row = history["rows"]
            .as_array()
            .unwrap_or_else(|| panic!("history: {history}"))
            .iter()
            .find(|r| r["txid"] == sent["transactions"][0]["txid"])
            .cloned();
        println!("history row {}", row.clone().unwrap_or_default());
        assert_eq!(row.map(|r| r["kind"].clone()), Some(json!("sent")));
    }

    // Shield the transparent coinbase: one transaction per address, nothing linked.
    if step("shield") {
        let b = c.balances();
        let t_before = total(&b, "transparent");
        let p = c.job("propose_shielding", json!({"address": ""}));
        println!("shield preview {}", p["preview"]);
        let sent = c.job(
            "sign_and_send",
            json!({"proposalId": p["proposalId"], "password": PW}),
        );
        println!("shielded {sent}");
        assert_eq!(sent["transactions"][0]["accepted"], true, "{sent}");
        c.confirm();
        let b = c.balances();
        println!(
            "after shielding: transparent {t_before} -> {}",
            total(&b, "transparent")
        );
        assert!(total(&b, "transparent") < t_before);
    }

    // The ZIP 318 run: plan, review, sign once, then the wallet drives it as blocks arrive.
    if step("migrate") {
        let b = c.balances();
        let orchard_before = b["orchardToMigrate"]["total"].as_u64().unwrap();
        let plan = c.job("plan_migration", json!({}));
        println!("migration plan {}", plan["preview"]);
        let signed = c.job(
            "sign_migration",
            json!({"planId": plan["planId"], "digest": plan["preview"]["digest"], "password": PW}),
        );
        println!("migration signed {signed}");
        let t1 = Instant::now();
        let mut last = Value::Null;
        loop {
            // The wallet drives the run every 3 s on regtest; mine in steps toward the schedule.
            c.mine(50);
            c.synced();
            std::thread::sleep(Duration::from_secs(4));
            let st = c.engine.migration_status();
            if st != last {
                println!("{:>5.0}s tip {} migration {st}", t1.elapsed().as_secs_f64(), c.tip());
                last = st.clone();
            }
            if st["status"] == "complete" {
                break;
            }
            assert!(st["status"] != "failed", "migration failed: {st}");
            assert!(t1.elapsed() < Duration::from_secs(3000), "migration did not complete");
        }
        c.synced();
        let b = c.balances();
        println!(
            "after migration: orchard {orchard_before} -> {}, ironwood {}",
            b["orchardToMigrate"]["total"],
            total(&b, "ironwood")
        );
        assert!(b["orchardToMigrate"]["total"].as_u64().unwrap() < orchard_before);
    }
}
