//! Where each call class goes, against a local regtest chain from tools/regtest/chain.sh. Two
//! operators sit behind counting fronts; stand-ins for tor_module and the local node count too.
//!
//! REGTEST_HEIGHTS=../tools/regtest/regtest.json REGTEST_PHRASE=phrase.txt REGTEST_RPC=http://127.0.0.1:28232 \
//! REGTEST_SERVERS=http://127.0.0.1:29061,http://127.0.0.1:29063 ZCASH_PARAMS_DIR=... \
//!   cargo test --release --no-default-features --features test-dial --test tor_use -- --ignored --nocapture
#![cfg(feature = "test-dial")]

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::future::Future;
use std::pin::Pin;
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use http_body_util::BodyExt;
use serde_json::{json, Value};
use tonic::body::Body;
use tonic::transport::{Channel, Endpoint, Server};
use tower::ServiceExt;
use zcash_wallet_core::engine::{Engine, Sink};
use zcash_wallet_core::net::client::test_dial;
use zcash_wallet_core::net::ipc::{set_local_node, GrpcCall, LOCAL_NODE_URL};
use zcash_wallet_core::net::tor::{set_tor, TorRequests};
use zcash_wallet_core::network::{configure_regtest, RegtestHeights};

const PW: &str = "regtest-pw";
const A: &str = "https://lwd-a.test:443";
const B: &str = "https://lwd-b.test:443";
/// The calls that name the wallet: lookups by txid or address, and sends.
const IDENTIFYING: [&str; 3] = ["GetTransaction", "GetTaddressTransactions", "SendTransaction"];
const SYNC: [&str; 3] = ["GetBlockRange", "GetTreeState", "GetSubtreeRoots"];

/// The methods one place saw, as "<operator> <method>" or just "<method>".
#[derive(Clone, Default)]
struct Seen(Arc<Mutex<Vec<String>>>);

impl Seen {
    fn push(&self, s: String) {
        self.0.lock().unwrap().push(s);
    }
    fn all(&self) -> Vec<String> {
        self.0.lock().unwrap().clone()
    }
    fn take(&self) -> Vec<String> {
        std::mem::take(&mut *self.0.lock().unwrap())
    }
    fn count(&self, method: &str) -> usize {
        self.all().iter().filter(|s| s.ends_with(method)).count()
    }
}

fn method(path: &str) -> String {
    path.rsplit('/').next().unwrap_or(path).to_string()
}

fn has(seen: &[String], methods: &[&str]) -> Vec<String> {
    seen.iter().filter(|s| methods.iter().any(|m| s.ends_with(m))).cloned().collect()
}

/// A plain HTTP/2 front before a lightwalletd: counts each call, then passes it on.
#[derive(Clone)]
struct Front {
    upstream: Channel,
    seen: Seen,
}

impl tower::Service<http::Request<Body>> for Front {
    type Response = http::Response<Body>;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Infallible>> + Send>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: http::Request<Body>) -> Self::Future {
        self.seen.push(method(req.uri().path()));
        let upstream = self.upstream.clone();
        Box::pin(async move {
            Ok(match upstream.oneshot(req).await {
                Ok(r) => r,
                Err(e) => tonic::Status::unavailable(e.to_string()).into_http(),
            })
        })
    }
}

async fn front(upstream: &str, seen: Seen) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let upstream = Endpoint::from_shared(upstream.to_string()).unwrap().connect_lazy();
    let incoming = futures_util::stream::unfold(listener, |l| async move { Some((l.accept().await.map(|(s, _)| s), l)) });
    tokio::spawn(Server::builder().serve_with_incoming(Front { upstream, seen }, incoming));
    format!("http://{addr}")
}

/// One framed gRPC call to a lightwalletd, answered as tor_module and the local node answer.
async fn forward(upstream: Channel, path: String, body: Vec<u8>) -> (i32, String, Vec<u8>) {
    let req = http::Request::builder()
        .method("POST")
        .uri(format!("http://upstream{path}"))
        .header("content-type", "application/grpc")
        .header("te", "trailers")
        .body(Body::new(http_body_util::Full::new(bytes::Bytes::from(body))))
        .unwrap();
    let resp = match upstream.oneshot(req).await {
        Ok(r) => r,
        Err(e) => return (14, e.to_string(), Vec::new()),
    };
    let (parts, body) = resp.into_parts();
    let got = match body.collect().await {
        Ok(b) => b,
        Err(e) => return (14, e.to_string(), Vec::new()),
    };
    let trailers = got.trailers().cloned().unwrap_or_default();
    let find = |k: &str| trailers.get(k).or_else(|| parts.headers.get(k)).and_then(|v| v.to_str().ok()).map(String::from);
    let code = find("grpc-status").and_then(|s| s.parse().ok()).unwrap_or(2);
    (code, find("grpc-message").unwrap_or_default(), got.to_bytes().to_vec())
}

/// Stands in for tor_module: every call it carries reaches the lightwalletd behind the host.
struct FakeTor {
    rt: tokio::runtime::Handle,
    upstream: BTreeMap<String, Channel>,
    seen: Seen,
}

impl TorRequests for FakeTor {
    fn grpc(&self, url: &str, body: &[u8], _key: &str, _budget: Option<Duration>) -> (i32, String, Vec<u8>) {
        let uri: http::Uri = url.parse().unwrap();
        let host = uri.host().unwrap_or_default().to_string();
        self.seen.push(format!("{host} {}", method(uri.path())));
        let Some(up) = self.upstream.get(&host).cloned() else { return (14, format!("no route to {host}"), Vec::new()) };
        self.rt.block_on(forward(up, uri.path().to_string(), body.to_vec()))
    }
}

/// Stands in for the local Zebra node's IPC: it answers from operator A's lightwalletd.
struct FakeNode {
    rt: tokio::runtime::Handle,
    upstream: Channel,
    seen: Seen,
}

impl GrpcCall for FakeNode {
    fn call(&self, path: &str, body: &[u8]) -> (i32, String, Vec<u8>) {
        self.seen.push(method(path));
        self.rt.block_on(forward(self.upstream.clone(), path.to_string(), body.to_vec()))
    }
}

fn rpc(url: &str, method: &str, params: Value) -> Value {
    let body = json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).to_string();
    let out = Command::new("curl")
        .args(["-s", "--max-time", "300", "-H", "content-type: application/json", "--data-binary", &body, url])
        .output()
        .expect("curl");
    let v: Value = serde_json::from_slice(&out.stdout).unwrap_or_else(|_| panic!("rpc {method}: {}", String::from_utf8_lossy(&out.stdout)));
    assert!(v["error"].is_null(), "rpc {method}: {v}");
    v["result"].clone()
}

/// A wallet restored from the regtest phrase with `routes`, in its own directory.
struct Wallet {
    engine: Arc<Engine>,
    rpc: String,
    _dir: tempfile::TempDir,
}

impl Wallet {
    fn restore(routes: Value, rpc: &str) -> Self {
        let env = |k: &str| std::env::var(k).unwrap_or_else(|_| panic!("{k} is not set"));
        let dir = tempfile::tempdir().unwrap();
        let sink: Sink = Arc::new(|_| {});
        let engine = Engine::new(dir.path().join("wallets"), sink, Some(10), Some(env("ZCASH_PARAMS_DIR").into()));
        let w = Self { engine, rpc: rpc.into(), _dir: dir };
        let phrase = std::fs::read_to_string(env("REGTEST_PHRASE")).unwrap();
        w.job("restore_wallet", json!({"network": "regtest", "name": "r1", "password": PW, "phrase": phrase.trim(), "birthdayHeight": 1, "routes": routes}));
        w.synced();
        w
    }

    fn job(&self, kind: &str, params: Value) -> Value {
        let r = self.engine.start_job(kind, params.to_string());
        assert_eq!(r["ok"], true, "{kind}: {r}");
        let (id, receipt) = (r["jobId"].as_str().unwrap().to_string(), r["receipt"].as_str().unwrap().to_string());
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

    fn synced(&self) {
        let want = rpc(&self.rpc, "getblockcount", json!([])).as_u64().unwrap();
        let t0 = Instant::now();
        loop {
            let s = self.engine.sync_status()["sync"].clone();
            if s["state"] == "synced" && s["fullyScanned"].as_u64() >= Some(want) {
                return;
            }
            assert!(t0.elapsed() < Duration::from_secs(300), "sync stalled: {s}");
            std::thread::sleep(Duration::from_millis(300));
        }
    }

    /// Pays its own transparent address, then mines the transaction in.
    fn send(&self) {
        let to = self.engine.addresses()["transparent"].as_str().unwrap().to_string();
        let p = self.job("propose", json!({"send": {"recipients": [{"address": to, "amount": 50_000_000u64}]}}));
        let sent = self.job("sign_and_send", json!({"proposalId": p["proposalId"], "password": PW}));
        assert_eq!(sent["transactions"][0]["accepted"], true, "{sent}");
        rpc(&self.rpc, "generate", json!([1]));
        self.synced();
    }
}

impl Drop for Wallet {
    fn drop(&mut self) {
        self.engine.shutdown();
    }
}

/// Waits until `seen` has every method in `methods`, or the time runs out.
fn wait_for(seen: &[&Seen], methods: &[&str], limit: Duration) -> bool {
    let t0 = Instant::now();
    while t0.elapsed() < limit {
        if methods.iter().all(|m| seen.iter().any(|s| s.count(m) > 0)) {
            return true;
        }
        std::thread::sleep(Duration::from_secs(1));
    }
    false
}

/// Drops what a closed wallet's last calls left, before the next one opens.
fn clear(seen: &[&Seen]) {
    std::thread::sleep(Duration::from_secs(2));
    seen.iter().for_each(|s| drop(s.take()));
}

#[test]
#[ignore]
fn each_class_takes_its_path_on_regtest() {
    if std::env::var_os("REGTEST_RPC").is_none() {
        eprintln!("skipped: REGTEST_RPC is not set (see tools/regtest/chain.sh)");
        return;
    }
    let env = |k: &str| std::env::var(k).unwrap_or_else(|_| panic!("{k} is not set"));
    let heights: RegtestHeights = serde_json::from_str(&std::fs::read_to_string(env("REGTEST_HEIGHTS")).unwrap()).unwrap();
    configure_regtest(&heights);
    let lwd: Vec<String> = env("REGTEST_SERVERS").split(',').map(String::from).collect();
    let chain = env("REGTEST_RPC");
    let _ = tracing_subscriber::fmt().with_max_level(tracing::Level::INFO).with_test_writer().try_init();

    let rt = tokio::runtime::Runtime::new().unwrap();
    let (direct_a, direct_b, tor, node) = (Seen::default(), Seen::default(), Seen::default(), Seen::default());
    let (fa, fb) = rt.block_on(async { (front(&lwd[0], direct_a.clone()).await, front(&lwd[1], direct_b.clone()).await) });
    test_dial::set("lwd-a.test", &fa);
    test_dial::set("lwd-b.test", &fb);
    let channel = |u: &str| rt.block_on(async { Endpoint::from_shared(u.to_string()).unwrap().connect_lazy() });
    let upstream = BTreeMap::from([("lwd-a.test".to_string(), channel(&lwd[0])), ("lwd-b.test".to_string(), channel(&lwd[1]))]);
    set_tor(Arc::new(FakeTor { rt: rt.handle().clone(), upstream, seen: tor.clone() }));
    set_local_node(Arc::new(FakeNode { rt: rt.handle().clone(), upstream: channel(&lwd[0]), seen: node.clone() }));
    let operators = json!({A: "Operator A", B: "Operator B", LOCAL_NODE_URL: "local"});
    let lookups = Duration::from_secs(420);

    // Tor for what identifies you: what the backend sends for the node's identifying table.
    let w = Wallet::restore(
        json!({"proxy": "tor", "servers": [A, B], "broadcast": [A, B], "directFor": ["sync", "tip"],
               "details": [A, B], "taddr": [A, B], "operators": operators}),
        &chain,
    );
    w.send();
    // Lookups go to B; A's GetTransaction over Tor is the check that it saw the send.
    let looked_up = wait_for(&[&tor], &["lwd-b.test GetTransaction", "lwd-b.test GetTaddressTransactions"], lookups);
    drop(w);
    let (da, db, t) = (direct_a.take(), direct_b.take(), tor.take());
    println!("identifying: direct A {da:?}\ndirect B {db:?}\ntor {t:?}");
    assert!(looked_up, "no lookups went out over Tor: {t:?}");
    assert!(!has(&da, &SYNC).is_empty(), "sync did not go direct: {da:?}");
    assert_eq!(has(&da, &IDENTIFYING), Vec::<String>::new(), "a call that names the wallet went direct");
    assert_eq!(db, Vec::<String>::new(), "sync used one operator; the other saw this IP");
    assert_eq!(has(&t, &SYNC), Vec::<String>::new(), "sync went over Tor");
    let at_a: Vec<&String> = t.iter().filter(|s| s.as_str() == "lwd-a.test GetTaddressTransactions").collect();
    assert!(at_a.is_empty(), "an address lookup went to the sync operator: {at_a:?}");
    assert!(t.iter().any(|s| s == "lwd-b.test SendTransaction"), "the send did not try the other operator first: {t:?}");

    // Tor for nothing: proxy direct, every server direct.
    clear(&[&direct_a, &direct_b, &tor, &node]);
    let w = Wallet::restore(json!({"proxy": "direct", "servers": [A, B], "broadcast": [A, B], "direct": [A, B]}), &chain);
    w.send();
    let looked_up = wait_for(&[&direct_a, &direct_b], &["GetTransaction"], lookups);
    drop(w);
    let (da, db, t) = (direct_a.take(), direct_b.take(), tor.take());
    println!("none: direct A {da:?}\ndirect B {db:?}\ntor {t:?}");
    assert!(looked_up, "no lookup went out");
    assert_eq!(t, Vec::<String>::new(), "a call went to tor_module with Tor off");

    // The local node keeps every read; the send leaves it over Tor, and the tip cross-check
    // (a tip, then a tree state at it) goes direct as a `tip` call.
    clear(&[&direct_a, &direct_b, &tor, &node]);
    let w = Wallet::restore(
        json!({"proxy": "tor", "servers": [LOCAL_NODE_URL], "broadcast": [A, B], "directFor": ["sync", "tip"],
               "details": [LOCAL_NODE_URL], "taddr": [LOCAL_NODE_URL], "operators": operators}),
        &chain,
    );
    w.send();
    let looked_up = wait_for(&[&node], &["GetTransaction", "GetTaddressTransactions"], lookups);
    drop(w);
    let (da, db, t, n) = (direct_a.take(), direct_b.take(), tor.take(), node.take());
    println!("local node: node {n:?}\ndirect A {da:?}\ndirect B {db:?}\ntor {t:?}");
    assert!(looked_up && !has(&n, &SYNC).is_empty(), "the node did not serve the reads: {n:?}");
    let cross_check = ["GetLatestBlock", "GetTreeState"];
    let direct: Vec<&String> = da.iter().chain(&db).filter(|s| !cross_check.contains(&s.as_str())).collect();
    assert!(direct.is_empty(), "a read left the local node directly: {direct:?}");
    let reads: Vec<&String> = t.iter().filter(|s| !s.ends_with("SendTransaction") && !s.ends_with("GetTransaction")).collect();
    assert!(reads.is_empty(), "a read left the local node over Tor: {reads:?}");
}
