//! The built-in Tor over Logos IPC: each gRPC call goes to tor_module as one HTTP/2 request,
//! and its answer comes back over IPC. No SOCKS port is involved.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use base64::Engine as _;
use bytes::Bytes;
use http_body_util::BodyExt;
use serde_json::Value;

use super::ipc::{BoxError, TransportError};

/// One gRPC call through tor_module: the framed request body to `url` on the circuit `key`
/// names; the grpc-status, its message and the framed response body back. Failures to reach
/// Tor come back as a status too (UNAVAILABLE).
pub trait TorRequests: Send + Sync {
    fn grpc(&self, url: &str, body: &[u8], key: &str) -> (i32, String, Vec<u8>);
}

static TOR: RwLock<Option<Arc<dyn TorRequests>>> = RwLock::new(None);
static CLOSED: AtomicBool = AtomicBool::new(false);
static ACTIVE: AtomicUsize = AtomicUsize::new(0);

/// Installed by the module glue, which owns the IPC client.
pub fn set_tor(tor: Arc<dyn TorRequests>) {
    *TOR.write().unwrap() = Some(tor);
}

pub fn tor() -> Option<Arc<dyn TorRequests>> {
    TOR.read().unwrap().clone()
}

/// For unload: no call to tor_module starts after this, and the calls in flight have ended
/// when it returns, unless `grace` ran out first.
pub fn close(grace: Duration) {
    CLOSED.store(true, Ordering::SeqCst);
    let until = Instant::now() + grace;
    while ACTIVE.load(Ordering::SeqCst) > 0 && Instant::now() < until {
        std::thread::sleep(Duration::from_millis(10));
    }
}

pub fn closed() -> bool {
    CLOSED.load(Ordering::SeqCst)
}

/// Counts a call to tor_module in flight, for close().
struct Active;

impl Active {
    fn enter() -> Self {
        ACTIVE.fetch_add(1, Ordering::SeqCst);
        Self
    }
}

impl Drop for Active {
    fn drop(&mut self) {
        ACTIVE.fetch_sub(1, Ordering::SeqCst);
    }
}

pub const UNAVAILABLE: i32 = 14;

/// tor_module's answer to a gRPC request, as (grpc-status, message, framed body).
pub fn grpc_answer(reply: &Value) -> (i32, String, Vec<u8>) {
    let find = |list: &Value, name: &str| {
        list.as_array()?.iter().find(|p| p[0].as_str().is_some_and(|n| n.eq_ignore_ascii_case(name)))?[1].as_str().map(str::to_string)
    };
    match reply["state"].as_str() {
        Some("done") => {
            let http = reply["status"].as_u64().unwrap_or(0);
            if http != 200 {
                // gRPC's own mapping of HTTP statuses.
                let code = match http {
                    400 => 13,
                    401 => 16,
                    403 => 7,
                    404 => 12,
                    429 | 502..=504 => UNAVAILABLE,
                    _ => 2,
                };
                return (code, format!("the server answered HTTP {http}"), Vec::new());
            }
            // A call that fails at once may answer with headers alone.
            let (trailers, headers) = (&reply["trailers"], &reply["headers"]);
            let status = find(trailers, "grpc-status").or_else(|| find(headers, "grpc-status"));
            let message = find(trailers, "grpc-message").or_else(|| find(headers, "grpc-message")).unwrap_or_default();
            let body = reply["body"].as_str().and_then(|b| base64::engine::general_purpose::STANDARD.decode(b).ok()).unwrap_or_default();
            match status.and_then(|s| s.trim().parse::<i32>().ok()) {
                Some(code) => (code, message, body),
                None => (2, "the server sent no grpc-status".into(), body),
            }
        }
        Some("failed") => (UNAVAILABLE, reply["error"].as_str().unwrap_or("the request failed").to_string(), Vec::new()),
        _ => (2, format!("tor_module answered {reply}"), Vec::new()),
    }
}

/// A tower service a tonic client runs over, in place of a network channel: every call goes
/// to `server` on the circuit `key` names.
#[derive(Clone)]
pub struct TorChannel {
    tor: Arc<dyn TorRequests>,
    server: String,
    key: String,
}

impl TorChannel {
    pub fn new(tor: Arc<dyn TorRequests>, server: &str, key: &str) -> Self {
        Self { tor, server: server.trim_end_matches('/').to_string(), key: key.to_string() }
    }

    async fn forward(self, req: http::Request<tonic::body::Body>) -> Result<http::Response<tonic::body::Body>, BoxError> {
        let path = req.uri().path_and_query().map(|p| p.as_str().to_string()).unwrap_or_else(|| "/".into());
        let url = format!("{}{path}", self.server);
        let body = req.into_body().collect().await?.to_bytes();
        // The IPC calls block; keep them off the async workers.
        let (tor, key) = (self.tor.clone(), self.key.clone());
        let (status, message, out) = tokio::task::spawn_blocking(move || {
            let _active = Active::enter();
            match closed() {
                true => (UNAVAILABLE, "the wallet is unloading".to_string(), Vec::new()),
                false => tor.grpc(&url, &body, &key),
            }
        })
        .await?;
        let mut trailers = http::HeaderMap::new();
        trailers.insert("grpc-status", status.to_string().parse()?);
        let message: String = message.chars().filter(|c| c.is_ascii_graphic() || *c == ' ').filter(|c| *c != '%').collect();
        if !message.is_empty() {
            trailers.insert("grpc-message", message.parse()?);
        }
        let frames: Vec<Result<http_body::Frame<Bytes>, BoxError>> =
            vec![Ok(http_body::Frame::data(Bytes::from(out))), Ok(http_body::Frame::trailers(trailers))];
        let body = http_body_util::StreamBody::new(futures_util::stream::iter(frames));
        Ok(http::Response::builder().status(200).header("content-type", "application/grpc").body(tonic::body::Body::new(body))?)
    }
}

impl tower::Service<http::Request<tonic::body::Body>> for TorChannel {
    type Response = http::Response<tonic::body::Body>;
    type Error = TransportError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, TransportError>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), TransportError>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: http::Request<tonic::body::Body>) -> Self::Future {
        let this = self.clone();
        Box::pin(async move { this.forward(req).await.map_err(TransportError) })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message;
    use serde_json::json;
    use zcash_client_backend::proto::service::compact_tx_streamer_client::CompactTxStreamerClient;
    use zcash_client_backend::proto::service::{BlockId, ChainSpec, Empty};

    fn frame(msg: &impl Message) -> Vec<u8> {
        let b = msg.encode_to_vec();
        let mut out = vec![0u8];
        out.extend_from_slice(&(b.len() as u32).to_be_bytes());
        out.extend_from_slice(&b);
        out
    }

    /// Answers as tor_module would, and records each URL and key.
    #[derive(Default)]
    struct Fake(std::sync::Mutex<Vec<(String, String)>>);

    impl TorRequests for Fake {
        fn grpc(&self, url: &str, body: &[u8], key: &str) -> (i32, String, Vec<u8>) {
            self.0.lock().unwrap().push((url.to_string(), key.to_string()));
            let b64 = base64::engine::general_purpose::STANDARD;
            let reply = if url.ends_with("/GetLatestBlock") {
                assert_eq!(body, [0, 0, 0, 0, 0], "an empty ChainSpec, framed");
                json!({ "state": "done", "status": 200, "headers": [], "trailers": [["grpc-status", "0"]],
                        "body": b64.encode(frame(&BlockId { height: 4242, hash: vec![7; 32] })) })
            } else {
                json!({ "state": "failed", "error": "Tor is not running" })
            };
            grpc_answer(&reply)
        }
    }

    #[tokio::test]
    async fn a_tonic_client_runs_over_tor_module() {
        let fake = Arc::new(Fake::default());
        let mut c = CompactTxStreamerClient::new(TorChannel::new(fake.clone(), "https://zec.rocks:443/", "k1"));
        assert_eq!(c.get_latest_block(ChainSpec {}).await.unwrap().into_inner().height, 4242);
        let e = c.get_lightd_info(Empty {}).await.unwrap_err();
        assert_eq!((e.code(), e.message()), (tonic::Code::Unavailable, "Tor is not running"));
        let seen = fake.0.lock().unwrap().clone();
        assert_eq!(seen[0], ("https://zec.rocks:443/cash.z.wallet.sdk.rpc.CompactTxStreamer/GetLatestBlock".into(), "k1".into()));
    }

    #[test]
    fn http_and_grpc_statuses() {
        let done = |status: u64, trailers: Value, headers: Value| {
            grpc_answer(&json!({ "state": "done", "status": status, "headers": headers, "trailers": trailers, "body": "" }))
        };
        assert_eq!(done(200, json!([["grpc-status", "5"], ["grpc-message", "no such block"]]), json!([])).0, 5);
        assert_eq!(done(200, json!([["grpc-status", "5"], ["grpc-message", "no such block"]]), json!([])).1, "no such block");
        assert_eq!(done(200, json!([]), json!([["Grpc-Status", "12"]])).0, 12, "headers alone");
        assert_eq!(done(200, json!([]), json!([])).0, 2);
        assert_eq!(done(503, json!([]), json!([])).0, UNAVAILABLE);
        assert_eq!(done(404, json!([]), json!([])).0, 12);
        assert_eq!(grpc_answer(&json!({ "state": "failed", "error": "connect: timed out" })), (UNAVAILABLE, "connect: timed out".into(), vec![]));
    }
}
