//! The local node over Logos IPC. gRPC calls travel as framed bytes to `zebrad_module`, which
//! answers them from Zebra's own lightwalletd service in memory: no port is opened.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock, RwLock};
use std::task::{Context, Poll};

use bytes::Bytes;
use http_body_util::BodyExt;

/// The route URL that selects the local node.
pub const LOCAL_NODE_URL: &str = "logos://zebrad_module";

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// A transport error with a concrete type: tonic's bounds over a bare boxed error trip
/// rustc's higher-ranked `Send` inference at every spawn.
#[derive(Debug)]
pub struct TransportError(pub BoxError);

impl std::fmt::Display for TransportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::error::Error for TransportError {}

/// One gRPC call: the framed request body in; the grpc-status, its message and the framed
/// response body out. Failures to reach the node come back as a status too (UNAVAILABLE).
pub trait GrpcCall: Send + Sync {
    fn call(&self, path: &str, body: &[u8]) -> (i32, String, Vec<u8>);
}

static LOCAL_NODE: RwLock<Option<Arc<dyn GrpcCall>>> = RwLock::new(None);

/// Installed by the module glue, which owns the IPC client.
pub fn set_local_node(call: Arc<dyn GrpcCall>) {
    *LOCAL_NODE.write().unwrap() = Some(call);
}

pub fn local_node() -> Option<Arc<dyn GrpcCall>> {
    LOCAL_NODE.read().unwrap().clone()
}

static CLOSED: AtomicBool = AtomicBool::new(false);
static CLOSING: LazyLock<tokio::sync::Notify> = LazyLock::new(tokio::sync::Notify::new);

/// For unload: calls in flight answer UNAVAILABLE at once and no new ones start. A call
/// runs on the module's main thread, which unload holds while it joins the wallet threads.
pub fn close() {
    CLOSED.store(true, Ordering::SeqCst);
    CLOSING.notify_waiters();
}

/// A tower service a tonic client can run over, in place of a network channel.
#[derive(Clone)]
pub struct IpcChannel {
    call: Arc<dyn GrpcCall>,
}

impl IpcChannel {
    pub fn new(call: Arc<dyn GrpcCall>) -> Self {
        Self { call }
    }
}

impl tower::Service<http::Request<tonic::body::Body>> for IpcChannel {
    type Response = http::Response<tonic::body::Body>;
    type Error = TransportError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, TransportError>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), TransportError>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: http::Request<tonic::body::Body>) -> Self::Future {
        let call = self.call.clone();
        Box::pin(async move { Self::forward(call, req).await.map_err(TransportError) })
    }
}

impl IpcChannel {
    async fn forward(
        call: Arc<dyn GrpcCall>,
        req: http::Request<tonic::body::Body>,
    ) -> Result<http::Response<tonic::body::Body>, BoxError> {
        let path = req.uri().path().to_string();
        let body = req.into_body().collect().await?.to_bytes();
        let closing = CLOSING.notified();
        tokio::pin!(closing);
        closing.as_mut().enable();
        let unloading = || (14, "the wallet is unloading".to_string(), Vec::new());
        let (status, message, out) = if CLOSED.load(Ordering::SeqCst) {
            unloading()
        } else {
            // The IPC call blocks; keep it off the async workers.
            let call = tokio::task::spawn_blocking(move || call.call(&path, &body));
            tokio::select! {
                r = call => r?,
                _ = closing => unloading(),
            }
        };
        let mut trailers = http::HeaderMap::new();
        trailers.insert("grpc-status", status.to_string().parse()?);
        let message: String = message.chars().filter(|c| c.is_ascii_graphic() || *c == ' ').filter(|c| *c != '%').collect();
        if !message.is_empty() {
            trailers.insert("grpc-message", message.parse()?);
        }
        let frames: Vec<Result<http_body::Frame<Bytes>, BoxError>> =
            vec![Ok(http_body::Frame::data(Bytes::from(out))), Ok(http_body::Frame::trailers(trailers))];
        let body = http_body_util::StreamBody::new(futures_util::stream::iter(frames));
        let resp = http::Response::builder()
            .status(200)
            .header("content-type", "application/grpc")
            .body(tonic::body::Body::new(body))?;
        Ok(resp)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message;
    use zcash_client_backend::proto::service::compact_tx_streamer_client::CompactTxStreamerClient;
    use zcash_client_backend::proto::service::{BlockId, ChainSpec};

    fn frame(msg: &impl Message) -> Vec<u8> {
        let b = msg.encode_to_vec();
        let mut out = vec![0u8];
        out.extend_from_slice(&(b.len() as u32).to_be_bytes());
        out.extend_from_slice(&b);
        out
    }

    struct Fake;

    impl GrpcCall for Fake {
        fn call(&self, path: &str, body: &[u8]) -> (i32, String, Vec<u8>) {
            match path {
                "/cash.z.wallet.sdk.rpc.CompactTxStreamer/GetLatestBlock" => {
                    assert_eq!(&body[..5], &[0, 0, 0, 0, 0], "an empty ChainSpec, framed");
                    (0, String::new(), frame(&BlockId { height: 4242, hash: vec![7; 32] }))
                }
                _ => (14, "node is not running".into(), vec![]),
            }
        }
    }

    /// Answers only when released.
    struct Stuck(std::sync::Mutex<std::sync::mpsc::Receiver<()>>);

    impl GrpcCall for Stuck {
        fn call(&self, _: &str, _: &[u8]) -> (i32, String, Vec<u8>) {
            let _ = self.0.lock().unwrap().recv();
            (0, String::new(), vec![])
        }
    }

    #[tokio::test]
    async fn a_tonic_client_runs_over_ipc() {
        let mut c = CompactTxStreamerClient::new(IpcChannel::new(Arc::new(Fake)));
        let tip = c.get_latest_block(ChainSpec {}).await.unwrap().into_inner();
        assert_eq!(tip.height, 4242);
        let e = c.get_lightd_info(zcash_client_backend::proto::service::Empty {}).await.unwrap_err();
        assert_eq!((e.code(), e.message()), (tonic::Code::Unavailable, "node is not running"));

        // Last, since closing is for good: a call waiting on the node answers at once.
        let (release, rx) = std::sync::mpsc::channel();
        let mut c = CompactTxStreamerClient::new(IpcChannel::new(Arc::new(Stuck(std::sync::Mutex::new(rx)))));
        let pending = tokio::spawn(async move { c.get_latest_block(ChainSpec {}).await });
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        close();
        let e = tokio::time::timeout(std::time::Duration::from_secs(2), pending).await.unwrap().unwrap().unwrap_err();
        assert_eq!((e.code(), e.message()), (tonic::Code::Unavailable, "the wallet is unloading"));
        release.send(()).unwrap();
    }
}
