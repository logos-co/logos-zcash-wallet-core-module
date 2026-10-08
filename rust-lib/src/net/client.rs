//! gRPC clients for lightwalletd-protocol servers, one Tor circuit per isolation.

use std::time::Duration;

use http::Uri;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};
use tower::ServiceExt;
use zcash_client_backend::proto::service::compact_tx_streamer_client::CompactTxStreamerClient;

use super::ipc::{local_node, IpcChannel, TransportError, LOCAL_NODE_URL};
use super::socks::{Isolation, ProxyAddr, Socks5hConnector};

/// A network channel or the local node's IPC, behind one type.
pub type GrpcChannel =
    tower::util::BoxCloneSyncService<http::Request<tonic::body::Body>, http::Response<tonic::body::Body>, TransportError>;

pub type Client = CompactTxStreamerClient<GrpcChannel>;

fn client(channel: Channel) -> Client {
    let channel = channel.map_err(|e| TransportError(Box::new(e)));
    CompactTxStreamerClient::new(GrpcChannel::new(channel)).max_decoding_message_size(16 * 1024 * 1024)
}

#[derive(Debug, thiserror::Error)]
pub enum NetError {
    #[error("bad server URL {0}")]
    BadUrl(String),
    #[error("connect to {server}: {source}")]
    Connect { server: String, source: tonic::transport::Error },
    #[error("{server}: {status}")]
    Status { server: String, status: tonic::Status },
    #[error("the local node is not available")]
    NoLocalNode,
}

const CONNECT_TIMEOUT: Duration = Duration::from_secs(45);

/// A v3 onion service. Tor authenticates and encrypts it end to end, so the wallet reaches it
/// over plain HTTP/2 through Tor, without TLS.
pub fn is_onion(host: &str) -> bool {
    host.strip_suffix(".onion")
        .is_some_and(|name| name.len() == 56 && name.bytes().all(|b| matches!(b, b'a'..=b'z' | b'2'..=b'7')))
}

/// `http://<onion>:port`, the only plain-HTTP server reached through Tor.
pub fn is_onion_url(server: &str) -> bool {
    server.parse::<Uri>().is_ok_and(|u| u.scheme_str() == Some("http") && u.host().is_some_and(is_onion))
}

/// Opens a TLS channel to `server` (https://host:port), or a plain one to an onion service,
/// through the proxy, on the circuit `isolation` selects; or, for `logos://zebrad_module`,
/// the local node over IPC.
pub async fn connect(server: &str, proxy: &ProxyAddr, isolation: Isolation) -> Result<Client, NetError> {
    if server == LOCAL_NODE_URL {
        let call = local_node().ok_or(NetError::NoLocalNode)?;
        let channel = GrpcChannel::new(IpcChannel::new(call));
        return Ok(CompactTxStreamerClient::new(channel).max_decoding_message_size(16 * 1024 * 1024));
    }
    let uri: Uri = server.parse().map_err(|_| NetError::BadUrl(server.into()))?;
    if proxy.is_direct() {
        // Regtest only: plaintext to a loopback lightwalletd, never anywhere else.
        let loopback = matches!(uri.host(), Some("127.0.0.1" | "localhost" | "[::1]" | "::1"));
        if uri.scheme_str() != Some("http") || !loopback {
            return Err(NetError::BadUrl(server.into()));
        }
        let channel = Endpoint::from_shared(server.to_string())
            .map_err(|source| NetError::Connect { server: server.into(), source })?
            .connect()
            .await
            .map_err(|source| NetError::Connect { server: server.into(), source })?;
        return Ok(client(channel));
    }
    let host = uri.host().ok_or_else(|| NetError::BadUrl(server.into()))?.to_string();
    let wrap = |source| NetError::Connect { server: server.into(), source };
    let endpoint = Endpoint::from_shared(server.to_string()).map_err(wrap)?.connect_timeout(CONNECT_TIMEOUT);
    let endpoint = match uri.scheme_str() {
        Some("https") => endpoint
            .tls_config(ClientTlsConfig::new().with_webpki_roots().domain_name(host))
            .map_err(wrap)?,
        Some("http") if is_onion(&host) => endpoint,
        _ => return Err(NetError::BadUrl(server.into())),
    };
    let channel = endpoint
        .connect_with_connector(Socks5hConnector::new(proxy.clone(), isolation))
        .await
        .map_err(wrap)?;
    Ok(client(channel))
}

#[cfg(test)]
mod tests {
    use super::*;
    use zcash_client_backend::proto::service::Empty;

    /// Needs a Tor SOCKS port in ZCASH_TEST_TOR, e.g. socks5h://127.0.0.1:19050.
    #[tokio::test]
    #[ignore]
    async fn lightd_info_over_tor() {
        let proxy = ProxyAddr::parse(&std::env::var("ZCASH_TEST_TOR").unwrap()).unwrap();
        let mut c = connect("https://testnet.zec.rocks:443", &proxy, Isolation::fresh()).await.unwrap();
        let info = c.get_lightd_info(Empty {}).await.unwrap().into_inner();
        println!("{} {} height {} branch {} lightwalletd {}", info.chain_name, info.vendor, info.block_height, info.consensus_branch_id, info.version);
        assert_eq!(info.chain_name, "test");
    }

    /// Needs ZCASH_TEST_TOR and an onion lightwalletd in ZCASH_TEST_ONION, http://<onion>:port.
    #[tokio::test]
    #[ignore]
    async fn lightd_info_from_an_onion_over_tor() {
        let proxy = ProxyAddr::parse(&std::env::var("ZCASH_TEST_TOR").unwrap()).unwrap();
        let server = std::env::var("ZCASH_TEST_ONION").unwrap();
        let mut c = connect(&server, &proxy, Isolation::fresh()).await.unwrap();
        let info = c.get_lightd_info(Empty {}).await.unwrap().into_inner();
        println!("{} {} height {} branch {} lightwalletd {}", info.chain_name, info.vendor, info.block_height, info.consensus_branch_id, info.version);
        assert_eq!(info.chain_name, "test");
    }

    #[test]
    fn onion_names() {
        let v3 = format!("{}.onion", "a2".repeat(28));
        assert!(is_onion(&v3));
        for refused in [
            "zec.rocks".to_string(),
            "expyuzz4wqqyqhjn.onion".to_string(),           // v2, retired
            format!("{}.onion", "A2".repeat(28)),          // URLs carry it in lower case
            format!("{}.onion", "a1".repeat(28)),          // 1 is not base32
            format!("www.{v3}"),
            format!("{}.onion", "a2".repeat(27)),
        ] {
            assert!(!is_onion(&refused), "{refused}");
        }
        assert!(is_onion_url(&format!("http://{v3}:9067")));
        assert!(!is_onion_url(&format!("https://{v3}:443")), "TLS servers take the https path");
        assert!(!is_onion_url("http://zec.rocks:80"));
    }

    #[tokio::test]
    async fn an_onion_needs_tor() {
        let server = format!("http://{}.onion:9067", "a2".repeat(28));
        assert!(matches!(connect(&server, &ProxyAddr::direct(), Isolation::fresh()).await, Err(NetError::BadUrl(_))));
    }
}
