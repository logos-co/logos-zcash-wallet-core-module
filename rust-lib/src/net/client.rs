//! gRPC clients for lightwalletd-protocol servers, one Tor circuit per isolation.

use std::time::Duration;

use http::Uri;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};
use zcash_client_backend::proto::service::compact_tx_streamer_client::CompactTxStreamerClient;

use super::socks::{Isolation, ProxyAddr, Socks5hConnector};

pub type Client = CompactTxStreamerClient<Channel>;

#[derive(Debug, thiserror::Error)]
pub enum NetError {
    #[error("bad server URL {0}")]
    BadUrl(String),
    #[error("connect to {server}: {source}")]
    Connect { server: String, source: tonic::transport::Error },
    #[error("{server}: {status}")]
    Status { server: String, status: tonic::Status },
}

const CONNECT_TIMEOUT: Duration = Duration::from_secs(45);

/// Opens a TLS channel to `server` (https://host:port) through the proxy, on the
/// circuit `isolation` selects. Only https URLs are accepted.
pub async fn connect(server: &str, proxy: &ProxyAddr, isolation: Isolation) -> Result<Client, NetError> {
    let uri: Uri = server.parse().map_err(|_| NetError::BadUrl(server.into()))?;
    if uri.scheme_str() != Some("https") {
        return Err(NetError::BadUrl(server.into()));
    }
    let host = uri.host().ok_or_else(|| NetError::BadUrl(server.into()))?.to_string();
    let wrap = |source| NetError::Connect { server: server.into(), source };
    let endpoint = Endpoint::from_shared(server.to_string())
        .map_err(wrap)?
        .tls_config(ClientTlsConfig::new().with_webpki_roots().domain_name(host))
        .map_err(wrap)?
        .connect_timeout(CONNECT_TIMEOUT);
    let channel = endpoint
        .connect_with_connector(Socks5hConnector::new(proxy.clone(), isolation))
        .await
        .map_err(wrap)?;
    Ok(CompactTxStreamerClient::new(channel).max_decoding_message_size(16 * 1024 * 1024))
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
}
