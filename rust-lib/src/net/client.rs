//! gRPC clients for lightwalletd-protocol servers, one Tor circuit per isolation.

use std::net::{IpAddr, SocketAddr};
use std::time::Duration;

use http::Uri;
use serde::Deserialize;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};
use tower::ServiceExt;
use zcash_client_backend::proto::service::compact_tx_streamer_client::CompactTxStreamerClient;

use super::ipc::{local_node, IpcChannel, TransportError, LOCAL_NODE_URL};
use super::socks::{Isolation, ProxyAddr, Socks5hConnector};
use super::tor::TorChannel;

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
    #[error("the built-in Tor is not available")]
    NoTor,
    #[error("{0} does not resolve to an address on your network")]
    NotLan(String),
}

const CONNECT_TIMEOUT: Duration = Duration::from_secs(45);

/// What a call asks a server for, as the node module's route table names it. With Tor for what
/// identifies you, `sync` and `tip` go direct and the rest name the wallet, so go through Tor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CallClass {
    /// Compact blocks, tree states and subtree roots.
    Sync,
    /// Tips and server info, and the cross-check's read of the chain the wallet syncs from.
    Tip,
    /// GetTransaction by txid.
    Details,
    /// Transactions by transparent address.
    Taddr,
    /// SendTransaction, and asking another operator whether it has the transaction.
    Broadcast,
    Mempool,
    /// The core's own: another operator's tip and tree state, checking the chain the wallet reads.
    /// Never direct by class, so that operator can't tie it to the IP that syncs.
    Check,
}

/// How a call reaches its server.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Path {
    Local,
    Lan,
    Direct,
    /// The built-in Tor, over IPC.
    Tor,
    Socks,
}

/// The path for a `class` call to `server`. An onion service takes Tor whatever the class.
pub fn path(server: &str, proxy: &ProxyAddr, class: CallClass) -> Path {
    if server == LOCAL_NODE_URL {
        Path::Local
    } else if is_lan_url(server) {
        Path::Lan
    } else if proxy.is_direct() || proxy.bypasses(server) || (proxy.dials_directly(class) && !is_onion_url(server)) {
        Path::Direct
    } else if proxy.is_tor() {
        Path::Tor
    } else {
        Path::Socks
    }
}

/// Every dial in a unit test, with its class, so a test can say which class a call site takes.
#[cfg(test)]
pub(crate) mod dials {
    use super::CallClass;
    use std::cell::RefCell;

    thread_local!(static SEEN: RefCell<Vec<(String, CallClass)>> = const { RefCell::new(Vec::new()) });

    pub fn note(server: &str, class: CallClass) {
        SEEN.with(|s| s.borrow_mut().push((server.into(), class)));
    }

    pub fn take() -> Vec<(String, CallClass)> {
        SEEN.with(|s| std::mem::take(&mut *s.borrow_mut()))
    }
}

/// Test builds only: a direct dial to a mapped host goes to a local plain-HTTP/2 front instead.
#[cfg(feature = "test-dial")]
pub mod test_dial {
    use std::collections::BTreeMap;
    use std::sync::RwLock;

    static FRONTS: RwLock<BTreeMap<String, String>> = RwLock::new(BTreeMap::new());

    pub fn set(host: &str, front: &str) {
        FRONTS.write().unwrap().insert(host.into(), front.into());
    }

    pub fn front(host: &str) -> Option<String> {
        FRONTS.read().unwrap().get(host).cloned()
    }
}

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

/// An address on the user's own network, which Tor cannot reach: loopback, the private
/// IPv4 ranges, CGNAT's 100.64/10 (Tailscale), link-local, and IPv6 unique-local.
pub fn is_lan_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => {
            let [a, b, ..] = ip.octets();
            ip.is_loopback() || ip.is_private() || ip.is_link_local() || (a == 100 && (64..128).contains(&b))
        }
        IpAddr::V6(ip) => ip.is_loopback() || (ip.segments()[0] & 0xfe00) == 0xfc00 || (ip.segments()[0] & 0xffc0) == 0xfe80,
    }
}

/// A LAN address, or a name only the user's network resolves: mDNS `.local`, and the `.lan`,
/// `.home.arpa` and `.internal` names home routers hand out.
pub fn is_lan_host(host: &str) -> bool {
    let host = host.trim_start_matches('[').trim_end_matches(']').trim_end_matches('.').to_ascii_lowercase();
    match host.parse::<IpAddr>() {
        Ok(ip) => is_lan_ip(ip),
        Err(_) => host == "localhost" || [".local", ".lan", ".home.arpa", ".internal"].iter().any(|s| host.len() > s.len() && host.ends_with(s)),
    }
}

/// A server on the user's own network, reached directly: Tor cannot reach it.
pub fn is_lan_url(server: &str) -> bool {
    server.parse::<Uri>().is_ok_and(|u| matches!(u.scheme_str(), Some("http" | "https")) && u.host().is_some_and(is_lan_host))
}

/// Opens a TLS channel to `server` (https://host:port), or a plain one to an onion service,
/// through the proxy, on the circuit `isolation` selects: the built-in Tor over IPC, which does
/// the TLS itself, or the user's own over SOCKS. A server on the user's own network, one the
/// proxy bypasses, or any but an onion for a class it dials directly, is dialled directly;
/// `logos://zebrad_module` is the local node over IPC.
pub async fn connect(server: &str, proxy: &ProxyAddr, class: CallClass, isolation: Isolation) -> Result<Client, NetError> {
    #[cfg(test)]
    dials::note(server, class);
    let path = path(server, proxy, class);
    if path == Path::Local {
        let call = local_node().ok_or(NetError::NoLocalNode)?;
        let channel = GrpcChannel::new(IpcChannel::new(call));
        return Ok(CompactTxStreamerClient::new(channel).max_decoding_message_size(16 * 1024 * 1024));
    }
    let uri: Uri = server.parse().map_err(|_| NetError::BadUrl(server.into()))?;
    if path == Path::Lan {
        let host = uri.host().unwrap_or_default().trim_start_matches('[').trim_end_matches(']').to_string();
        // A name must resolve onto the user's network, or this connection would bypass Tor.
        if host.parse::<IpAddr>().is_err() {
            let port = uri.port_u16().unwrap_or(if uri.scheme_str() == Some("https") { 443 } else { 80 });
            let addrs: Vec<SocketAddr> = tokio::net::lookup_host((host.as_str(), port)).await.map(Iterator::collect).unwrap_or_default();
            if addrs.is_empty() || !addrs.iter().all(|a| is_lan_ip(a.ip())) {
                return Err(NetError::NotLan(server.into()));
            }
        }
        let wrap = |source| NetError::Connect { server: server.into(), source };
        let endpoint = Endpoint::from_shared(server.to_string()).map_err(wrap)?.connect_timeout(CONNECT_TIMEOUT);
        let endpoint = match uri.scheme_str() {
            Some("https") => endpoint.tls_config(ClientTlsConfig::new().with_webpki_roots().domain_name(host)).map_err(wrap)?,
            _ => endpoint,
        };
        return Ok(client(endpoint.connect().await.map_err(wrap)?));
    }
    let host = uri.host().ok_or_else(|| NetError::BadUrl(server.into()))?.to_string();
    let wrap = |source| NetError::Connect { server: server.into(), source };
    if path == Path::Direct {
        // Without Tor only https: an onion service needs it, and plain http would be readable.
        if uri.scheme_str() != Some("https") {
            return Err(NetError::BadUrl(server.into()));
        }
        #[cfg(feature = "test-dial")]
        if let Some(front) = test_dial::front(&host) {
            return Ok(client(Endpoint::from_shared(front).map_err(wrap)?.connect().await.map_err(wrap)?));
        }
        let endpoint = Endpoint::from_shared(server.to_string()).map_err(wrap)?.connect_timeout(CONNECT_TIMEOUT);
        let endpoint = endpoint.tls_config(ClientTlsConfig::new().with_webpki_roots().domain_name(host)).map_err(wrap)?;
        return Ok(client(endpoint.connect().await.map_err(wrap)?));
    }
    if path == Path::Tor {
        if !(uri.scheme_str() == Some("https") || is_onion_url(server)) {
            return Err(NetError::BadUrl(server.into()));
        }
        let tor = super::tor::tor().ok_or(NetError::NoTor)?;
        let channel = GrpcChannel::new(TorChannel::new(tor, server, &isolation.key()));
        return Ok(CompactTxStreamerClient::new(channel).max_decoding_message_size(16 * 1024 * 1024));
    }
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

    #[test]
    fn lan_addresses() {
        for host in ["127.0.0.1", "10.0.0.5", "172.16.3.4", "172.31.255.1", "192.168.1.10", "100.64.0.1", "100.127.1.1", "169.254.1.1", "[::1]", "[fd12:3456::1]", "[fe80::1]"] {
            assert!(is_lan_host(host), "{host}");
        }
        for host in ["localhost", "nas.local", "framework.lan", "Node.LAN.", "box.home.arpa", "zebra.internal"] {
            assert!(is_lan_host(host), "{host}");
        }
        for host in ["8.8.8.8", "172.32.0.1", "100.128.0.1", "193.168.1.1", "[2001:db8::1]", "zec.rocks", "lan", ".local", "local.example.com", "planet"] {
            assert!(!is_lan_host(host), "{host}");
        }
        assert!(is_lan_url("http://192.168.1.10:9067"));
        assert!(is_lan_url("https://[fd00::2]:9067"));
        assert!(is_lan_url("http://framework.lan:9067"));
        assert!(!is_lan_url("http://zec.rocks:9067"));
        assert!(!is_lan_url("socks5h://192.168.1.10:9050"));
    }

    #[tokio::test]
    async fn a_lan_name_must_resolve_onto_the_lan() {
        let server = "http://this-name-does-not-exist.home.arpa:9067";
        assert!(matches!(connect(server, &ProxyAddr::direct(), CallClass::Sync, Isolation::fresh()).await, Err(NetError::NotLan(_))));
    }

    /// Needs a Tor SOCKS port in ZCASH_TEST_TOR, e.g. socks5h://127.0.0.1:19050.
    #[tokio::test]
    #[ignore]
    async fn lightd_info_over_tor() {
        let proxy = ProxyAddr::parse(&std::env::var("ZCASH_TEST_TOR").unwrap()).unwrap();
        let mut c = connect("https://testnet.zec.rocks:443", &proxy, CallClass::Tip, Isolation::fresh()).await.unwrap();
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
        let mut c = connect(&server, &proxy, CallClass::Tip, Isolation::fresh()).await.unwrap();
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
        assert!(matches!(connect(&server, &ProxyAddr::direct(), CallClass::Sync, Isolation::fresh()).await, Err(NetError::BadUrl(_))));
        // Even when the proxy is told to go around it.
        let tor = ProxyAddr::parse("socks5h://127.0.0.1:9").unwrap().bypassing([server.clone()]);
        assert!(matches!(connect(&server, &tor, CallClass::Sync, Isolation::fresh()).await, Err(NetError::BadUrl(_))));
    }

    #[test]
    fn classes_choose_the_path_with_tor_for_what_identifies_you() {
        let (rocks, onion, lan) = ("https://zec.rocks:443", format!("http://{}.onion:9067", "a2".repeat(28)), "http://192.168.1.10:9067");
        let split = ProxyAddr::tor().direct_for([CallClass::Sync, CallClass::Tip]);
        for class in [CallClass::Sync, CallClass::Tip] {
            assert_eq!(path(rocks, &split, class), Path::Direct, "{class:?}");
            assert_eq!(path(&onion, &split, class), Path::Tor, "an onion service takes Tor for {class:?}");
        }
        for class in [CallClass::Details, CallClass::Taddr, CallClass::Broadcast, CallClass::Mempool, CallClass::Check] {
            assert_eq!(path(rocks, &split, class), Path::Tor, "{class:?}");
            assert_eq!(path(lan, &split, class), Path::Lan, "{class:?}");
        }
        assert_eq!(path(LOCAL_NODE_URL, &split, CallClass::Details), Path::Local);
        let own = ProxyAddr::parse("socks5h://127.0.0.1:9050").unwrap().direct_for([CallClass::Sync]);
        assert_eq!((path(rocks, &own, CallClass::Sync), path(rocks, &own, CallClass::Tip)), (Path::Direct, Path::Socks));
        // Without directFor, every class goes through Tor, as before.
        for class in [CallClass::Sync, CallClass::Tip, CallClass::Details] {
            assert_eq!(path(rocks, &ProxyAddr::tor(), class), Path::Tor);
        }
    }
}
