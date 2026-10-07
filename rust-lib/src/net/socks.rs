//! A socks5h connector for tonic: names resolve at the proxy, and the SOCKS
//! credentials choose the Tor circuit (IsolateSOCKSAuth).

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use http::Uri;
use hyper_util::rt::TokioIo;
use rand::RngExt;
use tokio::net::TcpStream;
use tokio_socks::tcp::Socks5Stream;

/// The proxy URL scheme we accept. Plain socks5 resolves names locally and http
/// proxies cannot isolate circuits, so both are refused.
pub const SCHEME: &str = "socks5h://";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProxyAddr {
    pub host: String,
    pub port: u16,
}

impl ProxyAddr {
    pub fn parse(url: &str) -> Result<Self, String> {
        let rest = url
            .strip_prefix(SCHEME)
            .ok_or_else(|| format!("proxy must be {SCHEME}host:port"))?;
        let rest = rest.trim_end_matches('/');
        let (host, port) = rest
            .rsplit_once(':')
            .ok_or_else(|| "proxy needs a port".to_string())?;
        let port = port.parse::<u16>().map_err(|_| "bad proxy port".to_string())?;
        if host.is_empty() {
            return Err("proxy needs a host".into());
        }
        Ok(Self { host: host.trim_matches(['[', ']']).to_string(), port })
    }
}

/// SOCKS username and password. Tor never lets streams with different
/// credentials share a circuit.
#[derive(Clone)]
pub struct Isolation {
    user: String,
    pass: String,
}

impl Isolation {
    /// A circuit no other request will use.
    pub fn fresh() -> Self {
        let mut bytes = [0u8; 24];
        rand::rng().fill(&mut bytes[..]);
        // Hex never starts with "<", so arti's "<torS0X>" handling cannot trigger.
        Self { user: hex::encode(&bytes[..12]), pass: hex::encode(&bytes[12..]) }
    }
}

#[derive(Clone)]
pub struct Socks5hConnector {
    proxy: ProxyAddr,
    isolation: Isolation,
}

impl Socks5hConnector {
    pub fn new(proxy: ProxyAddr, isolation: Isolation) -> Self {
        Self { proxy, isolation }
    }
}

type BoxError = Box<dyn std::error::Error + Send + Sync>;

impl tower::Service<Uri> for Socks5hConnector {
    type Response = TokioIo<Socks5Stream<TcpStream>>;
    type Error = BoxError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, BoxError>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), BoxError>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, uri: Uri) -> Self::Future {
        let proxy = self.proxy.clone();
        let iso = self.isolation.clone();
        Box::pin(async move {
            let host = uri.host().ok_or("server URL has no host")?.to_string();
            let port = uri.port_u16().unwrap_or(match uri.scheme_str() {
                Some("http") => 80,
                _ => 443,
            });
            let stream = Socks5Stream::connect_with_password(
                (proxy.host.as_str(), proxy.port),
                (host.as_str(), port),
                &iso.user,
                &iso.pass,
            )
            .await?;
            Ok(TokioIo::new(stream))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn proxy_urls() {
        assert_eq!(
            ProxyAddr::parse("socks5h://127.0.0.1:9050").unwrap(),
            ProxyAddr { host: "127.0.0.1".into(), port: 9050 }
        );
        assert!(ProxyAddr::parse("socks5://127.0.0.1:9050").is_err());
        assert!(ProxyAddr::parse("http://127.0.0.1:8118").is_err());
        assert!(ProxyAddr::parse("socks5h://127.0.0.1").is_err());
    }

    #[test]
    fn fresh_isolation_differs() {
        let (a, b) = (Isolation::fresh(), Isolation::fresh());
        assert_ne!(a.user, b.user);
        assert_eq!(a.user.len(), 24);
    }
}
