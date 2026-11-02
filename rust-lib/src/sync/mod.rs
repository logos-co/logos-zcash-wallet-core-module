//! The sync loop: a fixed download grid, an encrypted cache, and scans bounded by work.

pub mod cache;
pub mod crosscheck;
pub mod frontier;
pub mod driver;
pub mod enhance;
pub mod fetch;

#[cfg(test)]
mod classes {
    use zcash_client_backend::data_api::{OutputStatusFilter, TransactionDataRequest, TransactionStatusFilter};
    use zcash_protocol::consensus::BlockHeight;
    use zcash_protocol::TxId;
    use zcash_transparent::address::TransparentAddress;

    use crate::net::client::{dials, CallClass};
    use crate::net::socks::{Isolation, ProxyAddr};
    use crate::network::ZNetwork;

    /// Refuses at once, so each call stops after its first dial.
    const SERVER: &str = "http://127.0.0.1:9";

    fn classes() -> Vec<CallClass> {
        dials::take().into_iter().map(|(s, c)| if s == SERVER { c } else { panic!("dialled {s}") }).collect()
    }

    #[tokio::test]
    async fn every_call_site_takes_its_class() {
        use CallClass::*;
        let p = ProxyAddr::direct();
        let net = ZNetwork::Test;
        dials::take();
        assert!(super::fetch::fetch_chunk(SERVER, &p, 10_000, 10_999).await.is_err());
        assert!(super::fetch::fetch_blocks(SERVER, &p, 10_000, 10_010).await.is_err());
        assert!(super::fetch::tree_state(SERVER, &p, 9_999).await.is_err());
        assert!(super::fetch::subtree_roots(SERVER, &p, 1, 0).await.is_err());
        assert_eq!(classes(), [Sync, Sync, Sync, Sync]);
        assert!(super::fetch::tip_and_info(SERVER, &p, Isolation::fresh()).await.is_err());
        assert!(super::crosscheck::check(net, SERVER, &p).await.is_err());
        assert_eq!(classes(), [Tip, Tip]);
        let txid = TxId::from_bytes([7; 32]);
        for req in [TransactionDataRequest::GetStatus(txid), TransactionDataRequest::Enhancement(txid)] {
            assert!(super::enhance::fetch(net, SERVER, &p, &req, 1).await.is_err());
        }
        let taddr = TransparentAddress::PublicKeyHash([1; 20]);
        let by_address = TransactionDataRequest::transactions_involving_address(
            taddr, BlockHeight::from(1), None, None, TransactionStatusFilter::All, OutputStatusFilter::All);
        assert_eq!(super::enhance::class(&by_address), Taddr);
        assert!(super::enhance::fetch(net, SERVER, &p, &by_address, 1).await.is_err());
        assert!(super::enhance::received(net, SERVER, &p, "tmBsTi2xWTjUdEXnuTceL7fecEQKeWaPDJd".into(), 1, 2, 2).await.is_err());
        assert_eq!(classes(), [Details, Details, Taddr, Taddr]);
        assert!(super::fetch::send_transaction(SERVER, &p, vec![0]).await.is_err());
        assert!(super::fetch::knows_transaction(SERVER, &p, &[7; 32]).await.is_err());
        assert_eq!(classes(), [Broadcast, Broadcast]);
    }

    #[test]
    fn lookups_wait_longer_while_sync_goes_direct() {
        use super::enhance::{delay, MAX_DELAY, MAX_JITTER};
        assert!((0..50).all(|_| delay(false) < MAX_DELAY));
        assert!((0..50).all(|_| delay(true) < MAX_DELAY + MAX_JITTER));
        assert!((0..50).any(|_| delay(true) >= MAX_DELAY), "the jitter adds to the spacing");
    }
}
