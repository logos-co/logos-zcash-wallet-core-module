//! Transaction details on a timer: each request goes out on a fresh circuit, to a
//! random server, after a random delay, so requests cannot be tied together.

use std::time::{Duration, SystemTime};

use futures_util::TryStreamExt;
use rand::RngExt;
use zcash_client_backend::data_api::{TransactionDataRequest, TransactionStatus};
use zcash_client_backend::proto::service::{BlockId, BlockRange, RawTransaction, TransparentAddressBlockFilter, TxFilter};
use zcash_keys::encoding::AddressCodec;
use zcash_primitives::transaction::Transaction;
use zcash_protocol::consensus::{BlockHeight, BranchId};
use zcash_protocol::TxId;

use crate::net::client::{connect, Client, NetError};
use crate::net::socks::{Isolation, ProxyAddr};
use crate::network::ZNetwork;

/// What a finished request hands back to the wallet thread.
pub enum Fetched {
    Status(TxId, TransactionStatus),
    Txs(Vec<(Transaction, Option<BlockHeight>)>),
}

/// Spreads requests out; the largest delay is short enough to stay responsive.
pub const MAX_DELAY: Duration = Duration::from_secs(20);

pub fn random_delay() -> Duration {
    Duration::from_millis(rand::rng().random_range(0..MAX_DELAY.as_millis() as u64))
}

pub fn pick<'a>(servers: &'a [String]) -> &'a str {
    &servers[rand::rng().random_range(0..servers.len())]
}

/// lightwalletd marks mempool transactions with height 0 and orphaned ones with u64::MAX.
fn mined(raw: &RawTransaction) -> Option<BlockHeight> {
    match raw.height {
        0 | u64::MAX => None,
        h => u32::try_from(h).ok().map(BlockHeight::from),
    }
}

fn parse(params: ZNetwork, raw: &RawTransaction, tip: u32) -> Result<(Transaction, Option<BlockHeight>), String> {
    let height = mined(raw);
    let branch = BranchId::for_height(&params, height.unwrap_or(BlockHeight::from(tip + 1)));
    let tx = Transaction::read(&raw.data[..], branch).map_err(|e| format!("transaction does not parse: {e}"))?;
    Ok((tx, height))
}

pub async fn fetch(
    params: ZNetwork,
    server: &str,
    proxy: &ProxyAddr,
    req: &TransactionDataRequest,
    tip: u32,
) -> Result<Fetched, NetError> {
    let status = |status| NetError::Status { server: server.into(), status };
    let bad = |detail: String| NetError::Status { server: server.into(), status: tonic::Status::data_loss(detail) };
    let mut c = connect(server, proxy, Isolation::fresh()).await?;
    match req {
        TransactionDataRequest::GetStatus(txid) | TransactionDataRequest::Enhancement(txid) => {
            let filter = TxFilter { block: None, index: 0, hash: txid.as_ref().to_vec() };
            let raw = match c.get_transaction(filter).await {
                Ok(r) => r.into_inner(),
                Err(s) if s.code() == tonic::Code::NotFound => {
                    return Ok(Fetched::Status(*txid, TransactionStatus::TxidNotRecognized));
                }
                Err(s) => return Err(status(s)),
            };
            if matches!(req, TransactionDataRequest::GetStatus(_)) {
                let st = match mined(&raw) {
                    Some(h) => TransactionStatus::Mined(h),
                    None => TransactionStatus::NotInMainChain,
                };
                return Ok(Fetched::Status(*txid, st));
            }
            Ok(Fetched::Txs(vec![parse(params, &raw, tip).map_err(bad)?]))
        }
        TransactionDataRequest::TransactionsInvolvingAddress(r) => {
            // The protocol needs both ends; an open range ends at the tip.
            let end = r.block_range_end().map_or(tip, |e| u32::from(e) - 1);
            let txs = address_txs(&mut c, params, server, r.address().encode(&params), u32::from(r.block_range_start()), end, tip).await?;
            Ok(Fetched::Txs(txs))
        }
    }
}

/// Transactions involving a transparent address, mined from `start` to `end`.
pub async fn received(
    params: ZNetwork,
    server: &str,
    proxy: &ProxyAddr,
    address: String,
    start: u32,
    end: u32,
    tip: u32,
) -> Result<Vec<(Transaction, Option<BlockHeight>)>, NetError> {
    let mut c = connect(server, proxy, Isolation::fresh()).await?;
    address_txs(&mut c, params, server, address, start, end, tip).await
}

async fn address_txs(
    c: &mut Client,
    params: ZNetwork,
    server: &str,
    address: String,
    start: u32,
    end: u32,
    tip: u32,
) -> Result<Vec<(Transaction, Option<BlockHeight>)>, NetError> {
    let status = |status| NetError::Status { server: server.into(), status };
    let bad = |detail: String| NetError::Status { server: server.into(), status: tonic::Status::data_loss(detail) };
    let range = BlockRange {
        start: Some(BlockId { height: start as u64, hash: vec![] }),
        end: Some(BlockId { height: end as u64, hash: vec![] }),
        pool_types: vec![],
    };
    let arg = TransparentAddressBlockFilter { address, range: Some(range) };
    let raws: Vec<RawTransaction> =
        c.get_taddress_transactions(arg).await.map_err(status)?.into_inner().try_collect().await.map_err(status)?;
    raws.iter().map(|raw| parse(params, raw, tip)).collect::<Result<Vec<_>, _>>().map_err(bad)
}

/// Address queries wait for the time the wallet suggested; others go out now.
pub fn not_before(req: &TransactionDataRequest) -> Option<SystemTime> {
    match req {
        TransactionDataRequest::TransactionsInvolvingAddress(r) => r.request_at(),
        _ => None,
    }
}
