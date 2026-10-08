//! Trial-decrypts a block range with a viewing key, outside any wallet database, to tell
//! "not on chain" from "on chain but missed".
//!
//! usage: find_payment <testnet|mainnet> <ufvk file> <first> [last]
//!        find_payment <testnet|mainnet> <ufvk file> mempool
//!        find_payment <testnet|mainnet> <ufvk file> tx <txid>
//! env: SERVER (default https://testnet.zec.rocks:443), PROXY (default socks5h://127.0.0.1:9050)

use zcash_client_backend::scanning::{scan_block, ScanningKeys, SpendIdentifiers};
use zcash_keys::keys::UnifiedFullViewingKey;
use zcash_wallet_core::net::socks::{Isolation, ProxyAddr};
use zcash_wallet_core::network::ZNetwork;
use zcash_wallet_core::sync::fetch;

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let net = ZNetwork::parse(&args[1]).expect("network");
    let ufvk =
        UnifiedFullViewingKey::decode(&net, std::fs::read_to_string(&args[2]).unwrap().trim())
            .expect("ufvk");
    let server = std::env::var("SERVER").unwrap_or("https://testnet.zec.rocks:443".into());
    let proxy =
        ProxyAddr::parse(&std::env::var("PROXY").unwrap_or("socks5h://127.0.0.1:9050".into()))
            .unwrap();
    if args[3] == "tx" {
        return one_tx(net, ufvk, &server, &proxy, &args[4]).await;
    }
    if args[3] == "mempool" {
        return mempool(net, ufvk, &server, &proxy).await;
    }
    let first: u32 = args[3].parse().unwrap();
    let last: u32 = match args.get(4) {
        Some(l) => l.parse().unwrap(),
        None => {
            fetch::tip_and_info(&server, &proxy, Isolation::fresh())
                .await
                .unwrap()
                .0
        }
    };
    println!(
        "ufvk receivers: orchard={} sapling={} transparent={}",
        ufvk.orchard().is_some(),
        ufvk.sapling().is_some(),
        ufvk.p2pkh().is_some()
    );
    let keys = ScanningKeys::from_account_ufvks([(0u32, ufvk)]);
    let (mut outputs, mut hits) = (0usize, 0usize);
    let mut h = first;
    while h <= last {
        let end = (h + 99).min(last);
        for block in fetch::fetch_blocks(&server, &proxy, h, end).await.unwrap() {
            let height = block.height;
            if std::env::var_os("HASHES").is_some() {
                let mut h = block.hash.clone();
                h.reverse();
                println!("{height} {} time {}", hex::encode(h), block.time);
            }
            for tx in &block.vtx {
                outputs += tx.outputs.len() + tx.actions.len() + tx.ironwood_actions.len();
            }
            let scanned = scan_block(&net, block, &keys, &SpendIdentifiers::empty(), None, |_| {
                Ok::<_, std::convert::Infallible>(None)
            })
            .unwrap_or_else(|e| panic!("scan {height}: {e:?}"));
            for tx in scanned.transactions() {
                hits += 1;
                let zat = |v: u64| v as f64 / 1e8;
                let s: u64 = tx
                    .sapling_outputs()
                    .iter()
                    .map(|o| o.note().value().inner())
                    .sum();
                let o: u64 = tx
                    .orchard_outputs()
                    .iter()
                    .map(|o| o.note().0.value().inner())
                    .sum();
                let i: u64 = tx
                    .ironwood_outputs()
                    .iter()
                    .map(|o| o.note().0.value().inner())
                    .sum();
                println!(
                    "{height} {} sapling={} orchard={} ironwood={}",
                    tx.txid(),
                    zat(s),
                    zat(o),
                    zat(i)
                );
            }
        }
        h = end + 1;
    }
    println!("scanned {first}..={last}: {outputs} shielded outputs, {hits} wallet transactions");
}

/// Wraps the mempool's compact transactions in a block above the tip, so the scanner
/// trial-decrypts them; tree positions are made up and only detection counts.
async fn mempool(net: ZNetwork, ufvk: UnifiedFullViewingKey, server: &str, proxy: &ProxyAddr) {
    use futures_util::TryStreamExt;
    use zcash_client_backend::proto::compact_formats::{ChainMetadata, CompactBlock};
    use zcash_client_backend::proto::service::GetMempoolTxRequest;
    let tip = fetch::tip_and_info(server, proxy, Isolation::fresh())
        .await
        .unwrap()
        .0;
    let mut client = zcash_wallet_core::net::client::connect(server, proxy, Isolation::fresh())
        .await
        .unwrap();
    let txs: Vec<_> = client
        .get_mempool_tx(GetMempoolTxRequest {
            exclude_txid_suffixes: vec![],
            pool_types: vec![],
        })
        .await
        .unwrap()
        .into_inner()
        .try_collect()
        .await
        .unwrap();
    println!("mempool at tip {tip}: {} transactions", txs.len());
    if let Some(t) = std::env::var("TADDR").ok() {
        use zcash_client_backend::proto::service::GetAddressUtxosArg;
        let utxos = client
            .get_address_utxos(GetAddressUtxosArg {
                addresses: vec![t.clone()],
                start_height: 0,
                max_entries: 0,
            })
            .await
            .unwrap()
            .into_inner();
        println!(
            "{t}: {} utxos {:?}",
            utxos.address_utxos.len(),
            utxos
                .address_utxos
                .iter()
                .map(|u| (u.height, u.value_zat))
                .collect::<Vec<_>>()
        );
    }
    if txs.is_empty() {
        return;
    }
    let big = 1 << 24;
    let block = CompactBlock {
        height: (tip + 1) as u64,
        vtx: txs,
        chain_metadata: Some(ChainMetadata {
            sapling_commitment_tree_size: big,
            orchard_commitment_tree_size: big,
            ironwood_commitment_tree_size: big,
        }),
        ..Default::default()
    };
    let keys = ScanningKeys::from_account_ufvks([(0u32, ufvk)]);
    let scanned = scan_block(&net, block, &keys, &SpendIdentifiers::empty(), None, |_| {
        Ok::<_, std::convert::Infallible>(None)
    })
    .unwrap();
    for tx in scanned.transactions() {
        let s: u64 = tx
            .sapling_outputs()
            .iter()
            .map(|o| o.note().value().inner())
            .sum();
        let o: u64 = tx
            .orchard_outputs()
            .iter()
            .map(|o| o.note().0.value().inner())
            .sum();
        let i: u64 = tx
            .ironwood_outputs()
            .iter()
            .map(|o| o.note().0.value().inner())
            .sum();
        println!(
            "mempool {} sapling={} orchard={} ironwood={}",
            tx.txid(),
            s as f64 / 1e8,
            o as f64 / 1e8,
            i as f64 / 1e8
        );
    }
    println!(
        "{} wallet transactions in the mempool",
        scanned.transactions().len()
    );
}

/// Fetches one transaction and decrypts it in full with the viewing key.
async fn one_tx(
    net: ZNetwork,
    ufvk: UnifiedFullViewingKey,
    server: &str,
    proxy: &ProxyAddr,
    txid: &str,
) {
    use zcash_client_backend::proto::service::TxFilter;
    use zcash_primitives::transaction::Transaction;
    use zcash_protocol::consensus::{BlockHeight, BranchId};
    let mut hash = hex::decode(txid).expect("hex txid");
    hash.reverse();
    let mut client = zcash_wallet_core::net::client::connect(server, proxy, Isolation::fresh())
        .await
        .unwrap();
    let raw = client
        .get_transaction(TxFilter {
            block: None,
            index: 0,
            hash,
        })
        .await
        .unwrap()
        .into_inner();
    println!("height {} ({} bytes)", raw.height, raw.data.len());
    let tip = fetch::tip_and_info(server, proxy, Isolation::fresh())
        .await
        .unwrap()
        .0;
    let mined = (raw.height > 0 && raw.height < u32::MAX as u64)
        .then(|| BlockHeight::from(raw.height as u32));
    let at = mined.unwrap_or(BlockHeight::from(tip + 1));
    let tx = Transaction::read(&raw.data[..], BranchId::for_height(&net, at)).expect("parse");
    println!(
        "version {:?}, transparent outs {}, sapling outputs {}, orchard actions {}, ironwood actions {}, expiry {}",
        tx.version(),
        tx.transparent_bundle().map_or(0, |b| b.vout.len()),
        tx.sapling_bundle().map_or(0, |b| b.shielded_outputs().len()),
        tx.orchard_bundle().map_or(0, |b| b.actions().len()),
        tx.ironwood_bundle().map_or(0, |b| b.actions().len()),
        u32::from(tx.expiry_height()),
    );
    let ufvks = std::collections::HashMap::from([(0u32, ufvk)]);
    let d = zcash_client_backend::decrypt_transaction(
        &net,
        mined,
        Some(BlockHeight::from(tip)),
        &tx,
        &ufvks,
    );
    for o in d.sapling_outputs() {
        println!(
            "  sapling output: {} zat, {:?}",
            o.note_value().into_u64(),
            o.transfer_type()
        );
    }
    for o in d.orchard_outputs() {
        println!(
            "  orchard output: {} zat, {:?}",
            o.note().0.value().inner(),
            o.transfer_type()
        );
    }
    for o in d.ironwood_outputs() {
        println!(
            "  ironwood output: {} zat, {:?}",
            o.note().0.value().inner(),
            o.transfer_type()
        );
    }
    println!("tip {tip}");
}
