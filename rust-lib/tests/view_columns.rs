//! Prints the columns of the wallet views the history query reads.

use zcash_wallet_core::keys::Phrase;
use zcash_wallet_core::network::ZNetwork;
use zcash_wallet_core::wallet::WalletDir;
use zcash_client_backend::data_api::chain::ChainState;
use zcash_primitives::block::BlockHash;
use zcash_protocol::consensus::{NetworkUpgrade, Parameters};

#[test]
fn history_views_have_the_columns_we_read() {
    let root = tempfile::tempdir().unwrap();
    let net = ZNetwork::Test;
    let w = WalletDir::new(root.path(), net, "v");
    let h = net.activation_height(NetworkUpgrade::Sapling).unwrap() - 1;
    w.create(net, "v", "pw", &Phrase::generate(), ChainState::empty(h, BlockHash([0; 32])), Some(10)).unwrap();
    let key = w.unlock_db_key("pw").unwrap();
    let conn = w.open_conn(&key).unwrap();
    for view in ["v_transactions_with_pending_migrations", "v_tx_outputs"] {
        let stmt = conn.prepare(&format!("SELECT * FROM {view} LIMIT 0")).unwrap();
        let cols: Vec<&str> = stmt.column_names();
        println!("{view}: {}", cols.join(", "));
    }
    // The history query itself must prepare against a fresh wallet.
    let page = zcash_wallet_core::history::page(&conn, net, w.account().unwrap(), 0).unwrap();
    assert_eq!(page["rows"].as_array().unwrap().len(), 0);
}
