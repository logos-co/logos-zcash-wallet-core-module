//! Zcash wallet engine for Logos.

pub mod keys;
pub mod net;
pub mod network;
pub mod storage;
pub mod sync;
pub mod wallet;

#[cfg(test)]
mod smoke {
    use rand::SeedableRng;
    use secrecy::SecretVec;
    use zcash_client_backend::data_api::{chain::ChainState, AccountBirthday, WalletWrite};
    use zcash_client_backend::util::SystemClock;
    use zcash_client_sqlite::{wallet::init::init_wallet_db, WalletDb};
    use zcash_primitives::block::BlockHash;
    use zcash_protocol::consensus::{Network, NetworkUpgrade, Parameters};

    #[test]
    fn sqlcipher_wallet_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wallet.db");
        let key = [7u8; 32];

        let conn = crate::storage::open_encrypted(&path, &key).unwrap();
        let version: String = conn.query_row("PRAGMA cipher_version", [], |r| r.get(0)).unwrap();
        assert!(!version.is_empty());

        let rng = rand_chacha::ChaCha20Rng::from_rng(&mut rand::rng());
        let mut db = WalletDb::from_connection(conn, Network::TestNetwork, SystemClock, rng);
        let seed = || SecretVec::new(vec![1u8; 32]);
        init_wallet_db(&mut db, Some(seed())).unwrap();

        let sapling = Network::TestNetwork.activation_height(NetworkUpgrade::Sapling).unwrap();
        let birthday = AccountBirthday::from_parts(ChainState::empty(sapling - 1, BlockHash([0; 32])), None);
        db.create_account("smoke", &seed(), &birthday, None).unwrap();
        drop(db);

        assert!(crate::storage::open_encrypted(&path, &[8u8; 32]).is_err());
        let raw = std::fs::read(&path).unwrap();
        assert!(!raw.windows(15).any(|w| w == b"SQLite format 3"));
    }
}
