//! A wallet on disk: public metadata, the sealed seed and database key, the
//! SQLCipher wallet database and the block cache.

use std::fs;
use std::path::{Path, PathBuf};

use rand::SeedableRng;
use rand_chacha::ChaCha20Rng;
use serde::{Deserialize, Serialize};
use zcash_client_backend::data_api::{chain::ChainState, AccountBirthday, WalletWrite};
use zcash_client_backend::util::SystemClock;
use zcash_client_sqlite::{wallet::init::init_wallet_db, AccountUuid, WalletDb};
use zeroize::Zeroizing;

use crate::keys::{self, KeyError, Phrase};
use crate::network::ZNetwork;
use crate::storage::{open_encrypted, StorageError};
use crate::sync::cache::{BlockCache, CacheError};

pub type Db = WalletDb<rusqlite::Connection, ZNetwork, SystemClock, ChaCha20Rng>;

const META: &str = "wallet.json";
const SEED: &str = "seed.age";
const DBKEY: &str = "dbkey.age";
const DB: &str = "wallet.db";
const CACHE: &str = "cache.db";
const FORMAT: u32 = 1;

#[derive(Debug, thiserror::Error)]
pub enum WalletError {
    #[error(transparent)]
    Key(#[from] KeyError),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Cache(#[from] CacheError),
    #[error("wallet database: {0}")]
    Db(String),
    #[error("a wallet named {0} already exists")]
    Exists(String),
    #[error("no wallet named {0}")]
    NotFound(String),
    #[error("wallet files: {0}")]
    Io(#[from] std::io::Error),
    #[error("wallet.json: {0}")]
    Meta(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Meta {
    pub format: u32,
    pub name: String,
    pub network: String,
    pub account_uuid: String,
    pub birthday_height: u32,
    pub created_at: u64,
}

pub struct WalletDir {
    pub path: PathBuf,
}

/// Wallet names become directory names, so they are kept plain.
pub fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 64
        && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
}

fn write_atomic(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let tmp = path.with_extension("tmp");
    fs::write(&tmp, bytes)?;
    fs::rename(tmp, path)
}

pub fn new_rng() -> ChaCha20Rng {
    ChaCha20Rng::from_rng(&mut rand::rng())
}

impl WalletDir {
    pub fn new(root: &Path, network: ZNetwork, name: &str) -> Self {
        Self { path: root.join(network.name()).join(name) }
    }

    pub fn exists(&self) -> bool {
        self.path.join(META).exists()
    }

    pub fn meta(&self) -> Result<Meta, WalletError> {
        let raw = fs::read(self.path.join(META))?;
        serde_json::from_slice(&raw).map_err(|e| WalletError::Meta(e.to_string()))
    }

    /// Creates the files and the account. `birthday` is the chain state just
    /// below the first block the wallet may hold funds in.
    pub fn create(
        &self,
        network: ZNetwork,
        name: &str,
        password: &str,
        phrase: &Phrase,
        birthday: ChainState,
        work_factor: Option<u8>,
    ) -> Result<Meta, WalletError> {
        if self.exists() {
            return Err(WalletError::Exists(name.into()));
        }
        fs::create_dir_all(&self.path)?;
        let db_key = keys::new_db_key();
        let sealed_seed = keys::seal_phrase(phrase, password, work_factor)?;
        let sealed_key = keys::seal(&db_key[..], password, work_factor)?;

        let conn = open_encrypted(&self.path.join(DB), &db_key)?;
        let mut db = Db::from_connection(conn, network, SystemClock, new_rng());
        init_wallet_db(&mut db, Some(phrase.seed())).map_err(|e| WalletError::Db(e.to_string()))?;
        let birthday_height = u32::from(birthday.block_height()) + 1;
        let (account, _usk) = db
            .create_account(name, &phrase.seed(), &AccountBirthday::from_parts(birthday, None), None)
            .map_err(|e| WalletError::Db(e.to_string()))?;
        drop(db);
        BlockCache::open(&self.path.join(CACHE), &db_key)?;

        write_atomic(&self.path.join(SEED), &sealed_seed)?;
        write_atomic(&self.path.join(DBKEY), &sealed_key)?;
        let meta = Meta {
            format: FORMAT,
            name: name.into(),
            network: network.name().into(),
            account_uuid: account.expose_uuid().to_string(),
            birthday_height,
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_secs()),
        };
        write_atomic(&self.path.join(META), &serde_json::to_vec_pretty(&meta).expect("plain struct"))?;
        Ok(meta)
    }

    pub fn unlock_db_key(&self, password: &str) -> Result<Zeroizing<[u8; 32]>, WalletError> {
        Ok(keys::open_db_key(&fs::read(self.path.join(DBKEY))?, password)?)
    }

    pub fn open_db(&self, network: ZNetwork, key: &[u8; 32]) -> Result<Db, WalletError> {
        let conn = open_encrypted(&self.path.join(DB), key)?;
        Ok(Db::from_connection(conn, network, SystemClock, new_rng()))
    }

    pub fn open_cache(&self, key: &[u8; 32]) -> Result<BlockCache, WalletError> {
        Ok(BlockCache::open(&self.path.join(CACHE), key)?)
    }

    /// Decrypts the recovery phrase. Only for signing and for showing it once.
    pub fn unseal_phrase(&self, password: &str) -> Result<Phrase, WalletError> {
        Ok(keys::open_phrase(&fs::read(self.path.join(SEED))?, password)?)
    }

    pub fn change_password(&self, old: &str, new: &str, work_factor: Option<u8>) -> Result<(), WalletError> {
        let phrase = self.unseal_phrase(old)?;
        let key = self.unlock_db_key(old)?;
        let sealed_seed = keys::seal_phrase(&phrase, new, work_factor)?;
        let sealed_key = keys::seal(&key[..], new, work_factor)?;
        write_atomic(&self.path.join(SEED), &sealed_seed)?;
        write_atomic(&self.path.join(DBKEY), &sealed_key)?;
        Ok(())
    }

    pub fn account(&self) -> Result<AccountUuid, WalletError> {
        let meta = self.meta()?;
        let uuid = uuid::Uuid::parse_str(&meta.account_uuid).map_err(|e| WalletError::Meta(e.to_string()))?;
        Ok(AccountUuid::from_uuid(uuid))
    }
}

/// Lists wallet names for a network, in name order.
pub fn list(root: &Path, network: ZNetwork) -> Result<Vec<Meta>, WalletError> {
    let dir = root.join(network.name());
    let mut out = vec![];
    if let Ok(entries) = fs::read_dir(&dir) {
        for e in entries.flatten() {
            let w = WalletDir { path: e.path() };
            if w.exists() {
                if let Ok(m) = w.meta() {
                    out.push(m);
                }
            }
        }
    }
    out.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use zcash_client_backend::data_api::WalletRead;
    use zcash_primitives::block::BlockHash;
    use zcash_protocol::consensus::{NetworkUpgrade, Parameters};

    fn pre_sapling(n: ZNetwork) -> ChainState {
        let h = n.activation_height(NetworkUpgrade::Sapling).unwrap() - 1;
        ChainState::empty(h, BlockHash([0; 32]))
    }

    #[test]
    fn create_open_change_password() {
        let root = tempfile::tempdir().unwrap();
        let net = ZNetwork::Test;
        let w = WalletDir::new(root.path(), net, "main");
        let phrase = Phrase::generate();
        let meta = w.create(net, "main", "pw1", &phrase, pre_sapling(net), Some(10)).unwrap();
        assert_eq!(meta.network, "testnet");
        assert!(matches!(w.create(net, "main", "pw1", &phrase, pre_sapling(net), Some(10)), Err(WalletError::Exists(_))));

        assert!(w.unlock_db_key("bad").is_err());
        let key = w.unlock_db_key("pw1").unwrap();
        let db = w.open_db(net, &key).unwrap();
        assert_eq!(db.get_account_ids().unwrap(), vec![w.account().unwrap()]);
        drop(db);

        w.change_password("pw1", "pw2", Some(10)).unwrap();
        assert!(w.unlock_db_key("pw1").is_err());
        assert_eq!(w.unseal_phrase("pw2").unwrap().as_str(), phrase.as_str());
        assert_eq!(list(root.path(), net).unwrap().len(), 1);
        assert!(list(root.path(), ZNetwork::Main).unwrap().is_empty());
    }

    #[test]
    fn names() {
        assert!(valid_name("main-1"));
        assert!(!valid_name("../x"));
        assert!(!valid_name(""));
    }
}
