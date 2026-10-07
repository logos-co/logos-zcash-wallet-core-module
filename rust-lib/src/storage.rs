//! SQLCipher connections for the wallet database and the block cache.

use std::path::Path;

use rusqlite::Connection;

#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("database: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("wrong key, or not a wallet database")]
    WrongKey,
}

/// Opens (or creates) an encrypted database with a raw 256-bit key and loads the
/// array module that `zcash_client_sqlite` needs.
pub fn open_encrypted(path: &Path, key: &[u8; 32]) -> Result<Connection, StorageError> {
    let conn = Connection::open(path)?;
    let mut pragma = format!("PRAGMA key = \"x'{}'\";", hex::encode(key));
    let keyed = conn.execute_batch(&pragma);
    zeroize::Zeroize::zeroize(&mut pragma);
    keyed?;
    // A wrong key only shows when the first page is read.
    conn.query_row("SELECT count(*) FROM sqlite_master", [], |r| r.get::<_, i64>(0))
        .map_err(|_| StorageError::WrongKey)?;
    conn.execute_batch("PRAGMA foreign_keys = ON; PRAGMA journal_mode = WAL;")?;
    rusqlite::vtab::array::load_module(&conn)?;
    Ok(conn)
}
