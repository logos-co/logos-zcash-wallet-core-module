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

/// Logs to `core.log` in the persistence directory, restarting it past 10 MB. Never
/// logs secrets: callers pass only heights, counts, txids and errors.
pub fn init_log(dir: &Path) {
    let path = dir.join("core.log");
    if std::fs::metadata(&path).is_ok_and(|m| m.len() > 10 * 1024 * 1024) {
        let _ = std::fs::remove_file(&path);
    }
    if let Ok(file) = std::fs::OpenOptions::new().create(true).append(true).open(&path) {
        let _ = tracing_subscriber::fmt()
            .with_writer(std::sync::Mutex::new(file))
            .with_ansi(false)
            .with_max_level(tracing::Level::INFO)
            .try_init();
    }
}
