//! The encrypted block cache: compact blocks fetched on the download grid, and
//! the tree states at grid heights.

use std::path::Path;
use std::sync::Mutex;

use prost::Message;
use rusqlite::{params, Connection, OptionalExtension};
use zcash_client_backend::data_api::chain::{error::Error as ChainError, BlockSource};
use zcash_client_backend::proto::compact_formats::CompactBlock;
use zcash_client_backend::proto::service::TreeState;
use zcash_protocol::consensus::BlockHeight;

use crate::storage::{open_encrypted, StorageError};

/// Downloads and tree states follow this grid, whatever the wallet holds.
pub const GRID: u32 = 1_000;

pub fn grid_floor(h: u32) -> u32 {
    h - h % GRID
}

/// The first block of the chunk at `start`. Genesis holds no notes, so chunk 0 starts at 1
/// (only a regtest wallet is born that low).
pub fn chunk_first(start: u32) -> u32 {
    start.max(1)
}

#[derive(Debug, thiserror::Error)]
pub enum CacheError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("cache: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("cached block {0} does not decode")]
    Decode(u32),
}

pub struct BlockCache {
    conn: Mutex<Connection>,
}

impl BlockCache {
    pub fn open(path: &Path, key: &[u8; 32]) -> Result<Self, CacheError> {
        let conn = open_encrypted(path, key)?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS blocks (
                height INTEGER PRIMARY KEY, hash BLOB NOT NULL, prev_hash BLOB NOT NULL, data BLOB NOT NULL);
             CREATE TABLE IF NOT EXISTS tree_states (
                height INTEGER PRIMARY KEY, hash TEXT NOT NULL, data BLOB NOT NULL, server TEXT NOT NULL);
             CREATE TABLE IF NOT EXISTS chunks (
                start INTEGER PRIMARY KEY, end INTEGER NOT NULL, server TEXT NOT NULL);",
        )?;
        Ok(Self { conn: Mutex::new(conn) })
    }

    pub fn insert_blocks(&self, blocks: &[CompactBlock]) -> Result<(), CacheError> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        {
            let mut stmt = tx.prepare_cached(
                "INSERT INTO blocks (height, hash, prev_hash, data) VALUES (?1, ?2, ?3, ?4)
                 ON CONFLICT (height) DO UPDATE SET hash = ?2, prev_hash = ?3, data = ?4",
            )?;
            for b in blocks {
                stmt.execute(params![b.height as i64, b.hash, b.prev_hash, b.encode_to_vec()])?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    /// Records that [start, end) is cached in full.
    pub fn mark_chunk(&self, start: u32, end: u32, server: &str) -> Result<(), CacheError> {
        self.conn.lock().unwrap().execute(
            "INSERT INTO chunks (start, end, server) VALUES (?1, ?2, ?3)
             ON CONFLICT (start) DO UPDATE SET end = ?2, server = ?3",
            params![start, end, server],
        )?;
        Ok(())
    }

    /// The end of the cached run of chunks that starts at `start`, if any.
    pub fn chunk_end(&self, start: u32) -> Result<Option<u32>, CacheError> {
        Ok(self
            .conn
            .lock()
            .unwrap()
            .query_row("SELECT end FROM chunks WHERE start = ?1", [start], |r| r.get(0))
            .optional()?)
    }

    pub fn put_tree_state(&self, ts: &TreeState, server: &str) -> Result<(), CacheError> {
        self.conn.lock().unwrap().execute(
            "INSERT INTO tree_states (height, hash, data, server) VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT (height) DO UPDATE SET hash = ?2, data = ?3, server = ?4",
            params![ts.height as i64, ts.hash, ts.encode_to_vec(), server],
        )?;
        Ok(())
    }

    pub fn tree_state(&self, height: u32) -> Result<Option<TreeState>, CacheError> {
        let data: Option<Vec<u8>> = self
            .conn
            .lock()
            .unwrap()
            .query_row("SELECT data FROM tree_states WHERE height = ?1", [height], |r| r.get(0))
            .optional()?;
        data.map(|d| TreeState::decode(&d[..]).map_err(|_| CacheError::Decode(height)))
            .transpose()
    }

    pub fn block(&self, height: u32) -> Result<Option<CompactBlock>, CacheError> {
        let data: Option<Vec<u8>> = self
            .conn
            .lock()
            .unwrap()
            .query_row("SELECT data FROM blocks WHERE height = ?1", [height], |r| r.get(0))
            .optional()?;
        data.map(|d| CompactBlock::decode(&d[..]).map_err(|_| CacheError::Decode(height)))
            .transpose()
    }

    /// Highest h such that every block in [from, h) is cached.
    pub fn contiguous_end(&self, from: u32) -> Result<u32, CacheError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare_cached("SELECT height FROM blocks WHERE height >= ?1 ORDER BY height")?;
        let mut next = from;
        let mut rows = stmt.query([from])?;
        while let Some(row) = rows.next()? {
            let h: u32 = row.get(0)?;
            if h != next {
                break;
            }
            next += 1;
        }
        Ok(next)
    }

    /// Drops cached blocks in [start, end), keeping their chunk records.
    pub fn delete_blocks(&self, start: u32, end: u32) -> Result<(), CacheError> {
        self.conn
            .lock()
            .unwrap()
            .execute("DELETE FROM blocks WHERE height >= ?1 AND height < ?2", params![start, end])?;
        Ok(())
    }

    /// Drops a scanned chunk's blocks and its record; its tree state stays.
    pub fn forget_chunk(&self, start: u32) -> Result<(), CacheError> {
        let conn = self.conn.lock().unwrap();
        conn.execute("DELETE FROM blocks WHERE height >= ?1 AND height < ?2", params![start, start + GRID])?;
        conn.execute("DELETE FROM chunks WHERE start = ?1", [start])?;
        Ok(())
    }

    /// Forgets everything at or above `height`, after a reorg.
    pub fn truncate_from(&self, height: u32) -> Result<(), CacheError> {
        let conn = self.conn.lock().unwrap();
        conn.execute("DELETE FROM blocks WHERE height >= ?1", [height])?;
        conn.execute("DELETE FROM tree_states WHERE height >= ?1", [height])?;
        conn.execute("DELETE FROM chunks WHERE end > ?1", [height])?;
        Ok(())
    }
}

impl BlockSource for BlockCache {
    type Error = CacheError;

    fn with_blocks<F, WalletErrT>(
        &self,
        from_height: Option<BlockHeight>,
        limit: Option<usize>,
        mut with_block: F,
    ) -> Result<(), ChainError<WalletErrT, Self::Error>>
    where
        F: FnMut(CompactBlock) -> Result<(), ChainError<WalletErrT, Self::Error>>,
    {
        let from = from_height.map_or(0, u32::from);
        let limit = limit.map_or(-1i64, |l| l as i64);
        let blocks: Vec<(u32, Vec<u8>)> = {
            let conn = self.conn.lock().unwrap();
            let mut stmt = conn
                .prepare_cached("SELECT height, data FROM blocks WHERE height >= ?1 ORDER BY height LIMIT ?2")
                .map_err(|e| ChainError::BlockSource(e.into()))?;
            let rows = stmt
                .query_map(params![from, limit], |r| Ok((r.get(0)?, r.get(1)?)))
                .map_err(|e| ChainError::BlockSource(e.into()))?;
            rows.collect::<Result<_, _>>().map_err(|e| ChainError::BlockSource(e.into()))?
        };
        for (h, data) in blocks {
            let block = CompactBlock::decode(&data[..]).map_err(|_| ChainError::BlockSource(CacheError::Decode(h)))?;
            with_block(block)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn block(h: u64) -> CompactBlock {
        CompactBlock { height: h, hash: vec![h as u8; 32], prev_hash: vec![(h - 1) as u8; 32], ..Default::default() }
    }

    #[test]
    fn grid() {
        assert_eq!(grid_floor(4_476_424), 4_476_000);
        assert_eq!(grid_floor(4_476_000), 4_476_000);
        assert_eq!((chunk_first(grid_floor(7)), chunk_first(4_476_000)), (1, 4_476_000));
    }

    #[test]
    fn blocks_and_runs() {
        let dir = tempfile::tempdir().unwrap();
        let cache = BlockCache::open(&dir.path().join("cache.db"), &[3; 32]).unwrap();
        cache.insert_blocks(&[block(10), block(11), block(12), block(14)]).unwrap();
        assert_eq!(cache.contiguous_end(10).unwrap(), 13);
        assert_eq!(cache.block(11).unwrap().unwrap().height, 11);

        let mut seen = vec![];
        cache
            .with_blocks::<_, ()>(Some(BlockHeight::from(11)), Some(2), |b| {
                seen.push(b.height);
                Ok(())
            })
            .unwrap();
        assert_eq!(seen, vec![11, 12]);

        cache.truncate_from(12).unwrap();
        assert_eq!(cache.contiguous_end(10).unwrap(), 12);
        assert!(cache.block(14).unwrap().is_none());
    }
}
