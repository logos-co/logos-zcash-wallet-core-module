//! Transaction history from the wallet's views, newest first.

use rusqlite::{params, Connection};
use serde_json::{json, Value};
use zcash_client_sqlite::AccountUuid;

use crate::network::ZNetwork;

pub const PAGE: u32 = 25;

fn pool(code: i64) -> &'static str {
    match code {
        0 => "transparent",
        2 => "sapling",
        3 => "orchard",
        4 => "ironwood",
        _ => "unknown",
    }
}

/// Memo bytes as text when they are text (ZIP 302), else nothing.
fn memo_text(raw: Option<Vec<u8>>) -> Option<String> {
    let raw = raw?;
    if raw.first().copied().unwrap_or(0xF6) > 0xF4 {
        return None;
    }
    let end = raw.iter().rposition(|b| *b != 0).map_or(0, |i| i + 1);
    String::from_utf8(raw[..end].to_vec()).ok().filter(|s| !s.is_empty())
}

pub fn page(conn: &Connection, network: ZNetwork, account: AccountUuid, page: u32) -> Result<Value, String> {
    let _ = network;
    let uuid = account.expose_uuid();
    let mut stmt = conn
        .prepare_cached(
            "SELECT txid, mined_height, expiry_height, account_balance_delta, fee_paid, sent_note_count,
                    received_note_count, block_time, expired_unmined, is_shielding, pool_crossing_value, zip318_kind
             FROM v_transactions_with_pending_migrations WHERE account_uuid = ?1
             ORDER BY COALESCE(mined_height, 4294967295) DESC, tx_index DESC LIMIT ?2 OFFSET ?3",
        )
        .map_err(|e| e.to_string())?;
    let rows = stmt
        .query_map(params![uuid, PAGE, page * PAGE], |r| {
            Ok((
                r.get::<_, Vec<u8>>(0)?,
                r.get::<_, Option<u32>>(1)?,
                r.get::<_, Option<u32>>(2)?,
                r.get::<_, i64>(3)?,
                r.get::<_, Option<i64>>(4)?,
                r.get::<_, i64>(5)?,
                r.get::<_, i64>(6)?,
                r.get::<_, Option<i64>>(7)?,
                r.get::<_, Option<bool>>(8)?,
                r.get::<_, Option<bool>>(9)?,
                r.get::<_, Option<i64>>(10)?,
                r.get::<_, Option<String>>(11)?,
            ))
        })
        .map_err(|e| e.to_string())?;
    let mut out = vec![];
    let mut outputs = conn
        .prepare_cached(
            "SELECT output_pool, value, is_change, memo, to_address, is_sent_row
             FROM v_tx_outputs WHERE txid = ?1 ORDER BY output_pool, output_index",
        )
        .map_err(|e| e.to_string())?;
    for row in rows {
        let (txid, height, expiry, delta, fee, sent, received, time, expired, shielding, crossing, zip318) =
            row.map_err(|e| e.to_string())?;
        let mut pools = std::collections::BTreeSet::new();
        let mut memos = vec![];
        let mut to = vec![];
        let outs = outputs
            .query_map([&txid], |r| {
                Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?, r.get::<_, bool>(2)?, r.get::<_, Option<Vec<u8>>>(3)?, r.get::<_, Option<String>>(4)?, r.get::<_, bool>(5)?))
            })
            .map_err(|e| e.to_string())?;
        for o in outs {
            let (p, value, change, memo, addr, sent_row) = o.map_err(|e| e.to_string())?;
            pools.insert(pool(p));
            if let Some(m) = memo_text(memo) {
                memos.push(m);
            }
            if sent_row && !change {
                to.push(json!({"address": addr, "pool": pool(p), "amount": value}));
            }
        }
        let kind = if zip318.is_some() {
            "migration"
        } else if shielding.unwrap_or(false) {
            "shielded"
        } else if sent > 0 || delta < 0 {
            "sent"
        } else {
            "received"
        };
        // txids display in reverse byte order.
        let mut display = txid.clone();
        display.reverse();
        out.push(json!({
            "txid": hex::encode(display),
            "kind": kind,
            "height": height,
            "pending": height.is_none() && !expired.unwrap_or(false),
            "expired": expired.unwrap_or(false),
            "expiryHeight": if height.is_none() { expiry } else { None },
            "time": time,
            "delta": delta,
            "fee": fee,
            "pools": pools,
            "memos": memos,
            "to": to,
            "receivedNotes": received,
            "amountMadePublic": crossing.unwrap_or(0),
        }));
    }
    Ok(json!({"ok": true, "page": page, "pageSize": PAGE, "rows": out}))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn memos() {
        let mut m = b"hello".to_vec();
        m.resize(512, 0);
        assert_eq!(memo_text(Some(m)).as_deref(), Some("hello"));
        let mut empty = vec![0xF6];
        empty.resize(512, 0);
        assert_eq!(memo_text(Some(empty)), None);
        assert_eq!(memo_text(None), None);
    }
}
