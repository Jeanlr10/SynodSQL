//! The Prolly tree of the state hash, kept level by level and updated in
//! place (specification Section 8).
//!
//! Level 0 holds the row hashes (`_synod_rowhash`). Each level above holds
//! one item per node of the level below: the node's last key and its hash
//! (`_synod_level`). Node boundaries depend only on keys, so a changed row
//! changes only the nodes on its path to the root, and an inserted or
//! removed key only splits or joins its neighbours. An update reads about
//! one node per changed key and level, not every row.
//!
//! The levels go up to the first level with exactly one item, whose hash is
//! the root hash; no level is kept above it. The base state M(e−1) has its
//! own copy (`_synod_base_hash`, `_synod_base_level`), from which a node is
//! rebuilt when a peer asks for it by hash. `_synod_tree_changed` lists the
//! items that changed since the last copy, so the copy is incremental too.

use std::collections::BTreeSet;

use rusqlite::{Connection, OptionalExtension, params};
use synod_core::RowId;
use synod_core::prolly::{NodeHash, empty_root, encode_node, is_boundary, node_hash};

use crate::error::{Error, Result};
use crate::store::parse_row_id;

/// Which tree: the current local state, or the base state.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Side {
    Current,
    Base,
}

/// An item key: a row ID, big-endian, as stored.
type Key = Vec<u8>;

type Items = Vec<(RowId, [u8; 32])>;

// The table, key column, and filter of one level. The filter ends with AND.
fn source(side: Side, level: u8) -> (&'static str, &'static str, String) {
    match (side, level) {
        (Side::Current, 0) => ("_synod_rowhash", "row_id", String::new()),
        (Side::Base, 0) => ("_synod_base_hash", "row_id", String::new()),
        (Side::Current, l) => ("_synod_level", "key", format!("level = {l} AND ")),
        (Side::Base, l) => ("_synod_base_level", "key", format!("level = {l} AND ")),
    }
}

fn hash32(v: Vec<u8>) -> Result<[u8; 32]> {
    v.try_into().map_err(|_| Error::Corrupt("tree hash is not 32 bytes".into()))
}

// Number of items of a level, up to 2.
fn count2(conn: &Connection, side: Side, level: u8) -> Result<u32> {
    let (t, _, f) = source(side, level);
    let sql = format!("SELECT count(*) FROM (SELECT 1 FROM {t} WHERE {f}1 LIMIT 2)");
    Ok(conn.prepare_cached(&sql)?.query_row([], |r| r.get(0))?)
}

// Returns the root hash of a tree whose levels are complete.
fn root(conn: &Connection, side: Side) -> Result<NodeHash> {
    if count2(conn, side, 0)? == 0 {
        return Ok(empty_root());
    }
    for level in 1..=u8::MAX {
        match count2(conn, side, level)? {
            0 => break,
            1 => {
                let (t, _, f) = source(side, level);
                let h: Vec<u8> =
                    conn.prepare_cached(&format!("SELECT hash FROM {t} WHERE {f}1"))?.query_row([], |r| r.get(0))?;
                return hash32(h);
            }
            _ => {}
        }
    }
    Err(Error::Corrupt("the state hash tree has no root".into()))
}

fn record(conn: &Connection, level: u8, keys: &BTreeSet<Key>) -> Result<()> {
    let mut stmt = conn.prepare_cached("INSERT OR IGNORE INTO _synod_tree_changed (level, key) VALUES (?1, ?2)")?;
    for k in keys {
        stmt.execute(params![level, k])?;
    }
    Ok(())
}

/// Updates the current tree after the row hashes of `rows` changed (in
/// `_synod_rowhash`), and returns the root hash. Runs inside a transaction.
pub(crate) fn update(conn: &Connection, rows: BTreeSet<Key>) -> Result<NodeHash> {
    record(conn, 0, &rows)?;
    let mut changed = rows;
    for level in 0..u8::MAX {
        let n = count2(conn, Side::Current, level)?;
        if (level == 0 && n == 0) || (level > 0 && n == 1) {
            let removed: BTreeSet<(u8, Key)> = conn
                .prepare_cached("DELETE FROM _synod_level WHERE level > ?1 RETURNING level, key")?
                .query_map([level], |r| Ok((r.get(0)?, r.get(1)?)))?
                .collect::<rusqlite::Result<_>>()?;
            for (l, k) in removed {
                record(conn, l, &BTreeSet::from([k]))?;
            }
            return root(conn, Side::Current);
        }
        let next = if count2(conn, Side::Current, level + 1)? == 0 {
            build_level(conn, level)?
        } else if changed.is_empty() {
            BTreeSet::new()
        } else {
            update_level(conn, level, &changed)?
        };
        record(conn, level + 1, &next)?;
        changed = next;
    }
    Err(Error::Corrupt("the state hash tree is too high".into()))
}

fn put_parent(conn: &Connection, level: u8, items: &Items) -> Result<Key> {
    let key = items.last().expect("a node has items").0.to_be_bytes().to_vec();
    let hash = node_hash(&encode_node(level, items));
    conn.prepare_cached("INSERT OR REPLACE INTO _synod_level (level, key, hash) VALUES (?1, ?2, ?3)")?
        .execute(params![level + 1, key, hash.to_vec()])?;
    Ok(key)
}

// Makes level + 1 from all items of `level`. Returns the new keys.
fn build_level(conn: &Connection, level: u8) -> Result<BTreeSet<Key>> {
    let (t, k, f) = source(Side::Current, level);
    let all: Vec<(Key, Vec<u8>)> = conn
        .prepare_cached(&format!("SELECT {k}, hash FROM {t} WHERE {f}1 ORDER BY {k}"))?
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?
        .collect::<rusqlite::Result<_>>()?;
    let mut out = BTreeSet::new();
    let mut node = Vec::new();
    for (key, hash) in all {
        let id = parse_row_id(&key)?;
        node.push((id, hash32(hash)?));
        if is_boundary(level, id) {
            out.insert(put_parent(conn, level, &node)?);
            node.clear();
        }
    }
    if !node.is_empty() {
        out.insert(put_parent(conn, level, &node)?);
    }
    Ok(out)
}

// The last boundary key of `level` before `key`, if any.
fn boundary_before(conn: &Connection, level: u8, key: &[u8]) -> Result<Option<Key>> {
    let (t, k, f) = source(Side::Current, level);
    let mut stmt = conn.prepare_cached(&format!("SELECT {k} FROM {t} WHERE {f}{k} < ?1 ORDER BY {k} DESC"))?;
    let mut rows = stmt.query([key])?;
    while let Some(r) = rows.next()? {
        let k: Key = r.get(0)?;
        if is_boundary(level, parse_row_id(&k)?) {
            return Ok(Some(k));
        }
    }
    Ok(None)
}

// The node of `level` that starts after `after` (None: at the start): its
// items, and true if it is the last node of the level.
fn node_after(conn: &Connection, level: u8, after: Option<&[u8]>) -> Result<(Items, bool)> {
    let (t, k, f) = source(Side::Current, level);
    let mut stmt = conn.prepare_cached(&format!("SELECT {k}, hash FROM {t} WHERE {f}{k} > ?1 ORDER BY {k}"))?;
    let mut rows = stmt.query([after.unwrap_or(&[])])?;
    let mut items = Vec::new();
    while let Some(r) = rows.next()? {
        let id = parse_row_id(&r.get::<_, Vec<u8>>(0)?)?;
        items.push((id, hash32(r.get(1)?)?));
        if is_boundary(level, id) {
            return Ok((items, rows.next()?.is_none()));
        }
    }
    Ok((items, true))
}

// Brings level + 1 up to date for the changed keys of `level`. Returns the
// keys of level + 1 that changed (set, replaced, or removed).
fn update_level(conn: &Connection, level: u8, changed: &BTreeSet<Key>) -> Result<BTreeSet<Key>> {
    let mut out = BTreeSet::new();
    // Every key up to `done` is in a node already made again.
    let mut done: Option<Key> = None;
    for key in changed {
        if done.as_ref().is_some_and(|d| key <= d) {
            continue;
        }
        let mut start = boundary_before(conn, level, key)?;
        loop {
            let (items, last) = node_after(conn, level, start.as_deref())?;
            // The parents of the old nodes in this key range: they end in
            // the range, or anywhere after its start for the last node.
            let lo = start.clone().unwrap_or_default();
            let removed: Vec<Key> = if last {
                conn.prepare_cached("DELETE FROM _synod_level WHERE level = ?1 AND key > ?2 RETURNING key")?
                    .query_map(params![level + 1, lo], |r| r.get(0))?
                    .collect::<rusqlite::Result<_>>()?
            } else {
                let hi = items.last().expect("a node before the last one has items").0.to_be_bytes().to_vec();
                conn.prepare_cached(
                    "DELETE FROM _synod_level WHERE level = ?1 AND key > ?2 AND key <= ?3 RETURNING key",
                )?
                .query_map(params![level + 1, lo, hi], |r| r.get(0))?
                .collect::<rusqlite::Result<_>>()?
            };
            out.extend(removed);
            if last {
                if !items.is_empty() {
                    out.insert(put_parent(conn, level, &items)?);
                }
                return Ok(out);
            }
            let end = put_parent(conn, level, &items)?;
            out.insert(end.clone());
            done = Some(end.clone());
            // A new boundary splits the old node that went past it: the
            // node after it changed too.
            if !changed.contains(&end) {
                break;
            }
            start = Some(end);
        }
    }
    Ok(out)
}

/// Makes the base tree equal to the current tree, by copying the items
/// that changed since the last copy, and returns its root hash. Runs when
/// the current state is the base state.
pub(crate) fn index_base(conn: &Connection) -> Result<NodeHash> {
    let changed: Vec<(u8, Key)> = conn
        .prepare_cached("SELECT level, key FROM _synod_tree_changed")?
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?
        .collect::<rusqlite::Result<_>>()?;
    for (level, key) in changed {
        if level == 0 {
            conn.prepare_cached("DELETE FROM _synod_base_hash WHERE row_id = ?1")?.execute([&key])?;
            conn.prepare_cached(
                "INSERT INTO _synod_base_hash SELECT row_id, hash FROM _synod_rowhash WHERE row_id = ?1",
            )?
            .execute([&key])?;
        } else {
            conn.prepare_cached("DELETE FROM _synod_base_level WHERE level = ?1 AND key = ?2")?
                .execute(params![level, key])?;
            conn.prepare_cached(
                "INSERT INTO _synod_base_level SELECT level, key, hash FROM _synod_level WHERE level = ?1 AND key = ?2",
            )?
            .execute(params![level, key])?;
        }
    }
    conn.prepare_cached("DELETE FROM _synod_tree_changed")?.execute([])?;
    root(conn, Side::Base)
}

/// Returns a node of the base tree by its hash: its items are those of the
/// level below, after the previous node's last key, up to its own.
pub(crate) fn base_node(conn: &Connection, hash: &NodeHash) -> Result<Option<Vec<u8>>> {
    if *hash == empty_root() {
        return Ok((count2(conn, Side::Base, 0)? == 0).then(|| encode_node(0, &[])));
    }
    let found: Option<(u8, Key)> = conn
        .prepare_cached("SELECT level, key FROM _synod_base_level WHERE hash = ?1 LIMIT 1")?
        .query_row([hash.to_vec()], |r| Ok((r.get(0)?, r.get(1)?)))
        .optional()?;
    let Some((parent, key)) = found else { return Ok(None) };
    let prev: Key = conn
        .prepare_cached("SELECT key FROM _synod_base_level WHERE level = ?1 AND key < ?2 ORDER BY key DESC LIMIT 1")?
        .query_row(params![parent, key], |r| r.get(0))
        .optional()?
        .unwrap_or_default();
    let level = parent - 1;
    let (t, k, f) = source(Side::Base, level);
    let items: Items = conn
        .prepare_cached(&format!("SELECT {k}, hash FROM {t} WHERE {f}{k} > ?1 AND {k} <= ?2 ORDER BY {k}"))?
        .query_map(params![prev, key], |r| Ok((r.get::<_, Vec<u8>>(0)?, r.get::<_, Vec<u8>>(1)?)))?
        .map(|r| {
            let (k, h) = r?;
            Ok((parse_row_id(&k)?, hash32(h)?))
        })
        .collect::<Result<_>>()?;
    let node = encode_node(level, &items);
    if node_hash(&node) != *hash {
        return Err(Error::Corrupt("a base tree node does not have its hash".into()));
    }
    Ok(Some(node))
}
