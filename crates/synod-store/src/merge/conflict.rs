//! Conflict records and the `_conflicts` table (specification Section 7.5).

use std::fmt;

use synod_core::{Cell, Hlc, NodeId, Row, RowId, Stamp, Value};

use crate::error::{Error, Result};

/// Name of the conflict table. Every store has it, as table 0.
pub const CONFLICTS_TABLE: &str = "_conflicts";

/// Its schema. `status` is the only column people change.
pub const CONFLICTS_DDL: &str = "CREATE TABLE _conflicts (
    conflict_id BLOB NOT NULL UNIQUE,
    epoch INTEGER NOT NULL,
    tbl TEXT NOT NULL,
    target BLOB NOT NULL,
    col TEXT,
    kind TEXT NOT NULL,
    rule TEXT NOT NULL,
    base_value BLOB,
    recorded BLOB NOT NULL,
    applied_value BLOB,
    other_row BLOB,
    status TEXT NOT NULL DEFAULT 'open' CHECK (status IN ('open', 'resolved'))
)";

/// The node ID of rows that the merge makes (conflict rows). No member node
/// may have it.
pub const MERGE_NODE: NodeId = NodeId(u32::MAX);

/// Key-derivation context for conflict IDs.
pub const CONFLICT_ID_CONTEXT: &str = "SynodSQL 2026-10-12 conflict id v1";

/// Kind of conflict (white paper, Section VII).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub enum ConflictKind {
    /// Same column, different values, under a policy that records.
    SameColumn,
    /// A delete and an update of one row.
    UpdateDelete,
    /// Two live rows with one unique key value.
    UniqueKey,
    /// A child with no live parent.
    ForeignKey,
    /// A CHECK constraint that the merged row breaks.
    Check,
}

impl ConflictKind {
    fn code(self) -> u8 {
        match self {
            ConflictKind::SameColumn => 1,
            ConflictKind::UpdateDelete => 2,
            ConflictKind::UniqueKey => 3,
            ConflictKind::ForeignKey => 4,
            ConflictKind::Check => 5,
        }
    }

    /// The name stored in the `kind` column.
    pub fn as_str(self) -> &'static str {
        match self {
            ConflictKind::SameColumn => "same_column",
            ConflictKind::UpdateDelete => "update_delete",
            ConflictKind::UniqueKey => "unique_key",
            ConflictKind::ForeignKey => "foreign_key",
            ConflictKind::Check => "check",
        }
    }

    fn parse(s: &str) -> Option<ConflictKind> {
        [Self::SameColumn, Self::UpdateDelete, Self::UniqueKey, Self::ForeignKey, Self::Check]
            .into_iter()
            .find(|k| k.as_str() == s)
    }
}

impl fmt::Display for ConflictKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A value that a node wrote and that the merge did not apply, or the
/// change that a rule undid.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Recorded {
    /// Node that wrote it.
    pub node: NodeId,
    /// HLC time of the write.
    pub hlc: Hlc,
    /// The value (one for a column conflict, all columns for a row).
    pub values: Vec<Value>,
}

/// One conflict.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Conflict {
    /// Conflict ID: a hash of (epoch, table, row, column, kind).
    pub id: [u8; 32],
    /// Epoch of the merge.
    pub epoch: u64,
    /// Table ID.
    pub table: u32,
    /// Table name.
    pub table_name: String,
    /// Row.
    pub row_id: RowId,
    /// Column index, or `None` for the whole row.
    pub column: Option<u16>,
    /// Column name, or `None` for the whole row.
    pub column_name: Option<String>,
    /// Kind.
    pub kind: ConflictKind,
    /// The rule that decided the result.
    pub rule: String,
    /// The value in the base state M(e−1), if the row was there.
    pub base: Option<Vec<Value>>,
    /// The values that the merge did not apply.
    pub recorded: Vec<Recorded>,
    /// The value in M(e), or `None` if the row is a tombstone.
    pub applied: Option<Vec<Value>>,
    /// Another row: the winner of a unique key, the parent or child of a
    /// foreign key.
    pub other_row: Option<RowId>,
}

/// Returns the conflict ID (specification Section 7.5).
pub fn conflict_id(epoch: u64, table_name: &str, row_id: RowId, column: Option<u16>, kind: ConflictKind) -> [u8; 32] {
    let mut h = blake3::Hasher::new_derive_key(CONFLICT_ID_CONTEXT);
    h.update(&epoch.to_be_bytes());
    h.update(&(table_name.len() as u32).to_be_bytes());
    h.update(table_name.as_bytes());
    h.update(&row_id.to_be_bytes());
    h.update(&column.unwrap_or(0xFFFF).to_be_bytes());
    h.update(&[kind.code()]);
    *h.finalize().as_bytes()
}

/// Encodes a list of values: `count:u16`, then each value.
pub fn encode_image(values: &[Value]) -> Vec<u8> {
    let mut out = (values.len() as u16).to_be_bytes().to_vec();
    for v in values {
        v.encode_into(&mut out);
    }
    out
}

/// Decodes [`encode_image`].
pub fn decode_image(bytes: &[u8]) -> Result<Vec<Value>> {
    let (values, used) = decode_image_prefix(bytes)?;
    if used != bytes.len() {
        return Err(Error::Corrupt("bytes after image".into()));
    }
    Ok(values)
}

fn decode_image_prefix(bytes: &[u8]) -> Result<(Vec<Value>, usize)> {
    let bad = || Error::Corrupt("bad image".into());
    let count = u16::from_be_bytes(bytes.get(..2).ok_or_else(bad)?.try_into().unwrap());
    let mut pos = 2;
    let mut values = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let (v, n) = Value::decode_prefix(&bytes[pos..]).map_err(|_| bad())?;
        values.push(v);
        pos += n;
    }
    Ok((values, pos))
}

/// Encodes recorded values: `count:u32`, then `(node:u32, hlc:u64, image)`.
pub fn encode_recorded(recorded: &[Recorded]) -> Vec<u8> {
    let mut out = (recorded.len() as u32).to_be_bytes().to_vec();
    for r in recorded {
        out.extend_from_slice(&r.node.0.to_be_bytes());
        out.extend_from_slice(&r.hlc.to_be_bytes());
        out.extend_from_slice(&encode_image(&r.values));
    }
    out
}

/// Decodes [`encode_recorded`].
pub fn decode_recorded(bytes: &[u8]) -> Result<Vec<Recorded>> {
    let bad = || Error::Corrupt("bad recorded values".into());
    let count = u32::from_be_bytes(bytes.get(..4).ok_or_else(bad)?.try_into().unwrap());
    let mut pos = 4;
    let mut out = Vec::new();
    for _ in 0..count {
        let head = bytes.get(pos..pos + 12).ok_or_else(bad)?;
        let node = NodeId(u32::from_be_bytes(head[..4].try_into().unwrap()));
        let hlc = Hlc::from_u64(u64::from_be_bytes(head[4..].try_into().unwrap()));
        let (values, n) = decode_image_prefix(&bytes[pos + 12..])?;
        pos += 12 + n;
        out.push(Recorded { node, hlc, values });
    }
    if pos != bytes.len() {
        return Err(bad());
    }
    Ok(out)
}

fn blob_or_null(b: Option<Vec<u8>>) -> Value {
    b.map_or(Value::Null, Value::Blob)
}

impl Conflict {
    /// The order of conflicts in a merge: table, row, column (the whole
    /// row last), kind.
    pub fn sort_key(&self) -> (u32, RowId, u16, u8) {
        (self.table, self.row_id, self.column.unwrap_or(0xFFFF), self.kind.code())
    }

    /// Row ID of this conflict in `_conflicts`: the merge node and the
    /// first 8 bytes of the conflict ID.
    pub fn row_id_in_table(&self) -> RowId {
        RowId::new(MERGE_NODE, u64::from_be_bytes(self.id[..8].try_into().unwrap()))
    }

    /// The conflict as a row of `_conflicts`, with every cell stamped
    /// `stamp`.
    pub fn to_row(&self, stamp: Stamp) -> Row {
        let values = [
            Value::Blob(self.id.to_vec()),
            Value::Integer(self.epoch as i64),
            Value::Text(self.table_name.clone()),
            Value::Blob(self.row_id.to_be_bytes().to_vec()),
            self.column_name.clone().map_or(Value::Null, Value::Text),
            Value::Text(self.kind.as_str().into()),
            Value::Text(self.rule.clone()),
            blob_or_null(self.base.as_deref().map(encode_image)),
            Value::Blob(encode_recorded(&self.recorded)),
            blob_or_null(self.applied.as_deref().map(encode_image)),
            blob_or_null(self.other_row.map(|r| r.to_be_bytes().to_vec())),
            Value::Text("open".into()),
        ];
        Row {
            row_id: self.row_id_in_table(),
            tombstone: false,
            cells: values.into_iter().map(|value| Cell { stamp, value }).collect(),
        }
    }

    /// Reads a conflict back from the values of a `_conflicts` row. Also
    /// returns its status.
    pub fn from_values(values: &[Value]) -> Result<(Conflict, String)> {
        let bad = |what: &str| Error::Corrupt(format!("_conflicts row: bad {what}"));
        let blob = |v: &Value| match v {
            Value::Blob(b) => Some(b.clone()),
            _ => None,
        };
        let text = |v: &Value| match v {
            Value::Text(t) => Some(t.clone()),
            _ => None,
        };
        let row_id = |v: &Value| blob(v).and_then(|b| RowId::from_be_bytes(b.try_into().ok()?));
        let [id, epoch, tbl, target, col, kind, rule, base, recorded, applied, other, status] = values else {
            return Err(bad("column count"));
        };
        let conflict = Conflict {
            id: blob(id).and_then(|b| b.try_into().ok()).ok_or_else(|| bad("id"))?,
            epoch: match epoch {
                Value::Integer(e) => *e as u64,
                _ => return Err(bad("epoch")),
            },
            table: 0,
            table_name: text(tbl).ok_or_else(|| bad("tbl"))?,
            row_id: row_id(target).ok_or_else(|| bad("target"))?,
            column: None,
            column_name: text(col),
            kind: text(kind).and_then(|k| ConflictKind::parse(&k)).ok_or_else(|| bad("kind"))?,
            rule: text(rule).ok_or_else(|| bad("rule"))?,
            base: blob(base).map(|b| decode_image(&b)).transpose()?,
            recorded: decode_recorded(&blob(recorded).ok_or_else(|| bad("recorded"))?)?,
            applied: blob(applied).map(|b| decode_image(&b)).transpose()?,
            other_row: row_id(other),
        };
        Ok((conflict, text(status).ok_or_else(|| bad("status"))?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_and_id() {
        let c = Conflict {
            id: conflict_id(4, "inventory", RowId::new(NodeId(1), 2), Some(3), ConflictKind::SameColumn),
            epoch: 4,
            table: 1,
            table_name: "inventory".into(),
            row_id: RowId::new(NodeId(1), 2),
            column: Some(3),
            column_name: Some("price".into()),
            kind: ConflictKind::SameColumn,
            rule: "lww_flag".into(),
            base: Some(vec![Value::Integer(1)]),
            recorded: vec![Recorded {
                node: NodeId(0xA),
                hlc: Hlc::new(10, 0).unwrap(),
                values: vec![Value::Integer(2)],
            }],
            applied: Some(vec![Value::Integer(3)]),
            other_row: None,
        };
        let row = c.to_row(Stamp::default());
        assert_eq!(row.row_id.node(), MERGE_NODE);
        let values: Vec<Value> = row.cells.into_iter().map(|c| c.value).collect();
        let (back, status) = Conflict::from_values(&values).unwrap();
        assert_eq!(status, "open");
        assert_eq!(back, Conflict { table: 0, column: None, ..c.clone() });
        assert_ne!(c.id, conflict_id(4, "inventory", c.row_id, None, ConflictKind::SameColumn));
        assert_ne!(c.id, conflict_id(5, "inventory", c.row_id, Some(3), ConflictKind::SameColumn));
    }
}
