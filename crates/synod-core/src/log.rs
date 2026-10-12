//! Epoch log format (specification v0.1, Section 5).
//!
//! A node records every write as a [`LogEntry`]. At the end of an epoch it
//! seals the entries of that epoch into one log: a header and the entries
//! in commit order. The log hash names the log in the manifest.

use std::fmt;

use crate::encoding::DecodeError;
use crate::{Hlc, NodeId, RowId, Value};

/// Magic bytes at the start of a sealed log.
pub const LOG_MAGIC: &[u8; 8] = b"SYNODLOG";

/// Format version of sealed logs.
pub const LOG_FORMAT_VERSION: u8 = 1;

/// Key-derivation context for log hashes.
pub const LOG_HASH_CONTEXT: &str = "SynodSQL 2026-10-12 log hash v1";

const ABSENT_IMAGE: u16 = 0xFFFF;

/// A transaction ID: the node and its local transaction counter.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Default)]
pub struct TxnId {
    /// Node that ran the transaction.
    pub node: NodeId,
    /// Local counter of that node.
    pub counter: u64,
}

impl fmt::Display for TxnId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.node, self.counter)
    }
}

/// Kind of change.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Op {
    /// A new row.
    Insert,
    /// A change to a live row.
    Update,
    /// A live row becomes a tombstone.
    Delete,
}

impl Op {
    fn code(self) -> u8 {
        match self {
            Op::Insert => 1,
            Op::Update => 2,
            Op::Delete => 3,
        }
    }

    fn from_code(code: u8) -> Option<Op> {
        Some(match code {
            1 => Op::Insert,
            2 => Op::Update,
            3 => Op::Delete,
            _ => return None,
        })
    }
}

/// One change to one row.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct LogEntry {
    /// Kind of change.
    pub op: Op,
    /// Table ID.
    pub table: u32,
    /// Row.
    pub row_id: RowId,
    /// Transaction.
    pub txn: TxnId,
    /// HLC time of the transaction. The stamp of each changed column is
    /// (this time, the transaction's node).
    pub hlc: Hlc,
    /// All columns before the change, in schema order. `None` for an
    /// insert.
    pub before: Option<Vec<Value>>,
    /// All columns after the change. `None` for a delete.
    pub after: Option<Vec<Value>>,
}

/// Errors from decoding a log or log entry.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum LogError {
    /// A value could not be decoded.
    Value(DecodeError),
    /// The input ended early.
    UnexpectedEnd,
    /// Bytes after the end.
    TrailingBytes,
    /// Wrong magic bytes or version.
    BadHeader,
    /// Unknown operation code.
    UnknownOp(u8),
    /// The images do not fit the operation, or have different lengths.
    BadImages,
}

impl fmt::Display for LogError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LogError::Value(e) => write!(f, "bad value: {e}"),
            LogError::UnexpectedEnd => write!(f, "unexpected end of log"),
            LogError::TrailingBytes => write!(f, "bytes after end of log"),
            LogError::BadHeader => write!(f, "not a SynodSQL v1 log"),
            LogError::UnknownOp(c) => write!(f, "unknown operation code {c}"),
            LogError::BadImages => write!(f, "images do not fit the operation"),
        }
    }
}

impl std::error::Error for LogError {}

impl From<DecodeError> for LogError {
    fn from(e: DecodeError) -> LogError {
        LogError::Value(e)
    }
}

struct Reader<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], LogError> {
        let end = self.pos.checked_add(n).filter(|&e| e <= self.bytes.len()).ok_or(LogError::UnexpectedEnd)?;
        let out = &self.bytes[self.pos..end];
        self.pos = end;
        Ok(out)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], LogError> {
        Ok(self.take(N)?.try_into().unwrap())
    }

    fn u64(&mut self) -> Result<u64, LogError> {
        Ok(u64::from_be_bytes(self.array()?))
    }

    fn u32(&mut self) -> Result<u32, LogError> {
        Ok(u32::from_be_bytes(self.array()?))
    }

    fn image(&mut self) -> Result<Option<Vec<Value>>, LogError> {
        let count = u16::from_be_bytes(self.array()?);
        if count == ABSENT_IMAGE {
            return Ok(None);
        }
        let mut values = Vec::with_capacity(count as usize);
        for _ in 0..count {
            let (v, n) = Value::decode_prefix(&self.bytes[self.pos..])?;
            self.pos += n;
            values.push(v);
        }
        Ok(Some(values))
    }

    fn finish(&self) -> Result<(), LogError> {
        if self.pos == self.bytes.len() { Ok(()) } else { Err(LogError::TrailingBytes) }
    }
}

fn put_image(out: &mut Vec<u8>, image: &Option<Vec<Value>>) {
    match image {
        None => out.extend_from_slice(&ABSENT_IMAGE.to_be_bytes()),
        Some(values) => {
            let count = u16::try_from(values.len()).ok().filter(|&c| c != ABSENT_IMAGE).expect("too many columns");
            out.extend_from_slice(&count.to_be_bytes());
            for v in values {
                v.encode_into(out);
            }
        }
    }
}

impl LogEntry {
    /// Returns the canonical encoding of the entry body.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.push(self.op.code());
        out.extend_from_slice(&self.table.to_be_bytes());
        out.extend_from_slice(&self.row_id.to_be_bytes());
        out.extend_from_slice(&self.txn.node.0.to_be_bytes());
        out.extend_from_slice(&self.txn.counter.to_be_bytes());
        out.extend_from_slice(&self.hlc.to_be_bytes());
        put_image(&mut out, &self.before);
        put_image(&mut out, &self.after);
        out
    }

    /// Decodes an entry body that fills all of `bytes`.
    pub fn decode(bytes: &[u8]) -> Result<LogEntry, LogError> {
        let mut r = Reader { bytes, pos: 0 };
        let entry = LogEntry::read(&mut r)?;
        r.finish()?;
        Ok(entry)
    }

    fn read(r: &mut Reader<'_>) -> Result<LogEntry, LogError> {
        let code = r.array::<1>()?[0];
        let op = Op::from_code(code).ok_or(LogError::UnknownOp(code))?;
        let table = r.u32()?;
        let row_id = RowId::from_be_bytes(r.array()?).ok_or(LogError::Value(DecodeError::ReservedBits))?;
        let txn = TxnId { node: NodeId(r.u32()?), counter: r.u64()? };
        let hlc = Hlc::from_u64(r.u64()?);
        let before = r.image()?;
        let after = r.image()?;
        let ok = match (op, &before, &after) {
            (Op::Insert, None, Some(_)) | (Op::Delete, Some(_), None) => true,
            (Op::Update, Some(b), Some(a)) => a.len() == b.len(),
            _ => false,
        };
        if !ok {
            return Err(LogError::BadImages);
        }
        Ok(LogEntry { op, table, row_id, txn, hlc, before, after })
    }
}

/// The header of a sealed log.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct LogHeader {
    /// Node that wrote the log.
    pub node: NodeId,
    /// Epoch of the log.
    pub epoch: u64,
    /// Root hash of the base state M(e−1).
    pub base_root: [u8; 32],
    /// Schema epoch.
    pub schema_epoch: u64,
    /// HLC time of the seal.
    pub sealed_at: Hlc,
}

/// Encodes a sealed log from a header and entry bodies (each from
/// [`LogEntry::encode`]), in commit order.
pub fn encode_log<'a>(header: &LogHeader, bodies: impl ExactSizeIterator<Item = &'a [u8]>) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(LOG_MAGIC);
    out.push(LOG_FORMAT_VERSION);
    out.extend_from_slice(&header.node.0.to_be_bytes());
    out.extend_from_slice(&header.epoch.to_be_bytes());
    out.extend_from_slice(&header.base_root);
    out.extend_from_slice(&header.schema_epoch.to_be_bytes());
    out.extend_from_slice(&(bodies.len() as u64).to_be_bytes());
    out.extend_from_slice(&header.sealed_at.to_be_bytes());
    for body in bodies {
        let len = u32::try_from(body.len()).expect("log entry longer than u32::MAX");
        out.extend_from_slice(&len.to_be_bytes());
        out.extend_from_slice(body);
    }
    out
}

/// Decodes a sealed log.
pub fn decode_log(bytes: &[u8]) -> Result<(LogHeader, Vec<LogEntry>), LogError> {
    let mut r = Reader { bytes, pos: 0 };
    if r.take(8)? != LOG_MAGIC || r.array::<1>()?[0] != LOG_FORMAT_VERSION {
        return Err(LogError::BadHeader);
    }
    let node = NodeId(r.u32()?);
    let epoch = r.u64()?;
    let base_root = r.array()?;
    let schema_epoch = r.u64()?;
    let count = r.u64()?;
    let sealed_at = Hlc::from_u64(r.u64()?);
    let mut entries = Vec::new();
    for _ in 0..count {
        let len = r.u32()? as usize;
        entries.push(LogEntry::decode(r.take(len)?)?);
    }
    r.finish()?;
    Ok((LogHeader { node, epoch, base_root, schema_epoch, sealed_at }, entries))
}

/// Returns the hash of a sealed log.
pub fn log_hash(bytes: &[u8]) -> [u8; 32] {
    let mut h = blake3::Hasher::new_derive_key(LOG_HASH_CONTEXT);
    h.update(bytes);
    *h.finalize().as_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(op: Op) -> LogEntry {
        let before = vec![Value::Integer(1), Value::Text("a".into())];
        let after = vec![Value::Integer(2), Value::Null];
        LogEntry {
            op,
            table: 7,
            row_id: RowId::new(NodeId(3), 9),
            txn: TxnId { node: NodeId(3), counter: 4 },
            hlc: Hlc::new(1_000, 2).unwrap(),
            before: (op != Op::Insert).then_some(before),
            after: (op != Op::Delete).then_some(after),
        }
    }

    #[test]
    fn entry_round_trip() {
        for op in [Op::Insert, Op::Update, Op::Delete] {
            let e = entry(op);
            assert_eq!(LogEntry::decode(&e.encode()).unwrap(), e);
        }
        let mut bad = entry(Op::Insert);
        bad.before = Some(vec![]);
        assert_eq!(LogEntry::decode(&bad.encode()), Err(LogError::BadImages));
    }

    #[test]
    fn log_round_trip() {
        let header = LogHeader {
            node: NodeId(3),
            epoch: 12,
            base_root: [9; 32],
            schema_epoch: 0,
            sealed_at: Hlc::new(5_000, 0).unwrap(),
        };
        let bodies: Vec<Vec<u8>> = [Op::Insert, Op::Update, Op::Delete].map(|op| entry(op).encode()).to_vec();
        let bytes = encode_log(&header, bodies.iter().map(Vec::as_slice));
        let (h, entries) = decode_log(&bytes).unwrap();
        assert_eq!(h, header);
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[1], entry(Op::Update));
        assert_eq!(decode_log(&bytes[..bytes.len() - 1]), Err(LogError::UnexpectedEnd));
        let mut extra = bytes.clone();
        extra.push(0);
        assert_eq!(decode_log(&extra), Err(LogError::TrailingBytes));
        assert_ne!(log_hash(&bytes), log_hash(&extra));
    }
}
