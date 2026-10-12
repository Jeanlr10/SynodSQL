//! Canonical encoding of values and rows (specification v0.1, Section 4).
//!
//! Every node must turn the same row into the same bytes, or root hashes
//! differ although the data agrees. So the encoding has exactly one form
//! for each value:
//!
//! - all integers are big-endian, with fixed widths;
//! - a decimal is normalized: no trailing zeros in the units;
//! - a float is its raw IEEE 754 bits, except that every NaN becomes one
//!   NaN (x86-64 and ARM64 make NaNs with different bits);
//! - text is its UTF-8 bytes, with no Unicode normalization;
//! - a set is sorted by the encoded bytes of its items, with no duplicates.
//!
//! The decoder accepts only the canonical form, so for each value there is
//! exactly one byte string, and the reverse.

use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::str::FromStr;

use crate::{RowId, Stamp};

/// Value type tags.
mod tag {
    pub const NULL: u8 = 0x00;
    pub const INTEGER: u8 = 0x01;
    pub const REAL: u8 = 0x02;
    pub const DECIMAL: u8 = 0x03;
    pub const TEXT: u8 = 0x04;
    pub const BLOB: u8 = 0x05;
    pub const SET: u8 = 0x06;
}

/// Version byte at the start of an encoded row.
pub const ROW_FORMAT_VERSION: u8 = 1;

/// Key-derivation context for row hashes (BLAKE3 `derive_key` mode).
pub const ROW_HASH_CONTEXT: &str = "SynodSQL 2026-10-12 row hash v1";

/// The one NaN bit pattern that the encoding uses.
pub const CANONICAL_NAN_BITS: u64 = 0x7ff8_0000_0000_0000;

/// Largest decimal scale (digits after the point).
pub const MAX_DECIMAL_SCALE: u8 = 38;

const FLAG_TOMBSTONE: u8 = 0x01;

/// An exact decimal number: `units × 10^(−scale)`.
///
/// The value is always normalized, so two equal numbers have the same
/// fields: 0.10 and 0.1 are both `units = 1, scale = 1`.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Decimal {
    units: i128,
    scale: u8,
}

impl Decimal {
    /// Makes `units × 10^(−scale)`. Returns `None` if `scale` is larger than
    /// [`MAX_DECIMAL_SCALE`].
    pub fn new(mut units: i128, mut scale: u8) -> Option<Decimal> {
        if scale > MAX_DECIMAL_SCALE {
            return None;
        }
        if units == 0 {
            scale = 0;
        }
        while scale > 0 && units % 10 == 0 {
            units /= 10;
            scale -= 1;
        }
        Some(Decimal { units, scale })
    }

    /// Returns the unscaled value.
    pub fn units(self) -> i128 {
        self.units
    }

    /// Returns the number of digits after the point.
    pub fn scale(self) -> u8 {
        self.scale
    }
}

impl fmt::Display for Decimal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let digits = self.units.unsigned_abs().to_string();
        let sign = if self.units < 0 { "-" } else { "" };
        let scale = self.scale as usize;
        if scale == 0 {
            write!(f, "{sign}{digits}")
        } else if digits.len() > scale {
            let (int, frac) = digits.split_at(digits.len() - scale);
            write!(f, "{sign}{int}.{frac}")
        } else {
            write!(f, "{sign}0.{digits:0>scale$}")
        }
    }
}

/// Error from parsing a [`Decimal`].
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct ParseDecimalError;

impl fmt::Display for ParseDecimalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid decimal")
    }
}

impl std::error::Error for ParseDecimalError {}

impl FromStr for Decimal {
    type Err = ParseDecimalError;

    /// Parses `[-]digits[.digits]`.
    fn from_str(s: &str) -> Result<Decimal, ParseDecimalError> {
        let (negative, body) = match s.strip_prefix('-') {
            Some(rest) => (true, rest),
            None => (false, s),
        };
        let (int, frac) = body.split_once('.').unwrap_or((body, ""));
        let all_digits = |p: &str| p.bytes().all(|b| b.is_ascii_digit());
        if int.is_empty() || !all_digits(int) || !all_digits(frac) || (body.contains('.') && frac.is_empty()) {
            return Err(ParseDecimalError);
        }
        let scale = u8::try_from(frac.len()).map_err(|_| ParseDecimalError)?;
        let mut units: i128 = 0;
        for b in int.bytes().chain(frac.bytes()) {
            units = units.checked_mul(10).and_then(|u| u.checked_add((b - b'0') as i128)).ok_or(ParseDecimalError)?;
        }
        Decimal::new(if negative { -units } else { units }, scale).ok_or(ParseDecimalError)
    }
}

/// A set of values for `union` columns.
///
/// The items are sorted by their canonical encoding and have no
/// duplicates. An item cannot be a set.
#[derive(Clone, PartialEq, Eq, Hash, Debug, Default)]
pub struct ValueSet(Vec<Value>);

impl ValueSet {
    /// Makes a set. Returns `None` if an item is a set.
    pub fn new(items: impl IntoIterator<Item = Value>) -> Option<ValueSet> {
        let mut items: Vec<Value> = items.into_iter().collect();
        if items.iter().any(|v| matches!(v, Value::Set(_))) {
            return None;
        }
        items.sort();
        items.dedup();
        Some(ValueSet(items))
    }

    /// Returns the items in canonical order.
    pub fn items(&self) -> &[Value] {
        &self.0
    }

    /// Returns the number of items.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns true if the set has no items.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns true if the set holds `value`.
    pub fn contains(&self, value: &Value) -> bool {
        self.0.binary_search(value).is_ok()
    }
}

/// A column value.
///
/// Equality, order, and hashing all use the canonical encoding. So two
/// values are equal exactly when every node stores the same bytes for them,
/// and the order is byte-wise, as the merge needs (white paper,
/// Section 6.4).
#[derive(Clone, Debug)]
pub enum Value {
    /// SQL NULL.
    Null,
    /// 64-bit signed integer.
    Integer(i64),
    /// 64-bit IEEE 754 float.
    Real(f64),
    /// Exact decimal.
    Decimal(Decimal),
    /// UTF-8 text.
    Text(String),
    /// Bytes.
    Blob(Vec<u8>),
    /// Set of values.
    Set(ValueSet),
}

impl Value {
    /// Appends the canonical encoding to `out`.
    ///
    /// # Panics
    ///
    /// Panics if a text, blob, or set is longer than `u32::MAX`.
    pub fn encode_into(&self, out: &mut Vec<u8>) {
        match self {
            Value::Null => out.push(tag::NULL),
            Value::Integer(i) => {
                out.push(tag::INTEGER);
                out.extend_from_slice(&i.to_be_bytes());
            }
            Value::Real(r) => {
                out.push(tag::REAL);
                let bits = if r.is_nan() { CANONICAL_NAN_BITS } else { r.to_bits() };
                out.extend_from_slice(&bits.to_be_bytes());
            }
            Value::Decimal(d) => {
                out.push(tag::DECIMAL);
                out.push(d.scale);
                out.extend_from_slice(&d.units.to_be_bytes());
            }
            Value::Text(s) => {
                out.push(tag::TEXT);
                put_len(out, s.len());
                out.extend_from_slice(s.as_bytes());
            }
            Value::Blob(b) => {
                out.push(tag::BLOB);
                put_len(out, b.len());
                out.extend_from_slice(b);
            }
            Value::Set(set) => {
                out.push(tag::SET);
                put_len(out, set.len());
                for item in set.items() {
                    item.encode_into(out);
                }
            }
        }
    }

    /// Returns the canonical encoding.
    pub fn to_canonical_bytes(&self) -> Vec<u8> {
        let mut out = Vec::new();
        self.encode_into(&mut out);
        out
    }

    /// Decodes a canonical encoding that fills all of `bytes`.
    pub fn from_canonical_bytes(bytes: &[u8]) -> Result<Value, DecodeError> {
        let mut r = Reader::new(bytes);
        let v = r.value(true)?;
        r.finish()?;
        Ok(v)
    }

    /// Decodes one canonical value at the start of `bytes`, and returns it
    /// with its length.
    pub fn decode_prefix(bytes: &[u8]) -> Result<(Value, usize), DecodeError> {
        let mut r = Reader::new(bytes);
        let v = r.value(true)?;
        Ok((v, r.pos))
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Value) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for Value {}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Value) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Value {
    fn cmp(&self, other: &Value) -> Ordering {
        self.to_canonical_bytes().cmp(&other.to_canonical_bytes())
    }
}

impl Hash for Value {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.to_canonical_bytes().hash(state);
    }
}

fn put_len(out: &mut Vec<u8>, len: usize) {
    let len = u32::try_from(len).expect("value longer than u32::MAX");
    out.extend_from_slice(&len.to_be_bytes());
}

/// One column of a row: the stamp of the last write and the value.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct Cell {
    /// Stamp of the last write to this column.
    pub stamp: Stamp,
    /// The value.
    pub value: Value,
}

/// A row in its replicated form: the part of the state that all nodes
/// share and that the root hash covers.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct Row {
    /// Logical row ID.
    pub row_id: RowId,
    /// True if the row is deleted.
    pub tombstone: bool,
    /// Columns, in schema order.
    pub cells: Vec<Cell>,
}

/// A 256-bit row hash.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RowHash(pub [u8; 32]);

impl fmt::Debug for RowHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RowHash({self})")
    }
}

impl fmt::Display for RowHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.iter().try_for_each(|b| write!(f, "{b:02x}"))
    }
}

impl Row {
    /// Appends the canonical encoding to `out`.
    ///
    /// # Panics
    ///
    /// Panics if the row has more than `u16::MAX` columns.
    pub fn encode_into(&self, out: &mut Vec<u8>) {
        out.push(ROW_FORMAT_VERSION);
        out.extend_from_slice(&self.row_id.to_be_bytes());
        out.push(if self.tombstone { FLAG_TOMBSTONE } else { 0 });
        let count = u16::try_from(self.cells.len()).expect("more than u16::MAX columns");
        out.extend_from_slice(&count.to_be_bytes());
        for cell in &self.cells {
            out.extend_from_slice(&cell.stamp.to_be_bytes());
            cell.value.encode_into(out);
        }
    }

    /// Returns the canonical encoding.
    pub fn to_canonical_bytes(&self) -> Vec<u8> {
        let mut out = Vec::new();
        self.encode_into(&mut out);
        out
    }

    /// Decodes a canonical encoding that fills all of `bytes`.
    pub fn from_canonical_bytes(bytes: &[u8]) -> Result<Row, DecodeError> {
        let mut r = Reader::new(bytes);
        let version = r.u8()?;
        if version != ROW_FORMAT_VERSION {
            return Err(DecodeError::UnsupportedVersion(version));
        }
        let row_id = RowId::from_be_bytes(r.array()?).ok_or(DecodeError::ReservedBits)?;
        let tombstone = match r.u8()? {
            0 => false,
            FLAG_TOMBSTONE => true,
            _ => return Err(DecodeError::ReservedBits),
        };
        let count = u16::from_be_bytes(r.array()?);
        let mut cells = Vec::with_capacity(count as usize);
        for _ in 0..count {
            let stamp = Stamp::from_be_bytes(r.array()?);
            let value = r.value(true)?;
            cells.push(Cell { stamp, value });
        }
        r.finish()?;
        Ok(Row { row_id, tombstone, cells })
    }

    /// Returns the row hash: BLAKE3 in key-derivation mode, with context
    /// [`ROW_HASH_CONTEXT`], over the canonical encoding.
    pub fn hash(&self) -> RowHash {
        let mut h = blake3::Hasher::new_derive_key(ROW_HASH_CONTEXT);
        h.update(&self.to_canonical_bytes());
        RowHash(*h.finalize().as_bytes())
    }
}

/// Errors from decoding a canonical encoding.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum DecodeError {
    /// The input ended inside a value.
    UnexpectedEnd,
    /// The input has bytes after the value.
    TrailingBytes,
    /// Unknown value tag.
    UnknownTag(u8),
    /// Unknown row format version.
    UnsupportedVersion(u8),
    /// Reserved bits are not zero.
    ReservedBits,
    /// Text is not valid UTF-8.
    InvalidUtf8,
    /// A NaN with bits other than [`CANONICAL_NAN_BITS`].
    NonCanonicalNan,
    /// A decimal that is not normalized, or has a scale that is too large.
    NonCanonicalDecimal,
    /// Set items are not in strictly ascending order.
    UnsortedSet,
    /// A set inside a set.
    NestedSet,
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DecodeError::UnexpectedEnd => write!(f, "unexpected end of input"),
            DecodeError::TrailingBytes => write!(f, "bytes after end of value"),
            DecodeError::UnknownTag(t) => write!(f, "unknown value tag 0x{t:02x}"),
            DecodeError::UnsupportedVersion(v) => write!(f, "unsupported row format version {v}"),
            DecodeError::ReservedBits => write!(f, "reserved bits are not zero"),
            DecodeError::InvalidUtf8 => write!(f, "text is not valid UTF-8"),
            DecodeError::NonCanonicalNan => write!(f, "NaN is not canonical"),
            DecodeError::NonCanonicalDecimal => write!(f, "decimal is not normalized"),
            DecodeError::UnsortedSet => write!(f, "set items are not sorted and unique"),
            DecodeError::NestedSet => write!(f, "set inside a set"),
        }
    }
}

impl std::error::Error for DecodeError {}

struct Reader<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(bytes: &'a [u8]) -> Reader<'a> {
        Reader { bytes, pos: 0 }
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8], DecodeError> {
        let end = self.pos.checked_add(n).filter(|&e| e <= self.bytes.len()).ok_or(DecodeError::UnexpectedEnd)?;
        let out = &self.bytes[self.pos..end];
        self.pos = end;
        Ok(out)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], DecodeError> {
        Ok(self.take(N)?.try_into().unwrap())
    }

    fn u8(&mut self) -> Result<u8, DecodeError> {
        Ok(self.take(1)?[0])
    }

    fn len(&mut self) -> Result<usize, DecodeError> {
        Ok(u32::from_be_bytes(self.array()?) as usize)
    }

    fn finish(&self) -> Result<(), DecodeError> {
        if self.pos == self.bytes.len() { Ok(()) } else { Err(DecodeError::TrailingBytes) }
    }

    fn value(&mut self, allow_set: bool) -> Result<Value, DecodeError> {
        Ok(match self.u8()? {
            tag::NULL => Value::Null,
            tag::INTEGER => Value::Integer(i64::from_be_bytes(self.array()?)),
            tag::REAL => {
                let bits = u64::from_be_bytes(self.array()?);
                let r = f64::from_bits(bits);
                if r.is_nan() && bits != CANONICAL_NAN_BITS {
                    return Err(DecodeError::NonCanonicalNan);
                }
                Value::Real(r)
            }
            tag::DECIMAL => {
                let scale = self.u8()?;
                let units = i128::from_be_bytes(self.array()?);
                let d = Decimal::new(units, scale).ok_or(DecodeError::NonCanonicalDecimal)?;
                if d.units != units || d.scale != scale {
                    return Err(DecodeError::NonCanonicalDecimal);
                }
                Value::Decimal(d)
            }
            tag::TEXT => {
                let len = self.len()?;
                let s = std::str::from_utf8(self.take(len)?).map_err(|_| DecodeError::InvalidUtf8)?;
                Value::Text(s.to_owned())
            }
            tag::BLOB => {
                let len = self.len()?;
                Value::Blob(self.take(len)?.to_vec())
            }
            tag::SET => {
                if !allow_set {
                    return Err(DecodeError::NestedSet);
                }
                let count = self.len()?;
                let mut items = Vec::new();
                let mut prev: Option<&[u8]> = None;
                for _ in 0..count {
                    let start = self.pos;
                    let item = self.value(false)?;
                    let encoded = &self.bytes[start..self.pos];
                    if prev.is_some_and(|p| p >= encoded) {
                        return Err(DecodeError::UnsortedSet);
                    }
                    prev = Some(encoded);
                    items.push(item);
                }
                Value::Set(ValueSet(items))
            }
            t => return Err(DecodeError::UnknownTag(t)),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dec(s: &str) -> Decimal {
        s.parse().unwrap()
    }

    #[test]
    fn decimal_normalizes() {
        assert_eq!(dec("0.10"), dec("0.1"));
        assert_eq!(dec("0.10").units(), 1);
        assert_eq!(dec("0.10").scale(), 1);
        assert_eq!(dec("-0.000"), dec("0"));
        assert_eq!(dec("100"), Decimal::new(100, 0).unwrap());
        assert_eq!(dec("12.50").to_string(), "12.5");
        assert_eq!(dec("-0.05").to_string(), "-0.05");
        for bad in ["", "-", "1.", ".5", "1.2.3", "1e5", "+1", "１"] {
            assert!(bad.parse::<Decimal>().is_err(), "{bad:?} parsed");
        }
    }

    #[test]
    fn nan_is_canonical() {
        let quiet = Value::Real(f64::NAN);
        let negative = Value::Real(f64::from_bits(0xfff8_0000_0000_0000));
        let payload = Value::Real(f64::from_bits(0x7ff0_0000_0000_0001));
        assert_eq!(quiet.to_canonical_bytes(), negative.to_canonical_bytes());
        assert_eq!(quiet.to_canonical_bytes(), payload.to_canonical_bytes());
        assert_eq!(quiet, negative);
        let mut bad = vec![tag::REAL];
        bad.extend_from_slice(&0xfff8_0000_0000_0000u64.to_be_bytes());
        assert_eq!(Value::from_canonical_bytes(&bad), Err(DecodeError::NonCanonicalNan));
    }

    #[test]
    fn signed_zero_is_kept() {
        assert_ne!(Value::Real(0.0), Value::Real(-0.0));
    }

    #[test]
    fn set_is_sorted_and_unique() {
        let a = ValueSet::new([Value::Text("b".into()), Value::Text("a".into()), Value::Text("b".into())]).unwrap();
        let b = ValueSet::new([Value::Text("a".into()), Value::Text("b".into())]).unwrap();
        assert_eq!(a, b);
        assert_eq!(a.len(), 2);
        assert!(a.contains(&Value::Text("a".into())));
        assert!(ValueSet::new([Value::Set(ValueSet::default())]).is_none());

        let mut unsorted = vec![tag::SET, 0, 0, 0, 2];
        Value::Integer(2).encode_into(&mut unsorted);
        Value::Integer(1).encode_into(&mut unsorted);
        assert_eq!(Value::from_canonical_bytes(&unsorted), Err(DecodeError::UnsortedSet));
    }

    #[test]
    fn rejects_non_canonical_input() {
        let mut d = vec![tag::DECIMAL, 2];
        d.extend_from_slice(&10i128.to_be_bytes());
        assert_eq!(Value::from_canonical_bytes(&d), Err(DecodeError::NonCanonicalDecimal));
        assert_eq!(Value::from_canonical_bytes(&[tag::TEXT, 0, 0, 0, 1, 0xff]), Err(DecodeError::InvalidUtf8));
        assert_eq!(Value::from_canonical_bytes(&[0x7f]), Err(DecodeError::UnknownTag(0x7f)));
        assert_eq!(Value::from_canonical_bytes(&[tag::NULL, 0]), Err(DecodeError::TrailingBytes));
        assert_eq!(Value::from_canonical_bytes(&[tag::INTEGER, 0]), Err(DecodeError::UnexpectedEnd));
    }
}
