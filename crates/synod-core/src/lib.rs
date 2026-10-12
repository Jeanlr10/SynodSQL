//! Core data formats for SynodSQL.
//!
//! This crate holds the parts that every node must agree on byte for byte:
//! hybrid logical clocks ([`hlc`]), row IDs ([`row_id`]), the canonical
//! encoding of values and rows ([`encoding`]), the epoch log ([`log`]), and
//! the Prolly tree that gives the state hash ([`prolly`]). The formats are specified in
//! `docs/spec/v0.1.md`.

pub mod encoding;
pub mod hlc;
pub mod log;
pub mod manifest;
pub mod prolly;
pub mod row_id;

pub use encoding::{Cell, Decimal, Row, RowHash, Value, ValueSet};
pub use hlc::{Hlc, HlcClock, HlcError, Stamp};
pub use log::{LogEntry, Op, TxnId};
pub use manifest::{LogRef, Manifest};
pub use row_id::{AllocError, CounterStore, RowId, RowIdAllocator};

/// A 32-bit node ID. Each member node of a cluster has a unique ID.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Default)]
pub struct NodeId(pub u32);

impl std::fmt::Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:08x}", self.0)
    }
}
