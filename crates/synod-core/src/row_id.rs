//! Row IDs and the row ID allocator.
//!
//! A [`RowId`] is 128 bits: origin node ID (32), local counter (64), and
//! reserved bits (32, zero in format v0.1). Two nodes cannot make the same
//! ID, and a node never gives out the same counter twice, also after a
//! crash. So a merge never renumbers rows.

use std::fmt;

use crate::NodeId;

/// A 128-bit logical row ID. It never changes and is never reused.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RowId(u128);

impl RowId {
    /// Makes the ID for counter `counter` on node `node`.
    pub const fn new(node: NodeId, counter: u64) -> RowId {
        RowId(((node.0 as u128) << 96) | ((counter as u128) << 32))
    }

    /// Returns the origin node.
    pub const fn node(self) -> NodeId {
        NodeId((self.0 >> 96) as u32)
    }

    /// Returns the local counter of the origin node.
    pub const fn counter(self) -> u64 {
        (self.0 >> 32) as u64
    }

    /// Returns the reserved bits. They are zero in format v0.1.
    pub const fn reserved(self) -> u32 {
        self.0 as u32
    }

    /// Returns the 128-bit representation.
    pub const fn as_u128(self) -> u128 {
        self.0
    }

    /// Returns the canonical 16-byte big-endian encoding. Byte order equals
    /// ID order, which is (node, counter).
    pub const fn to_be_bytes(self) -> [u8; 16] {
        self.0.to_be_bytes()
    }

    /// Decodes the canonical encoding. Returns `None` if the reserved bits
    /// are not zero.
    pub const fn from_be_bytes(bytes: [u8; 16]) -> Option<RowId> {
        let raw = u128::from_be_bytes(bytes);
        if raw as u32 == 0 { Some(RowId(raw)) } else { None }
    }
}

impl fmt::Debug for RowId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RowId({self})")
    }
}

impl fmt::Display for RowId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:08x}-{:016x}", self.node().0, self.counter())
    }
}

/// Durable storage for the allocator's high-water mark.
///
/// The high-water mark is the first counter value that the allocator may
/// not give out without a new [`persist`](CounterStore::persist).
pub trait CounterStore {
    /// Error type of the store.
    type Error;

    /// Reads the persisted high-water mark. A new store returns 0.
    fn load(&mut self) -> Result<u64, Self::Error>;

    /// Records a new high-water mark. This must not return `Ok` until the
    /// value is durable: after a crash, [`load`](CounterStore::load) must
    /// return this value or a larger one.
    fn persist(&mut self, high_water: u64) -> Result<(), Self::Error>;
}

/// A [`CounterStore`] in memory. It is not durable, so use it only in
/// tests.
#[derive(Clone, Debug, Default)]
pub struct MemoryCounterStore {
    /// The stored high-water mark.
    pub high_water: u64,
}

impl CounterStore for MemoryCounterStore {
    type Error = std::convert::Infallible;

    fn load(&mut self) -> Result<u64, Self::Error> {
        Ok(self.high_water)
    }

    fn persist(&mut self, high_water: u64) -> Result<(), Self::Error> {
        self.high_water = high_water;
        Ok(())
    }
}

/// Errors from [`RowIdAllocator`].
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum AllocError<E> {
    /// The 64-bit counter of this node is used up.
    Exhausted,
    /// The counter store failed.
    Store(E),
}

impl<E: fmt::Display> fmt::Display for AllocError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AllocError::Exhausted => write!(f, "row ID counter exhausted"),
            AllocError::Store(e) => write!(f, "row ID counter store failed: {e}"),
        }
    }
}

impl<E: fmt::Debug + fmt::Display> std::error::Error for AllocError<E> {}

/// Gives out row IDs for one node.
///
/// The allocator reserves counters in blocks. Before it gives out the first
/// counter of a block, it makes the end of the block durable. After a crash,
/// it continues from the persisted end, so it can skip counters but never
/// reuses one.
#[derive(Debug)]
pub struct RowIdAllocator<S> {
    node: NodeId,
    store: S,
    next: u64,
    limit: u64,
    block: u64,
}

impl<S: CounterStore> RowIdAllocator<S> {
    /// Opens an allocator that reserves `block` counters per persist.
    ///
    /// # Panics
    ///
    /// Panics if `block` is 0.
    pub fn open(node: NodeId, mut store: S, block: u64) -> Result<Self, AllocError<S::Error>> {
        assert!(block > 0, "block size must be at least 1");
        let high_water = store.load().map_err(AllocError::Store)?;
        Ok(RowIdAllocator { node, store, next: high_water, limit: high_water, block })
    }

    /// Returns the node of this allocator.
    pub fn node(&self) -> NodeId {
        self.node
    }

    /// Gives out a new row ID.
    pub fn allocate(&mut self) -> Result<RowId, AllocError<S::Error>> {
        if self.next == self.limit {
            let limit = self.limit.checked_add(self.block).ok_or(AllocError::Exhausted)?;
            self.store.persist(limit).map_err(AllocError::Store)?;
            self.limit = limit;
        }
        let id = RowId::new(self.node, self.next);
        self.next += 1;
        Ok(id)
    }

    /// Returns the counter store.
    pub fn store(&self) -> &S {
        &self.store
    }

    /// Returns the counter store, and drops the allocator.
    pub fn into_store(self) -> S {
        self.store
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn layout() {
        let id = RowId::new(NodeId(0xaabb_ccdd), 0x0102_0304_0506_0708);
        assert_eq!(id.as_u128(), 0xaabb_ccdd_0102_0304_0506_0708_0000_0000);
        assert_eq!(id.node(), NodeId(0xaabb_ccdd));
        assert_eq!(id.counter(), 0x0102_0304_0506_0708);
        assert_eq!(id.reserved(), 0);
        assert_eq!(id.to_string(), "aabbccdd-0102030405060708");
        assert_eq!(RowId::from_be_bytes(id.to_be_bytes()), Some(id));
        let mut bad = id.to_be_bytes();
        bad[15] = 1;
        assert_eq!(RowId::from_be_bytes(bad), None);
    }

    #[test]
    fn persists_before_use() {
        let mut a = RowIdAllocator::open(NodeId(1), MemoryCounterStore::default(), 4).unwrap();
        assert_eq!(a.allocate().unwrap().counter(), 0);
        assert_eq!(a.store().high_water, 4);
        for _ in 0..3 {
            a.allocate().unwrap();
        }
        assert_eq!(a.store().high_water, 4);
        assert_eq!(a.allocate().unwrap().counter(), 4);
        assert_eq!(a.store().high_water, 8);

        // Restart: continue after the persisted block, skipping 5..8.
        let mut b = RowIdAllocator::open(NodeId(1), a.into_store(), 4).unwrap();
        assert_eq!(b.allocate().unwrap().counter(), 8);
    }

    #[test]
    fn exhaustion() {
        let store = MemoryCounterStore { high_water: u64::MAX - 1 };
        let mut a = RowIdAllocator::open(NodeId(1), store, 1).unwrap();
        assert_eq!(a.allocate().unwrap().counter(), u64::MAX - 1);
        assert_eq!(a.allocate(), Err(AllocError::Exhausted));
    }
}
