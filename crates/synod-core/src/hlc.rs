//! Hybrid logical clock (HLC), after Kulkarni et al., "Logical Physical
//! Clocks", OPODIS 2014.
//!
//! An [`Hlc`] is 64 bits: the high 48 bits hold physical time in
//! milliseconds since the Unix epoch, and the low 16 bits hold a logical
//! counter. Because of this layout, the numeric order of the `u64` is the
//! HLC order, and a logical counter that overflows carries into the
//! physical part.

use std::fmt;

use crate::NodeId;

/// Number of bits in the logical counter.
pub const LOGICAL_BITS: u32 = 16;

/// Largest physical time, in milliseconds, that an [`Hlc`] can hold.
pub const MAX_PHYSICAL_MS: u64 = (1 << (64 - LOGICAL_BITS)) - 1;

const LOGICAL_MASK: u64 = (1 << LOGICAL_BITS) - 1;

/// A hybrid logical clock value.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Hlc(u64);

impl Hlc {
    /// The smallest value.
    pub const ZERO: Hlc = Hlc(0);

    /// Makes a value from physical milliseconds and a logical counter.
    ///
    /// Returns `None` if `physical_ms` is larger than [`MAX_PHYSICAL_MS`].
    pub fn new(physical_ms: u64, logical: u16) -> Option<Hlc> {
        (physical_ms <= MAX_PHYSICAL_MS).then_some(Hlc((physical_ms << LOGICAL_BITS) | logical as u64))
    }

    /// Makes a value from its 64-bit representation.
    pub const fn from_u64(raw: u64) -> Hlc {
        Hlc(raw)
    }

    /// Returns the 64-bit representation.
    pub const fn as_u64(self) -> u64 {
        self.0
    }

    /// Returns the physical part, in milliseconds.
    pub const fn physical_ms(self) -> u64 {
        self.0 >> LOGICAL_BITS
    }

    /// Returns the logical counter.
    pub const fn logical(self) -> u16 {
        (self.0 & LOGICAL_MASK) as u16
    }

    /// Returns the canonical 8-byte big-endian encoding.
    pub const fn to_be_bytes(self) -> [u8; 8] {
        self.0.to_be_bytes()
    }

    /// Decodes the canonical 8-byte big-endian encoding.
    pub const fn from_be_bytes(bytes: [u8; 8]) -> Hlc {
        Hlc(u64::from_be_bytes(bytes))
    }

    fn successor(self) -> Result<Hlc, HlcError> {
        self.0.checked_add(1).map(Hlc).ok_or(HlcError::Overflow)
    }
}

impl fmt::Debug for Hlc {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Hlc({}.{})", self.physical_ms(), self.logical())
    }
}

impl fmt::Display for Hlc {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.physical_ms(), self.logical())
    }
}

/// A write stamp: an HLC value and the node that made the write.
///
/// The order is (HLC, node ID). This is the total order that
/// last-writer-wins uses (white paper, Section 6.4).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Default)]
pub struct Stamp {
    /// HLC time of the write.
    pub hlc: Hlc,
    /// Node that made the write.
    pub node: NodeId,
}

impl Stamp {
    /// Length of the canonical encoding, in bytes.
    pub const ENCODED_LEN: usize = 12;

    /// Returns the canonical encoding: HLC (8 bytes) then node ID (4 bytes),
    /// both big-endian. Byte order equals stamp order.
    pub fn to_be_bytes(self) -> [u8; Self::ENCODED_LEN] {
        let mut out = [0; Self::ENCODED_LEN];
        out[..8].copy_from_slice(&self.hlc.to_be_bytes());
        out[8..].copy_from_slice(&self.node.0.to_be_bytes());
        out
    }

    /// Decodes the canonical encoding.
    pub fn from_be_bytes(bytes: [u8; Self::ENCODED_LEN]) -> Stamp {
        let (hlc, node) = bytes.split_at(8);
        Stamp {
            hlc: Hlc::from_be_bytes(hlc.try_into().unwrap()),
            node: NodeId(u32::from_be_bytes(node.try_into().unwrap())),
        }
    }
}

/// Errors from [`HlcClock`].
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum HlcError {
    /// A remote HLC is too far ahead of the local physical clock.
    DriftExceeded {
        /// Physical part of the remote HLC.
        remote_ms: u64,
        /// Local physical clock.
        local_ms: u64,
        /// Configured limit.
        max_drift_ms: u64,
    },
    /// The physical clock is larger than [`MAX_PHYSICAL_MS`], or the HLC
    /// cannot advance.
    Overflow,
}

impl fmt::Display for HlcError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HlcError::DriftExceeded { remote_ms, local_ms, max_drift_ms } => write!(
                f,
                "remote clock {remote_ms} ms is more than {max_drift_ms} ms ahead of local clock {local_ms} ms"
            ),
            HlcError::Overflow => write!(f, "hybrid logical clock overflow"),
        }
    }
}

impl std::error::Error for HlcError {}

/// The HLC state of one node.
///
/// The clock does not read the system time. The caller gives the physical
/// time to each call, so the simulator can control it.
#[derive(Clone, Debug)]
pub struct HlcClock {
    last: Hlc,
    max_drift_ms: u64,
}

impl HlcClock {
    /// Makes a clock that rejects remote values more than `max_drift_ms`
    /// ahead of the local physical clock.
    pub fn new(max_drift_ms: u64) -> HlcClock {
        HlcClock::resume(Hlc::ZERO, max_drift_ms)
    }

    /// Makes a clock that continues after `last`, for example after a
    /// restart with a persisted value.
    pub fn resume(last: Hlc, max_drift_ms: u64) -> HlcClock {
        HlcClock { last, max_drift_ms }
    }

    /// Returns the last value that this clock gave out.
    pub fn last(&self) -> Hlc {
        self.last
    }

    /// Returns the drift limit in milliseconds.
    pub fn max_drift_ms(&self) -> u64 {
        self.max_drift_ms
    }

    /// Returns a new value for a local event or a send.
    ///
    /// The result is always larger than every earlier result, also if the
    /// physical clock goes backward.
    pub fn now(&mut self, physical_ms: u64) -> Result<Hlc, HlcError> {
        let next = self.next_after(self.last, physical_ms)?;
        self.last = next;
        Ok(next)
    }

    /// Checks a remote value against the drift limit, with no change to
    /// the clock.
    pub fn check(&self, remote: Hlc, physical_ms: u64) -> Result<(), HlcError> {
        if remote.physical_ms() > physical_ms.saturating_add(self.max_drift_ms) {
            return Err(HlcError::DriftExceeded {
                remote_ms: remote.physical_ms(),
                local_ms: physical_ms,
                max_drift_ms: self.max_drift_ms,
            });
        }
        Ok(())
    }

    /// Merges a remote value at a receive, and returns a new value that is
    /// larger than both the remote value and every earlier local value.
    ///
    /// If the remote value is too far ahead, the clock does not change.
    pub fn update(&mut self, remote: Hlc, physical_ms: u64) -> Result<Hlc, HlcError> {
        self.check(remote, physical_ms)?;
        let next = self.next_after(self.last.max(remote), physical_ms)?;
        self.last = next;
        Ok(next)
    }

    // If the physical clock is ahead of `floor`, start a new millisecond
    // with counter 0. Otherwise add 1 to `floor`; an overflowing counter
    // carries into the physical part.
    fn next_after(&self, floor: Hlc, physical_ms: u64) -> Result<Hlc, HlcError> {
        if physical_ms > MAX_PHYSICAL_MS {
            return Err(HlcError::Overflow);
        }
        if physical_ms > floor.physical_ms() { Ok(Hlc(physical_ms << LOGICAL_BITS)) } else { floor.successor() }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hlc(ms: u64, c: u16) -> Hlc {
        Hlc::new(ms, c).unwrap()
    }

    #[test]
    fn layout() {
        let h = hlc(0x1234_5678_9abc, 0xdef0);
        assert_eq!(h.as_u64(), 0x1234_5678_9abc_def0);
        assert_eq!(h.physical_ms(), 0x1234_5678_9abc);
        assert_eq!(h.logical(), 0xdef0);
        assert_eq!(Hlc::new(MAX_PHYSICAL_MS + 1, 0), None);
    }

    #[test]
    fn now_uses_physical_time_when_it_advances() {
        let mut c = HlcClock::new(100);
        assert_eq!(c.now(10).unwrap(), hlc(10, 0));
        assert_eq!(c.now(10).unwrap(), hlc(10, 1));
        assert_eq!(c.now(12).unwrap(), hlc(12, 0));
        // Physical clock goes backward: the logical counter advances.
        assert_eq!(c.now(5).unwrap(), hlc(12, 1));
    }

    #[test]
    fn update_follows_the_paper() {
        let mut c = HlcClock::new(100);
        c.now(10).unwrap();
        // Remote ahead of local HLC and physical clock.
        assert_eq!(c.update(hlc(20, 3), 15).unwrap(), hlc(20, 4));
        // Physical clock ahead of both.
        assert_eq!(c.update(hlc(18, 9), 30).unwrap(), hlc(30, 0));
        // Same millisecond on all three: max counter + 1.
        assert_eq!(c.update(hlc(30, 7), 30).unwrap(), hlc(30, 8));
    }

    #[test]
    fn logical_overflow_carries() {
        let mut c = HlcClock::resume(hlc(10, u16::MAX), 100);
        assert_eq!(c.now(10).unwrap(), hlc(11, 0));
    }

    #[test]
    fn drift_is_rejected_and_state_kept() {
        let mut c = HlcClock::new(50);
        let before = c.now(1000).unwrap();
        let err = c.update(hlc(1051, 0), 1000).unwrap_err();
        assert!(matches!(err, HlcError::DriftExceeded { remote_ms: 1051, local_ms: 1000, max_drift_ms: 50 }));
        assert_eq!(c.last(), before);
        assert!(c.update(hlc(1050, 0), 1000).is_ok());
    }

    #[test]
    fn stamp_order_and_bytes() {
        let a = Stamp { hlc: hlc(5, 0), node: NodeId(9) };
        let b = Stamp { hlc: hlc(5, 0), node: NodeId(10) };
        let c = Stamp { hlc: hlc(5, 1), node: NodeId(1) };
        assert!(a < b && b < c);
        assert!(a.to_be_bytes() < b.to_be_bytes() && b.to_be_bytes() < c.to_be_bytes());
        assert_eq!(Stamp::from_be_bytes(c.to_be_bytes()), c);
    }
}
