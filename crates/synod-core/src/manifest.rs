//! Manifest format (specification Section 6).
//!
//! A manifest is the list of logs that the merge of one epoch takes. The
//! epoch's leader publishes it. Every node that merges the same manifest on
//! the same base gets the same state, so the manifest hash names a merge.

use std::fmt;

use crate::NodeId;
use crate::log::LogError;

/// Magic bytes at the start of a manifest.
pub const MANIFEST_MAGIC: &[u8; 8] = b"SYNODMAN";

/// Format version of manifests.
pub const MANIFEST_FORMAT_VERSION: u8 = 1;

/// Key-derivation context for manifest hashes.
pub const MANIFEST_HASH_CONTEXT: &str = "SynodSQL 2026-10-12 manifest hash v1";

/// One log in a manifest.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct LogRef {
    /// Node that wrote the log.
    pub node: NodeId,
    /// Epoch of the log. Earlier than the manifest's epoch: a late log.
    pub epoch: u64,
    /// Log hash.
    pub hash: [u8; 32],
}

/// The logs that one epoch's merge takes.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct Manifest {
    /// The merged epoch.
    pub epoch: u64,
    /// Rank of the leader that made it: 0 has the highest priority.
    pub rank: u32,
    /// The leader.
    pub leader: NodeId,
    /// Garbage collection through this epoch (specification Section 7.7).
    pub gc_through: Option<u64>,
    /// The logs, sorted by (node, epoch), at most one per (node, epoch).
    pub logs: Vec<LogRef>,
}

impl Manifest {
    /// Makes a manifest; sorts the logs.
    pub fn new(epoch: u64, rank: u32, leader: NodeId, gc_through: Option<u64>, mut logs: Vec<LogRef>) -> Manifest {
        logs.sort();
        logs.dedup();
        Manifest { epoch, rank, leader, gc_through, logs }
    }

    /// Returns the canonical encoding.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = MANIFEST_MAGIC.to_vec();
        out.push(MANIFEST_FORMAT_VERSION);
        out.extend_from_slice(&self.epoch.to_be_bytes());
        out.extend_from_slice(&self.rank.to_be_bytes());
        out.extend_from_slice(&self.leader.0.to_be_bytes());
        match self.gc_through {
            None => out.push(0),
            Some(e) => {
                out.push(1);
                out.extend_from_slice(&e.to_be_bytes());
            }
        }
        out.extend_from_slice(&(self.logs.len() as u32).to_be_bytes());
        for l in &self.logs {
            out.extend_from_slice(&l.node.0.to_be_bytes());
            out.extend_from_slice(&l.epoch.to_be_bytes());
            out.extend_from_slice(&l.hash);
        }
        out
    }

    /// Decodes a canonical encoding. Rejects logs that are not sorted and
    /// unique, or that are not earlier than or equal to the epoch.
    pub fn decode(bytes: &[u8]) -> Result<Manifest, LogError> {
        let mut pos = 0;
        let mut take = |n: usize| -> Result<&[u8], LogError> {
            let end = pos + n;
            let out = bytes.get(pos..end).ok_or(LogError::UnexpectedEnd)?;
            pos = end;
            Ok(out)
        };
        if take(8)? != MANIFEST_MAGIC || take(1)?[0] != MANIFEST_FORMAT_VERSION {
            return Err(LogError::BadHeader);
        }
        let u64_at = |b: &[u8]| u64::from_be_bytes(b.try_into().unwrap());
        let u32_at = |b: &[u8]| u32::from_be_bytes(b.try_into().unwrap());
        let epoch = u64_at(take(8)?);
        let rank = u32_at(take(4)?);
        let leader = NodeId(u32_at(take(4)?));
        let gc_through = match take(1)?[0] {
            0 => None,
            1 => Some(u64_at(take(8)?)),
            _ => return Err(LogError::BadHeader),
        };
        let count = u32_at(take(4)?);
        let mut logs = Vec::new();
        for _ in 0..count {
            let node = NodeId(u32_at(take(4)?));
            let log_epoch = u64_at(take(8)?);
            let hash: [u8; 32] = take(32)?.try_into().unwrap();
            logs.push(LogRef { node, epoch: log_epoch, hash });
        }
        if pos != bytes.len() {
            return Err(LogError::TrailingBytes);
        }
        let ordered = logs.windows(2).all(|w| (w[0].node, w[0].epoch) < (w[1].node, w[1].epoch));
        if !ordered || logs.iter().any(|l| l.epoch > epoch) {
            return Err(LogError::BadHeader);
        }
        Ok(Manifest { epoch, rank, leader, gc_through, logs })
    }

    /// Returns the manifest hash.
    pub fn hash(&self) -> [u8; 32] {
        let mut h = blake3::Hasher::new_derive_key(MANIFEST_HASH_CONTEXT);
        h.update(&self.encode());
        *h.finalize().as_bytes()
    }
}

impl fmt::Display for Manifest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "manifest e{} rank {} by {} ({} logs)", self.epoch, self.rank, self.leader, self.logs.len())
    }
}

/// The leader of rank `rank` for `epoch`, among `members` sorted by node ID:
/// `members[(epoch + rank) mod N]` (specification Section 6).
pub fn leader(members: &[NodeId], epoch: u64, rank: u32) -> NodeId {
    members[((epoch + rank as u64) % members.len() as u64) as usize]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_and_rules() {
        let r = |n: u32, e: u64| LogRef { node: NodeId(n), epoch: e, hash: [n as u8; 32] };
        let m = Manifest::new(9, 1, NodeId(3), Some(1), vec![r(2, 9), r(1, 8), r(1, 9), r(1, 9)]);
        assert_eq!(m.logs, vec![r(1, 8), r(1, 9), r(2, 9)]);
        assert_eq!(Manifest::decode(&m.encode()).unwrap(), m);
        let bad = Manifest { logs: vec![r(2, 9), r(1, 9)], ..m.clone() };
        assert!(Manifest::decode(&bad.encode()).is_err());
        let future = Manifest { logs: vec![r(1, 10)], ..m.clone() };
        assert!(Manifest::decode(&future.encode()).is_err());
        assert_ne!(m.hash(), Manifest { rank: 0, ..m.clone() }.hash());
        let members = [NodeId(1), NodeId(5), NodeId(9)];
        assert_eq!([0, 1, 2, 3].map(|k| leader(&members, 7, k)), [NodeId(5), NodeId(9), NodeId(1), NodeId(5)]);
    }
}
