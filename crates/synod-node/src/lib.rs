//! A SynodSQL node: the epoch cycle over a network (white paper, Sections
//! V and XI; specification Section 6).
//!
//! Each epoch, every node seals its log and sends it to the others. The
//! epoch's leader (rank 0: member `e mod N`; rank r: the r-th next member,
//! after r time slots) publishes a manifest, the list of logs to merge.
//! Every node merges the manifest of highest priority on its last merge,
//! and sends the root hash of M(e) to the others, which compare it with
//! their own. A node that is behind follows a peer's manifests, or takes
//! over its base state by a Prolly tree diff when it is more than L epochs
//! behind; its own logs go into later manifests as late logs, or to
//! `_conflicts` after L epochs.
//!
//! [`Peer`] is a state machine without I/O: the simulator and the network
//! transport drive the same code.

mod metrics;
pub mod msg;
mod peer;

pub use metrics::Metrics;
pub use msg::{Link, Msg, Status};
pub use peer::{Event, Peer, PeerConfig};
