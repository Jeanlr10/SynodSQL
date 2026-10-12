//! Deterministic cluster simulator for SynodSQL.
//!
//! The simulator runs a whole cluster in one thread, with a virtual clock,
//! a virtual network (delay, reordering, loss, partitions), and a virtual
//! disk per node with crash injection. All randomness comes from one seed,
//! so a seed replays a run exactly, byte for byte. This is the main test
//! tool of the project, in the style of FoundationDB's simulation testing.
//!
//! Use [`check_seeds`] to run a test over many seeds; on a failure it
//! prints the seed and the command that replays it.

pub mod disk;
pub mod rng;
mod seeds;
mod sim;
#[allow(unsafe_code)]
pub mod sqlite;

pub use disk::Disk;
pub use rng::SimRng;
pub use seeds::check_seeds;
pub use sim::{ClockConfig, Config, Ctx, FaultConfig, NetConfig, Node, NodeIndex, Sim, Stats};
pub use sqlite::CrashFs;
