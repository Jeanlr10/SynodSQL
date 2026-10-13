//! The SynodSQL node store on SQLite, and the deterministic merge.
//!
//! A [`Store`] keeps the replicated state of one node in one SQLite file.
//! Managed tables are declared with merge policies ([`schema`]). Every
//! write goes through a [`Txn`], which gives each row a [`RowId`] and each
//! change an HLC stamp, and records the change in the epoch log in the same
//! SQLite transaction. [`Store::seal`] closes an epoch's log and computes
//! the state hash. [`Store::merge`] merges the epoch's logs of all nodes
//! into the next base state M(e), with the pure function in [`merge`].
//!
//! Reads use normal SQL on a view per table, which hides tombstones:
//!
//! ```
//! use synod_core::Value;
//! use synod_store::{Env, ManualClock, Store, StoreConfig};
//! # let dir = std::env::temp_dir().join(format!("synod-doc-{}", std::process::id()));
//! # std::fs::create_dir_all(&dir).unwrap();
//! # let path = dir.join("node.db");
//! # let path = path.to_str().unwrap();
//!
//! let clock = ManualClock::new(1_791_763_200_000);
//! let env = Env { clock: std::sync::Arc::new(clock), ..Env::os() };
//! let config = StoreConfig { node: synod_core::NodeId(1), genesis_ms: 0, epoch_len_ms: 60_000, max_drift_ms: 500, lateness: 16 };
//! let mut store = Store::open(path, config, env)?;
//! store.create_table("CREATE TABLE stock (sku TEXT UNIQUE, qty INTEGER CHECK (qty >= 0) MERGE delta)")?;
//!
//! let mut txn = store.begin()?;
//! txn.insert("stock", &[("sku", Value::Text("A-100".into())), ("qty", Value::Integer(500))])?;
//! txn.commit()?;
//!
//! let rows = store.query("SELECT sku, qty FROM stock", &[])?;
//! assert_eq!(rows.len(), 1);
//! # std::fs::remove_dir_all(&dir).ok();
//! # Ok::<(), synod_store::Error>(())
//! ```
//!
//! [`RowId`]: synod_core::RowId

pub mod env;
mod error;
pub mod merge;
pub mod schema;
mod store;
mod sync;
mod tree;
pub mod values;

pub use env::{Clock, Env, FileSystem, ManualClock, OsFs, SystemClock};
pub use error::{Error, Result};
pub use store::{Commit, QueryLimits, ResultColumn, ResultKind, STORE_FORMAT, SealedLog, Store, StoreConfig, Txn};
pub use sync::{BaseImage, BaseRow, MergeReport, MergedLog, MergedRecord, RevertReport};
