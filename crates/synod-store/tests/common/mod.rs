//! Test helpers: a store on the crash file system with a manual clock.

#![allow(dead_code)]

use std::io;
use std::sync::Arc;

use synod_core::{Cell, Hlc, LogEntry, NodeId, Op, Row, RowId, Stamp, Value};
use synod_sim::CrashFs;
use synod_store::{Env, FileSystem, ManualClock, Store, StoreConfig};

pub const START_MS: u64 = 1_791_763_200_000;
pub const EPOCH_MS: u64 = 60_000;
pub const PATH: &str = "/node.db";

pub const INVENTORY: &str = "CREATE TABLE inventory (
    row_id  ROWID,
    sku     TEXT UNIQUE                MERGE lww,
    name    TEXT                       MERGE lww,
    price   DECIMAL(10,2)              MERGE lww_flag,
    qty     INTEGER CHECK (qty >= 0)   MERGE delta,
    tags    SET<TEXT>                  MERGE union,
    notes   TEXT                       MERGE text3
)";

/// The crash file system, seen through the store's trait.
pub struct SimFs(pub Arc<CrashFs>);

impl FileSystem for SimFs {
    fn vfs(&self) -> Option<&str> {
        Some(self.0.vfs_name())
    }
    fn exists(&self, path: &str) -> io::Result<bool> {
        Ok(self.0.exists(path))
    }
    fn sync_file(&self, path: &str) -> io::Result<()> {
        self.0.sync_file(path)
    }
    fn rename(&self, from: &str, to: &str) -> io::Result<()> {
        self.0.rename(from, to)
    }
    fn sync_dir(&self, _path: &str) -> io::Result<()> {
        self.0.sync_dir()
    }
    fn remove(&self, path: &str) -> io::Result<()> {
        self.0.remove(path)
    }
}

pub struct Node {
    pub fs: Arc<CrashFs>,
    pub clock: ManualClock,
    pub config: StoreConfig,
}

impl Node {
    pub fn new(node: u32, seed: u64) -> Node {
        Node {
            fs: Arc::new(CrashFs::new(seed)),
            clock: ManualClock::new(START_MS),
            config: StoreConfig { node: NodeId(node), genesis_ms: START_MS, epoch_len_ms: EPOCH_MS, max_drift_ms: 500 },
        }
    }

    pub fn env(&self) -> Env {
        Env { fs: Arc::new(SimFs(self.fs.clone())), clock: Arc::new(self.clock.clone()) }
    }

    pub fn open(&self) -> synod_store::Result<Store> {
        Store::open(PATH, self.config.clone(), self.env())
    }
}

pub fn text(s: &str) -> Value {
    Value::Text(s.into())
}

pub fn dec(s: &str) -> Value {
    Value::Decimal(s.parse().unwrap())
}

/// Replays log entries onto rows, starting from `rows`, and checks that
/// each entry's before-image equals the current values. Returns the rows
/// in their replicated form: this is what the log says the state is.
pub fn replay(rows: &mut std::collections::BTreeMap<RowId, Row>, entries: &[LogEntry]) {
    for e in entries {
        let stamp = Stamp { hlc: e.hlc, node: e.txn.node };
        let current = rows.get(&e.row_id).map(|r| r.cells.iter().map(|c| c.value.clone()).collect::<Vec<_>>());
        match e.op {
            Op::Insert => {
                assert!(current.is_none(), "insert of existing row {}", e.row_id);
                let after = e.after.clone().unwrap();
                let cells = after.into_iter().map(|value| Cell { stamp, value }).collect();
                rows.insert(e.row_id, Row { row_id: e.row_id, tombstone: false, cells });
            }
            Op::Update => {
                let row = rows.get_mut(&e.row_id).expect("update of missing row");
                assert!(!row.tombstone, "update of tombstone {}", e.row_id);
                assert_eq!(current.as_ref(), e.before.as_ref(), "before-image of {}", e.row_id);
                let (before, after) = (e.before.as_ref().unwrap(), e.after.as_ref().unwrap());
                for (i, cell) in row.cells.iter_mut().enumerate() {
                    if after[i] != before[i] {
                        *cell = Cell { stamp, value: after[i].clone() };
                    }
                }
            }
            Op::Delete => {
                let row = rows.get_mut(&e.row_id).expect("delete of missing row");
                assert!(!row.tombstone, "delete of tombstone {}", e.row_id);
                assert_eq!(current.as_ref(), e.before.as_ref(), "before-image of {}", e.row_id);
                row.tombstone = true;
            }
        }
    }
}

pub fn hlc(ms: u64) -> Hlc {
    Hlc::new(ms, 0).unwrap()
}
