//! A virtual disk with crash injection.
//!
//! Writes go to a page cache first. Only [`Disk::sync`] makes them
//! durable. At a crash, each file keeps its durable content plus a random
//! prefix of its unsynced operations, and the last kept append can be torn
//! (only part of its bytes reach the disk). Files are independent, so
//! unsynced writes to two files can survive in any combination.
//!
//! The simulator can also arm a disk to crash at a later operation. The
//! operation then does not happen, and the node stops in the middle of its
//! callback. This tests a crash at every write, not only between events.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;

use crate::rng::SimRng;

/// The panic payload that stops a node at an injected crash. The simulator
/// catches it; node code must not.
#[derive(Debug)]
pub struct CrashSignal;

#[derive(Clone, Debug)]
enum Op {
    Write(Vec<u8>),
    Append(Vec<u8>),
    Remove,
}

#[derive(Clone, Debug, Default)]
struct File {
    durable: Option<Vec<u8>>,
    pending: Vec<Op>,
}

impl File {
    fn apply(content: &mut Option<Vec<u8>>, op: &Op) {
        match op {
            Op::Write(data) => *content = Some(data.clone()),
            Op::Append(data) => content.get_or_insert_with(Vec::new).extend_from_slice(data),
            Op::Remove => *content = None,
        }
    }

    fn current(&self) -> Option<Vec<u8>> {
        let mut content = self.durable.clone();
        for op in &self.pending {
            File::apply(&mut content, op);
        }
        content
    }
}

#[derive(Debug, Default)]
struct State {
    files: BTreeMap<String, File>,
    crash_after_ops: Option<u64>,
}

/// The disk of one simulated node. Clones share the same disk.
#[derive(Clone, Debug, Default)]
pub struct Disk(Rc<RefCell<State>>);

impl Disk {
    /// Makes an empty disk.
    pub fn new() -> Disk {
        Disk::default()
    }

    /// Reads a file as the node sees it, with unsynced writes.
    pub fn read(&self, name: &str) -> Option<Vec<u8>> {
        self.0.borrow().files.get(name).and_then(File::current)
    }

    /// Returns true if the file exists.
    pub fn exists(&self, name: &str) -> bool {
        self.read(name).is_some()
    }

    /// Returns the names of all files that exist.
    pub fn list(&self) -> Vec<String> {
        let state = self.0.borrow();
        state.files.iter().filter(|(_, f)| f.current().is_some()).map(|(n, _)| n.clone()).collect()
    }

    /// Replaces the content of a file. At a crash, the replacement is kept
    /// fully or not at all.
    pub fn write(&self, name: &str, data: &[u8]) {
        self.op(name, Op::Write(data.to_vec()));
    }

    /// Appends to a file. At a crash, an unsynced append can be torn.
    pub fn append(&self, name: &str, data: &[u8]) {
        self.op(name, Op::Append(data.to_vec()));
    }

    /// Removes a file.
    pub fn remove(&self, name: &str) {
        self.op(name, Op::Remove);
    }

    /// Makes all earlier operations on a file durable.
    pub fn sync(&self, name: &str) {
        self.maybe_crash();
        let mut state = self.0.borrow_mut();
        if let Some(file) = state.files.get_mut(name) {
            file.durable = file.current();
            file.pending.clear();
        }
    }

    /// Returns the content of a file that is durable now, without
    /// unsynced writes.
    pub fn read_durable(&self, name: &str) -> Option<Vec<u8>> {
        self.0.borrow().files.get(name).and_then(|f| f.durable.clone())
    }

    fn op(&self, name: &str, op: Op) {
        self.maybe_crash();
        self.0.borrow_mut().files.entry(name.to_owned()).or_default().pending.push(op);
    }

    fn maybe_crash(&self) {
        let mut state = self.0.borrow_mut();
        match &mut state.crash_after_ops {
            Some(0) => {
                state.crash_after_ops = None;
                drop(state);
                std::panic::panic_any(CrashSignal);
            }
            Some(n) => *n -= 1,
            None => {}
        }
    }

    /// Makes the disk crash at its `ops`-th next operation (0 is the next
    /// one).
    pub(crate) fn arm_crash(&self, ops: u64) {
        self.0.borrow_mut().crash_after_ops = Some(ops);
    }

    /// Removes an armed crash that has not happened yet.
    pub(crate) fn disarm(&self) {
        self.0.borrow_mut().crash_after_ops = None;
    }

    /// Loses unsynced data as a power loss would. Returns the number of
    /// unsynced operations that were lost or torn.
    pub(crate) fn crash(&self, rng: &mut SimRng) -> usize {
        let mut state = self.0.borrow_mut();
        state.crash_after_ops = None;
        let mut lost = 0;
        for file in state.files.values_mut() {
            let pending = std::mem::take(&mut file.pending);
            let keep = rng.below(pending.len() as u64 + 1) as usize;
            lost += pending.len() - keep;
            for op in &pending[..keep] {
                File::apply(&mut file.durable, op);
            }
            if let Some(Op::Append(data)) = pending.get(keep)
                && !data.is_empty()
                && rng.chance(500_000)
            {
                let torn = rng.below(data.len() as u64) as usize;
                File::apply(&mut file.durable, &Op::Append(data[..torn].to_vec()));
            }
        }
        state.files.retain(|_, f| f.durable.is_some());
        lost
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sync_makes_writes_durable() {
        let disk = Disk::new();
        disk.write("a", b"one");
        disk.append("a", b"two");
        assert_eq!(disk.read("a").unwrap(), b"onetwo");
        assert_eq!(disk.read_durable("a"), None);
        disk.sync("a");
        assert_eq!(disk.read_durable("a").unwrap(), b"onetwo");
        disk.remove("a");
        assert!(!disk.exists("a"));
        disk.sync("a");
        assert_eq!(disk.read_durable("a"), None);
    }

    #[test]
    fn crash_keeps_synced_data_and_a_prefix_of_the_rest() {
        for seed in 0..200 {
            let disk = Disk::new();
            disk.write("f", b"base");
            disk.sync("f");
            disk.append("f", b"-1");
            disk.append("f", b"-2");
            disk.crash(&mut SimRng::new(seed));
            let after = disk.read("f").unwrap();
            assert!(b"base-1-2".starts_with(&after), "{:?}", String::from_utf8_lossy(&after));
            assert!(after.starts_with(b"base"));
        }
    }

    #[test]
    fn armed_crash_stops_the_operation() {
        crate::sim::install_crash_hook();
        let disk = Disk::new();
        disk.arm_crash(1);
        disk.write("f", b"x");
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| disk.sync("f")));
        assert!(result.unwrap_err().is::<CrashSignal>());
        assert_eq!(disk.read_durable("f"), None);
    }
}
