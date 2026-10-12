//! The store's environment: file system operations and the physical clock.
//!
//! The store never reads the system clock or calls the file system
//! directly. Tests give it a simulated clock and a crash-injecting file
//! system.

use std::io;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// File operations that SQLite does not do for the store.
pub trait FileSystem: Send + Sync {
    /// The SQLite VFS to open databases with. `None` is SQLite's default.
    fn vfs(&self) -> Option<&str>;
    /// Returns true if the file exists.
    fn exists(&self, path: &str) -> io::Result<bool>;
    /// Makes the content of a file durable.
    fn sync_file(&self, path: &str) -> io::Result<()>;
    /// Renames a file atomically, replacing `to`.
    fn rename(&self, from: &str, to: &str) -> io::Result<()>;
    /// Makes creates, deletes, and renames in the directory of `path`
    /// durable.
    fn sync_dir(&self, path: &str) -> io::Result<()>;
    /// Removes a file.
    fn remove(&self, path: &str) -> io::Result<()>;
}

/// The operating system's file system.
#[derive(Clone, Copy, Debug, Default)]
pub struct OsFs;

impl FileSystem for OsFs {
    fn vfs(&self) -> Option<&str> {
        None
    }

    fn exists(&self, path: &str) -> io::Result<bool> {
        Path::new(path).try_exists()
    }

    fn sync_file(&self, path: &str) -> io::Result<()> {
        std::fs::File::open(path)?.sync_all()
    }

    fn rename(&self, from: &str, to: &str) -> io::Result<()> {
        std::fs::rename(from, to)
    }

    fn sync_dir(&self, path: &str) -> io::Result<()> {
        let dir = Path::new(path).parent().filter(|p| !p.as_os_str().is_empty()).unwrap_or(Path::new("."));
        #[cfg(unix)]
        {
            std::fs::File::open(dir)?.sync_all()
        }
        #[cfg(not(unix))]
        {
            let _ = dir;
            Ok(())
        }
    }

    fn remove(&self, path: &str) -> io::Result<()> {
        std::fs::remove_file(path)
    }
}

/// A physical clock in milliseconds since the Unix epoch.
pub trait Clock: Send + Sync {
    /// Returns the current time.
    fn now_ms(&self) -> u64;
}

/// The system clock.
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_ms(&self) -> u64 {
        SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_millis() as u64)
    }
}

/// A clock that moves only when told to. Clones share the same time.
#[derive(Clone, Debug, Default)]
pub struct ManualClock(Arc<AtomicU64>);

impl ManualClock {
    /// Makes a clock at `ms`.
    pub fn new(ms: u64) -> ManualClock {
        ManualClock(Arc::new(AtomicU64::new(ms)))
    }

    /// Sets the time.
    pub fn set(&self, ms: u64) {
        self.0.store(ms, Ordering::SeqCst);
    }

    /// Moves the time forward.
    pub fn advance(&self, ms: u64) {
        self.0.fetch_add(ms, Ordering::SeqCst);
    }
}

impl Clock for ManualClock {
    fn now_ms(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
}

/// The file system and clock of a store.
#[derive(Clone)]
pub struct Env {
    /// File operations.
    pub fs: Arc<dyn FileSystem>,
    /// Physical clock.
    pub clock: Arc<dyn Clock>,
}

impl Env {
    /// The operating system's file system and clock.
    pub fn os() -> Env {
        Env { fs: Arc::new(OsFs), clock: Arc::new(SystemClock) }
    }
}
