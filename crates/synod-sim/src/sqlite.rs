//! A SQLite VFS over an in-memory disk with crash injection.
//!
//! [`CrashFs`] registers a SQLite VFS. A connection opened with that VFS
//! keeps its files in memory, with the same durability rules as
//! [`Disk`](crate::Disk):
//!
//! - a write reaches the "disk" only at `xSync` of its file;
//! - a create, delete, or rename reaches the disk only at a directory sync
//!   (an `xSync` of any file also syncs the directory, as SQLite does);
//! - at a [`crash`](CrashFs::crash), each file keeps a random subset of its
//!   unsynced writes, a kept write can be torn at a 512-byte sector
//!   boundary, and the directory keeps a random prefix of its unsynced
//!   operations.
//!
//! [`crash_after`](CrashFs::crash_after) makes the file system die at a
//! later operation: that operation and all later ones fail with an I/O
//! error, as if the power went off. The caller then drops its connections
//! and calls `crash`.
//!
//! One file can have only one connection at a time. Locks are no-ops, and
//! the WAL index (`-shm`) is in memory and does not survive a crash.

use std::collections::BTreeMap;
use std::ffi::{CStr, CString, c_char, c_int, c_void};
use std::io;
use std::ptr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard};

use rusqlite::ffi;

use crate::rng::SimRng;

const SECTOR: usize = 512;

#[derive(Clone, Debug)]
enum DataOp {
    Write { offset: usize, data: Vec<u8> },
    Truncate(usize),
}

impl DataOp {
    fn apply(&self, content: &mut Vec<u8>) {
        match self {
            DataOp::Write { offset, data } => {
                let end = offset + data.len();
                if content.len() < end {
                    content.resize(end, 0);
                }
                content[*offset..end].copy_from_slice(data);
            }
            DataOp::Truncate(len) => content.truncate(*len),
        }
    }
}

#[derive(Clone, Debug)]
enum DirOp {
    Link(String, usize),
    Unlink(String),
    Rename(String, String),
}

impl DirOp {
    fn apply(&self, names: &mut BTreeMap<String, usize>) {
        match self {
            DirOp::Link(name, ino) => {
                names.insert(name.clone(), *ino);
            }
            DirOp::Unlink(name) => {
                names.remove(name);
            }
            DirOp::Rename(from, to) => {
                if let Some(ino) = names.remove(from) {
                    names.insert(to.clone(), ino);
                }
            }
        }
    }
}

#[derive(Default)]
struct Inode {
    durable: Vec<u8>,
    live: Vec<u8>,
    pending: Vec<DataOp>,
    shm: Vec<Box<[u8]>>,
    open: u32,
}

struct State {
    inodes: Vec<Inode>,
    live_names: BTreeMap<String, usize>,
    durable_names: BTreeMap<String, usize>,
    dir_pending: Vec<DirOp>,
    ops: u64,
    die_at: Option<u64>,
    dead: bool,
    open_files: u32,
    random: SimRng,
    temp: u64,
}

impl State {
    // Counts one mutating operation. Returns false if the file system is
    // dead, or dies at this operation.
    fn tick(&mut self) -> bool {
        if self.dead {
            return false;
        }
        if self.die_at == Some(self.ops) {
            self.dead = true;
            return false;
        }
        self.ops += 1;
        true
    }

    fn sync_dir(&mut self) {
        for op in self.dir_pending.drain(..) {
            op.apply(&mut self.durable_names);
        }
    }

    fn sync_inode(&mut self, ino: usize) {
        let inode = &mut self.inodes[ino];
        inode.durable = inode.live.clone();
        inode.pending.clear();
        self.sync_dir();
    }
}

struct Shared {
    state: Mutex<State>,
    name: CString,
    vfs: *mut ffi::sqlite3_vfs,
}

impl Shared {
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }
}

/// An in-memory file system for SQLite with crash injection. See the
/// module documentation.
pub struct CrashFs {
    // Owned; freed in `Drop` unless a file is still open.
    shared: *mut Shared,
}

// SAFETY: all access to the shared state goes through its mutex.
unsafe impl Send for CrashFs {}
unsafe impl Sync for CrashFs {}

static NEXT_ID: AtomicU64 = AtomicU64::new(0);

fn dead_error() -> io::Error {
    io::Error::other("crash file system is dead")
}

impl CrashFs {
    /// Makes an empty file system and registers its VFS. `seed` drives
    /// SQLite's `xRandomness`.
    pub fn new(seed: u64) -> CrashFs {
        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        let name = CString::new(format!("synod-crash-{id}")).unwrap();
        let mut state = empty_state();
        state.random = SimRng::new(seed);
        let shared = Box::into_raw(Box::new(Shared { state: Mutex::new(state), name, vfs: ptr::null_mut() }));
        // SAFETY: `shared` was just made from a box and is valid.
        let shared_ref = unsafe { &mut *shared };
        let vfs = Box::new(ffi::sqlite3_vfs {
            iVersion: 2,
            szOsFile: std::mem::size_of::<File>() as c_int,
            mxPathname: 512,
            pNext: ptr::null_mut(),
            zName: shared_ref.name.as_ptr(),
            pAppData: shared as *mut c_void,
            xOpen: Some(x_open),
            xDelete: Some(x_delete),
            xAccess: Some(x_access),
            xFullPathname: Some(x_full_pathname),
            xDlOpen: None,
            xDlError: None,
            xDlSym: None,
            xDlClose: None,
            xRandomness: Some(x_randomness),
            xSleep: Some(x_sleep),
            xCurrentTime: Some(x_current_time),
            xGetLastError: Some(x_get_last_error),
            xCurrentTimeInt64: Some(x_current_time_int64),
            xSetSystemCall: None,
            xGetSystemCall: None,
            xNextSystemCall: None,
        });
        shared_ref.vfs = Box::into_raw(vfs);
        // SAFETY: the VFS struct and its name live until `Drop` unregisters
        // them.
        let rc = unsafe { ffi::sqlite3_vfs_register(shared_ref.vfs, 0) };
        assert_eq!(rc, ffi::SQLITE_OK, "sqlite3_vfs_register failed");
        CrashFs { shared }
    }

    fn shared(&self) -> &Shared {
        // SAFETY: `shared` is valid until `Drop`.
        unsafe { &*self.shared }
    }

    /// Returns the VFS name to pass to SQLite.
    pub fn vfs_name(&self) -> &str {
        self.shared().name.to_str().unwrap()
    }

    /// Returns the number of mutating operations so far (writes,
    /// truncates, syncs, creates, deletes, renames).
    pub fn ops(&self) -> u64 {
        self.shared().lock().ops
    }

    /// Makes the file system die at the `n`-th next mutating operation (0
    /// is the next one).
    pub fn crash_after(&self, n: u64) {
        let mut s = self.shared().lock();
        s.die_at = Some(s.ops + n);
    }

    /// Removes a crash set with [`crash_after`](CrashFs::crash_after) that
    /// has not happened yet.
    pub fn disarm(&self) {
        self.shared().lock().die_at = None;
    }

    /// Returns true if the file system has died.
    pub fn is_dead(&self) -> bool {
        self.shared().lock().dead
    }

    /// Loses unsynced data, as a power loss would, and makes the file
    /// system work again.
    ///
    /// # Panics
    ///
    /// Panics if a file is still open. Close all connections first.
    pub fn crash(&self, rng: &mut SimRng) {
        let mut guard = self.shared().lock();
        let s = &mut *guard;
        assert_eq!(s.open_files, 0, "crash with open files");
        let keep = rng.below(s.dir_pending.len() as u64 + 1) as usize;
        for op in s.dir_pending.drain(..).take(keep) {
            op.apply(&mut s.durable_names);
        }
        for inode in &mut s.inodes {
            for op in std::mem::take(&mut inode.pending) {
                if !rng.chance(500_000) {
                    continue;
                }
                match op {
                    DataOp::Write { offset, mut data } if data.len() > SECTOR && rng.chance(250_000) => {
                        let sectors = data.len().div_ceil(SECTOR) as u64;
                        data.truncate(rng.below(sectors) as usize * SECTOR);
                        DataOp::Write { offset, data }.apply(&mut inode.durable);
                    }
                    op => op.apply(&mut inode.durable),
                }
            }
            inode.live = inode.durable.clone();
            inode.shm.clear();
        }
        s.live_names = s.durable_names.clone();
        s.dead = false;
        s.die_at = None;
    }

    /// Returns true if a file exists.
    pub fn exists(&self, path: &str) -> bool {
        self.shared().lock().live_names.contains_key(path)
    }

    /// Makes a file's content durable.
    pub fn sync_file(&self, path: &str) -> io::Result<()> {
        let mut s = self.shared().lock();
        let ino = *s.live_names.get(path).ok_or(io::ErrorKind::NotFound)?;
        if !s.tick() {
            return Err(dead_error());
        }
        s.sync_inode(ino);
        Ok(())
    }

    /// Renames a file atomically. The rename is durable after
    /// [`sync_dir`](CrashFs::sync_dir).
    pub fn rename(&self, from: &str, to: &str) -> io::Result<()> {
        let mut s = self.shared().lock();
        let ino = *s.live_names.get(from).ok_or(io::ErrorKind::NotFound)?;
        if !s.tick() {
            return Err(dead_error());
        }
        s.live_names.remove(from);
        s.live_names.insert(to.to_owned(), ino);
        s.dir_pending.push(DirOp::Rename(from.to_owned(), to.to_owned()));
        Ok(())
    }

    /// Makes all creates, deletes, and renames durable.
    pub fn sync_dir(&self) -> io::Result<()> {
        let mut s = self.shared().lock();
        if !s.tick() {
            return Err(dead_error());
        }
        s.sync_dir();
        Ok(())
    }

    /// Removes a file. The removal is durable after a directory sync.
    pub fn remove(&self, path: &str) -> io::Result<()> {
        let mut s = self.shared().lock();
        if !s.live_names.contains_key(path) {
            return Err(io::ErrorKind::NotFound.into());
        }
        if !s.tick() {
            return Err(dead_error());
        }
        s.live_names.remove(path);
        s.dir_pending.push(DirOp::Unlink(path.to_owned()));
        Ok(())
    }

    /// Returns the names of all files that exist.
    pub fn list(&self) -> Vec<String> {
        self.shared().lock().live_names.keys().cloned().collect()
    }

    /// Writes a whole file, durably, as a copy from another machine would.
    /// It replaces a file of the same name.
    pub fn write_file(&self, path: &str, data: &[u8]) {
        let mut s = self.shared().lock();
        let ino = s.inodes.len();
        s.inodes.push(Inode { durable: data.to_vec(), live: data.to_vec(), ..Inode::default() });
        s.live_names.insert(path.to_owned(), ino);
        s.durable_names.insert(path.to_owned(), ino);
    }

    /// Returns the content of a file as the process sees it.
    pub fn read(&self, path: &str) -> Option<Vec<u8>> {
        let s = self.shared().lock();
        s.live_names.get(path).map(|&ino| s.inodes[ino].live.clone())
    }
}

impl Drop for CrashFs {
    fn drop(&mut self) {
        if self.shared().lock().open_files > 0 {
            // A connection still uses the VFS. Leak it instead of freeing
            // memory that SQLite can still reach.
            return;
        }
        // SAFETY: no file is open, so SQLite holds no pointer into the VFS
        // or the shared state. Both were made with `Box::into_raw`.
        unsafe {
            ffi::sqlite3_vfs_unregister(self.shared().vfs);
            drop(Box::from_raw(self.shared().vfs));
            drop(Box::from_raw(self.shared));
        }
    }
}

fn empty_state() -> State {
    State {
        inodes: Vec::new(),
        live_names: BTreeMap::new(),
        durable_names: BTreeMap::new(),
        dir_pending: Vec::new(),
        ops: 0,
        die_at: None,
        dead: false,
        open_files: 0,
        random: SimRng::new(0),
        temp: 0,
    }
}

// ------------------------------------------------------------ FFI layer

#[repr(C)]
struct File {
    base: ffi::sqlite3_file,
    shared: *const Shared,
    ino: usize,
    delete_on_close: *mut String,
}

static IO_METHODS: ffi::sqlite3_io_methods = ffi::sqlite3_io_methods {
    iVersion: 2,
    xClose: Some(x_close),
    xRead: Some(x_read),
    xWrite: Some(x_write),
    xTruncate: Some(x_truncate),
    xSync: Some(x_sync),
    xFileSize: Some(x_file_size),
    xLock: Some(x_lock),
    xUnlock: Some(x_lock),
    xCheckReservedLock: Some(x_check_reserved_lock),
    xFileControl: Some(x_file_control),
    xSectorSize: Some(x_sector_size),
    xDeviceCharacteristics: Some(x_device_characteristics),
    xShmMap: Some(x_shm_map),
    xShmLock: Some(x_shm_lock),
    xShmBarrier: Some(x_shm_barrier),
    xShmUnmap: Some(x_shm_unmap),
    xFetch: None,
    xUnfetch: None,
};

unsafe fn shared_of_vfs<'a>(vfs: *mut ffi::sqlite3_vfs) -> &'a Shared {
    // SAFETY: pAppData was set to the `Shared` that owns this VFS.
    unsafe { &*((*vfs).pAppData as *const Shared) }
}

unsafe fn file_of<'a>(f: *mut ffi::sqlite3_file) -> &'a File {
    // SAFETY: SQLite gives back the `File` that `x_open` filled in.
    unsafe { &*(f as *const File) }
}

unsafe fn shared_of_file<'a>(f: &File) -> &'a Shared {
    // SAFETY: the `Shared` outlives every open file (see `Drop`).
    unsafe { &*f.shared }
}

unsafe extern "C" fn x_open(
    vfs: *mut ffi::sqlite3_vfs,
    name: ffi::sqlite3_filename,
    file: *mut ffi::sqlite3_file,
    flags: c_int,
    out_flags: *mut c_int,
) -> c_int {
    unsafe {
        (*file).pMethods = ptr::null();
        let shared = shared_of_vfs(vfs);
        let mut s = shared.lock();
        let name = if name.is_null() {
            s.temp += 1;
            format!("/__temp_{}", s.temp)
        } else {
            CStr::from_ptr(name).to_string_lossy().into_owned()
        };
        let ino = match s.live_names.get(&name) {
            Some(&ino) => ino,
            None if flags & ffi::SQLITE_OPEN_CREATE != 0 => {
                if !s.tick() {
                    return ffi::SQLITE_CANTOPEN;
                }
                let ino = s.inodes.len();
                s.inodes.push(Inode::default());
                s.live_names.insert(name.clone(), ino);
                s.dir_pending.push(DirOp::Link(name.clone(), ino));
                ino
            }
            None => return ffi::SQLITE_CANTOPEN,
        };
        if s.dead {
            return ffi::SQLITE_CANTOPEN;
        }
        s.inodes[ino].open += 1;
        s.open_files += 1;
        let delete_on_close =
            if flags & ffi::SQLITE_OPEN_DELETEONCLOSE != 0 { Box::into_raw(Box::new(name)) } else { ptr::null_mut() };
        ptr::write(
            file as *mut File,
            File { base: ffi::sqlite3_file { pMethods: &IO_METHODS }, shared, ino, delete_on_close },
        );
        if !out_flags.is_null() {
            *out_flags = flags;
        }
        ffi::SQLITE_OK
    }
}

unsafe extern "C" fn x_close(file: *mut ffi::sqlite3_file) -> c_int {
    unsafe {
        let f = file_of(file);
        let mut s = shared_of_file(f).lock();
        s.inodes[f.ino].open -= 1;
        s.open_files -= 1;
        if !f.delete_on_close.is_null() {
            let name = Box::from_raw(f.delete_on_close);
            if s.live_names.get(&*name) == Some(&f.ino) {
                s.live_names.remove(&*name);
                s.dir_pending.push(DirOp::Unlink(*name));
            }
        }
        ffi::SQLITE_OK
    }
}

unsafe extern "C" fn x_read(file: *mut ffi::sqlite3_file, buf: *mut c_void, amt: c_int, offset: i64) -> c_int {
    unsafe {
        let f = file_of(file);
        let s = shared_of_file(f).lock();
        if s.dead {
            return ffi::SQLITE_IOERR_READ;
        }
        let out = std::slice::from_raw_parts_mut(buf as *mut u8, amt as usize);
        let data = &s.inodes[f.ino].live;
        let start = (offset as usize).min(data.len());
        let n = (data.len() - start).min(out.len());
        out[..n].copy_from_slice(&data[start..start + n]);
        if n < out.len() {
            out[n..].fill(0);
            return ffi::SQLITE_IOERR_SHORT_READ;
        }
        ffi::SQLITE_OK
    }
}

unsafe extern "C" fn x_write(file: *mut ffi::sqlite3_file, buf: *const c_void, amt: c_int, offset: i64) -> c_int {
    unsafe {
        let f = file_of(file);
        let mut s = shared_of_file(f).lock();
        if !s.tick() {
            return ffi::SQLITE_IOERR_WRITE;
        }
        let data = std::slice::from_raw_parts(buf as *const u8, amt as usize).to_vec();
        let op = DataOp::Write { offset: offset as usize, data };
        let inode = &mut s.inodes[f.ino];
        op.apply(&mut inode.live);
        inode.pending.push(op);
        ffi::SQLITE_OK
    }
}

unsafe extern "C" fn x_truncate(file: *mut ffi::sqlite3_file, size: i64) -> c_int {
    unsafe {
        let f = file_of(file);
        let mut s = shared_of_file(f).lock();
        if !s.tick() {
            return ffi::SQLITE_IOERR_TRUNCATE;
        }
        let op = DataOp::Truncate(size as usize);
        let inode = &mut s.inodes[f.ino];
        op.apply(&mut inode.live);
        inode.pending.push(op);
        ffi::SQLITE_OK
    }
}

unsafe extern "C" fn x_sync(file: *mut ffi::sqlite3_file, _flags: c_int) -> c_int {
    unsafe {
        let f = file_of(file);
        let mut s = shared_of_file(f).lock();
        if !s.tick() {
            return ffi::SQLITE_IOERR_FSYNC;
        }
        s.sync_inode(f.ino);
        ffi::SQLITE_OK
    }
}

unsafe extern "C" fn x_file_size(file: *mut ffi::sqlite3_file, size: *mut i64) -> c_int {
    unsafe {
        let f = file_of(file);
        let s = shared_of_file(f).lock();
        if s.dead {
            return ffi::SQLITE_IOERR_FSTAT;
        }
        *size = s.inodes[f.ino].live.len() as i64;
        ffi::SQLITE_OK
    }
}

unsafe extern "C" fn x_lock(_file: *mut ffi::sqlite3_file, _level: c_int) -> c_int {
    ffi::SQLITE_OK
}

unsafe extern "C" fn x_check_reserved_lock(_file: *mut ffi::sqlite3_file, out: *mut c_int) -> c_int {
    unsafe { *out = 0 };
    ffi::SQLITE_OK
}

unsafe extern "C" fn x_file_control(_file: *mut ffi::sqlite3_file, _op: c_int, _arg: *mut c_void) -> c_int {
    ffi::SQLITE_NOTFOUND
}

unsafe extern "C" fn x_sector_size(_file: *mut ffi::sqlite3_file) -> c_int {
    SECTOR as c_int
}

unsafe extern "C" fn x_device_characteristics(_file: *mut ffi::sqlite3_file) -> c_int {
    ffi::SQLITE_IOCAP_POWERSAFE_OVERWRITE
}

unsafe extern "C" fn x_shm_map(
    file: *mut ffi::sqlite3_file,
    region: c_int,
    size: c_int,
    extend: c_int,
    out: *mut *mut c_void,
) -> c_int {
    unsafe {
        let f = file_of(file);
        let mut s = shared_of_file(f).lock();
        if s.dead {
            return ffi::SQLITE_IOERR_SHMMAP;
        }
        let shm = &mut s.inodes[f.ino].shm;
        let region = region as usize;
        if shm.len() <= region {
            if extend == 0 {
                *out = ptr::null_mut();
                return ffi::SQLITE_OK;
            }
            shm.resize_with(region + 1, || vec![0u8; size as usize].into_boxed_slice());
        }
        *out = shm[region].as_mut_ptr() as *mut c_void;
        ffi::SQLITE_OK
    }
}

unsafe extern "C" fn x_shm_lock(_file: *mut ffi::sqlite3_file, _offset: c_int, _n: c_int, _flags: c_int) -> c_int {
    ffi::SQLITE_OK
}

unsafe extern "C" fn x_shm_barrier(_file: *mut ffi::sqlite3_file) {
    std::sync::atomic::fence(Ordering::SeqCst);
}

unsafe extern "C" fn x_shm_unmap(file: *mut ffi::sqlite3_file, delete: c_int) -> c_int {
    unsafe {
        let f = file_of(file);
        if delete != 0 {
            shared_of_file(f).lock().inodes[f.ino].shm.clear();
        }
        ffi::SQLITE_OK
    }
}

unsafe extern "C" fn x_delete(vfs: *mut ffi::sqlite3_vfs, name: *const c_char, sync_dir: c_int) -> c_int {
    unsafe {
        let mut s = shared_of_vfs(vfs).lock();
        let name = CStr::from_ptr(name).to_string_lossy().into_owned();
        if !s.live_names.contains_key(&name) {
            return ffi::SQLITE_IOERR_DELETE_NOENT;
        }
        if !s.tick() {
            return ffi::SQLITE_IOERR_DELETE;
        }
        s.live_names.remove(&name);
        s.dir_pending.push(DirOp::Unlink(name));
        if sync_dir != 0 {
            s.sync_dir();
        }
        ffi::SQLITE_OK
    }
}

unsafe extern "C" fn x_access(
    vfs: *mut ffi::sqlite3_vfs,
    name: *const c_char,
    _flags: c_int,
    out: *mut c_int,
) -> c_int {
    unsafe {
        let s = shared_of_vfs(vfs).lock();
        if s.dead {
            return ffi::SQLITE_IOERR_ACCESS;
        }
        let name = CStr::from_ptr(name).to_string_lossy();
        *out = s.live_names.contains_key(name.as_ref()) as c_int;
        ffi::SQLITE_OK
    }
}

unsafe extern "C" fn x_full_pathname(
    _vfs: *mut ffi::sqlite3_vfs,
    name: *const c_char,
    n_out: c_int,
    out: *mut c_char,
) -> c_int {
    unsafe {
        let bytes = CStr::from_ptr(name).to_bytes_with_nul();
        if bytes.len() > n_out as usize {
            return ffi::SQLITE_CANTOPEN;
        }
        ptr::copy_nonoverlapping(bytes.as_ptr() as *const c_char, out, bytes.len());
        ffi::SQLITE_OK
    }
}

unsafe extern "C" fn x_randomness(vfs: *mut ffi::sqlite3_vfs, n: c_int, out: *mut c_char) -> c_int {
    unsafe {
        let mut s = shared_of_vfs(vfs).lock();
        let out = std::slice::from_raw_parts_mut(out as *mut u8, n as usize);
        for chunk in out.chunks_mut(8) {
            let r = s.random.next_u64().to_le_bytes();
            chunk.copy_from_slice(&r[..chunk.len()]);
        }
        n
    }
}

unsafe extern "C" fn x_sleep(_vfs: *mut ffi::sqlite3_vfs, micros: c_int) -> c_int {
    micros
}

// 2026-10-12 00:00 UTC as a Julian day number. The simulation has no wall
// clock; SQL functions that read 'now' see this fixed time.
const JULIAN_DAY_MS: i64 = 212_658_523_200_000;

unsafe extern "C" fn x_current_time(_vfs: *mut ffi::sqlite3_vfs, out: *mut f64) -> c_int {
    unsafe { *out = JULIAN_DAY_MS as f64 / 86_400_000.0 };
    ffi::SQLITE_OK
}

unsafe extern "C" fn x_current_time_int64(_vfs: *mut ffi::sqlite3_vfs, out: *mut ffi::sqlite3_int64) -> c_int {
    unsafe { *out = JULIAN_DAY_MS };
    ffi::SQLITE_OK
}

unsafe extern "C" fn x_get_last_error(_vfs: *mut ffi::sqlite3_vfs, _n: c_int, _out: *mut c_char) -> c_int {
    0
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::{Connection, OpenFlags};

    fn open(fs: &CrashFs, path: &str) -> rusqlite::Result<Connection> {
        let c = Connection::open_with_flags_and_vfs(
            path,
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
            fs.vfs_name(),
        )?;
        c.pragma_update(None, "journal_mode", "WAL")?;
        c.pragma_update(None, "synchronous", "FULL")?;
        Ok(c)
    }

    fn count(fs: &CrashFs) -> i64 {
        let c = open(fs, "/t.db").unwrap();
        c.query_row("SELECT count(*) FROM t", [], |r| r.get(0)).unwrap_or(0)
    }

    #[test]
    fn committed_rows_survive_a_crash() {
        let fs = CrashFs::new(1);
        {
            let c = open(&fs, "/t.db").unwrap();
            c.execute_batch("CREATE TABLE t(x); INSERT INTO t VALUES (1), (2);").unwrap();
        }
        fs.crash(&mut SimRng::new(5));
        assert_eq!(count(&fs), 2);
    }

    #[test]
    fn julian_day_is_the_start_date() {
        let fs = CrashFs::new(1);
        let c = open(&fs, "/t.db").unwrap();
        let now: String = c.query_row("SELECT datetime('now')", [], |r| r.get(0)).unwrap();
        assert_eq!(now, "2026-10-12 00:00:00");
    }

    /// For each operation index, die there, crash, and check that the
    /// table holds exactly the inserts that committed.
    #[test]
    fn crash_at_every_operation() {
        let run = |fs: &CrashFs| -> u64 {
            let mut committed = 0;
            if let Ok(c) = open(fs, "/t.db")
                && c.execute_batch("CREATE TABLE IF NOT EXISTS t(x)").is_ok()
            {
                for i in 0..20 {
                    if c.execute("INSERT INTO t VALUES (?1)", [i]).is_err() {
                        break;
                    }
                    committed += 1;
                }
            }
            committed
        };
        let total = {
            let fs = CrashFs::new(0);
            run(&fs);
            fs.ops()
        };
        assert!(total > 40);
        for at in 0..total {
            for seed in 0..3 {
                let fs = CrashFs::new(seed);
                fs.crash_after(at);
                let committed = run(&fs);
                assert!(fs.is_dead(), "did not die at {at}");
                fs.crash(&mut SimRng::new(seed * 1000 + at));
                let found = count(&fs) as u64;
                assert!(
                    found == committed || found == committed + 1,
                    "die at op {at}, seed {seed}: {committed} committed, {found} found"
                );
            }
        }
    }

    /// The harness must find a real bug: with `synchronous = OFF`, SQLite
    /// does not sync at commit, and a crash loses committed rows.
    #[test]
    fn finds_missing_sync() {
        let lost = (0..20).any(|seed| {
            let fs = CrashFs::new(seed);
            {
                let c = open(&fs, "/t.db").unwrap();
                c.pragma_update(None, "synchronous", "OFF").unwrap();
                c.execute_batch("CREATE TABLE t(x)").unwrap();
                for i in 0..20 {
                    c.execute("INSERT INTO t VALUES (?1)", [i]).unwrap();
                }
            }
            fs.crash(&mut SimRng::new(seed));
            count(&fs) < 20
        });
        assert!(lost, "no seed lost a row with synchronous = OFF");
    }
}
