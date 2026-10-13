//! The SynodSQL C API: `include/synod.h` declares these functions, and
//! `include/synod.hpp` wraps them for C++.
//!
//! Every function returns `SYNOD_OK` (0) or `SYNOD_ERROR` (1); after an
//! error, `synod_last_error` gives the message on that thread. Strings and
//! bytes that the library returns are freed with `synod_free_string` and
//! `synod_free_bytes`. A `synod_db` is used by one thread at a time.

#![allow(unsafe_code)]

use std::cell::RefCell;
use std::ffi::{CStr, CString, c_char, c_int};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::ptr;

use synodsql::{Cell, Database, Options, Param, Rows, ServeOptions};

/// Success.
pub const SYNOD_OK: c_int = 0;
/// Failure: see `synod_last_error`.
pub const SYNOD_ERROR: c_int = 1;

const NULL: c_int = 0;
const INTEGER: c_int = 1;
const REAL: c_int = 2;
const TEXT: c_int = 3;
const BLOB: c_int = 4;

thread_local! {
    static LAST_ERROR: RefCell<CString> = RefCell::new(CString::default());
}

/// An open node store.
pub struct SynodDb(Database);

/// A query result, with each text cell kept NUL-terminated.
pub struct SynodRows {
    rows: Rows,
    names: Vec<CString>,
    texts: Vec<Vec<Option<CString>>>,
}

/// A query parameter (`synod_value` in C).
#[repr(C)]
pub struct SynodValue {
    /// `SYNOD_NULL`, `SYNOD_INTEGER`, `SYNOD_REAL`, `SYNOD_TEXT` or `SYNOD_BLOB`.
    pub kind: c_int,
    /// The integer.
    pub integer: i64,
    /// The float.
    pub real: f64,
    /// The text (NUL-terminated) or blob bytes.
    pub data: *const u8,
    /// The blob length.
    pub len: usize,
}

fn set_error(e: &str) {
    let c = CString::new(e.replace('\0', " ")).unwrap_or_default();
    LAST_ERROR.with(|l| *l.borrow_mut() = c);
}

// Runs `f`, turning errors and panics into SYNOD_ERROR.
fn guard(f: impl FnOnce() -> Result<(), String>) -> c_int {
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(Ok(())) => SYNOD_OK,
        Ok(Err(e)) => {
            set_error(&e);
            SYNOD_ERROR
        }
        Err(_) => {
            set_error("internal error (panic)");
            SYNOD_ERROR
        }
    }
}

unsafe fn str_arg<'a>(p: *const c_char, what: &str) -> Result<&'a str, String> {
    if p.is_null() {
        return Err(format!("{what} is NULL"));
    }
    unsafe { CStr::from_ptr(p) }.to_str().map_err(|_| format!("{what} is not UTF-8"))
}

unsafe fn db_arg<'a>(db: *mut SynodDb) -> Result<&'a mut Database, String> {
    unsafe { db.as_mut() }.map(|d| &mut d.0).ok_or_else(|| "db is NULL".to_owned())
}

unsafe fn strs<'a>(n: usize, p: *const *const c_char, what: &str) -> Result<Vec<&'a str>, String> {
    if n == 0 {
        return Ok(Vec::new());
    }
    if p.is_null() {
        return Err(format!("{what} is NULL"));
    }
    (0..n).map(|i| unsafe { str_arg(*p.add(i), what) }).collect()
}

// (column, value) pairs; a NULL value is SQL NULL.
unsafe fn pairs<'a>(
    n: usize,
    columns: *const *const c_char,
    values: *const *const c_char,
) -> Result<Vec<(&'a str, &'a str)>, String> {
    let columns = unsafe { strs(n, columns, "a column name") }?;
    if n > 0 && values.is_null() {
        return Err("values is NULL".into());
    }
    let mut out = Vec::new();
    for (i, c) in columns.into_iter().enumerate() {
        let v = unsafe { *values.add(i) };
        out.push((c, if v.is_null() { "NULL" } else { unsafe { str_arg(v, "a value") }? }));
    }
    Ok(out)
}

fn out_string(out: *mut *mut c_char, s: String) -> Result<(), String> {
    if !out.is_null() {
        let c = CString::new(s).map_err(|_| "the output has a NUL byte".to_owned())?;
        unsafe { *out = c.into_raw() };
    }
    Ok(())
}

/// The library version, as a static string.
#[unsafe(no_mangle)]
pub extern "C" fn synod_version() -> *const c_char {
    concat!(env!("CARGO_PKG_VERSION"), "\0").as_ptr().cast()
}

/// The last error on this thread. Valid until the next failing call on it.
#[unsafe(no_mangle)]
pub extern "C" fn synod_last_error() -> *const c_char {
    LAST_ERROR.with(|l| l.borrow().as_ptr())
}

/// Makes a new node store.
///
/// # Safety
/// `path` is a NUL-terminated string; `out` points to writable memory.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn synod_create(
    path: *const c_char,
    node: u32,
    epoch_ms: u64,
    lateness: u64,
    out: *mut *mut SynodDb,
) -> c_int {
    guard(|| {
        let path = unsafe { str_arg(path, "path") }?;
        let mut o = Options::new(node);
        if epoch_ms > 0 {
            o.epoch_ms = epoch_ms;
        }
        if lateness > 0 {
            o.lateness = lateness;
        }
        let db = Database::create(path, &o).map_err(|e| e.0)?;
        if out.is_null() {
            return Err("out is NULL".into());
        }
        unsafe { *out = Box::into_raw(Box::new(SynodDb(db))) };
        Ok(())
    })
}

/// Opens a node store.
///
/// # Safety
/// `path` is a NUL-terminated string; `out` points to writable memory.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn synod_open(path: *const c_char, out: *mut *mut SynodDb) -> c_int {
    guard(|| {
        let db = Database::open(unsafe { str_arg(path, "path") }?).map_err(|e| e.0)?;
        if out.is_null() {
            return Err("out is NULL".into());
        }
        unsafe { *out = Box::into_raw(Box::new(SynodDb(db))) };
        Ok(())
    })
}

/// Stops the node if it serves, closes the store, and frees `db`.
///
/// # Safety
/// `db` comes from `synod_create` or `synod_open`, or is NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn synod_close(db: *mut SynodDb) -> c_int {
    guard(|| {
        if db.is_null() {
            return Ok(());
        }
        let mut db = unsafe { Box::from_raw(db) };
        db.0.close().map_err(|e| e.0)
    })
}

/// Adds a table: `CREATE TABLE ... MERGE ...`.
///
/// # Safety
/// `db` is open; `ddl` is a NUL-terminated string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn synod_create_table(db: *mut SynodDb, ddl: *const c_char) -> c_int {
    guard(|| {
        let ddl = unsafe { str_arg(ddl, "ddl") }?;
        unsafe { db_arg(db) }?.create_table(ddl).map(drop).map_err(|e| e.0)
    })
}

/// Adds a row. Values are text as the command line takes them; a NULL
/// value is SQL NULL. Writes the new row ID to `*row_id` if not NULL.
///
/// # Safety
/// `columns` and `values` hold `n` pointers; strings are NUL-terminated.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn synod_insert(
    db: *mut SynodDb,
    table: *const c_char,
    n: usize,
    columns: *const *const c_char,
    values: *const *const c_char,
    row_id: *mut *mut c_char,
) -> c_int {
    guard(|| {
        let table = unsafe { str_arg(table, "table") }?;
        let given = unsafe { pairs(n, columns, values) }?;
        let id = unsafe { db_arg(db) }?.insert_text(table, &given).map_err(|e| e.0)?;
        out_string(row_id, id)
    })
}

/// Changes columns of a row; values as for `synod_insert`.
///
/// # Safety
/// As `synod_insert`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn synod_update(
    db: *mut SynodDb,
    table: *const c_char,
    row_id: *const c_char,
    n: usize,
    columns: *const *const c_char,
    values: *const *const c_char,
) -> c_int {
    guard(|| {
        let table = unsafe { str_arg(table, "table") }?;
        let id = unsafe { str_arg(row_id, "row_id") }?;
        let given = unsafe { pairs(n, columns, values) }?;
        unsafe { db_arg(db) }?.update_text(table, id, &given).map_err(|e| e.0)
    })
}

/// Deletes a row.
///
/// # Safety
/// `db` is open; strings are NUL-terminated.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn synod_delete(db: *mut SynodDb, table: *const c_char, row_id: *const c_char) -> c_int {
    guard(|| {
        let table = unsafe { str_arg(table, "table") }?;
        let id = unsafe { str_arg(row_id, "row_id") }?;
        unsafe { db_arg(db) }?.delete(table, id).map_err(|e| e.0)
    })
}

/// Runs a read-only SQL statement with `n` parameters (`?1`, `?2`, ...).
/// Free the result with `synod_rows_free`.
///
/// # Safety
/// `params` holds `n` values; `out` points to writable memory.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn synod_query(
    db: *mut SynodDb,
    sql: *const c_char,
    n: usize,
    params: *const SynodValue,
    out: *mut *mut SynodRows,
) -> c_int {
    guard(|| {
        let sql = unsafe { str_arg(sql, "sql") }?;
        if n > 0 && params.is_null() {
            return Err("params is NULL".into());
        }
        let mut ps = Vec::new();
        for i in 0..n {
            let v = unsafe { &*params.add(i) };
            ps.push(match v.kind {
                NULL => Param::Null,
                INTEGER => Param::Integer(v.integer),
                REAL => Param::Real(v.real),
                TEXT => Param::Text(unsafe { str_arg(v.data.cast(), "a text parameter") }?.to_owned()),
                BLOB if v.len == 0 => Param::Blob(Vec::new()),
                BLOB if !v.data.is_null() => Param::Blob(unsafe { std::slice::from_raw_parts(v.data, v.len) }.to_vec()),
                _ => return Err(format!("parameter {}: bad value", i + 1)),
            });
        }
        let rows = unsafe { db_arg(db) }?.query(sql, &ps).map_err(|e| e.0)?;
        if out.is_null() {
            return Err("out is NULL".into());
        }
        let names = rows.columns.iter().map(|c| CString::new(c.as_str()).unwrap_or_default()).collect();
        let texts = rows
            .rows
            .iter()
            .map(|r| {
                r.iter()
                    .map(|c| match c {
                        Cell::Text(t) => Some(CString::new(t.as_str()).unwrap_or_default()),
                        _ => None,
                    })
                    .collect()
            })
            .collect();
        unsafe { *out = Box::into_raw(Box::new(SynodRows { rows, names, texts })) };
        Ok(())
    })
}

unsafe fn cell<'a>(r: *const SynodRows, row: usize, col: usize) -> Option<(&'a SynodRows, &'a Cell)> {
    let r = unsafe { r.as_ref() }?;
    Some((r, r.rows.rows.get(row)?.get(col)?))
}

/// Number of rows.
///
/// # Safety
/// `r` comes from `synod_query`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn synod_rows_count(r: *const SynodRows) -> usize {
    unsafe { r.as_ref() }.map_or(0, |r| r.rows.rows.len())
}

/// Number of columns.
///
/// # Safety
/// `r` comes from `synod_query`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn synod_rows_columns(r: *const SynodRows) -> usize {
    unsafe { r.as_ref() }.map_or(0, |r| r.names.len())
}

/// Name of a column, or NULL.
///
/// # Safety
/// `r` comes from `synod_query`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn synod_rows_name(r: *const SynodRows, col: usize) -> *const c_char {
    unsafe { r.as_ref() }.and_then(|r| r.names.get(col)).map_or(ptr::null(), |c| c.as_ptr())
}

/// Type of a cell: `SYNOD_NULL` (also out of range), `SYNOD_INTEGER`,
/// `SYNOD_REAL`, `SYNOD_TEXT` or `SYNOD_BLOB`.
///
/// # Safety
/// `r` comes from `synod_query`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn synod_rows_type(r: *const SynodRows, row: usize, col: usize) -> c_int {
    match unsafe { cell(r, row, col) } {
        Some((_, Cell::Integer(_))) => INTEGER,
        Some((_, Cell::Real(_))) => REAL,
        Some((_, Cell::Text(_))) => TEXT,
        Some((_, Cell::Blob(_))) => BLOB,
        _ => NULL,
    }
}

/// An integer cell (a float is truncated; others give 0).
///
/// # Safety
/// `r` comes from `synod_query`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn synod_rows_int(r: *const SynodRows, row: usize, col: usize) -> i64 {
    match unsafe { cell(r, row, col) } {
        Some((_, Cell::Integer(i))) => *i,
        Some((_, Cell::Real(f))) => *f as i64,
        _ => 0,
    }
}

/// A float cell (an integer is converted; others give 0).
///
/// # Safety
/// `r` comes from `synod_query`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn synod_rows_real(r: *const SynodRows, row: usize, col: usize) -> f64 {
    match unsafe { cell(r, row, col) } {
        Some((_, Cell::Integer(i))) => *i as f64,
        Some((_, Cell::Real(f))) => *f,
        _ => 0.0,
    }
}

/// A text cell, NUL-terminated, valid until `synod_rows_free`; NULL for
/// other types.
///
/// # Safety
/// `r` comes from `synod_query`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn synod_rows_text(r: *const SynodRows, row: usize, col: usize) -> *const c_char {
    let Some(r) = (unsafe { r.as_ref() }) else { return ptr::null() };
    r.texts.get(row).and_then(|t| t.get(col)).and_then(Option::as_ref).map_or(ptr::null(), |c| c.as_ptr())
}

/// A blob cell and its length, valid until `synod_rows_free`; NULL for
/// other types.
///
/// # Safety
/// `r` comes from `synod_query`; `len` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn synod_rows_blob(r: *const SynodRows, row: usize, col: usize, len: *mut usize) -> *const u8 {
    let (p, n) = match unsafe { cell(r, row, col) } {
        Some((_, Cell::Blob(b))) => (b.as_ptr(), b.len()),
        _ => (ptr::null(), 0),
    };
    if !len.is_null() {
        unsafe { *len = n };
    }
    p
}

/// A cell as the command line shows it (row IDs and sets decoded). Free it
/// with `synod_free_string`; NULL if out of range.
///
/// # Safety
/// `r` comes from `synod_query`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn synod_rows_display(r: *const SynodRows, row: usize, col: usize) -> *mut c_char {
    unsafe { cell(r, row, col) }
        .and_then(|(_, c)| CString::new(c.display()).ok())
        .map_or(ptr::null_mut(), CString::into_raw)
}

/// Frees a query result.
///
/// # Safety
/// `r` comes from `synod_query`, or is NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn synod_rows_free(r: *mut SynodRows) {
    if !r.is_null() {
        drop(unsafe { Box::from_raw(r) });
    }
}

/// Runs a store command of the `synod` command line, without the store path
/// (`status`, `conflicts`, `resolve <id>`, ...). Writes its output to `*out`.
///
/// # Safety
/// `argv` holds `argc` NUL-terminated strings.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn synod_exec(
    db: *mut SynodDb,
    argc: usize,
    argv: *const *const c_char,
    out: *mut *mut c_char,
) -> c_int {
    guard(|| {
        let args = unsafe { strs(argc, argv, "an argument") }?;
        let text = unsafe { db_arg(db) }?.exec(&args).map_err(|e| e.0)?;
        out_string(out, text)
    })
}

/// Seals every epoch that has ended (file sync). The sealed epochs are
/// `*first` to `*first + *count - 1`; either pointer may be NULL.
///
/// # Safety
/// `db` is open.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn synod_seal(db: *mut SynodDb, first: *mut u64, count: *mut usize) -> c_int {
    guard(|| {
        let sealed = unsafe { db_arg(db) }?.seal().map_err(|e| e.0)?;
        unsafe {
            if !first.is_null() {
                *first = sealed.first().copied().unwrap_or(0);
            }
            if !count.is_null() {
                *count = sealed.len();
            }
        }
        Ok(())
    })
}

/// The sealed log of an epoch. Free it with `synod_free_bytes`.
///
/// # Safety
/// `data` and `len` point to writable memory.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn synod_export(db: *mut SynodDb, epoch: u64, data: *mut *mut u8, len: *mut usize) -> c_int {
    guard(|| {
        if data.is_null() || len.is_null() {
            return Err("data or len is NULL".into());
        }
        let bytes = unsafe { db_arg(db) }?.export(epoch).map_err(|e| e.0)?.into_boxed_slice();
        unsafe {
            *len = bytes.len();
            *data = Box::into_raw(bytes).cast();
        }
        Ok(())
    })
}

/// Merges the next sealed epochs with `n` logs of other nodes. Writes the
/// number of merged epochs to `*merged` if not NULL.
///
/// # Safety
/// `logs` and `lens` hold `n` entries.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn synod_merge(
    db: *mut SynodDb,
    n: usize,
    logs: *const *const u8,
    lens: *const usize,
    merged: *mut usize,
) -> c_int {
    guard(|| {
        if n > 0 && (logs.is_null() || lens.is_null()) {
            return Err("logs or lens is NULL".into());
        }
        let mut given = Vec::new();
        for i in 0..n {
            let (p, l) = unsafe { (*logs.add(i), *lens.add(i)) };
            if p.is_null() {
                return Err(format!("log {i} is NULL"));
            }
            given.push(unsafe { std::slice::from_raw_parts(p, l) }.to_vec());
        }
        let count = unsafe { db_arg(db) }?.merge(&given).map_err(|e| e.0)?.len();
        if !merged.is_null() {
            unsafe { *merged = count };
        }
        Ok(())
    })
}

/// Makes a new node store at `path` from this node's base state, opened in
/// `*out`.
///
/// # Safety
/// `db` is open; `path` is NUL-terminated; `out` is writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn synod_clone(
    db: *mut SynodDb,
    path: *const c_char,
    node: u32,
    out: *mut *mut SynodDb,
) -> c_int {
    guard(|| {
        let path = unsafe { str_arg(path, "path") }?;
        if out.is_null() {
            return Err("out is NULL".into());
        }
        let copy = unsafe { db_arg(db) }?.clone_to(path, node).map_err(|e| e.0)?;
        unsafe { *out = Box::into_raw(Box::new(SynodDb(copy))) };
        Ok(())
    })
}

fn members(peers: &str) -> Result<Vec<(u32, String)>, String> {
    let mut out = Vec::new();
    for p in peers.split(',').map(str::trim).filter(|p| !p.is_empty()) {
        let (id, addr) = p.split_once('=').ok_or_else(|| format!("peers: expected id=host:port, found {p}"))?;
        out.push((id.parse().map_err(|_| format!("peers: not a node ID: {id}"))?, addr.to_owned()));
    }
    Ok(out)
}

/// Runs the node in the background, listening on `listen` (`host:port`),
/// with `peers` as `id=host:port,...` and the certificates in `certs`.
/// Other calls go on working, on the node's thread.
///
/// # Safety
/// `db` is open; strings are NUL-terminated (`peers` may be NULL).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn synod_serve(
    db: *mut SynodDb,
    listen: *const c_char,
    peers: *const c_char,
    certs: *const c_char,
) -> c_int {
    guard(|| {
        let listen = unsafe { str_arg(listen, "listen") }?;
        let peers = if peers.is_null() { Vec::new() } else { members(unsafe { str_arg(peers, "peers") }?)? };
        let certs = unsafe { str_arg(certs, "certs") }?;
        let mut o = ServeOptions::new(listen, &[], certs);
        o.peers = peers;
        unsafe { db_arg(db) }?.serve(&o).map_err(|e| e.0)
    })
}

/// Makes a cluster CA and certificates in `dir` for `nodes` (`1,2,3`) and
/// `clients` (`admin,...`; NULL for `admin`). `hosts` (`1=10.0.0.1,...`,
/// or NULL) adds host names or IP addresses to node certificates.
///
/// # Safety
/// Strings are NUL-terminated.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn synod_make_certs(
    dir: *const c_char,
    nodes: *const c_char,
    clients: *const c_char,
    hosts: *const c_char,
) -> c_int {
    guard(|| {
        let dir = unsafe { str_arg(dir, "dir") }?;
        let nodes: Vec<u32> = unsafe { str_arg(nodes, "nodes") }?
            .split(',')
            .map(str::trim)
            .filter(|n| !n.is_empty())
            .map(|n| n.parse().map_err(|_| format!("nodes: not a node ID: {n}")))
            .collect::<Result<_, _>>()?;
        let clients = if clients.is_null() { "admin" } else { unsafe { str_arg(clients, "clients") }? };
        let clients: Vec<&str> = clients.split(',').map(str::trim).filter(|c| !c.is_empty()).collect();
        let hosts = if hosts.is_null() { Vec::new() } else { members(unsafe { str_arg(hosts, "hosts") }?)? };
        let hosts: Vec<(u32, &str)> = hosts.iter().map(|(n, h)| (*n, h.as_str())).collect();
        synodsql::make_certs(dir, &nodes, &clients, &hosts).map(drop).map_err(|e| e.0)
    })
}

/// Runs a store command on a running node at `addr`, as the client `name`
/// (NULL for `admin`) of the certificates in `certs`. Writes its output to
/// `*out`.
///
/// # Safety
/// Strings are NUL-terminated; `argv` holds `argc` of them.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn synod_remote(
    addr: *const c_char,
    node: u32,
    certs: *const c_char,
    name: *const c_char,
    argc: usize,
    argv: *const *const c_char,
    out: *mut *mut c_char,
) -> c_int {
    guard(|| {
        let addr = unsafe { str_arg(addr, "addr") }?;
        let certs = unsafe { str_arg(certs, "certs") }?;
        let name = if name.is_null() { "admin" } else { unsafe { str_arg(name, "name") }? };
        let args = unsafe { strs(argc, argv, "an argument") }?;
        out_string(out, synodsql::remote(addr, node, certs, name, &args).map_err(|e| e.0)?)
    })
}

/// Frees a string that the library returned.
///
/// # Safety
/// `s` comes from this library, or is NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn synod_free_string(s: *mut c_char) {
    if !s.is_null() {
        drop(unsafe { CString::from_raw(s) });
    }
}

/// Frees bytes that `synod_export` returned.
///
/// # Safety
/// `data` and `len` come from `synod_export`, or `data` is NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn synod_free_bytes(data: *mut u8, len: usize) {
    if !data.is_null() {
        drop(unsafe { Box::from_raw(ptr::slice_from_raw_parts_mut(data, len)) });
    }
}
