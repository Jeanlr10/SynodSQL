//! The `synodsql` Python module: [`synodsql::Database`] for Python.
//!
//! Calls release the GIL while they run, so other Python threads go on.

use std::sync::Mutex;

use pyo3::create_exception;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use pyo3::types::{
    PyBool, PyByteArray, PyBytes, PyDict, PyFloat, PyFrozenSet, PyInt, PyList, PySet, PyString, PyTuple,
};
use synodsql::synod_core::RowId;
use synodsql::{Cell, Options, Param, ServeOptions};

create_exception!(synodsql, Error, PyException, "A SynodSQL error.");

fn err(e: synodsql::Error) -> PyErr {
    Error::new_err(e.0)
}

// A Python value as a parameter.
fn param(v: &Bound<'_, PyAny>) -> PyResult<Param> {
    if v.is_none() {
        Ok(Param::Null)
    } else if let Ok(b) = v.cast::<PyBool>() {
        Ok(Param::Integer(b.is_true() as i64))
    } else if v.is_instance_of::<PyInt>() {
        Ok(Param::Integer(v.extract()?))
    } else if v.is_instance_of::<PyFloat>() {
        Ok(Param::Real(v.extract()?))
    } else if v.is_instance_of::<PyString>() {
        Ok(Param::Text(v.extract()?))
    } else if let Ok(b) = v.cast::<PyBytes>() {
        Ok(Param::Blob(b.as_bytes().to_vec()))
    } else if let Ok(b) = v.cast::<PyByteArray>() {
        Ok(Param::Blob(b.to_vec()))
    } else if v.is_instance_of::<PyList>()
        || v.is_instance_of::<PyTuple>()
        || v.is_instance_of::<PySet>()
        || v.is_instance_of::<PyFrozenSet>()
    {
        Ok(Param::List(v.try_iter()?.map(|i| param(&i?)).collect::<PyResult<_>>()?))
    } else if v.get_type().name()? == "Decimal" {
        Ok(Param::Text(v.str()?.to_string()))
    } else {
        Err(Error::new_err(format!("cannot store a {}", v.get_type().name()?)))
    }
}

fn columns(values: Option<&Bound<'_, PyDict>>, kwargs: Option<&Bound<'_, PyDict>>) -> PyResult<Vec<(String, Param)>> {
    let mut out = Vec::new();
    for d in [values, kwargs].into_iter().flatten() {
        for (k, v) in d.iter() {
            out.push((k.extract()?, param(&v)?));
        }
    }
    Ok(out)
}

// A row ID: its text, or the 16 bytes a query returns.
fn row_id(v: &Bound<'_, PyAny>) -> PyResult<String> {
    if let Ok(b) = v.cast::<PyBytes>() {
        let id = b.as_bytes().try_into().ok().and_then(RowId::from_be_bytes);
        return id.map(|id| id.to_string()).ok_or_else(|| Error::new_err("not a row ID"));
    }
    v.extract()
}

fn cell<'py>(py: Python<'py>, c: Cell) -> PyResult<Bound<'py, PyAny>> {
    Ok(match c {
        Cell::Null => py.None().into_bound(py),
        Cell::Integer(i) => i.into_pyobject(py)?.into_any(),
        Cell::Real(r) => r.into_pyobject(py)?.into_any(),
        Cell::Text(t) => t.into_pyobject(py)?.into_any(),
        Cell::Blob(b) => PyBytes::new(py, &b).into_any(),
    })
}

/// One node's store: open it, write, query, sync by files, or serve it as
/// a cluster node in the background.
#[pyclass(module = "synodsql")]
struct Database {
    db: Mutex<synodsql::Database>,
}

impl Database {
    fn run<R: Send>(
        &self,
        py: Python<'_>,
        f: impl FnOnce(&mut synodsql::Database) -> synodsql::Result<R> + Send,
    ) -> PyResult<R> {
        py.detach(|| f(&mut self.db.lock().unwrap_or_else(|e| e.into_inner()))).map_err(err)
    }

    fn new(db: synodsql::Database) -> Database {
        Database { db: Mutex::new(db) }
    }
}

#[pymethods]
impl Database {
    /// Makes a new node store at `path`.
    #[staticmethod]
    #[pyo3(signature = (path, node, *, epoch_ms = 60_000, lateness = 16, genesis_ms = 0, max_drift_ms = 500))]
    fn create(
        py: Python<'_>,
        path: String,
        node: u32,
        epoch_ms: u64,
        lateness: u64,
        genesis_ms: u64,
        max_drift_ms: u64,
    ) -> PyResult<Database> {
        let o = Options { node, epoch_ms, lateness, genesis_ms, max_drift_ms };
        py.detach(|| synodsql::Database::create(&path, &o)).map(Database::new).map_err(err)
    }

    /// Opens a node store.
    #[staticmethod]
    fn open(py: Python<'_>, path: String) -> PyResult<Database> {
        py.detach(|| synodsql::Database::open(&path)).map(Database::new).map_err(err)
    }

    /// The store's file.
    #[getter]
    fn path(&self) -> String {
        self.db.lock().unwrap_or_else(|e| e.into_inner()).path().to_owned()
    }

    /// This node's ID.
    #[getter]
    fn node(&self) -> u32 {
        self.db.lock().unwrap_or_else(|e| e.into_inner()).node()
    }

    /// True while the node serves.
    #[getter]
    fn serving(&self) -> bool {
        self.db.lock().unwrap_or_else(|e| e.into_inner()).is_serving()
    }

    /// Adds a table (`CREATE TABLE ... MERGE ...`); returns its DDL.
    fn create_table(&self, py: Python<'_>, ddl: String) -> PyResult<String> {
        self.run(py, |db| db.create_table(&ddl))
    }

    /// Adds a row from a dict and/or keyword arguments; returns its row ID.
    #[pyo3(signature = (table, values = None, **kwargs))]
    fn insert(
        &self,
        py: Python<'_>,
        table: String,
        values: Option<&Bound<'_, PyDict>>,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<String> {
        let cols = columns(values, kwargs)?;
        self.run(py, move |db| {
            let given: Vec<(&str, Param)> = cols.iter().map(|(n, p)| (n.as_str(), p.clone())).collect();
            db.insert(&table, &given)
        })
    }

    /// Changes columns of a row (row ID as text or as the bytes a query
    /// returns).
    #[pyo3(signature = (table, row_id, values = None, **kwargs))]
    fn update(
        &self,
        py: Python<'_>,
        table: String,
        row_id: &Bound<'_, PyAny>,
        values: Option<&Bound<'_, PyDict>>,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<()> {
        let id = self::row_id(row_id)?;
        let cols = columns(values, kwargs)?;
        self.run(py, move |db| {
            let given: Vec<(&str, Param)> = cols.iter().map(|(n, p)| (n.as_str(), p.clone())).collect();
            db.update(&table, &id, &given)
        })
    }

    /// Deletes a row.
    fn delete(&self, py: Python<'_>, table: String, row_id: &Bound<'_, PyAny>) -> PyResult<()> {
        let id = self::row_id(row_id)?;
        self.run(py, move |db| db.delete(&table, &id))
    }

    /// Runs a read-only SQL statement with `?1`, `?2`, ... bound to
    /// `params`; returns a list of tuples.
    #[pyo3(signature = (sql, params = None))]
    fn query<'py>(
        &self,
        py: Python<'py>,
        sql: String,
        params: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<Vec<Bound<'py, PyTuple>>> {
        let (_, rows) = self.fetch(py, sql, params)?;
        rows.into_iter()
            .map(|r| PyTuple::new(py, r.into_iter().map(|c| cell(py, c)).collect::<PyResult<Vec<_>>>()?))
            .collect()
    }

    /// As `query`, with each row as a dict by column name.
    #[pyo3(signature = (sql, params = None))]
    fn query_dicts<'py>(
        &self,
        py: Python<'py>,
        sql: String,
        params: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<Vec<Bound<'py, PyDict>>> {
        let (names, rows) = self.fetch(py, sql, params)?;
        rows.into_iter()
            .map(|r| {
                let d = PyDict::new(py);
                for (n, c) in names.iter().zip(r) {
                    d.set_item(n, cell(py, c)?)?;
                }
                Ok(d)
            })
            .collect()
    }

    /// Runs a store command of the `synod` command line, without the store
    /// path (`status`, `conflicts`, `resolve <id>`, ...); returns its output.
    #[pyo3(signature = (*args))]
    fn exec(&self, py: Python<'_>, args: Vec<String>) -> PyResult<String> {
        self.run(py, move |db| db.exec(&args.iter().map(String::as_str).collect::<Vec<_>>()))
    }

    /// Seals every epoch that has ended (file sync); returns the epochs.
    fn seal(&self, py: Python<'_>) -> PyResult<Vec<u64>> {
        self.run(py, |db| db.seal())
    }

    /// The sealed log of an epoch, to send to the other nodes.
    fn export<'py>(&self, py: Python<'py>, epoch: u64) -> PyResult<Bound<'py, PyBytes>> {
        let bytes = self.run(py, move |db| db.export(epoch))?;
        Ok(PyBytes::new(py, &bytes))
    }

    /// Merges the next sealed epochs with other nodes' logs; returns the
    /// merged epochs.
    fn merge(&self, py: Python<'_>, logs: Vec<Vec<u8>>) -> PyResult<Vec<u64>> {
        self.run(py, move |db| db.merge(&logs))
    }

    /// Makes a new node store at `path` from this node's base state.
    fn clone_to(&self, py: Python<'_>, path: String, node: u32) -> PyResult<Database> {
        self.run(py, move |db| db.clone_to(&path, node)).map(Database::new)
    }

    /// Runs the node in the background: `peers` maps node IDs to
    /// `host:port`, `certs` is the cluster's certificate directory.
    #[pyo3(signature = (listen, peers, certs, *, tick_ms = None, quorum = true, gossip = None))]
    #[allow(clippy::too_many_arguments)]
    fn serve(
        &self,
        py: Python<'_>,
        listen: String,
        peers: Vec<(u32, String)>,
        certs: String,
        tick_ms: Option<u64>,
        quorum: bool,
        gossip: Option<usize>,
    ) -> PyResult<()> {
        let o = ServeOptions { listen, peers, certs, tick_ms, quorum, gossip };
        self.run(py, move |db| db.serve(&o))
    }

    /// Stops the node if it serves, and closes the store.
    fn close(&self, py: Python<'_>) -> PyResult<()> {
        self.run(py, |db| db.close())
    }

    fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    #[pyo3(signature = (*_exc))]
    fn __exit__(&self, py: Python<'_>, _exc: &Bound<'_, PyTuple>) -> PyResult<bool> {
        self.close(py)?;
        Ok(false)
    }
}

impl Database {
    fn fetch(
        &self,
        py: Python<'_>,
        sql: String,
        params: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<(Vec<String>, Vec<Vec<Cell>>)> {
        let ps = match params {
            None => Vec::new(),
            Some(p) => p.try_iter()?.map(|v| param(&v?)).collect::<PyResult<_>>()?,
        };
        let rows = self.run(py, move |db| db.query(&sql, &ps))?;
        Ok((rows.columns, rows.rows))
    }
}

/// Makes a cluster CA and certificates in `dir` for the node IDs `nodes`
/// and the client names `clients`; `hosts`, as (node, host or IP), adds
/// names to node certificates. Returns the number of certificates.
#[pyfunction]
#[pyo3(signature = (dir, nodes, clients = vec!["admin".to_owned()], hosts = Vec::new()))]
fn make_certs(
    py: Python<'_>,
    dir: String,
    nodes: Vec<u32>,
    clients: Vec<String>,
    hosts: Vec<(u32, String)>,
) -> PyResult<usize> {
    py.detach(|| {
        let clients: Vec<&str> = clients.iter().map(String::as_str).collect();
        let hosts: Vec<(u32, &str)> = hosts.iter().map(|(n, h)| (*n, h.as_str())).collect();
        synodsql::make_certs(&dir, &nodes, &clients, &hosts)
    })
    .map_err(err)
}

/// Runs a store command on a running node at `addr`, as a client of the
/// cluster's certificates; `net-status` and `metrics` read its status.
#[pyfunction]
#[pyo3(signature = (addr, node, certs, *args, name = "admin".to_owned()))]
fn remote(py: Python<'_>, addr: String, node: u32, certs: String, args: Vec<String>, name: String) -> PyResult<String> {
    py.detach(|| synodsql::remote(&addr, node, &certs, &name, &args.iter().map(String::as_str).collect::<Vec<_>>()))
        .map_err(err)
}

/// Shows a row ID that a query returned as 16 bytes.
#[pyfunction]
fn row_id_str(id: &Bound<'_, PyAny>) -> PyResult<String> {
    row_id(id)
}

#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Database>()?;
    m.add("Error", m.py().get_type::<Error>())?;
    m.add_function(wrap_pyfunction!(make_certs, m)?)?;
    m.add_function(wrap_pyfunction!(remote, m)?)?;
    m.add_function(wrap_pyfunction!(row_id_str, m)?)?;
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
