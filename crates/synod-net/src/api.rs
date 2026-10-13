//! The client API: JSON over HTTP (specification Section 10). Every
//! request is one call on the node's store, on the node's thread; a write
//! request is one transaction.

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use rusqlite::types::Value as Sql;
use serde_json::{Map, Value as Json, json};
use synod_core::{NodeId, RowId, Value, ValueSet};
use synod_store::schema::ColumnType;
use synod_store::{Error, ResultKind, Store};

/// An API response: HTTP status and JSON body.
pub(crate) struct Reply {
    pub status: u16,
    pub body: Json,
}

fn ok(body: Json) -> Reply {
    Reply { status: 200, body }
}

fn fail(status: u16, code: &str, message: impl Into<String>) -> Reply {
    Reply { status, body: json!({ "error": { "code": code, "message": message.into() } }) }
}

fn bad(message: impl Into<String>) -> Reply {
    fail(400, "bad_request", message)
}

fn store_error(e: Error) -> Reply {
    let (status, code) = match &e {
        Error::Constraint(_) => (409, "constraint"),
        Error::Schema(_) => (400, "schema"),
        Error::Invalid(_) => (400, "invalid"),
        Error::Sql(_) => (400, "sql"),
        Error::Clock(_) | Error::TooEarly { .. } => (503, "clock"),
        Error::Io(_) | Error::Corrupt(_) => (500, "internal"),
    };
    fail(status, code, e.to_string())
}

/// Options of a request that shape the values it returns.
#[derive(Default)]
struct Shape {
    /// Integers as strings, for clients whose numbers are doubles.
    ints_as_strings: bool,
}

/// Runs one API request on the store.
pub(crate) fn handle(s: &mut Store, method: &str, path: &str, query: &str, body: &[u8]) -> Reply {
    let segments: Vec<&str> = path.trim_start_matches("/v1").split('/').filter(|p| !p.is_empty()).collect();
    let parse = || -> Result<Map<String, Json>, Reply> {
        if body.iter().all(u8::is_ascii_whitespace) {
            return Ok(Map::new());
        }
        match serde_json::from_slice(body) {
            Ok(Json::Object(m)) => Ok(m),
            Ok(_) => Err(bad("the body is not a JSON object")),
            Err(e) => Err(bad(format!("the body is not JSON: {e}"))),
        }
    };
    let r = match (method, segments.as_slice()) {
        ("GET", ["status"]) => status(s),
        ("GET", ["tables"]) => Ok(tables(s)),
        ("POST", ["tables"]) => parse().and_then(|b| create_table(s, &b)),
        ("POST", ["query"]) => parse().and_then(|b| run_query(s, &b)),
        ("POST", ["write"]) => parse().and_then(|b| write(s, &b)),
        ("GET", ["conflicts"]) => conflicts(s, query.split('&').any(|p| p == "all=true" || p == "all=1" || p == "all")),
        ("POST", ["conflicts", id, "resolve"]) => resolve(s, id),
        (_, ["status" | "tables" | "query" | "write" | "conflicts"] | ["conflicts", _, "resolve"]) => {
            Err(fail(405, "method_not_allowed", format!("{method} is not allowed on {path}")))
        }
        _ => Err(fail(404, "not_found", format!("no API endpoint {path}"))),
    };
    r.unwrap_or_else(|e| e)
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn status(s: &mut Store) -> Result<Reply, Reply> {
    let c = s.config().clone();
    Ok(ok(json!({
        "node": c.node.0,
        "epoch_ms": c.epoch_len_ms,
        "genesis_ms": c.genesis_ms,
        "lateness": c.lateness,
        "open_epoch": s.open_epoch().map_err(store_error)?,
        "merged_epoch": s.merged_epoch().map_err(store_error)?,
        "next_merge_epoch": s.next_merge_epoch().map_err(store_error)?,
        "head": hex(&s.head().map_err(store_error)?),
        "base_root": hex(&s.base_root().map_err(store_error)?),
        "unsealed_writes": s.unsealed_entries().map_err(store_error)?.len(),
        "tables": s.tables().iter().map(|t| t.name.clone()).collect::<Vec<_>>(),
    })))
}

fn tables(s: &Store) -> Reply {
    let tables: Vec<Json> = s
        .tables()
        .iter()
        .map(|t| {
            json!({
                "name": t.name,
                "ddl": t.ddl,
                "row_id": t.row_id_column,
                "columns": t.columns.iter().map(|c| json!({
                    "name": c.name,
                    "type": c.ty.to_string(),
                    "merge": c.merge.to_string(),
                    "not_null": c.not_null,
                })).collect::<Vec<_>>(),
            })
        })
        .collect();
    ok(json!({ "tables": tables }))
}

fn field<'a>(b: &'a Map<String, Json>, name: &str) -> Result<&'a Json, Reply> {
    b.get(name).ok_or_else(|| bad(format!("missing \"{name}\"")))
}

fn text<'a>(b: &'a Map<String, Json>, name: &str) -> Result<&'a str, Reply> {
    field(b, name)?.as_str().ok_or_else(|| bad(format!("\"{name}\" is not a string")))
}

fn create_table(s: &mut Store, b: &Map<String, Json>) -> Result<Reply, Reply> {
    let t = s.create_table(text(b, "ddl")?).map_err(store_error)?;
    Ok(Reply { status: 201, body: json!({ "name": t.name, "ddl": t.ddl }) })
}

fn shape(b: &Map<String, Json>) -> Result<Shape, Reply> {
    match b.get("ints_as_strings") {
        None => Ok(Shape::default()),
        Some(Json::Bool(v)) => Ok(Shape { ints_as_strings: *v }),
        Some(_) => Err(bad("\"ints_as_strings\" is not true or false")),
    }
}

// Query and condition parameters: values with no column type.
fn params(b: &Map<String, Json>) -> Result<Vec<Value>, Reply> {
    match b.get("params") {
        None | Some(Json::Null) => Ok(Vec::new()),
        Some(Json::Array(ps)) => {
            ps.iter().enumerate().map(|(i, p)| param(p).map_err(|e| bad(format!("parameter {}: {e}", i + 1)))).collect()
        }
        Some(_) => Err(bad("\"params\" is not an array")),
    }
}

fn param(j: &Json) -> Result<Value, String> {
    Ok(match j {
        Json::Null => Value::Null,
        Json::Bool(b) => Value::Integer(*b as i64),
        Json::Number(n) => match n.as_i64() {
            Some(i) => Value::Integer(i),
            None if n.is_u64() => return Err(format!("{n} is out of the 64-bit integer range")),
            None => Value::Real(n.as_f64().ok_or("not a number")?),
        },
        Json::String(t) => Value::Text(t.clone()),
        Json::Object(_) => Value::Blob(blob(j)?),
        Json::Array(_) => return Err("an array is not a parameter".into()),
    })
}

fn blob(j: &Json) -> Result<Vec<u8>, String> {
    let b64 = j.as_object().filter(|o| o.len() == 1).and_then(|o| o.get("base64")).and_then(Json::as_str);
    let b64 = b64.ok_or("a blob is {\"base64\": \"...\"}")?;
    BASE64.decode(b64).map_err(|e| format!("not base64: {e}"))
}

// A value for a column of type `ty`.
fn typed(ty: &ColumnType, j: &Json) -> Result<Value, String> {
    let wrong = || format!("{j} is not a {ty}");
    Ok(match (ty, j) {
        (_, Json::Null) => Value::Null,
        (ColumnType::Integer, Json::Bool(b)) => Value::Integer(*b as i64),
        (ColumnType::Integer, Json::Number(n)) => Value::Integer(n.as_i64().ok_or_else(wrong)?),
        (ColumnType::Integer, Json::String(t)) => Value::Integer(t.parse().map_err(|_| wrong())?),
        (ColumnType::Real, Json::Number(n)) => Value::Real(n.as_f64().ok_or_else(wrong)?),
        (ColumnType::Decimal { .. }, Json::Number(n)) => Value::Decimal(n.to_string().parse().map_err(|_| wrong())?),
        (ColumnType::Decimal { .. }, Json::String(t)) => Value::Decimal(t.parse().map_err(|_| wrong())?),
        (ColumnType::Text, Json::String(t)) => Value::Text(t.clone()),
        (ColumnType::Blob, Json::Object(_)) => Value::Blob(blob(j)?),
        (ColumnType::Set(item), Json::Array(items)) => {
            let items: Vec<Value> = items.iter().map(|i| typed(item, i)).collect::<Result<_, _>>()?;
            Value::Set(ValueSet::new(items).ok_or_else(wrong)?)
        }
        _ => return Err(wrong()),
    })
}

fn int(i: i64, shape: &Shape) -> Json {
    if shape.ints_as_strings { Json::String(i.to_string()) } else { Json::from(i) }
}

fn real(r: f64) -> Json {
    serde_json::Number::from_f64(r).map_or_else(|| Json::String(r.to_string()), Json::Number)
}

/// A stored value as JSON.
fn value(v: &Value, shape: &Shape) -> Json {
    match v {
        Value::Null => Json::Null,
        Value::Integer(i) => int(*i, shape),
        Value::Real(r) => real(*r),
        Value::Decimal(d) => Json::String(d.to_string()),
        Value::Text(t) => Json::String(t.clone()),
        Value::Blob(b) => json!({ "base64": BASE64.encode(b) }),
        Value::Set(s) => Json::Array(s.items().iter().map(|i| value(i, shape)).collect()),
    }
}

/// A query result value as JSON, by where its column comes from.
fn cell(v: Sql, kind: &ResultKind, shape: &Shape) -> Json {
    match (kind, v) {
        (_, Sql::Null) => Json::Null,
        (ResultKind::RowId, Sql::Blob(b)) if b.len() == 16 => {
            match b.as_slice().try_into().ok().and_then(RowId::from_be_bytes) {
                Some(id) => Json::String(id.to_string()),
                None => json!({ "base64": BASE64.encode(&b) }),
            }
        }
        (ResultKind::Column(ColumnType::Set(_)), Sql::Blob(b)) => match Value::from_canonical_bytes(&b) {
            Ok(v @ Value::Set(_)) => value(&v, shape),
            _ => json!({ "base64": BASE64.encode(&b) }),
        },
        (_, Sql::Integer(i)) => int(i, shape),
        (_, Sql::Real(r)) => real(r),
        (_, Sql::Text(t)) => Json::String(t),
        (_, Sql::Blob(b)) => json!({ "base64": BASE64.encode(&b) }),
    }
}

fn kind_name(k: &ResultKind) -> Json {
    match k {
        ResultKind::RowId => Json::String("ROWID".into()),
        ResultKind::Column(ty) => Json::String(ty.to_string()),
        ResultKind::Expression => Json::Null,
    }
}

fn run_query(s: &mut Store, b: &Map<String, Json>) -> Result<Reply, Reply> {
    let shape = shape(b)?;
    let (columns, rows) = s.query_typed(text(b, "sql")?, &params(b)?).map_err(store_error)?;
    let rows: Vec<Json> = rows
        .into_iter()
        .map(|r| Json::Array(r.into_iter().zip(&columns).map(|(v, c)| cell(v, &c.kind, &shape)).collect()))
        .collect();
    let columns: Vec<Json> = columns.iter().map(|c| json!({ "name": c.name, "type": kind_name(&c.kind) })).collect();
    Ok(ok(json!({ "columns": columns, "rows": rows })))
}

fn row_id(j: &Json) -> Result<RowId, String> {
    let t = j.as_str().ok_or("\"row_id\" is not a string")?;
    let bad = || format!("not a row ID: {t} (expected like 0000000a-0000000000000003)");
    let (n, c) = t.split_once('-').ok_or_else(bad)?;
    Ok(RowId::new(
        NodeId(u32::from_str_radix(n, 16).map_err(|_| bad())?),
        u64::from_str_radix(c, 16).map_err(|_| bad())?,
    ))
}

fn write(s: &mut Store, b: &Map<String, Json>) -> Result<Reply, Reply> {
    let ops = field(b, "ops")?.as_array().ok_or_else(|| bad("\"ops\" is not an array"))?;
    // Check every op first; then apply them all in one transaction.
    let mut planned = Vec::new();
    for (i, op) in ops.iter().enumerate() {
        planned.push(plan(s, op).map_err(|r| at(r, i))?);
    }
    let mut t = s.begin().map_err(store_error)?;
    let mut results = Vec::new();
    for (i, op) in planned.into_iter().enumerate() {
        let r = match op {
            Op::Insert(table, vs) => {
                let cols: Vec<(&str, Value)> = vs.iter().map(|(n, v)| (n.as_str(), v.clone())).collect();
                t.insert(&table, &cols).map(|id| json!({ "row_id": id.to_string() }))
            }
            Op::Update(table, target, vs) => {
                let cols: Vec<(&str, Value)> = vs.iter().map(|(n, v)| (n.as_str(), v.clone())).collect();
                match target {
                    Target::Row(id) => t.update(&table, id, &cols).map(|()| vec![id]),
                    Target::Where(w, ps) => t.update_where(&table, &w, &ps, &cols),
                }
                .map(|ids| row_ids(&ids))
            }
            Op::Delete(table, target) => match target {
                Target::Row(id) => t.delete(&table, id).map(|()| vec![id]),
                Target::Where(w, ps) => t.delete_where(&table, &w, &ps),
            }
            .map(|ids| row_ids(&ids)),
        };
        results.push(r.map_err(|e| at(store_error(e), i))?);
    }
    let c = t.commit().map_err(store_error)?;
    Ok(ok(json!({ "results": results, "epoch": c.epoch, "hlc": c.hlc.to_string() })))
}

fn row_ids(ids: &[RowId]) -> Json {
    json!({ "row_ids": ids.iter().map(RowId::to_string).collect::<Vec<_>>() })
}

// Adds the index of the failing op to an error.
fn at(mut r: Reply, i: usize) -> Reply {
    if let Some(e) = r.body.get_mut("error").and_then(Json::as_object_mut) {
        e.insert("op".into(), Json::from(i));
    }
    r
}

enum Op {
    Insert(String, Vec<(String, Value)>),
    Update(String, Target, Vec<(String, Value)>),
    Delete(String, Target),
}

fn plan(s: &Store, op: &Json) -> Result<Op, Reply> {
    let (name, args) = match op.as_object() {
        Some(o) if o.len() == 1 => o.iter().next().expect("one key"),
        _ => return Err(bad("an op is {\"insert\" | \"update\" | \"delete\": {...}}")),
    };
    let args = args.as_object().ok_or_else(|| bad(format!("\"{name}\" is not an object")))?;
    let table = s.table(text(args, "table")?).map_err(store_error)?;
    let values = || -> Result<Vec<(String, Value)>, Reply> {
        let vs = field(args, "values")?.as_object().ok_or_else(|| bad("\"values\" is not an object"))?;
        vs.iter()
            .map(|(k, j)| {
                let c = table
                    .columns
                    .iter()
                    .find(|c| c.name.eq_ignore_ascii_case(k))
                    .ok_or_else(|| fail(400, "invalid", format!("no column {k} in {}", table.name)))?;
                let v = typed(&c.ty, j).map_err(|e| fail(400, "invalid", format!("{}: {e}", c.name)))?;
                Ok((c.name.clone(), v))
            })
            .collect()
    };
    // The rows an update or delete applies to: one by ID, or a condition.
    let target = || -> Result<Target, Reply> {
        match (args.get("row_id"), args.get("where")) {
            (Some(id), None) => Ok(Target::Row(row_id(id).map_err(bad)?)),
            (None, Some(Json::String(w))) => Ok(Target::Where(w.clone(), params(args)?)),
            _ => Err(bad("give \"row_id\" or \"where\" (with \"params\")")),
        }
    };
    let t = table.name.clone();
    match name.as_str() {
        "insert" => Ok(Op::Insert(t, values()?)),
        "update" => Ok(Op::Update(t, target()?, values()?)),
        "delete" => Ok(Op::Delete(t, target()?)),
        other => Err(bad(format!("unknown op \"{other}\""))),
    }
}

enum Target {
    Row(RowId),
    Where(String, Vec<Value>),
}

fn values_json(vs: Option<&Vec<Value>>, shape: &Shape) -> Json {
    vs.map_or(Json::Null, |vs| Json::Array(vs.iter().map(|v| value(v, shape)).collect()))
}

fn conflicts(s: &Store, all: bool) -> Result<Reply, Reply> {
    let shape = Shape::default();
    let mut out = Vec::new();
    for (id, c, status) in s.conflicts().map_err(store_error)? {
        if status != "open" && !all {
            continue;
        }
        out.push(json!({
            "id": id.to_string(),
            "epoch": c.epoch,
            "kind": c.kind.to_string(),
            "table": c.table_name,
            "column": c.column_name,
            "row_id": c.row_id.to_string(),
            "rule": c.rule,
            "base": values_json(c.base.as_ref(), &shape),
            "applied": values_json(c.applied.as_ref(), &shape),
            "recorded": c.recorded.iter().map(|r| json!({
                "node": r.node.0,
                "hlc": r.hlc.to_string(),
                "values": values_json(Some(&r.values), &shape),
            })).collect::<Vec<_>>(),
            "status": status,
        }));
    }
    Ok(ok(json!({ "conflicts": out })))
}

fn resolve(s: &mut Store, id: &str) -> Result<Reply, Reply> {
    let id = row_id(&Json::String(id.to_owned())).map_err(bad)?;
    let mut t = s.begin().map_err(store_error)?;
    if t.get("_conflicts", id).map_err(store_error)?.is_none() {
        return Err(fail(404, "not_found", format!("no conflict {id}")));
    }
    t.update("_conflicts", id, &[("status", Value::Text("resolved".into()))]).map_err(store_error)?;
    t.commit().map_err(store_error)?;
    Ok(ok(json!({ "resolved": id.to_string() })))
}
