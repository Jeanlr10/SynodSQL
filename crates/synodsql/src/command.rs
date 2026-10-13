//! The store commands of the `synod` command, as text: the same commands
//! run on a store file, on a running node (`synod remote`), and through
//! [`Database::exec`](crate::Database::exec).

use std::fmt::Write as _;

use rusqlite::types::Value as Sql;
use synod_core::{NodeId, RowId, Value, ValueSet};
use synod_store::schema::{Column, ColumnType};
use synod_store::{Error, Store};

type R<T> = Result<T, String>;

/// Commands that work on an open store. A running node takes them too.
pub const STORE_COMMANDS: [&str; 9] =
    ["status", "create-table", "insert", "update", "delete", "query", "conflicts", "resolve", "revert"];

pub(crate) fn err(e: Error) -> String {
    e.to_string()
}

/// Splits `--name value` options from positional arguments.
pub struct Args {
    /// Positional arguments.
    pub positional: Vec<String>,
    /// Options, with their values.
    pub options: Vec<(String, Option<String>)>,
}

impl Args {
    /// Splits `args`; the options in `flags` take no value.
    pub fn parse(args: &[String], flags: &[&str]) -> Args {
        let mut positional = Vec::new();
        let mut options = Vec::new();
        let mut it = args.iter();
        while let Some(a) = it.next() {
            if let Some(name) = a.strip_prefix("--") {
                if flags.contains(&name) {
                    options.push((name.to_owned(), None));
                } else {
                    options.push((name.to_owned(), it.next().cloned()));
                }
            } else {
                positional.push(a.clone());
            }
        }
        Args { positional, options }
    }

    /// The value of an option.
    pub fn opt(&self, name: &str) -> Option<&str> {
        self.options.iter().find(|(n, _)| n == name).and_then(|(_, v)| v.as_deref())
    }

    /// True if a flag is given.
    pub fn flag(&self, name: &str) -> bool {
        self.options.iter().any(|(n, _)| n == name)
    }

    /// The value of a numeric option, or `default`.
    pub fn num(&self, name: &str, default: u64) -> R<u64> {
        self.opt(name).map_or(Ok(default), |v| v.parse().map_err(|_| format!("--{name}: not a number: {v}")))
    }

    /// A positional argument; `what` names it in the error.
    pub fn at(&self, i: usize, what: &str) -> R<&str> {
        self.positional.get(i).map(String::as_str).ok_or_else(|| format!("missing {what}"))
    }

    /// Fails on an option not in `names`.
    pub fn known(&self, names: &[&str]) -> R<()> {
        match self.options.iter().find(|(n, _)| !names.contains(&n.as_str())) {
            Some((n, _)) => Err(format!("unknown option --{n}")),
            None => Ok(()),
        }
    }
}

/// Runs a store command: its name and arguments, without the store path.
/// Returns its output.
pub fn run(s: &mut Store, args: &[String]) -> R<String> {
    let (cmd, rest) = args.split_first().ok_or("empty command")?;
    if !STORE_COMMANDS.contains(&cmd.as_str()) {
        return Err(format!("unknown store command {cmd}"));
    }
    let mut a = Args::parse(rest, &["all"]);
    a.positional.insert(0, "-".into());
    store_command(s, cmd, &a)
}

/// Runs a store command. `a.positional[0]` names the store and is not read.
pub fn store_command(s: &mut Store, cmd: &str, a: &Args) -> R<String> {
    let mut out = String::new();
    match cmd {
        "status" => {
            a.known(&[])?;
            let c = s.config().clone();
            let merged = s.merged_epoch().map_err(err)?.map_or("none".into(), |e| e.to_string());
            let _ = writeln!(out, "node            {}", c.node);
            let _ = writeln!(out, "epoch length    {} ms (genesis {} ms)", c.epoch_len_ms, c.genesis_ms);
            let _ = writeln!(out, "lateness        {} epochs", c.lateness);
            let _ = writeln!(out, "open epoch      {}", s.open_epoch().map_err(err)?);
            let _ = writeln!(out, "merged epoch    {merged}");
            let _ = writeln!(out, "next to merge   {}", s.next_merge_epoch().map_err(err)?);
            let _ = writeln!(out, "head manifest   {}", hex(&s.head().map_err(err)?));
            let _ = writeln!(out, "unsealed writes {}", s.unsealed_entries().map_err(err)?.len());
            let _ = writeln!(out, "base root       {}", hex(&s.base_root().map_err(err)?));
            let _ = writeln!(out, "state hash      {}", hex(&s.state_hash().map_err(err)?));
            let tables: Vec<&str> = s.tables().iter().map(|t| t.name.as_str()).collect();
            let _ = writeln!(out, "tables          {}", tables.join(", "));
        }
        "create-table" => {
            a.known(&[])?;
            let t = s.create_table(a.at(1, "<statement>")?).map_err(err)?;
            let _ = writeln!(out, "{}", t.ddl);
        }
        "insert" => {
            a.known(&[])?;
            let table = s.table(a.at(1, "<table>")?).map_err(err)?;
            let cols = assignments(&table.columns, &a.positional[2..])?;
            let given: Vec<(&str, Value)> = cols.iter().map(|(n, v)| (n.as_str(), v.clone())).collect();
            let mut t = s.begin().map_err(err)?;
            let id = t.insert(&table.name, &given).map_err(err)?;
            t.commit().map_err(err)?;
            let _ = writeln!(out, "{id}");
        }
        "update" => {
            a.known(&[])?;
            let table = s.table(a.at(1, "<table>")?).map_err(err)?;
            let id = parse_row_id(a.at(2, "<row-id>")?)?;
            let cols = assignments(&table.columns, &a.positional[3..])?;
            let given: Vec<(&str, Value)> = cols.iter().map(|(n, v)| (n.as_str(), v.clone())).collect();
            let mut t = s.begin().map_err(err)?;
            t.update(&table.name, id, &given).map_err(err)?;
            t.commit().map_err(err)?;
        }
        "delete" => {
            a.known(&[])?;
            let table = a.at(1, "<table>")?.to_owned();
            let id = parse_row_id(a.at(2, "<row-id>")?)?;
            let mut t = s.begin().map_err(err)?;
            t.delete(&table, id).map_err(err)?;
            t.commit().map_err(err)?;
        }
        "query" => {
            a.known(&[])?;
            let (names, rows) = s.query_named(a.at(1, "<select>")?, &[]).map_err(err)?;
            out += &table(&names, &rows.iter().map(|r| r.iter().map(show).collect()).collect::<Vec<_>>());
        }
        "conflicts" => {
            a.known(&["all"])?;
            let mut rows = Vec::new();
            for (id, c, status) in s.conflicts().map_err(err)? {
                if status != "open" && !a.flag("all") {
                    continue;
                }
                let recorded: Vec<String> =
                    c.recorded.iter().map(|r| format!("{}@{}: {}", r.node, r.hlc, values(&r.values))).collect();
                rows.push(vec![
                    id.to_string(),
                    c.epoch.to_string(),
                    c.kind.to_string(),
                    format!("{}{}", c.table_name, c.column_name.map(|n| format!(".{n}")).unwrap_or_default()),
                    c.row_id.to_string(),
                    c.rule.clone(),
                    c.applied.as_deref().map_or("(deleted)".into(), values),
                    recorded.join("; "),
                    status,
                ]);
            }
            let names = ["conflict", "epoch", "kind", "target", "row", "rule", "applied", "recorded", "status"];
            out += &table(&names.map(String::from), &rows);
        }
        "resolve" => {
            a.known(&[])?;
            let id = parse_row_id(a.at(1, "<conflict-row-id>")?)?;
            let mut t = s.begin().map_err(err)?;
            t.update("_conflicts", id, &[("status", Value::Text("resolved".into()))]).map_err(err)?;
            t.commit().map_err(err)?;
        }
        "revert" => {
            a.known(&["node", "from", "to"])?;
            let node = a.opt("node").ok_or("revert needs --node <id>")?;
            let node = NodeId(node.parse().map_err(|_| format!("--node: not a node ID: {node}"))?);
            let from = a.num("from", 0)?;
            let to = a.num("to", u64::MAX)?;
            let mut t = s.begin().map_err(err)?;
            let r = t.revert(node, from..=to).map_err(err)?;
            t.commit().map_err(err)?;
            let _ = writeln!(
                out,
                "reverted node {node}: {} rows deleted, {} columns restored, {} rows inserted again, {} skipped",
                r.deleted, r.restored, r.reinserted, r.skipped
            );
        }
        other => return Err(format!("unknown command {other}")),
    }
    Ok(out)
}

/// Lower-case hex.
pub fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

/// Parses a row ID as it shows (`0000000a-0000000000000003`).
pub fn parse_row_id(s: &str) -> R<RowId> {
    let bad = || format!("not a row ID: {s} (expected like 0000000a-0000000000000003)");
    let (n, c) = s.split_once('-').ok_or_else(bad)?;
    let node = u32::from_str_radix(n, 16).map_err(|_| bad())?;
    let counter = u64::from_str_radix(c, 16).map_err(|_| bad())?;
    Ok(RowId::new(NodeId(node), counter))
}

/// Parses `col=value` arguments for the columns of a table.
pub fn assignments(columns: &[Column], args: &[String]) -> R<Vec<(String, Value)>> {
    args.iter()
        .map(|a| {
            let (name, raw) = a.split_once('=').ok_or_else(|| format!("expected col=value, found {a}"))?;
            let column = columns
                .iter()
                .find(|c| c.name.eq_ignore_ascii_case(name))
                .ok_or_else(|| format!("no column {name}"))?;
            Ok((column.name.clone(), parse_value(&column.ty, raw)?))
        })
        .collect()
}

/// Parses a value as the command line gives it, for a column type.
pub fn parse_value(ty: &ColumnType, raw: &str) -> R<Value> {
    if raw == "NULL" {
        return Ok(Value::Null);
    }
    let bad = || format!("{raw:?} is not a {ty}");
    Ok(match ty {
        ColumnType::Integer => Value::Integer(raw.parse().map_err(|_| bad())?),
        ColumnType::Real => Value::Real(raw.parse().map_err(|_| bad())?),
        ColumnType::Text => Value::Text(raw.to_owned()),
        ColumnType::Decimal { .. } => Value::Decimal(raw.parse().map_err(|_| bad())?),
        ColumnType::Blob => {
            let digits = raw.strip_prefix("x'").and_then(|r| r.strip_suffix('\'')).ok_or_else(bad)?;
            if digits.len() % 2 != 0 {
                return Err(bad());
            }
            let bytes: Option<Vec<u8>> =
                (0..digits.len()).step_by(2).map(|i| u8::from_str_radix(&digits[i..i + 2], 16).ok()).collect();
            Value::Blob(bytes.ok_or_else(bad)?)
        }
        ColumnType::Set(item) => {
            let items = if raw.is_empty() {
                Vec::new()
            } else {
                raw.split(',').map(|p| parse_value(item, p)).collect::<R<_>>()?
            };
            Value::Set(ValueSet::new(items).ok_or_else(bad)?)
        }
    })
}

/// Shows a value as the command line does.
pub fn value(v: &Value) -> String {
    match v {
        Value::Null => "NULL".into(),
        Value::Integer(i) => i.to_string(),
        Value::Real(r) => r.to_string(),
        Value::Decimal(d) => d.to_string(),
        Value::Text(t) => t.escape_debug().to_string(),
        Value::Blob(b) => format!("x'{}'", hex(b)),
        Value::Set(s) => format!("{{{}}}", s.items().iter().map(value).collect::<Vec<_>>().join(", ")),
    }
}

/// Shows values, separated by commas.
pub fn values(vs: &[Value]) -> String {
    vs.iter().map(value).collect::<Vec<_>>().join(", ")
}

// Shows a SQL result value. A 16-byte blob that is a row ID shows as one,
// and a blob that is an encoded set shows as the set.
/// Shows a SQL result value as the command line does.
pub fn show(v: &Sql) -> String {
    match v {
        Sql::Null => "NULL".into(),
        Sql::Integer(i) => i.to_string(),
        Sql::Real(r) => r.to_string(),
        Sql::Text(t) => t.escape_debug().to_string(),
        Sql::Blob(b) => {
            if let Some(id) = b.as_slice().try_into().ok().and_then(RowId::from_be_bytes) {
                id.to_string()
            } else if let Ok(set @ Value::Set(_)) = Value::from_canonical_bytes(b) {
                value(&set)
            } else {
                format!("x'{}'", hex(b))
            }
        }
    }
}

/// Lays out a result table, with a row count.
pub fn table(names: &[String], rows: &[Vec<String>]) -> String {
    let mut out = String::new();
    let mut width: Vec<usize> = names.iter().map(|n| n.chars().count()).collect();
    for r in rows {
        for (w, c) in width.iter_mut().zip(r) {
            *w = (*w).max(c.chars().count());
        }
    }
    let line = |cells: &[String]| {
        let parts: Vec<String> = cells.iter().zip(&width).map(|(c, w)| format!("{c:<w$}")).collect();
        format!("{}\n", parts.join("  ").trim_end())
    };
    out += &line(names);
    for r in rows {
        out += &line(r);
    }
    let _ = writeln!(out, "({} row{})", rows.len(), if rows.len() == 1 { "" } else { "s" });
    out
}
