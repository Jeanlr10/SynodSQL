//! Table definitions with merge policies (specification v0.1, Section 4.6).
//!
//! A managed table is declared with `CREATE TABLE`, as in SQLite, plus a
//! `MERGE <policy>` clause on each column and an optional
//! `MERGE DELETE <rule>` clause after the column list:
//!
//! ```sql
//! CREATE TABLE inventory (
//!   row_id  ROWID,
//!   sku     TEXT UNIQUE                MERGE lww,
//!   price   DECIMAL(10,2)              MERGE lww_flag,
//!   qty     INTEGER CHECK (qty >= 0)   MERGE delta,
//!   tags    SET<TEXT>                  MERGE union
//! ) MERGE DELETE delete_wins;
//! ```
//!
//! The parser accepts a subset of SQLite's `CREATE TABLE`: plain
//! identifiers only, and no `COLLATE` (a collation would make comparison
//! depend on the node).

use std::fmt;

use crate::error::{Error, Result};

/// Type of a column.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum ColumnType {
    /// 64-bit integer.
    Integer,
    /// 64-bit float.
    Real,
    /// UTF-8 text.
    Text,
    /// Bytes.
    Blob,
    /// Exact decimal, with optional precision (total digits) and scale
    /// (digits after the point).
    Decimal {
        /// Largest number of digits.
        precision: Option<u8>,
        /// Largest number of digits after the point.
        scale: Option<u8>,
    },
    /// Set of values of one scalar type.
    Set(Box<ColumnType>),
}

impl ColumnType {
    /// The SQLite STRICT type that stores this type.
    pub fn storage(&self) -> &'static str {
        match self {
            ColumnType::Integer => "INTEGER",
            ColumnType::Real => "REAL",
            ColumnType::Text | ColumnType::Decimal { .. } => "TEXT",
            ColumnType::Blob | ColumnType::Set(_) => "BLOB",
        }
    }
}

impl fmt::Display for ColumnType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ColumnType::Integer => write!(f, "INTEGER"),
            ColumnType::Real => write!(f, "REAL"),
            ColumnType::Text => write!(f, "TEXT"),
            ColumnType::Blob => write!(f, "BLOB"),
            ColumnType::Decimal { precision: Some(p), scale: Some(s) } => write!(f, "DECIMAL({p},{s})"),
            ColumnType::Decimal { precision: Some(p), scale: None } => write!(f, "DECIMAL({p})"),
            ColumnType::Decimal { .. } => write!(f, "DECIMAL"),
            ColumnType::Set(t) => write!(f, "SET<{t}>"),
        }
    }
}

macro_rules! keyword_enum {
    ($(#[$doc:meta])* $name:ident { $($(#[$vdoc:meta])* $variant:ident = $text:literal,)* }) => {
        $(#[$doc])*
        #[derive(Clone, Copy, PartialEq, Eq, Debug)]
        pub enum $name {
            $($(#[$vdoc])* $variant,)*
        }

        impl $name {
            /// The keyword in the schema language.
            pub fn as_str(self) -> &'static str {
                match self { $($name::$variant => $text,)* }
            }

            fn parse(s: &str) -> Option<$name> {
                match s.to_ascii_lowercase().as_str() { $($text => Some($name::$variant),)* _ => None }
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(self.as_str())
            }
        }
    };
}

keyword_enum! {
    /// How the merge resolves different values in one column (white paper,
    /// Section VII-B).
    MergePolicy {
        /// Highest (HLC, node ID) wins.
        Lww = "lww",
        /// As `lww`; the losing values go to `_conflicts`.
        LwwFlag = "lww_flag",
        /// Current value plus the sum of all changes.
        Delta = "delta",
        /// Largest value.
        Max = "max",
        /// Smallest value.
        Min = "min",
        /// Set union of additions, minus removals.
        Union = "union",
        /// Line-level three-way text merge.
        Text3 = "text3",
        /// Keep the current value; record all values.
        Flag = "flag",
    }
}

keyword_enum! {
    /// How the merge resolves a delete and an update of one row (white
    /// paper, Section VII-C).
    DeleteRule {
        /// The row becomes a tombstone; the updates are recorded.
        DeleteWins = "delete_wins",
        /// The row stays; the delete is recorded.
        UpdateWins = "update_wins",
        /// The row stays a tombstone with the updates attached.
        Flag = "flag",
    }
}

/// A column of a managed table.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Column {
    /// Name.
    pub name: String,
    /// Type.
    pub ty: ColumnType,
    /// Merge policy.
    pub merge: MergePolicy,
    /// True if NULL is not allowed.
    pub not_null: bool,
    /// SQL expression for the default value, if any.
    pub default: Option<String>,
}

/// A single-column foreign key.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ForeignKey {
    /// Index of the child column.
    pub column: usize,
    /// Parent table.
    pub parent: String,
    /// Parent column.
    pub parent_column: String,
    /// True if a parent delete deletes the children; otherwise a parent
    /// delete with live children fails.
    pub cascade: bool,
}

/// A managed table.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct TableSchema {
    /// Table ID; the order of creation, from 0.
    pub id: u32,
    /// Name.
    pub name: String,
    /// Name under which the view shows the row ID, if declared with type
    /// `ROWID`.
    pub row_id_column: Option<String>,
    /// Columns, in schema order.
    pub columns: Vec<Column>,
    /// Unique keys among live rows, as column indexes.
    pub uniques: Vec<Vec<usize>>,
    /// CHECK expressions.
    pub checks: Vec<String>,
    /// Foreign keys.
    pub foreign_keys: Vec<ForeignKey>,
    /// Delete rule.
    pub delete_rule: DeleteRule,
    /// The statement this schema came from.
    pub ddl: String,
}

/// Prefix of the physical table that stores a managed table.
pub const PHYSICAL_PREFIX: &str = "_synod_t_";

impl TableSchema {
    /// Returns the index of a column, by name (case-insensitive).
    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c.name.eq_ignore_ascii_case(name))
    }

    /// Name of the physical table.
    pub fn physical(&self) -> String {
        format!("{PHYSICAL_PREFIX}{}", self.name)
    }

    /// Statements that make the physical table, its indexes, and the view.
    ///
    /// The physical table has no UNIQUE, CHECK, or foreign key
    /// constraints: tombstones keep their old values, and a merge result
    /// can break a constraint (it then becomes a conflict row). The store
    /// enforces these constraints on local writes, among live rows.
    pub fn create_statements(&self) -> Vec<String> {
        let physical = self.physical();
        let mut columns = vec![
            "\"_row_id\" BLOB NOT NULL PRIMARY KEY".to_owned(),
            "\"_tomb\" INTEGER NOT NULL".to_owned(),
            "\"_stamps\" BLOB NOT NULL".to_owned(),
        ];
        for c in &self.columns {
            columns.push(format!("\"{}\" {}{}", c.name, c.ty.storage(), if c.not_null { " NOT NULL" } else { "" }));
        }
        let mut out = vec![format!("CREATE TABLE \"{physical}\" ({}) STRICT, WITHOUT ROWID", columns.join(", "))];
        for (i, u) in self.uniques.iter().enumerate() {
            out.push(format!(
                "CREATE UNIQUE INDEX \"_synod_u_{}_{i}\" ON \"{physical}\" ({}) WHERE \"_tomb\" = 0",
                self.name,
                self.quoted(u)
            ));
        }
        for (i, fk) in self.foreign_keys.iter().enumerate() {
            out.push(format!(
                "CREATE INDEX \"_synod_f_{}_{i}\" ON \"{physical}\" (\"{}\") WHERE \"_tomb\" = 0",
                self.name, self.columns[fk.column].name
            ));
        }
        let mut shown: Vec<String> = Vec::new();
        if let Some(r) = &self.row_id_column {
            shown.push(format!("\"_row_id\" AS \"{r}\""));
        }
        shown.extend(self.columns.iter().map(|c| format!("\"{}\"", c.name)));
        out.push(format!(
            "CREATE VIEW \"{}\" AS SELECT {} FROM \"{physical}\" WHERE \"_tomb\" = 0",
            self.name,
            shown.join(", ")
        ));
        out
    }

    fn quoted(&self, cols: &[usize]) -> String {
        cols.iter().map(|&i| format!("\"{}\"", self.columns[i].name)).collect::<Vec<_>>().join(", ")
    }
}

// ------------------------------------------------------------ lexer

#[derive(Clone, PartialEq, Debug)]
enum Tok {
    Ident(String),
    Number,
    Str,
    Punct(char),
}

#[derive(Clone, Debug)]
struct Token {
    tok: Tok,
    start: usize,
    end: usize,
}

fn lex(src: &str) -> Result<Vec<Token>> {
    let b = src.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < b.len() {
        let c = b[i];
        let start = i;
        if c.is_ascii_whitespace() {
            i += 1;
            continue;
        }
        if src[i..].starts_with("--") {
            i = src[i..].find('\n').map_or(b.len(), |n| i + n);
            continue;
        }
        if src[i..].starts_with("/*") {
            i = src[i + 2..].find("*/").map(|n| i + n + 4).ok_or_else(|| schema_err("unclosed comment"))?;
            continue;
        }
        let tok = if c.is_ascii_alphabetic() || c == b'_' {
            while i < b.len() && (b[i].is_ascii_alphanumeric() || b[i] == b'_') {
                i += 1;
            }
            Tok::Ident(src[start..i].to_owned())
        } else if c.is_ascii_digit() || (c == b'.' && b.get(i + 1).is_some_and(u8::is_ascii_digit)) {
            while i < b.len() && (b[i].is_ascii_alphanumeric() || b[i] == b'.') {
                i += 1;
            }
            Tok::Number
        } else if c == b'\'' {
            i += 1;
            loop {
                match b.get(i) {
                    None => return Err(schema_err("unclosed string")),
                    Some(b'\'') if b.get(i + 1) == Some(&b'\'') => i += 2,
                    Some(b'\'') => {
                        i += 1;
                        break;
                    }
                    Some(_) => i += 1,
                }
            }
            Tok::Str
        } else if c == b'"' || c == b'`' || c == b'[' {
            return Err(schema_err("quoted identifiers are not supported; use plain names"));
        } else if c.is_ascii() {
            i += 1;
            Tok::Punct(c as char)
        } else {
            return Err(schema_err(format!("unexpected character {:?}", src[i..].chars().next().unwrap())));
        };
        out.push(Token { tok, start, end: i });
    }
    Ok(out)
}

fn schema_err(msg: impl Into<String>) -> Error {
    Error::Schema(msg.into())
}

// ------------------------------------------------------------ parser

struct Parser<'a> {
    src: &'a str,
    toks: Vec<Token>,
    pos: usize,
}

impl<'a> Parser<'a> {
    fn peek(&self) -> Option<&Tok> {
        self.toks.get(self.pos).map(|t| &t.tok)
    }

    fn peek_kw(&self, kw: &str) -> bool {
        matches!(self.peek(), Some(Tok::Ident(s)) if s.eq_ignore_ascii_case(kw))
    }

    fn eat_kw(&mut self, kw: &str) -> bool {
        let found = self.peek_kw(kw);
        if found {
            self.pos += 1;
        }
        found
    }

    fn expect_kw(&mut self, kw: &str) -> Result<()> {
        if self.eat_kw(kw) { Ok(()) } else { Err(self.unexpected(kw)) }
    }

    fn eat_punct(&mut self, c: char) -> bool {
        let found = self.peek() == Some(&Tok::Punct(c));
        if found {
            self.pos += 1;
        }
        found
    }

    fn expect_punct(&mut self, c: char) -> Result<()> {
        if self.eat_punct(c) { Ok(()) } else { Err(self.unexpected(&format!("'{c}'"))) }
    }

    fn ident(&mut self, what: &str) -> Result<String> {
        match self.peek() {
            Some(Tok::Ident(s)) => {
                let s = s.clone();
                self.pos += 1;
                Ok(s)
            }
            _ => Err(self.unexpected(what)),
        }
    }

    fn number(&mut self) -> Result<u8> {
        let t = self.toks.get(self.pos).ok_or_else(|| self.unexpected("number"))?;
        if t.tok != Tok::Number {
            return Err(self.unexpected("number"));
        }
        let n = self.src[t.start..t.end].parse().map_err(|_| schema_err("number out of range (0 to 255)"))?;
        self.pos += 1;
        Ok(n)
    }

    fn unexpected(&self, wanted: &str) -> Error {
        match self.toks.get(self.pos) {
            Some(t) => schema_err(format!("expected {wanted}, found {:?}", &self.src[t.start..t.end])),
            None => schema_err(format!("expected {wanted}, found end of statement")),
        }
    }

    // Text of a parenthesized expression, without the outer parentheses.
    fn paren_text(&mut self) -> Result<String> {
        self.expect_punct('(')?;
        let start = self.toks[self.pos - 1].end;
        let mut depth = 1;
        while let Some(t) = self.toks.get(self.pos) {
            match t.tok {
                Tok::Punct('(') => depth += 1,
                Tok::Punct(')') => {
                    depth -= 1;
                    if depth == 0 {
                        let text = self.src[start..t.start].trim().to_owned();
                        self.pos += 1;
                        return Ok(text);
                    }
                }
                _ => {}
            }
            self.pos += 1;
        }
        Err(schema_err("unclosed parenthesis"))
    }

    fn name_list(&mut self) -> Result<Vec<String>> {
        self.expect_punct('(')?;
        let mut names = vec![self.ident("column name")?];
        while self.eat_punct(',') {
            names.push(self.ident("column name")?);
        }
        self.expect_punct(')')?;
        Ok(names)
    }

    fn column_type(&mut self) -> Result<Option<ColumnType>> {
        let name = self.ident("column type")?;
        Ok(Some(match name.to_ascii_uppercase().as_str() {
            "INTEGER" | "INT" => ColumnType::Integer,
            "REAL" => ColumnType::Real,
            "TEXT" => ColumnType::Text,
            "BLOB" => ColumnType::Blob,
            "ROWID" => return Ok(None),
            "DECIMAL" | "NUMERIC" => {
                let (mut precision, mut scale) = (None, None);
                if self.eat_punct('(') {
                    precision = Some(self.number()?);
                    if self.eat_punct(',') {
                        scale = Some(self.number()?);
                    }
                    self.expect_punct(')')?;
                }
                if let (Some(p), Some(s)) = (precision, scale)
                    && (s > p || p > 38)
                {
                    return Err(schema_err(format!("DECIMAL({p},{s}) is not valid")));
                }
                ColumnType::Decimal { precision, scale }
            }
            "SET" => {
                self.expect_punct('<')?;
                let item = self.column_type()?.ok_or_else(|| schema_err("SET<ROWID> is not valid"))?;
                if matches!(item, ColumnType::Set(_)) {
                    return Err(schema_err("a set of sets is not valid"));
                }
                self.expect_punct('>')?;
                ColumnType::Set(Box::new(item))
            }
            other => return Err(schema_err(format!("unknown type {other}"))),
        }))
    }

    fn on_delete(&mut self) -> Result<bool> {
        if !self.eat_kw("ON") {
            return Ok(false);
        }
        self.expect_kw("DELETE")?;
        if self.eat_kw("CASCADE") {
            Ok(true)
        } else if self.eat_kw("RESTRICT") {
            Ok(false)
        } else if self.eat_kw("NO") {
            self.expect_kw("ACTION")?;
            Ok(false)
        } else {
            Err(self.unexpected("CASCADE, RESTRICT, or NO ACTION"))
        }
    }

    fn at_item_end(&self) -> bool {
        matches!(self.peek(), Some(Tok::Punct(',')) | Some(Tok::Punct(')')) | None)
    }
}

struct Builder {
    columns: Vec<Column>,
    row_id_column: Option<String>,
    uniques: Vec<Vec<String>>,
    not_null: Vec<String>,
    checks: Vec<String>,
    fks: Vec<(String, String, String, bool)>,
}

/// Parses a `CREATE TABLE` statement. `id` becomes the table ID.
pub fn parse_create_table(ddl: &str, id: u32) -> Result<TableSchema> {
    let mut p = Parser { src: ddl, toks: lex(ddl)?, pos: 0 };
    p.expect_kw("CREATE")?;
    p.expect_kw("TABLE")?;
    let name = p.ident("table name")?;
    check_name(&name, "table")?;
    p.expect_punct('(')?;
    let mut b = Builder {
        columns: Vec::new(),
        row_id_column: None,
        uniques: Vec::new(),
        not_null: Vec::new(),
        checks: Vec::new(),
        fks: Vec::new(),
    };
    loop {
        parse_item(&mut p, &mut b)?;
        if !p.eat_punct(',') {
            break;
        }
    }
    p.expect_punct(')')?;
    let mut delete_rule = DeleteRule::DeleteWins;
    if p.eat_kw("MERGE") {
        p.expect_kw("DELETE")?;
        let rule = p.ident("delete rule")?;
        delete_rule = DeleteRule::parse(&rule).ok_or_else(|| schema_err(format!("unknown delete rule {rule}")))?;
    }
    p.eat_punct(';');
    if p.peek().is_some() {
        return Err(p.unexpected("end of statement"));
    }
    finish(b, id, name, delete_rule, ddl.trim().to_owned())
}

fn parse_item(p: &mut Parser<'_>, b: &mut Builder) -> Result<()> {
    if p.eat_kw("CONSTRAINT") {
        p.ident("constraint name")?;
    }
    if p.eat_kw("PRIMARY") {
        p.expect_kw("KEY")?;
        let cols = p.name_list()?;
        b.not_null.extend(cols.iter().cloned());
        b.uniques.push(cols);
        return Ok(());
    }
    if p.eat_kw("UNIQUE") {
        let cols = p.name_list()?;
        b.uniques.push(cols);
        return Ok(());
    }
    if p.peek_kw("CHECK") {
        p.pos += 1;
        let check = p.paren_text()?;
        b.checks.push(check);
        return Ok(());
    }
    if p.eat_kw("FOREIGN") {
        p.expect_kw("KEY")?;
        let cols = p.name_list()?;
        let [col] = <[String; 1]>::try_from(cols).map_err(|_| schema_err("only single-column foreign keys"))?;
        p.expect_kw("REFERENCES")?;
        let parent = p.ident("parent table")?;
        let [parent_col] =
            <[String; 1]>::try_from(p.name_list()?).map_err(|_| schema_err("only single-column foreign keys"))?;
        let cascade = p.on_delete()?;
        b.fks.push((col, parent, parent_col, cascade));
        return Ok(());
    }
    // A column.
    let name = p.ident("column name")?;
    check_name(&name, "column")?;
    let Some(ty) = p.column_type()? else {
        if b.row_id_column.is_some() {
            return Err(schema_err("more than one ROWID column"));
        }
        if !p.at_item_end() {
            return Err(schema_err("a ROWID column takes no constraints"));
        }
        b.row_id_column = Some(name);
        return Ok(());
    };
    let mut col = Column { name: name.clone(), ty, merge: MergePolicy::Lww, not_null: false, default: None };
    let mut merge_set = false;
    while !p.at_item_end() {
        if p.eat_kw("CONSTRAINT") {
            p.ident("constraint name")?;
        } else if p.eat_kw("NOT") {
            p.expect_kw("NULL")?;
            col.not_null = true;
        } else if p.eat_kw("NULL") {
        } else if p.eat_kw("PRIMARY") {
            p.expect_kw("KEY")?;
            let _ = p.eat_kw("ASC") || p.eat_kw("DESC");
            col.not_null = true;
            b.uniques.push(vec![name.clone()]);
        } else if p.eat_kw("UNIQUE") {
            b.uniques.push(vec![name.clone()]);
        } else if p.peek_kw("CHECK") {
            p.pos += 1;
            let check = p.paren_text()?;
            b.checks.push(check);
        } else if p.eat_kw("DEFAULT") {
            col.default = Some(default_text(p)?);
        } else if p.eat_kw("REFERENCES") {
            let parent = p.ident("parent table")?;
            let [parent_col] =
                <[String; 1]>::try_from(p.name_list()?).map_err(|_| schema_err("only single-column foreign keys"))?;
            let cascade = p.on_delete()?;
            b.fks.push((name.clone(), parent, parent_col, cascade));
        } else if p.eat_kw("MERGE") {
            if merge_set {
                return Err(schema_err(format!("column {name} has two MERGE clauses")));
            }
            let policy = p.ident("merge policy")?;
            col.merge =
                MergePolicy::parse(&policy).ok_or_else(|| schema_err(format!("unknown merge policy {policy}")))?;
            merge_set = true;
        } else if p.peek_kw("COLLATE") {
            return Err(schema_err("COLLATE is not supported: comparison must be byte-wise on every node"));
        } else {
            return Err(p.unexpected("column constraint or MERGE"));
        }
    }
    b.columns.push(col);
    Ok(())
}

fn default_text(p: &mut Parser<'_>) -> Result<String> {
    if p.peek() == Some(&Tok::Punct('(')) {
        return Ok(format!("({})", p.paren_text()?));
    }
    let start = p.toks.get(p.pos).ok_or_else(|| p.unexpected("default value"))?.start;
    let _sign = p.eat_punct('-') || p.eat_punct('+');
    let t = p.toks.get(p.pos).ok_or_else(|| p.unexpected("default value"))?;
    match t.tok {
        Tok::Number | Tok::Str | Tok::Ident(_) => {
            let end = t.end;
            p.pos += 1;
            Ok(p.src[start..end].to_owned())
        }
        _ => Err(p.unexpected("default value")),
    }
}

fn check_name(name: &str, what: &str) -> Result<()> {
    let lower = name.to_ascii_lowercase();
    if lower.starts_with("_synod")
        || lower.starts_with("sqlite_")
        || ["_row_id", "_tomb", "_stamps"].contains(&lower.as_str())
    {
        return Err(schema_err(format!("{what} name {name} is reserved")));
    }
    Ok(())
}

fn finish(b: Builder, id: u32, name: String, delete_rule: DeleteRule, ddl: String) -> Result<TableSchema> {
    let mut columns = b.columns;
    if columns.is_empty() {
        return Err(schema_err("a table needs at least one column"));
    }
    if columns.len() > 2000 {
        return Err(schema_err("more than 2000 columns"));
    }
    for (i, c) in columns.iter().enumerate() {
        if columns[..i].iter().any(|d| d.name.eq_ignore_ascii_case(&c.name))
            || b.row_id_column.as_deref().is_some_and(|r| r.eq_ignore_ascii_case(&c.name))
        {
            return Err(schema_err(format!("duplicate column {}", c.name)));
        }
        let ok = match c.merge {
            MergePolicy::Delta => matches!(c.ty, ColumnType::Integer | ColumnType::Decimal { .. }),
            MergePolicy::Union => matches!(c.ty, ColumnType::Set(_)),
            MergePolicy::Text3 => c.ty == ColumnType::Text,
            _ => true,
        };
        if !ok {
            return Err(schema_err(format!("MERGE {} does not fit column {} of type {}", c.merge, c.name, c.ty)));
        }
    }
    let index_of = |n: &str| {
        columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(n))
            .ok_or_else(|| schema_err(format!("unknown column {n}")))
    };
    let mut uniques: Vec<Vec<usize>> = Vec::new();
    for u in &b.uniques {
        let idx = u.iter().map(|n| index_of(n)).collect::<Result<Vec<_>>>()?;
        if !uniques.contains(&idx) {
            uniques.push(idx);
        }
    }
    let not_null = b.not_null.iter().map(|n| index_of(n)).collect::<Result<Vec<_>>>()?;
    let mut foreign_keys = Vec::new();
    for (col, parent, parent_column, cascade) in b.fks {
        foreign_keys.push(ForeignKey { column: index_of(&col)?, parent, parent_column, cascade });
    }
    for i in not_null {
        columns[i].not_null = true;
    }
    Ok(TableSchema {
        id,
        name,
        row_id_column: b.row_id_column,
        columns,
        uniques,
        checks: b.checks,
        foreign_keys,
        delete_rule,
        ddl,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const INVENTORY: &str = "
        CREATE TABLE inventory (
          row_id  ROWID,
          sku     TEXT UNIQUE                MERGE lww,
          name    TEXT                       MERGE lww,
          price   DECIMAL(10,2)              MERGE lww_flag,
          qty     INTEGER CHECK (qty >= 0)   MERGE delta,
          tags    SET<TEXT>                  MERGE union,
          notes   TEXT                       MERGE text3
        );";

    #[test]
    fn white_paper_example() {
        let t = parse_create_table(INVENTORY, 3).unwrap();
        assert_eq!(t.id, 3);
        assert_eq!(t.row_id_column.as_deref(), Some("row_id"));
        let names: Vec<_> = t.columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, ["sku", "name", "price", "qty", "tags", "notes"]);
        assert_eq!(t.columns[2].ty, ColumnType::Decimal { precision: Some(10), scale: Some(2) });
        assert_eq!(t.columns[2].merge, MergePolicy::LwwFlag);
        assert_eq!(t.columns[3].merge, MergePolicy::Delta);
        assert_eq!(t.columns[4].ty, ColumnType::Set(Box::new(ColumnType::Text)));
        assert_eq!(t.uniques, vec![vec![0]]);
        assert_eq!(t.checks, vec!["qty >= 0"]);
        assert_eq!(t.delete_rule, DeleteRule::DeleteWins);
        let sql = t.create_statements();
        assert!(sql[0].contains("STRICT, WITHOUT ROWID"), "{}", sql[0]);
        assert!(!sql[0].contains("CHECK") && !sql[0].contains("UNIQUE"), "{}", sql[0]);
        assert!(sql[1].contains("WHERE \"_tomb\" = 0"));
        assert!(sql.last().unwrap().starts_with("CREATE VIEW \"inventory\" AS SELECT \"_row_id\" AS \"row_id\""));
    }

    #[test]
    fn constraints_and_options() {
        let t = parse_create_table(
            "create table orders (
               id INTEGER PRIMARY KEY,
               customer INTEGER NOT NULL REFERENCES customers(id) ON DELETE CASCADE, -- comment
               placed TEXT DEFAULT CURRENT_TIMESTAMP,
               total DECIMAL DEFAULT -1.5 MERGE max,
               note TEXT DEFAULT 'it''s' /* inline */,
               CONSTRAINT c1 CHECK (total > 0 OR note IS NOT NULL),
               UNIQUE (customer, placed)
             ) MERGE DELETE update_wins",
            0,
        )
        .unwrap();
        assert!(t.columns[0].not_null);
        assert_eq!(t.uniques, vec![vec![0], vec![1, 2]]);
        assert_eq!(
            t.foreign_keys,
            vec![ForeignKey { column: 1, parent: "customers".into(), parent_column: "id".into(), cascade: true }]
        );
        assert_eq!(t.columns[2].default.as_deref(), Some("CURRENT_TIMESTAMP"));
        assert_eq!(t.columns[3].default.as_deref(), Some("-1.5"));
        assert_eq!(t.columns[4].default.as_deref(), Some("'it''s'"));
        assert_eq!(t.columns[3].merge, MergePolicy::Max);
        assert_eq!(t.checks, vec!["total > 0 OR note IS NOT NULL"]);
        assert_eq!(t.delete_rule, DeleteRule::UpdateWins);
    }

    #[test]
    fn rejects_bad_statements() {
        for (ddl, wanted) in [
            ("CREATE TABLE t (a TEXT MERGE delta)", "does not fit"),
            ("CREATE TABLE t (a INTEGER MERGE union)", "does not fit"),
            ("CREATE TABLE t (a INTEGER MERGE sum)", "unknown merge policy"),
            ("CREATE TABLE t (a TEXT COLLATE NOCASE)", "COLLATE"),
            ("CREATE TABLE t (\"a\" TEXT)", "quoted"),
            ("CREATE TABLE _synod_x (a TEXT)", "reserved"),
            ("CREATE TABLE t (_tomb TEXT)", "reserved"),
            ("CREATE TABLE t (a TEXT, A INTEGER)", "duplicate"),
            ("CREATE TABLE t (a TEXT, UNIQUE (b))", "unknown column"),
            ("CREATE TABLE t (r ROWID, s ROWID)", "more than one ROWID"),
            ("CREATE TABLE t (a SET<SET<TEXT>>)", "set of sets"),
            ("CREATE TABLE t (a DECIMAL(2,5))", "not valid"),
            ("CREATE TABLE t (a TEXT) MERGE DELETE sometimes", "unknown delete rule"),
            ("CREATE TABLE t (a TEXT) extra", "end of statement"),
            ("CREATE TABLE t (r ROWID)", "at least one column"),
        ] {
            let err = parse_create_table(ddl, 0).unwrap_err().to_string();
            assert!(err.contains(wanted), "{ddl}: {err}");
        }
    }
}
