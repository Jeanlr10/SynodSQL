//! Column merge policies (specification Section 7.2).

use std::cmp::Ordering;
use std::collections::BTreeSet;

use synod_core::{Cell, NodeId, Stamp, Value, ValueSet};

use super::text3;
use crate::schema::{Column, MergePolicy};
use crate::values;

/// One node's change to one column: its first before-value and its last
/// after-value in the logs, and the stamp of its last write to the column.
#[derive(Clone, Debug)]
pub(crate) struct Write {
    pub node: NodeId,
    pub stamp: Stamp,
    pub before: Value,
    pub after: Value,
}

/// A conflict found in one column.
#[derive(Clone, Debug)]
pub(crate) struct ColumnConflict {
    pub rule: &'static str,
    pub recorded: Vec<(NodeId, Stamp, Value)>,
}

/// Resolves one column. `cur` is the cell in the base state; `writes` are
/// the nodes that changed the column, in node ID order.
pub(crate) fn resolve(column: &Column, cur: &Cell, writes: &[Write]) -> (Cell, Option<ColumnConflict>) {
    if writes.is_empty() {
        return (cur.clone(), None);
    }
    let newest = writes.iter().map(|w| w.stamp).max().unwrap();
    let fresh = writes.iter().all(|w| w.before == cur.value);
    // One writer that saw the current value: every policy takes its value.
    // Several writers with one value: every policy but delta takes it (two
    // equal sales are still two sales).
    let same = writes.iter().all(|w| w.after == writes[0].after);
    if fresh && (writes.len() == 1 || (same && column.merge != MergePolicy::Delta)) {
        return (Cell { stamp: newest, value: writes[0].after.clone() }, None);
    }
    let all = |ws: &[Write]| ws.iter().map(|w| (w.node, w.stamp, w.after.clone())).collect::<Vec<_>>();
    match column.merge {
        MergePolicy::Lww => (lww(cur, writes), None),
        MergePolicy::LwwFlag => {
            let won = lww(cur, writes);
            let losers: Vec<_> =
                writes.iter().filter(|w| w.after != won.value).map(|w| (w.node, w.stamp, w.after.clone())).collect();
            let conflict = (!losers.is_empty()).then_some(ColumnConflict { rule: "lww_flag", recorded: losers });
            (won, conflict)
        }
        MergePolicy::Delta => match delta(column, cur, writes) {
            Some(value) => (keep_or(cur, value, newest), None),
            // NULL, overflow, or a result that does not fit the column.
            None => (lww(cur, writes), Some(ColumnConflict { rule: "delta", recorded: all(writes) })),
        },
        MergePolicy::Max | MergePolicy::Min => {
            let mut best = cur.clone();
            for w in writes {
                let ord = type_order(&w.after, &best.value);
                let better =
                    if column.merge == MergePolicy::Max { ord == Ordering::Greater } else { ord == Ordering::Less };
                if better || (ord == Ordering::Equal && w.stamp > best.stamp) {
                    best = Cell { stamp: w.stamp, value: w.after.clone() };
                }
            }
            if best.value == cur.value { (cur.clone(), None) } else { (best, None) }
        }
        MergePolicy::Union => (keep_or(cur, union(cur, writes), newest), None),
        MergePolicy::Text3 => {
            let text = as_text;
            let mut result = match text(&cur.value) {
                Some(s) => s.to_owned(),
                None => return (lww(cur, writes), Some(ColumnConflict { rule: "text3", recorded: all(writes) })),
            };
            let mut overlap = false;
            for w in writes {
                let (Some(before), Some(after)) = (text(&w.before), text(&w.after)) else {
                    return (lww(cur, writes), Some(ColumnConflict { rule: "text3", recorded: all(writes) }));
                };
                let (merged, clash) = text3::merge3(before, &result, after);
                result = merged;
                overlap |= clash;
            }
            let conflict = overlap.then(|| ColumnConflict { rule: "text3", recorded: all(writes) });
            (keep_or(cur, Value::Text(result), newest), conflict)
        }
        MergePolicy::Flag => (cur.clone(), Some(ColumnConflict { rule: "flag", recorded: all(writes) })),
    }
}

// Highest stamp wins, the current cell included: an old write loses to a
// newer one (white paper, Section XI-A).
fn lww(cur: &Cell, writes: &[Write]) -> Cell {
    let mut best = cur.clone();
    for w in writes {
        if w.stamp > best.stamp {
            best = Cell { stamp: w.stamp, value: w.after.clone() };
        }
    }
    best
}

fn as_text(v: &Value) -> Option<&str> {
    match v {
        Value::Text(s) => Some(s),
        _ => None,
    }
}

fn keep_or(cur: &Cell, value: Value, stamp: Stamp) -> Cell {
    if value == cur.value { cur.clone() } else { Cell { stamp, value } }
}

// current + Σ (after − before). None if a value is NULL or not a number of
// the column's type, on overflow, or if the sum does not fit the column.
fn delta(column: &Column, cur: &Cell, writes: &[Write]) -> Option<Value> {
    let value = match &cur.value {
        Value::Integer(c) => {
            let mut acc = *c as i128;
            for w in writes {
                let (Value::Integer(b), Value::Integer(a)) = (&w.before, &w.after) else { return None };
                acc += *a as i128 - *b as i128;
            }
            Value::Integer(i64::try_from(acc).ok()?)
        }
        Value::Decimal(c) => {
            let mut acc = *c;
            for w in writes {
                let (Value::Decimal(b), Value::Decimal(a)) = (&w.before, &w.after) else { return None };
                acc = acc.checked_add(a.checked_sub(*b)?)?;
            }
            Value::Decimal(acc)
        }
        _ => return None,
    };
    values::check(column, value).ok()
}

// (current ∪ additions) − removals. A node's additions are its after-set
// minus its before-set; its removals are the reverse. NULL is the empty set.
fn union(cur: &Cell, writes: &[Write]) -> Value {
    let set = |v: &Value| -> BTreeSet<Value> {
        match v {
            Value::Set(s) => s.items().iter().cloned().collect(),
            _ => BTreeSet::new(),
        }
    };
    let mut result = set(&cur.value);
    let mut removed = BTreeSet::new();
    for w in writes {
        let (before, after) = (set(&w.before), set(&w.after));
        result.extend(after.difference(&before).cloned());
        removed.extend(before.difference(&after).cloned());
    }
    let result: Vec<Value> = result.into_iter().filter(|v| !removed.contains(v)).collect();
    if result.is_empty() && matches!(cur.value, Value::Null) {
        return Value::Null;
    }
    Value::Set(ValueSet::new(result).expect("set items are scalars"))
}

/// The order of `max` and `min`: NULL first; numbers by value (REAL by IEEE
/// total order); text and blobs byte-wise; other values by encoding.
pub(crate) fn type_order(a: &Value, b: &Value) -> Ordering {
    match (a, b) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Null, _) => Ordering::Less,
        (_, Value::Null) => Ordering::Greater,
        (Value::Integer(x), Value::Integer(y)) => x.cmp(y),
        (Value::Real(x), Value::Real(y)) => x.total_cmp(y),
        (Value::Decimal(x), Value::Decimal(y)) => x.cmp(y),
        (Value::Text(x), Value::Text(y)) => x.as_bytes().cmp(y.as_bytes()),
        (Value::Blob(x), Value::Blob(y)) => x.cmp(y),
        _ => a.cmp(b),
    }
}

/// True if two values are equal as SQL compares them in a unique index:
/// NULL equals nothing, and REAL 0.0 equals −0.0.
pub(crate) fn key_eq(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Null, _) | (_, Value::Null) => false,
        (Value::Real(x), Value::Real(y)) => x == y,
        _ => a == b,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::ColumnType;
    use synod_core::Hlc;

    fn col(ty: ColumnType, merge: MergePolicy) -> Column {
        Column { name: "c".into(), ty, merge, not_null: false, default: None }
    }

    fn st(t: u64, n: u32) -> Stamp {
        Stamp { hlc: Hlc::new(t, 0).unwrap(), node: NodeId(n) }
    }

    fn cell(v: Value, t: u64) -> Cell {
        Cell { stamp: st(t, 0), value: v }
    }

    fn w(n: u32, t: u64, before: Value, after: Value) -> Write {
        Write { node: NodeId(n), stamp: st(t, n), before, after }
    }

    fn int(i: i64) -> Value {
        Value::Integer(i)
    }

    fn dec(s: &str) -> Value {
        Value::Decimal(s.parse().unwrap())
    }

    #[test]
    fn worked_example_columns() {
        // r1.price, lww_flag: A 0.10 -> 0.12 at 10, C 0.10 -> 0.11 at 17.
        let price = col(ColumnType::Decimal { precision: Some(10), scale: Some(2) }, MergePolicy::LwwFlag);
        let (c, conflict) = resolve(
            &price,
            &cell(dec("0.10"), 1),
            &[w(0xA, 10, dec("0.10"), dec("0.12")), w(0xC, 17, dec("0.10"), dec("0.11"))],
        );
        assert_eq!(c.value, dec("0.11"));
        assert_eq!(c.stamp, st(17, 0xC));
        let conflict = conflict.unwrap();
        assert_eq!(conflict.rule, "lww_flag");
        assert_eq!(conflict.recorded, vec![(NodeId(0xA), st(10, 0xA), dec("0.12"))]);
        // r2.qty, delta: 800 + (650 - 800) + (700 - 800) = 550.
        let qty = col(ColumnType::Integer, MergePolicy::Delta);
        let (c, conflict) =
            resolve(&qty, &cell(int(800), 1), &[w(0xA, 12, int(800), int(650)), w(0xB, 13, int(800), int(700))]);
        assert_eq!((c.value, conflict.is_none()), (int(550), true));
    }

    #[test]
    fn delta_counts_equal_writes_twice_and_late_writes() {
        let qty = col(ColumnType::Integer, MergePolicy::Delta);
        let (c, _) = resolve(&qty, &cell(int(800), 1), &[w(1, 5, int(800), int(650)), w(2, 6, int(800), int(650))]);
        assert_eq!(c.value, int(500), "two equal sales are two sales");
        // A late sale, made against an older value, still counts.
        let (c, _) = resolve(&qty, &cell(int(550), 20), &[w(1, 5, int(800), int(790))]);
        assert_eq!(c.value, int(540));
        // NULL falls back to lww and records a conflict.
        let (c, conflict) =
            resolve(&qty, &cell(Value::Null, 1), &[w(1, 5, Value::Null, int(3)), w(2, 6, Value::Null, int(4))]);
        assert_eq!((c.value, conflict.unwrap().rule), (int(4), "delta"));
        // One writer from NULL is not a conflict.
        let (c, conflict) = resolve(&qty, &cell(Value::Null, 1), &[w(1, 5, Value::Null, int(3))]);
        assert_eq!((c.value, conflict.is_none()), (int(3), true));
        // A decimal sum that no longer fits the column is a conflict.
        let small = col(ColumnType::Decimal { precision: Some(3), scale: Some(1) }, MergePolicy::Delta);
        let (_, conflict) =
            resolve(&small, &cell(dec("90"), 1), &[w(1, 5, dec("90"), dec("95")), w(2, 6, dec("90"), dec("99"))]);
        assert!(conflict.is_some());
    }

    #[test]
    fn lww_and_late_writes() {
        let c = col(ColumnType::Text, MergePolicy::Lww);
        let t = |s: &str| Value::Text(s.into());
        // A late write older than the current stamp loses.
        let (r, _) = resolve(&c, &cell(t("new"), 50), &[w(1, 10, t("old"), t("late"))]);
        assert_eq!(r.value, t("new"));
        // Equal HLC: the higher node ID wins.
        let (r, _) = resolve(&c, &cell(t("x"), 1), &[w(1, 10, t("x"), t("a")), w(2, 10, t("x"), t("b"))]);
        assert_eq!(r.value, t("b"));
    }

    #[test]
    fn max_min_use_numeric_order() {
        let mx = col(ColumnType::Integer, MergePolicy::Max);
        let (r, _) = resolve(&mx, &cell(int(-5), 1), &[w(1, 2, int(-5), int(-7)), w(2, 3, int(-5), int(3))]);
        assert_eq!(r.value, int(3));
        let mn = col(ColumnType::Integer, MergePolicy::Min);
        let (r, _) = resolve(&mn, &cell(int(-5), 1), &[w(1, 2, int(-5), int(-7)), w(2, 3, int(-5), int(3))]);
        assert_eq!(r.value, int(-7));
        // A late, lower value cannot lower a max.
        let (r, _) = resolve(&mx, &cell(int(10), 9), &[w(1, 2, int(1), int(4))]);
        assert_eq!(r, cell(int(10), 9));
    }

    #[test]
    fn union_adds_and_removes() {
        let c = col(ColumnType::Set(Box::new(ColumnType::Text)), MergePolicy::Union);
        let set = |items: &[&str]| Value::Set(ValueSet::new(items.iter().map(|s| Value::Text(s.to_string()))).unwrap());
        let (r, _) = resolve(
            &c,
            &cell(set(&["a", "b"]), 1),
            &[w(1, 2, set(&["a", "b"]), set(&["a", "b", "c"])), w(2, 3, set(&["a", "b"]), set(&["b"]))],
        );
        assert_eq!(r.value, set(&["b", "c"]));
    }

    #[test]
    fn text3_merges_lines_in_node_order() {
        let c = col(ColumnType::Text, MergePolicy::Text3);
        let t = |s: &str| Value::Text(s.into());
        let base = "one\ntwo\nthree\n";
        let (r, conflict) = resolve(
            &c,
            &cell(t(base), 1),
            &[w(1, 2, t(base), t("ONE\ntwo\nthree\n")), w(2, 3, t(base), t("one\ntwo\nTHREE\n"))],
        );
        assert_eq!((r.value, conflict.is_none()), (t("ONE\ntwo\nTHREE\n"), true));
        let (r, conflict) = resolve(
            &c,
            &cell(t(base), 1),
            &[w(1, 2, t(base), t("one\n2\nthree\n")), w(2, 3, t(base), t("one\nzwei\nthree\n"))],
        );
        assert_eq!(r.value, t("one\n2\nthree\n"), "an overlap keeps the earlier node's text");
        assert_eq!(conflict.unwrap().rule, "text3");
    }

    #[test]
    fn flag_keeps_the_value_unless_one_writer() {
        let c = col(ColumnType::Integer, MergePolicy::Flag);
        let (r, conflict) = resolve(&c, &cell(int(1), 1), &[w(1, 2, int(1), int(2)), w(2, 3, int(1), int(3))]);
        assert_eq!((r.value, conflict.unwrap().recorded.len()), (int(1), 2));
        let (r, conflict) = resolve(&c, &cell(int(1), 1), &[w(1, 2, int(1), int(2))]);
        assert_eq!((r.value, conflict.is_none()), (int(2), true));
    }

    #[test]
    fn key_equality_is_sql_equality() {
        assert!(key_eq(&Value::Real(0.0), &Value::Real(-0.0)));
        assert!(!key_eq(&Value::Null, &Value::Null));
        assert!(key_eq(&dec("0.10"), &dec("0.1")));
    }
}
