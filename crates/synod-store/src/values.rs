//! Conversion between [`Value`] and SQLite values, by column type.
//!
//! | Column type | SQLite storage |
//! | --- | --- |
//! | INTEGER | INTEGER |
//! | REAL | REAL (NaN is rejected: SQLite stores it as NULL) |
//! | TEXT | TEXT |
//! | BLOB | BLOB |
//! | DECIMAL | TEXT, the normalized decimal (`0.1`, not `0.10`) |
//! | SET | BLOB, the canonical encoding of the set |

use rusqlite::types::{Value as SqlValue, ValueRef};
use synod_core::{Decimal, Value, ValueSet};

use crate::error::{Error, Result};
use crate::schema::{Column, ColumnType};

/// Checks a value against a column and returns it in canonical form.
pub fn check(column: &Column, value: Value) -> Result<Value> {
    let bad = |v: &Value| Error::Constraint(format!("column {} of type {} cannot hold {v:?}", column.name, column.ty));
    if matches!(value, Value::Null) {
        if column.not_null {
            return Err(Error::Constraint(format!("column {} is NOT NULL", column.name)));
        }
        return Ok(Value::Null);
    }
    check_type(&column.ty, value).map_err(|v| bad(&v))
}

// Returns the value in canonical form, or gives it back if the type does
// not fit.
fn check_type(ty: &ColumnType, value: Value) -> std::result::Result<Value, Value> {
    match (ty, value) {
        (ColumnType::Integer, v @ Value::Integer(_)) => Ok(v),
        (ColumnType::Real, Value::Real(r)) if !r.is_nan() => Ok(Value::Real(r)),
        (ColumnType::Real, Value::Integer(i)) => Ok(Value::Real(i as f64)),
        (ColumnType::Text, v @ Value::Text(_)) => Ok(v),
        (ColumnType::Blob, v @ Value::Blob(_)) => Ok(v),
        (ColumnType::Decimal { precision, scale }, value) => {
            let d = match &value {
                Value::Decimal(d) => *d,
                Value::Integer(i) => Decimal::new(*i as i128, 0).unwrap(),
                _ => return Err(value),
            };
            let digits = d.units().unsigned_abs().checked_ilog10().map_or(1, |n| n + 1);
            let int_digits = digits.saturating_sub(d.scale() as u32);
            let fits = scale.is_none_or(|s| d.scale() <= s)
                && precision.is_none_or(|p| int_digits + scale.unwrap_or(d.scale()) as u32 <= p as u32);
            if fits { Ok(Value::Decimal(d)) } else { Err(value) }
        }
        (ColumnType::Set(item), Value::Set(set)) => {
            let items: std::result::Result<Vec<Value>, Value> =
                set.items().iter().cloned().map(|v| check_type(item, v)).collect();
            match items {
                Ok(items) => Ok(Value::Set(ValueSet::new(items).expect("no nested sets"))),
                Err(_) => Err(Value::Set(set)),
            }
        }
        (_, v) => Err(v),
    }
}

/// Converts a canonical value to its SQLite storage form.
pub fn to_sql(value: &Value) -> SqlValue {
    match value {
        Value::Null => SqlValue::Null,
        Value::Integer(i) => SqlValue::Integer(*i),
        Value::Real(r) => SqlValue::Real(*r),
        Value::Decimal(d) => SqlValue::Text(d.to_string()),
        Value::Text(s) => SqlValue::Text(s.clone()),
        Value::Blob(b) => SqlValue::Blob(b.clone()),
        Value::Set(_) => SqlValue::Blob(value.to_canonical_bytes()),
    }
}

/// Reads a stored value of a column back into its canonical form.
pub fn from_sql(column: &Column, raw: ValueRef<'_>) -> Result<Value> {
    let corrupt = || Error::Corrupt(format!("column {} of type {} holds {raw:?}", column.name, column.ty));
    Ok(match (&column.ty, raw) {
        (_, ValueRef::Null) => Value::Null,
        (ColumnType::Integer, ValueRef::Integer(i)) => Value::Integer(i),
        (ColumnType::Real, ValueRef::Real(r)) => Value::Real(r),
        (ColumnType::Text, ValueRef::Text(t)) => Value::Text(String::from_utf8(t.to_vec()).map_err(|_| corrupt())?),
        (ColumnType::Blob, ValueRef::Blob(b)) => Value::Blob(b.to_vec()),
        (ColumnType::Decimal { .. }, ValueRef::Text(t)) => {
            let text = std::str::from_utf8(t).map_err(|_| corrupt())?;
            let d: Decimal = text.parse().map_err(|_| corrupt())?;
            if d.to_string() != text {
                return Err(corrupt());
            }
            Value::Decimal(d)
        }
        (ColumnType::Set(_), ValueRef::Blob(b)) => {
            let v = Value::from_canonical_bytes(b).map_err(|_| corrupt())?;
            if !matches!(v, Value::Set(_)) {
                return Err(corrupt());
            }
            v
        }
        _ => return Err(corrupt()),
    })
}

/// Converts the result of a SQL expression (for example a DEFAULT) to a
/// value of a column.
pub fn from_expression(column: &Column, raw: ValueRef<'_>) -> Result<Value> {
    let value = match (&column.ty, raw) {
        (_, ValueRef::Null) => Value::Null,
        (ColumnType::Decimal { .. }, ValueRef::Text(t)) => Value::Decimal(
            std::str::from_utf8(t)
                .ok()
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| Error::Schema(format!("default of column {} is not a decimal", column.name)))?,
        ),
        (ColumnType::Set(_), ValueRef::Blob(b)) => {
            Value::from_canonical_bytes(b).map_err(|e| Error::Schema(format!("default of {}: {e}", column.name)))?
        }
        (_, ValueRef::Integer(i)) => Value::Integer(i),
        (_, ValueRef::Real(r)) => Value::Real(r),
        (_, ValueRef::Text(t)) => Value::Text(String::from_utf8_lossy(t).into_owned()),
        (_, ValueRef::Blob(b)) => Value::Blob(b.to_vec()),
    };
    check(column, value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::MergePolicy;

    fn col(ty: ColumnType) -> Column {
        Column { name: "c".into(), ty, merge: MergePolicy::Lww, not_null: false, default: None }
    }

    fn dec(s: &str) -> Value {
        Value::Decimal(s.parse().unwrap())
    }

    #[test]
    fn decimal_precision_and_scale() {
        let c = col(ColumnType::Decimal { precision: Some(5), scale: Some(2) });
        assert_eq!(check(&c, dec("123.45")).unwrap(), dec("123.45"));
        assert_eq!(check(&c, dec("0.1")).unwrap(), dec("0.10"));
        assert_eq!(check(&c, Value::Integer(999)).unwrap(), dec("999"));
        assert!(check(&c, dec("1234")).is_err());
        assert!(check(&c, dec("1.234")).is_err());
        assert!(check(&c, Value::Text("1".into())).is_err());
    }

    #[test]
    fn round_trips_through_sql() {
        let cases = [
            (ColumnType::Integer, Value::Integer(-7)),
            (ColumnType::Real, Value::Real(-0.0)),
            (ColumnType::Text, Value::Text("é".into())),
            (ColumnType::Blob, Value::Blob(vec![0, 1])),
            (ColumnType::Decimal { precision: None, scale: None }, dec("-12.50")),
            (
                ColumnType::Set(Box::new(ColumnType::Text)),
                Value::Set(ValueSet::new([Value::Text("b".into()), Value::Text("a".into())]).unwrap()),
            ),
        ];
        for (ty, v) in cases {
            let c = col(ty);
            let v = check(&c, v).unwrap();
            let sql = to_sql(&v);
            let back = from_sql(&c, ValueRef::from(&sql)).unwrap();
            assert_eq!(back.to_canonical_bytes(), v.to_canonical_bytes());
        }
    }

    #[test]
    fn rejects_nan_and_wrong_set_items() {
        assert!(check(&col(ColumnType::Real), Value::Real(f64::NAN)).is_err());
        let set = Value::Set(ValueSet::new([Value::Integer(1)]).unwrap());
        assert!(check(&col(ColumnType::Set(Box::new(ColumnType::Text))), set).is_err());
        let mut c = col(ColumnType::Text);
        c.not_null = true;
        assert!(check(&c, Value::Null).is_err());
    }
}
