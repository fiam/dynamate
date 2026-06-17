//! Conversion between SQL rows and the neutral [`Value`] / [`Item`] model, and
//! binding neutral values as query parameters. Decoding is by the column's
//! declared type; unknown types fall back to a string (or NULL) rather than
//! failing the whole row.

use sqlx::mysql::MySqlRow;
use sqlx::postgres::PgRow;
use sqlx::query::Query;
use sqlx::{Column, Row, TypeInfo};

use crate::core::value::{Item, Number, Value};

// ---- row -> Item ----

pub fn pg_row_to_item(row: &PgRow) -> Item {
    let mut item = Item::with_capacity(row.columns().len());
    for (idx, col) in row.columns().iter().enumerate() {
        let ty = col.type_info().name().to_ascii_uppercase();
        item.insert(col.name().to_string(), pg_decode(row, idx, &ty));
    }
    item
}

fn pg_decode(row: &PgRow, i: usize, ty: &str) -> Value {
    macro_rules! num {
        ($t:ty) => {
            row.try_get::<Option<$t>, _>(i)
                .ok()
                .flatten()
                .map_or(Value::Null, |v| Value::Num(Number::new(v.to_string())))
        };
    }
    match ty {
        "BOOL" => row
            .try_get::<Option<bool>, _>(i)
            .ok()
            .flatten()
            .map_or(Value::Null, Value::Bool),
        "INT2" => num!(i16),
        "INT4" => num!(i32),
        "INT8" => num!(i64),
        "FLOAT4" => num!(f32),
        "FLOAT8" => num!(f64),
        "NUMERIC" => row
            .try_get::<Option<sqlx::types::BigDecimal>, _>(i)
            .ok()
            .flatten()
            .map_or(Value::Null, |v| Value::Num(Number::new(v.to_string()))),
        "BYTEA" => row
            .try_get::<Option<Vec<u8>>, _>(i)
            .ok()
            .flatten()
            .map_or(Value::Null, Value::Bytes),
        "UUID" => str_value(
            row.try_get::<Option<sqlx::types::Uuid>, _>(i)
                .ok()
                .flatten(),
        ),
        "JSON" | "JSONB" => row
            .try_get::<Option<serde_json::Value>, _>(i)
            .ok()
            .flatten()
            .map_or(Value::Null, json_to_value),
        "TIMESTAMP" => str_value(
            row.try_get::<Option<sqlx::types::chrono::NaiveDateTime>, _>(i)
                .ok()
                .flatten(),
        ),
        "TIMESTAMPTZ" => str_value(
            row.try_get::<Option<sqlx::types::chrono::DateTime<sqlx::types::chrono::Utc>>, _>(i)
                .ok()
                .flatten(),
        ),
        "DATE" => str_value(
            row.try_get::<Option<sqlx::types::chrono::NaiveDate>, _>(i)
                .ok()
                .flatten(),
        ),
        "TIME" => str_value(
            row.try_get::<Option<sqlx::types::chrono::NaiveTime>, _>(i)
                .ok()
                .flatten(),
        ),
        _ => pg_text_fallback(row, i),
    }
}

fn pg_text_fallback(row: &PgRow, i: usize) -> Value {
    row.try_get::<Option<String>, _>(i)
        .ok()
        .flatten()
        .map_or(Value::Null, Value::Str)
}

pub fn mysql_row_to_item(row: &MySqlRow) -> Item {
    let mut item = Item::with_capacity(row.columns().len());
    for (idx, col) in row.columns().iter().enumerate() {
        let ty = col.type_info().name().to_ascii_uppercase();
        item.insert(col.name().to_string(), mysql_decode(row, idx, &ty));
    }
    item
}

fn mysql_decode(row: &MySqlRow, i: usize, ty: &str) -> Value {
    let int_value = || {
        // MySQL integer types decode into i64; unsigned ones may need u64.
        if let Ok(Some(v)) = row.try_get::<Option<i64>, _>(i) {
            Some(Value::Num(Number::new(v.to_string())))
        } else if let Ok(Some(v)) = row.try_get::<Option<u64>, _>(i) {
            Some(Value::Num(Number::new(v.to_string())))
        } else {
            None
        }
    };
    match ty {
        "BOOL" | "BOOLEAN" | "TINYINT" | "SMALLINT" | "MEDIUMINT" | "INT" | "INTEGER"
        | "BIGINT" => int_value().unwrap_or(Value::Null),
        "FLOAT" => row
            .try_get::<Option<f32>, _>(i)
            .ok()
            .flatten()
            .map_or(Value::Null, |v| Value::Num(Number::new(v.to_string()))),
        "DOUBLE" => row
            .try_get::<Option<f64>, _>(i)
            .ok()
            .flatten()
            .map_or(Value::Null, |v| Value::Num(Number::new(v.to_string()))),
        "DECIMAL" | "NUMERIC" => row
            .try_get::<Option<sqlx::types::BigDecimal>, _>(i)
            .ok()
            .flatten()
            .map_or(Value::Null, |v| Value::Num(Number::new(v.to_string()))),
        "BLOB" | "TINYBLOB" | "MEDIUMBLOB" | "LONGBLOB" | "VARBINARY" | "BINARY" => row
            .try_get::<Option<Vec<u8>>, _>(i)
            .ok()
            .flatten()
            .map_or(Value::Null, Value::Bytes),
        "JSON" => row
            .try_get::<Option<serde_json::Value>, _>(i)
            .ok()
            .flatten()
            .map_or(Value::Null, json_to_value),
        "DATETIME" | "TIMESTAMP" => str_value(
            row.try_get::<Option<sqlx::types::chrono::NaiveDateTime>, _>(i)
                .ok()
                .flatten(),
        ),
        "DATE" => str_value(
            row.try_get::<Option<sqlx::types::chrono::NaiveDate>, _>(i)
                .ok()
                .flatten(),
        ),
        "TIME" => str_value(
            row.try_get::<Option<sqlx::types::chrono::NaiveTime>, _>(i)
                .ok()
                .flatten(),
        ),
        _ => row
            .try_get::<Option<String>, _>(i)
            .ok()
            .flatten()
            .map_or(Value::Null, Value::Str),
    }
}

fn str_value<T: ToString>(value: Option<T>) -> Value {
    value.map_or(Value::Null, |v| Value::Str(v.to_string()))
}

fn json_to_value(json: serde_json::Value) -> Value {
    match json {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(b) => Value::Bool(b),
        serde_json::Value::Number(n) => Value::Num(Number::new(n.to_string())),
        serde_json::Value::String(s) => Value::Str(s),
        serde_json::Value::Array(a) => Value::List(a.into_iter().map(json_to_value).collect()),
        serde_json::Value::Object(o) => {
            Value::Map(o.into_iter().map(|(k, v)| (k, json_to_value(v))).collect())
        }
    }
}

// ---- Value -> bound parameter ----

/// Bind a neutral value onto a Postgres query.
pub fn bind_pg<'q>(
    query: Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>,
    value: &Value,
) -> Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments> {
    match value {
        Value::Null => query.bind(Option::<String>::None),
        Value::Bool(b) => query.bind(*b),
        Value::Str(s) => query.bind(s.clone()),
        Value::Num(n) => bind_number_pg(query, n),
        Value::Bytes(b) => query.bind(b.clone()),
        other => query.bind(stringify_value(other)),
    }
}

fn bind_number_pg<'q>(
    query: Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>,
    n: &Number,
) -> Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments> {
    if let Some(i) = n.as_i64() {
        query.bind(i)
    } else if let Some(f) = n.as_f64() {
        query.bind(f)
    } else {
        query.bind(n.as_str().to_string())
    }
}

/// Bind a neutral value onto a MySQL query.
pub fn bind_mysql<'q>(
    query: Query<'q, sqlx::MySql, sqlx::mysql::MySqlArguments>,
    value: &Value,
) -> Query<'q, sqlx::MySql, sqlx::mysql::MySqlArguments> {
    match value {
        Value::Null => query.bind(Option::<String>::None),
        Value::Bool(b) => query.bind(*b),
        Value::Str(s) => query.bind(s.clone()),
        Value::Num(n) => bind_number_mysql(query, n),
        Value::Bytes(b) => query.bind(b.clone()),
        other => query.bind(stringify_value(other)),
    }
}

fn bind_number_mysql<'q>(
    query: Query<'q, sqlx::MySql, sqlx::mysql::MySqlArguments>,
    n: &Number,
) -> Query<'q, sqlx::MySql, sqlx::mysql::MySqlArguments> {
    if let Some(i) = n.as_i64() {
        query.bind(i)
    } else if let Some(f) = n.as_f64() {
        query.bind(f)
    } else {
        query.bind(n.as_str().to_string())
    }
}

/// Render a non-scalar value as a string. Containers (lists/maps/sets) become
/// JSON text so they round-trip into `json`/`jsonb` columns; scalars render
/// plainly.
fn stringify_value(value: &Value) -> String {
    match value {
        Value::Str(s) => s.clone(),
        Value::Num(n) => n.as_str().to_string(),
        Value::Bool(b) => b.to_string(),
        _ => value_to_json(value).to_string(),
    }
}

/// Convert a neutral value into a `serde_json::Value` (used to render container
/// values as JSON text for `json`/`jsonb` columns).
fn value_to_json(value: &Value) -> serde_json::Value {
    use serde_json::Value as J;
    match value {
        Value::Null => J::Null,
        Value::Bool(b) => J::Bool(*b),
        Value::Str(s) => J::String(s.clone()),
        Value::Num(n) => {
            serde_json::from_str(n.as_str()).unwrap_or_else(|_| J::String(n.as_str().to_string()))
        }
        Value::Bytes(b) => J::String(base64_encode(b)),
        Value::List(items) => J::Array(items.iter().map(value_to_json).collect()),
        Value::Map(map) => J::Object(
            map.iter()
                .map(|(k, v)| (k.clone(), value_to_json(v)))
                .collect(),
        ),
        Value::StringSet(items) => J::Array(items.iter().cloned().map(J::String).collect()),
        Value::NumberSet(items) => J::Array(
            items
                .iter()
                .map(|n| {
                    serde_json::from_str(n.as_str())
                        .unwrap_or_else(|_| J::String(n.as_str().to_string()))
                })
                .collect(),
        ),
        Value::BytesSet(items) => {
            J::Array(items.iter().map(|b| J::String(base64_encode(b))).collect())
        }
    }
}

fn base64_encode(bytes: &[u8]) -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

#[cfg(test)]
mod tests {
    use super::json_to_value;
    use crate::core::value::{Number, Value};

    #[test]
    fn json_scalars_map_to_neutral_values() {
        assert_eq!(json_to_value(serde_json::json!(null)), Value::Null);
        assert_eq!(json_to_value(serde_json::json!(true)), Value::Bool(true));
        assert_eq!(
            json_to_value(serde_json::json!(42)),
            Value::Num(Number::from(42))
        );
        assert_eq!(
            json_to_value(serde_json::json!("hi")),
            Value::Str("hi".to_string())
        );
    }

    #[test]
    fn json_containers_map_recursively() {
        let value = json_to_value(serde_json::json!({"a": [1, "b"], "c": null}));
        let Value::Map(map) = value else {
            panic!("expected a map");
        };
        assert_eq!(
            map.get("a"),
            Some(&Value::List(vec![
                Value::Num(Number::from(1)),
                Value::Str("b".to_string()),
            ]))
        );
        assert_eq!(map.get("c"), Some(&Value::Null));
    }
}
