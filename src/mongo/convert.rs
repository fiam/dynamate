//! Conversion between MongoDB BSON and the neutral [`Value`] / [`Item`] model.
//!
//! This is the MongoDB backend's value boundary. The mapping is lossless for the
//! common types; a few BSON-only types (ObjectId, DateTime, …) are rendered as
//! strings for display (see the per-arm notes), and the neutral set types have
//! no BSON analogue so they collapse to arrays.

use mongodb::bson::{Binary, Bson, Document, spec::BinarySubtype};

use crate::core::value::{Item, Number, Value};

/// Convert a neutral [`Value`] into BSON.
pub fn value_to_bson(value: &Value) -> Bson {
    match value {
        Value::Null => Bson::Null,
        Value::Bool(b) => Bson::Boolean(*b),
        Value::Str(s) => Bson::String(s.clone()),
        Value::Num(n) => number_to_bson(n),
        Value::Bytes(bytes) => Bson::Binary(Binary {
            subtype: BinarySubtype::Generic,
            bytes: bytes.clone(),
        }),
        Value::List(list) => Bson::Array(list.iter().map(value_to_bson).collect()),
        Value::Map(item) => Bson::Document(item_to_document(item)),
        // MongoDB has no set types; store as arrays.
        Value::StringSet(set) => Bson::Array(set.iter().map(|s| Bson::String(s.clone())).collect()),
        Value::NumberSet(set) => Bson::Array(set.iter().map(number_to_bson).collect()),
        Value::BytesSet(set) => Bson::Array(
            set.iter()
                .map(|bytes| {
                    Bson::Binary(Binary {
                        subtype: BinarySubtype::Generic,
                        bytes: bytes.clone(),
                    })
                })
                .collect(),
        ),
    }
}

fn number_to_bson(n: &Number) -> Bson {
    let text = n.as_str();
    if let Ok(i) = text.parse::<i64>() {
        Bson::Int64(i)
    } else if let Ok(f) = text.parse::<f64>() {
        Bson::Double(f)
    } else {
        // Not representable as a BSON number; keep the text.
        Bson::String(text.to_string())
    }
}

/// Convert a BSON value into a neutral [`Value`].
pub fn bson_to_value(bson: &Bson) -> Value {
    match bson {
        Bson::Null | Bson::Undefined => Value::Null,
        Bson::Boolean(b) => Value::Bool(*b),
        Bson::String(s) => Value::Str(s.clone()),
        Bson::Int32(i) => Value::Num(Number::new(i.to_string())),
        Bson::Int64(i) => Value::Num(Number::new(i.to_string())),
        Bson::Double(f) => Value::Num(Number::new(format_f64(*f))),
        Bson::Decimal128(d) => Value::Num(Number::new(d.to_string())),
        Bson::Array(list) => Value::List(list.iter().map(bson_to_value).collect()),
        Bson::Document(doc) => Value::Map(document_to_item(doc)),
        Bson::Binary(bin) => Value::Bytes(bin.bytes.clone()),
        // BSON-only types: render for display (write-back becomes a string).
        Bson::ObjectId(oid) => Value::Str(oid.to_hex()),
        Bson::DateTime(dt) => Value::Str(
            dt.try_to_rfc3339_string()
                .unwrap_or_else(|_| dt.to_string()),
        ),
        other => Value::Str(format!("{other}")),
    }
}

/// Convert a neutral [`Item`] into a BSON document (insertion order preserved).
pub fn item_to_document(item: &Item) -> Document {
    let mut doc = Document::new();
    for (key, value) in item {
        doc.insert(key.clone(), value_to_bson(value));
    }
    doc
}

/// Convert a BSON document into a neutral [`Item`].
pub fn document_to_item(doc: &Document) -> Item {
    doc.iter()
        .map(|(key, value)| (key.clone(), bson_to_value(value)))
        .collect()
}

fn format_f64(f: f64) -> String {
    if f.fract() == 0.0 && f.is_finite() {
        format!("{f:.0}")
    } else {
        f.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn item(entries: Vec<(&str, Value)>) -> Item {
        entries
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect()
    }

    #[test]
    fn scalars_round_trip() {
        let value = item(vec![
            ("s", Value::Str("hi".to_string())),
            ("b", Value::Bool(true)),
            ("n", Value::Null),
            ("i", Value::Num(Number::new("42"))),
        ]);
        let doc = item_to_document(&value);
        assert_eq!(document_to_item(&doc), value);
    }

    #[test]
    fn integer_and_float_distinguished() {
        assert!(matches!(
            value_to_bson(&Value::Num(Number::new("42"))),
            Bson::Int64(42)
        ));
        assert!(matches!(
            value_to_bson(&Value::Num(Number::new("3.5"))),
            Bson::Double(_)
        ));
    }

    #[test]
    fn binary_round_trips() {
        let v = Value::Bytes(vec![1, 2, 3]);
        assert_eq!(bson_to_value(&value_to_bson(&v)), v);
    }

    #[test]
    fn nested_document_round_trips() {
        let value = item(vec![
            (
                "list",
                Value::List(vec![Value::Num(Number::new("1")), Value::Bool(false)]),
            ),
            (
                "map",
                Value::Map(item(vec![("inner", Value::Str("x".to_string()))])),
            ),
        ]);
        let doc = item_to_document(&value);
        assert_eq!(document_to_item(&doc), value);
    }

    #[test]
    fn object_id_renders_as_hex_string() {
        let oid = mongodb::bson::oid::ObjectId::new();
        assert_eq!(
            bson_to_value(&Bson::ObjectId(oid)),
            Value::Str(oid.to_hex())
        );
    }
}
