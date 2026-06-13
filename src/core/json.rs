//! Conversion between the neutral [`Value`] / [`Item`] model and
//! `serde_json::Value`.
//!
//! Two flavors are supported, matching what the item editor and tree view use:
//! - **standard** JSON — human-readable, lossy: binary and set types have no
//!   representation and produce [`JsonConversionError::UnsupportedType`].
//! - **typed** JSON — the tagged `{ "S": .. }` / `{ "N": .. }` encoding that
//!   round-trips every [`Value`] variant losslessly. The tag names happen to
//!   match DynamoDB's wire format, but here they are simply the canonical typed
//!   serialization of a neutral [`Value`].

use std::{fmt, str::FromStr};

use base64::{Engine, engine::general_purpose::STANDARD as BASE64};
use serde_json::{Map, Number as JsonNumber, Value as Json};

use super::value::{Item, Number, Value};

#[derive(Debug, PartialEq, Eq)]
pub enum JsonConversionError {
    InvalidNumber { value: String },
    InvalidStructure { message: String },
    UnsupportedType { attribute_type: String },
    SerializationError(String),
    DeserializationError(String),
}

impl fmt::Display for JsonConversionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            JsonConversionError::InvalidNumber { value } => write!(f, "invalid number: {value}"),
            JsonConversionError::InvalidStructure { message } => {
                write!(f, "invalid JSON structure: {message}")
            }
            JsonConversionError::UnsupportedType { attribute_type } => {
                write!(f, "unsupported value type: {attribute_type}")
            }
            JsonConversionError::SerializationError(inner) => {
                write!(f, "failed to serialize JSON value: {inner}")
            }
            JsonConversionError::DeserializationError(inner) => {
                write!(f, "failed to parse JSON value: {inner}")
            }
        }
    }
}

impl std::error::Error for JsonConversionError {}

pub type Result<T> = std::result::Result<T, JsonConversionError>;

// ---- standard (lossy, human-readable) format ----

pub fn item_to_json(item: &Item) -> Result<Json> {
    let mut map = Map::with_capacity(item.len());
    for (key, value) in item {
        map.insert(key.clone(), value_to_json(value)?);
    }
    Ok(Json::Object(map))
}

pub fn item_to_json_string(item: &Item) -> Result<String> {
    serde_json::to_string_pretty(&item_to_json(item)?)
        .map_err(|err| JsonConversionError::SerializationError(err.to_string()))
}

pub fn item_from_json(value: &Json) -> Result<Item> {
    let Json::Object(map) = value else {
        return Err(JsonConversionError::InvalidStructure {
            message: "expected a JSON object at the top level".to_string(),
        });
    };
    let mut item = Item::with_capacity(map.len());
    for (key, value) in map {
        item.insert(key.clone(), value_from_json(value)?);
    }
    Ok(item)
}

pub fn item_from_json_string(input: &str) -> Result<Item> {
    let value: Json = serde_json::from_str(input)
        .map_err(|err| JsonConversionError::DeserializationError(err.to_string()))?;
    item_from_json(&value)
}

fn value_to_json(value: &Value) -> Result<Json> {
    match value {
        Value::Bool(b) => Ok(Json::Bool(*b)),
        Value::Str(s) => Ok(Json::String(s.clone())),
        Value::Num(n) => JsonNumber::from_str(n.as_str())
            .map(Json::Number)
            .map_err(|_| JsonConversionError::InvalidNumber {
                value: n.as_str().to_string(),
            }),
        Value::Null => Ok(Json::Null),
        Value::List(list) => {
            let mut array = Vec::with_capacity(list.len());
            for element in list {
                array.push(value_to_json(element)?);
            }
            Ok(Json::Array(array))
        }
        Value::Map(map) => item_to_json(map),
        Value::Bytes(_) => Err(unsupported("binary")),
        Value::StringSet(_) => Err(unsupported("string-set")),
        Value::NumberSet(_) => Err(unsupported("number-set")),
        Value::BytesSet(_) => Err(unsupported("binary-set")),
    }
}

fn value_from_json(value: &Json) -> Result<Value> {
    match value {
        Json::String(text) => Ok(Value::Str(text.clone())),
        Json::Number(number) => Ok(Value::Num(Number::new(number.to_string()))),
        Json::Bool(b) => Ok(Value::Bool(*b)),
        Json::Null => Ok(Value::Null),
        Json::Array(values) => {
            let mut list = Vec::with_capacity(values.len());
            for value in values {
                list.push(value_from_json(value)?);
            }
            Ok(Value::List(list))
        }
        Json::Object(map) => {
            let mut item = Item::with_capacity(map.len());
            for (key, value) in map {
                item.insert(key.clone(), value_from_json(value)?);
            }
            Ok(Value::Map(item))
        }
    }
}

// ---- typed (lossless, tagged) format ----

pub fn item_to_typed_json(item: &Item) -> Result<Json> {
    let mut map = Map::with_capacity(item.len());
    for (key, value) in item {
        map.insert(key.clone(), value_to_typed_json(value));
    }
    Ok(Json::Object(map))
}

pub fn item_to_typed_json_string(item: &Item) -> Result<String> {
    serde_json::to_string_pretty(&item_to_typed_json(item)?)
        .map_err(|err| JsonConversionError::SerializationError(err.to_string()))
}

pub fn item_from_typed_json(value: &Json) -> Result<Item> {
    let Json::Object(map) = value else {
        return Err(JsonConversionError::InvalidStructure {
            message: "expected a JSON object at the top level".to_string(),
        });
    };
    let mut item = Item::with_capacity(map.len());
    for (key, value) in map {
        item.insert(key.clone(), value_from_typed_json(value)?);
    }
    Ok(item)
}

pub fn item_from_typed_json_string(input: &str) -> Result<Item> {
    let value: Json = serde_json::from_str(input)
        .map_err(|err| JsonConversionError::DeserializationError(err.to_string()))?;
    item_from_typed_json(&value)
}

fn value_to_typed_json(value: &Value) -> Json {
    match value {
        Value::Str(s) => tagged("S", Json::String(s.clone())),
        Value::Num(n) => tagged("N", Json::String(n.as_str().to_string())),
        Value::Bool(b) => tagged("BOOL", Json::Bool(*b)),
        Value::Null => tagged("NULL", Json::Bool(true)),
        Value::List(list) => tagged(
            "L",
            Json::Array(list.iter().map(value_to_typed_json).collect()),
        ),
        Value::Map(map) => {
            let mut object = Map::with_capacity(map.len());
            for (key, value) in map {
                object.insert(key.clone(), value_to_typed_json(value));
            }
            tagged("M", Json::Object(object))
        }
        Value::Bytes(bytes) => tagged("B", Json::String(BASE64.encode(bytes))),
        Value::BytesSet(set) => tagged(
            "BS",
            Json::Array(
                set.iter()
                    .map(|bytes| Json::String(BASE64.encode(bytes)))
                    .collect(),
            ),
        ),
        Value::NumberSet(set) => tagged("NS", string_array(set.iter().map(Number::as_str))),
        Value::StringSet(set) => tagged("SS", string_array(set.iter().map(String::as_str))),
    }
}

fn value_from_typed_json(value: &Json) -> Result<Value> {
    let Json::Object(map) = value else {
        return Err(JsonConversionError::InvalidStructure {
            message: "expected a typed attribute object".to_string(),
        });
    };
    if map.len() != 1 {
        return Err(JsonConversionError::InvalidStructure {
            message: "expected a single type key".to_string(),
        });
    }
    let Some((key, value)) = map.iter().next() else {
        return Err(JsonConversionError::InvalidStructure {
            message: "expected a single type key".to_string(),
        });
    };
    match key.as_str() {
        "S" => value
            .as_str()
            .map(|text| Value::Str(text.to_string()))
            .ok_or_else(|| structure("S must be a string")),
        "N" => value
            .as_str()
            .map(|text| Value::Num(Number::new(text)))
            .ok_or_else(|| structure("N must be a string")),
        "BOOL" => value
            .as_bool()
            .map(Value::Bool)
            .ok_or_else(|| structure("BOOL must be true/false")),
        "NULL" => value
            .as_bool()
            .map(|_| Value::Null)
            .ok_or_else(|| structure("NULL must be true")),
        "B" => parse_blob(value, "B").map(Value::Bytes),
        "BS" => parse_blob_array(value, "BS").map(Value::BytesSet),
        "SS" => parse_string_array(value, "SS").map(Value::StringSet),
        "NS" => parse_string_array(value, "NS")
            .map(|set| Value::NumberSet(set.into_iter().map(Number::new).collect())),
        "L" => {
            let Json::Array(values) = value else {
                return Err(structure("L must be an array"));
            };
            let mut list = Vec::with_capacity(values.len());
            for value in values {
                list.push(value_from_typed_json(value)?);
            }
            Ok(Value::List(list))
        }
        "M" => {
            let Json::Object(values) = value else {
                return Err(structure("M must be an object"));
            };
            let mut item = Item::with_capacity(values.len());
            for (key, value) in values {
                item.insert(key.clone(), value_from_typed_json(value)?);
            }
            Ok(Value::Map(item))
        }
        other => Err(JsonConversionError::UnsupportedType {
            attribute_type: other.to_string(),
        }),
    }
}

// ---- helpers ----

fn unsupported(kind: &str) -> JsonConversionError {
    JsonConversionError::UnsupportedType {
        attribute_type: kind.to_string(),
    }
}

fn structure(message: &str) -> JsonConversionError {
    JsonConversionError::InvalidStructure {
        message: message.to_string(),
    }
}

fn tagged(key: &str, value: Json) -> Json {
    let mut map = Map::with_capacity(1);
    map.insert(key.to_string(), value);
    Json::Object(map)
}

fn string_array<'a>(values: impl Iterator<Item = &'a str>) -> Json {
    Json::Array(values.map(|s| Json::String(s.to_string())).collect())
}

fn parse_string_array(value: &Json, type_name: &str) -> Result<Vec<String>> {
    let Json::Array(values) = value else {
        return Err(structure(&format!(
            "{type_name} must be an array of strings"
        )));
    };
    values
        .iter()
        .map(|value| {
            value
                .as_str()
                .map(str::to_owned)
                .ok_or_else(|| structure(&format!("{type_name} must be an array of strings")))
        })
        .collect()
}

fn parse_blob(value: &Json, type_name: &str) -> Result<Vec<u8>> {
    let encoded = value
        .as_str()
        .ok_or_else(|| structure(&format!("{type_name} must be a base64 string")))?;
    BASE64
        .decode(encoded)
        .map_err(|_| structure(&format!("{type_name} must be a base64 string")))
}

fn parse_blob_array(value: &Json, type_name: &str) -> Result<Vec<Vec<u8>>> {
    let Json::Array(values) = value else {
        return Err(structure(&format!(
            "{type_name} must be an array of base64 strings"
        )));
    };
    values
        .iter()
        .map(|value| parse_blob(value, type_name))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn item(entries: Vec<(&str, Value)>) -> Item {
        entries
            .into_iter()
            .map(|(key, value)| (key.to_string(), value))
            .collect()
    }

    #[test]
    fn standard_scalars() {
        let value = item(vec![
            ("string", Value::Str("hello".to_string())),
            ("bool", Value::Bool(true)),
            ("null", Value::Null),
            ("int", Value::Num(Number::new("42"))),
        ]);
        let json = item_to_json(&value).expect("conversion succeeds");
        assert_eq!(json["string"], json!("hello"));
        assert_eq!(json["bool"], json!(true));
        assert_eq!(json["null"], Json::Null);
        assert_eq!(json["int"], json!(42));
    }

    #[test]
    fn standard_rejects_sets_and_binary() {
        let err = value_to_json(&Value::StringSet(vec!["a".to_string()])).unwrap_err();
        assert_eq!(
            err,
            JsonConversionError::UnsupportedType {
                attribute_type: "string-set".to_string()
            }
        );
        assert!(matches!(
            value_to_json(&Value::Bytes(vec![1])).unwrap_err(),
            JsonConversionError::UnsupportedType { .. }
        ));
    }

    #[test]
    fn standard_rejects_invalid_number() {
        let err = value_to_json(&Value::Num(Number::new("nope"))).unwrap_err();
        assert_eq!(
            err,
            JsonConversionError::InvalidNumber {
                value: "nope".to_string()
            }
        );
    }

    #[test]
    fn typed_sets_round_trip() {
        let value = item(vec![
            (
                "tags",
                Value::StringSet(vec!["a".to_string(), "b".to_string()]),
            ),
            (
                "scores",
                Value::NumberSet(vec![Number::new("42"), Number::new("3.14")]),
            ),
        ]);
        let json = item_to_typed_json(&value).expect("conversion succeeds");
        assert_eq!(
            json,
            json!({ "tags": { "SS": ["a", "b"] }, "scores": { "NS": ["42", "3.14"] } })
        );
        assert_eq!(item_from_typed_json(&json).expect("round-trip"), value);
    }

    #[test]
    fn typed_binary_round_trips() {
        let value = item(vec![
            ("blob", Value::Bytes(vec![1, 2, 3])),
            ("blob_set", Value::BytesSet(vec![vec![0], vec![255, 1]])),
        ]);
        let json = item_to_typed_json(&value).expect("conversion succeeds");
        assert_eq!(
            json,
            json!({ "blob": { "B": "AQID" }, "blob_set": { "BS": ["AA==", "/wE="] } })
        );
        assert_eq!(item_from_typed_json(&json).expect("round-trip"), value);
    }

    #[test]
    fn typed_nested_round_trips() {
        let value = item(vec![(
            "list",
            Value::List(vec![
                Value::Bool(false),
                Value::Num(Number::new("1")),
                Value::Map(item(vec![("nested", Value::Null)])),
            ]),
        )]);
        let json = item_to_typed_json(&value).expect("conversion succeeds");
        assert_eq!(item_from_typed_json(&json).expect("round-trip"), value);
    }
}
