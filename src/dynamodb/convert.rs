//! Conversion between DynamoDB's `AttributeValue` and the neutral
//! [`crate::core::value::Value`].
//!
//! This is the DynamoDB backend's value boundary: every `AttributeValue` that
//! enters the rest of the app is converted to a neutral `Value` here, and every
//! `Value` headed for the SDK is converted back. The mapping is 1:1 and
//! lossless — `Value` is a strict superset of `AttributeValue`'s type system.

use std::collections::HashMap;

use aws_sdk_dynamodb::{primitives::Blob, types::AttributeValue};

use crate::core::value::{Item, Number, Value};

/// Convert a single DynamoDB attribute into a neutral [`Value`].
pub fn attribute_value_to_value(attr: &AttributeValue) -> Value {
    match attr {
        AttributeValue::S(s) => Value::Str(s.clone()),
        AttributeValue::N(n) => Value::Num(Number::new(n.clone())),
        AttributeValue::Bool(b) => Value::Bool(*b),
        AttributeValue::Null(_) => Value::Null,
        AttributeValue::B(blob) => Value::Bytes(blob.as_ref().to_vec()),
        AttributeValue::L(list) => Value::List(list.iter().map(attribute_value_to_value).collect()),
        AttributeValue::M(map) => Value::Map(item_from_attribute_map(map)),
        AttributeValue::Ss(set) => Value::StringSet(set.clone()),
        AttributeValue::Ns(set) => Value::NumberSet(set.iter().map(Number::new).collect()),
        AttributeValue::Bs(set) => {
            Value::BytesSet(set.iter().map(|blob| blob.as_ref().to_vec()).collect())
        }
        // `AttributeValue` is `#[non_exhaustive]`; unknown future variants have
        // no neutral representation. This is unreachable for the variants the
        // SDK emits today.
        _ => Value::Null,
    }
}

/// Convert a neutral [`Value`] into a DynamoDB attribute.
pub fn value_to_attribute_value(value: &Value) -> AttributeValue {
    match value {
        Value::Null => AttributeValue::Null(true),
        Value::Bool(b) => AttributeValue::Bool(*b),
        Value::Str(s) => AttributeValue::S(s.clone()),
        Value::Num(n) => AttributeValue::N(n.as_str().to_string()),
        Value::Bytes(bytes) => AttributeValue::B(Blob::new(bytes.clone())),
        Value::List(list) => AttributeValue::L(list.iter().map(value_to_attribute_value).collect()),
        Value::Map(item) => AttributeValue::M(attribute_map_from_item(item)),
        Value::StringSet(set) => AttributeValue::Ss(set.clone()),
        Value::NumberSet(set) => {
            AttributeValue::Ns(set.iter().map(|n| n.as_str().to_string()).collect())
        }
        Value::BytesSet(set) => {
            AttributeValue::Bs(set.iter().map(|bytes| Blob::new(bytes.clone())).collect())
        }
    }
}

/// Convert a DynamoDB item (attribute map) into a neutral [`Item`].
pub fn item_from_attribute_map(map: &HashMap<String, AttributeValue>) -> Item {
    map.iter()
        .map(|(key, value)| (key.clone(), attribute_value_to_value(value)))
        .collect()
}

/// Convert a neutral [`Item`] into a DynamoDB item (attribute map).
pub fn attribute_map_from_item(item: &Item) -> HashMap<String, AttributeValue> {
    item.iter()
        .map(|(key, value)| (key.clone(), value_to_attribute_value(value)))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn attr_map(entries: Vec<(&str, AttributeValue)>) -> HashMap<String, AttributeValue> {
        entries
            .into_iter()
            .map(|(key, value)| (key.to_string(), value))
            .collect()
    }

    #[test]
    fn scalars_round_trip() {
        let item = attr_map(vec![
            ("string", AttributeValue::S("hello".to_string())),
            ("bool", AttributeValue::Bool(true)),
            ("null", AttributeValue::Null(true)),
            ("int", AttributeValue::N("42".to_string())),
            ("float", AttributeValue::N("3.14159265358979".to_string())),
        ]);

        let neutral = item_from_attribute_map(&item);
        let back = attribute_map_from_item(&neutral);
        assert_eq!(back, item);
    }

    #[test]
    fn number_precision_is_preserved() {
        // A 38-digit value would be mangled by an f64 round trip.
        let precise = "123456789012345678901234567890.12345678";
        let value = attribute_value_to_value(&AttributeValue::N(precise.to_string()));
        assert_eq!(value, Value::Num(Number::new(precise)));
        assert_eq!(
            value_to_attribute_value(&value),
            AttributeValue::N(precise.to_string())
        );
    }

    #[test]
    fn sets_round_trip() {
        let item = attr_map(vec![
            (
                "tags",
                AttributeValue::Ss(vec!["a".to_string(), "b".to_string()]),
            ),
            (
                "scores",
                AttributeValue::Ns(vec!["42".to_string(), "3.14".to_string()]),
            ),
            (
                "blobs",
                AttributeValue::Bs(vec![Blob::new([0_u8]), Blob::new([255_u8, 1])]),
            ),
        ]);

        let neutral = item_from_attribute_map(&item);
        assert_eq!(back_keys(&neutral), 3);
        assert_eq!(attribute_map_from_item(&neutral), item);
    }

    #[test]
    fn binary_round_trips() {
        let value = attribute_value_to_value(&AttributeValue::B(Blob::new([1_u8, 2, 3])));
        assert_eq!(value, Value::Bytes(vec![1, 2, 3]));
        assert_eq!(
            value_to_attribute_value(&value),
            AttributeValue::B(Blob::new([1_u8, 2, 3]))
        );
    }

    #[test]
    fn nested_structures_round_trip() {
        let item = attr_map(vec![
            (
                "list",
                AttributeValue::L(vec![
                    AttributeValue::Bool(false),
                    AttributeValue::N("1".to_string()),
                    AttributeValue::M(attr_map(vec![("nested", AttributeValue::Null(true))])),
                ]),
            ),
            (
                "map",
                AttributeValue::M(attr_map(vec![(
                    "inner",
                    AttributeValue::S("value".to_string()),
                )])),
            ),
        ]);

        let neutral = item_from_attribute_map(&item);
        assert_eq!(attribute_map_from_item(&neutral), item);
    }

    fn back_keys(item: &Item) -> usize {
        item.len()
    }
}
