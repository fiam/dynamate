//! DynamoDB-flavored JSON conversion.
//!
//! This is a thin shim over the backend-neutral [`crate::core::json`]: a DynamoDB
//! item is converted to a neutral [`Item`](crate::core::value::Item) via
//! [`convert`](super::convert), then serialized by `core::json`. The public API
//! (functions over `HashMap<String, AttributeValue>` and [`JsonConversionError`])
//! is preserved so existing callers compile unchanged. New code should prefer
//! `core::json` directly.

use std::collections::HashMap;

use aws_sdk_dynamodb::types::AttributeValue;
use serde_json::Value as Json;

use crate::core::json as core_json;
use crate::dynamodb::convert::{attribute_map_from_item, item_from_attribute_map};

pub use crate::core::json::{JsonConversionError, Result};

pub fn to_json(item: &HashMap<String, AttributeValue>) -> Result<Json> {
    core_json::item_to_json(&item_from_attribute_map(item))
}

pub fn to_json_string(item: &HashMap<String, AttributeValue>) -> Result<String> {
    core_json::item_to_json_string(&item_from_attribute_map(item))
}

pub fn from_json(value: &Json) -> Result<HashMap<String, AttributeValue>> {
    core_json::item_from_json(value).map(|item| attribute_map_from_item(&item))
}

pub fn from_json_string(input: &str) -> Result<HashMap<String, AttributeValue>> {
    core_json::item_from_json_string(input).map(|item| attribute_map_from_item(&item))
}

pub fn to_dynamodb_json(item: &HashMap<String, AttributeValue>) -> Result<Json> {
    core_json::item_to_typed_json(&item_from_attribute_map(item))
}

pub fn to_dynamodb_json_string(item: &HashMap<String, AttributeValue>) -> Result<String> {
    core_json::item_to_typed_json_string(&item_from_attribute_map(item))
}

pub fn from_dynamodb_json(value: &Json) -> Result<HashMap<String, AttributeValue>> {
    core_json::item_from_typed_json(value).map(|item| attribute_map_from_item(&item))
}

pub fn from_dynamodb_json_string(input: &str) -> Result<HashMap<String, AttributeValue>> {
    core_json::item_from_typed_json_string(input).map(|item| attribute_map_from_item(&item))
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_sdk_dynamodb::primitives::Blob;
    use serde_json::json;

    fn attr_map(entries: Vec<(&str, AttributeValue)>) -> HashMap<String, AttributeValue> {
        entries
            .into_iter()
            .map(|(key, value)| (key.to_string(), value))
            .collect()
    }

    #[test]
    fn scalars_convert_to_standard_json() {
        let item = attr_map(vec![
            ("string", AttributeValue::S("hello".to_string())),
            ("bool", AttributeValue::Bool(true)),
            ("null", AttributeValue::Null(true)),
            ("int", AttributeValue::N("42".to_string())),
        ]);
        let json = to_json(&item).expect("conversion succeeds");
        assert_eq!(json["string"], json!("hello"));
        assert_eq!(json["bool"], json!(true));
        assert_eq!(json["null"], Json::Null);
        assert_eq!(json["int"], json!(42));
    }

    #[test]
    fn sets_and_binary_round_trip_via_dynamodb_json() {
        let item = attr_map(vec![
            (
                "tags",
                AttributeValue::Ss(vec!["a".to_string(), "b".to_string()]),
            ),
            ("blob", AttributeValue::B(Blob::new([1_u8, 2, 3]))),
        ]);
        let json = to_dynamodb_json(&item).expect("conversion succeeds");
        assert_eq!(from_dynamodb_json(&json).expect("round-trip"), item);
    }

    #[test]
    fn standard_json_rejects_sets() {
        let item = attr_map(vec![("set", AttributeValue::Ss(vec!["a".to_string()]))]);
        assert!(matches!(
            to_json(&item).unwrap_err(),
            JsonConversionError::UnsupportedType { .. }
        ));
    }

    #[test]
    fn pretty_string_contains_values() {
        let item = attr_map(vec![
            ("value", AttributeValue::N("5".to_string())),
            ("text", AttributeValue::S("hi".to_string())),
        ]);
        let json_string = to_json_string(&item).expect("string conversion succeeds");
        assert!(json_string.contains("\"value\": 5"));
        assert!(json_string.contains("\"text\": \"hi\""));
    }
}
