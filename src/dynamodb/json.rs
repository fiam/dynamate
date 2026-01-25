use std::{collections::HashMap, fmt, str::FromStr};

use aws_sdk_dynamodb::types::AttributeValue;
use serde_json::{Map, Number, Value};

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
            JsonConversionError::InvalidNumber { value } => {
                write!(f, "invalid DynamoDB number: {value}")
            }
            JsonConversionError::InvalidStructure { message } => {
                write!(f, "invalid JSON structure: {message}")
            }
            JsonConversionError::UnsupportedType { attribute_type } => {
                write!(f, "unsupported DynamoDB attribute type: {attribute_type}")
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

pub fn to_json(item: &HashMap<String, AttributeValue>) -> Result<Value> {
    let mut json_map = Map::with_capacity(item.len());

    for (key, attr_value) in item {
        json_map.insert(key.clone(), to_json_value(attr_value)?);
    }

    Ok(Value::Object(json_map))
}

pub fn to_json_string(item: &HashMap<String, AttributeValue>) -> Result<String> {
    let json_value = to_json(item)?;
    // Serializing a pre-built Value into a string should not fail in practice, but we
    // still propagate the error to avoid hiding potential issues.
    serde_json::to_string_pretty(&json_value)
        .map_err(|err| JsonConversionError::SerializationError(err.to_string()))
}

pub fn from_json_string(input: &str) -> Result<HashMap<String, AttributeValue>> {
    let value: Value = serde_json::from_str(input)
        .map_err(|err| JsonConversionError::DeserializationError(err.to_string()))?;
    from_json(&value)
}

pub fn from_json(value: &Value) -> Result<HashMap<String, AttributeValue>> {
    let Value::Object(map) = value else {
        return Err(JsonConversionError::InvalidStructure {
            message: "expected a JSON object at the top level".to_string(),
        });
    };

    let mut result = HashMap::with_capacity(map.len());
    for (key, value) in map {
        result.insert(key.clone(), from_json_value(value)?);
    }
    Ok(result)
}

fn from_json_value(value: &Value) -> Result<AttributeValue> {
    match value {
        Value::String(text) => Ok(AttributeValue::S(text.clone())),
        Value::Number(number) => Ok(AttributeValue::N(number.to_string())),
        Value::Bool(value) => Ok(AttributeValue::Bool(*value)),
        Value::Null => Ok(AttributeValue::Null(true)),
        Value::Array(values) => {
            let mut list = Vec::with_capacity(values.len());
            for value in values {
                list.push(from_json_value(value)?);
            }
            Ok(AttributeValue::L(list))
        }
        Value::Object(map) => {
            let mut result = HashMap::with_capacity(map.len());
            for (key, value) in map {
                result.insert(key.clone(), from_json_value(value)?);
            }
            Ok(AttributeValue::M(result))
        }
    }
}

pub fn to_dynamodb_json_string(item: &HashMap<String, AttributeValue>) -> Result<String> {
    let json_value = to_dynamodb_json(item)?;
    serde_json::to_string_pretty(&json_value)
        .map_err(|err| JsonConversionError::SerializationError(err.to_string()))
}

pub fn to_dynamodb_json(item: &HashMap<String, AttributeValue>) -> Result<Value> {
    let mut json_map = Map::with_capacity(item.len());
    for (key, attr_value) in item {
        json_map.insert(key.clone(), to_dynamodb_json_value(attr_value)?);
    }
    Ok(Value::Object(json_map))
}

pub fn from_dynamodb_json_string(input: &str) -> Result<HashMap<String, AttributeValue>> {
    let value: Value = serde_json::from_str(input)
        .map_err(|err| JsonConversionError::DeserializationError(err.to_string()))?;
    from_dynamodb_json(&value)
}

pub fn from_dynamodb_json(value: &Value) -> Result<HashMap<String, AttributeValue>> {
    let Value::Object(map) = value else {
        return Err(JsonConversionError::InvalidStructure {
            message: "expected a JSON object at the top level".to_string(),
        });
    };

    let mut result = HashMap::with_capacity(map.len());
    for (key, value) in map {
        result.insert(key.clone(), from_dynamodb_json_value(value)?);
    }
    Ok(result)
}

fn to_dynamodb_json_value(value: &AttributeValue) -> Result<Value> {
    match value {
        AttributeValue::S(string_value) => Ok(json_type("S", Value::String(string_value.clone()))),
        AttributeValue::N(number_value) => Ok(json_type("N", Value::String(number_value.clone()))),
        AttributeValue::Bool(value) => Ok(json_type("BOOL", Value::Bool(*value))),
        AttributeValue::Null(_) => Ok(json_type("NULL", Value::Bool(true))),
        AttributeValue::L(list) => {
            let mut array = Vec::with_capacity(list.len());
            for element in list {
                array.push(to_dynamodb_json_value(element)?);
            }
            Ok(json_type("L", Value::Array(array)))
        }
        AttributeValue::M(map) => {
            let mut object = Map::with_capacity(map.len());
            for (key, attribute_value) in map {
                object.insert(key.clone(), to_dynamodb_json_value(attribute_value)?);
            }
            Ok(json_type("M", Value::Object(object)))
        }
        AttributeValue::B(_) => Err(JsonConversionError::UnsupportedType {
            attribute_type: "B".to_string(),
        }),
        AttributeValue::Bs(_) => Err(JsonConversionError::UnsupportedType {
            attribute_type: "BS".to_string(),
        }),
        AttributeValue::Ns(_) => Err(JsonConversionError::UnsupportedType {
            attribute_type: "NS".to_string(),
        }),
        AttributeValue::Ss(_) => Err(JsonConversionError::UnsupportedType {
            attribute_type: "SS".to_string(),
        }),
        _ => Err(JsonConversionError::UnsupportedType {
            attribute_type: "Unknown".to_string(),
        }),
    }
}

fn from_dynamodb_json_value(value: &Value) -> Result<AttributeValue> {
    let Value::Object(map) = value else {
        return Err(JsonConversionError::InvalidStructure {
            message: "expected a DynamoDB JSON attribute object".to_string(),
        });
    };

    if map.len() != 1 {
        return Err(JsonConversionError::InvalidStructure {
            message: "expected a single DynamoDB type key".to_string(),
        });
    }

    let (key, value) = map.iter().next().unwrap();
    match key.as_str() {
        "S" => value
            .as_str()
            .map(|text| AttributeValue::S(text.to_string()))
            .ok_or_else(|| JsonConversionError::InvalidStructure {
                message: "S must be a string".to_string(),
            }),
        "N" => value
            .as_str()
            .map(|text| AttributeValue::N(text.to_string()))
            .ok_or_else(|| JsonConversionError::InvalidStructure {
                message: "N must be a string".to_string(),
            }),
        "BOOL" => value.as_bool().map(AttributeValue::Bool).ok_or_else(|| {
            JsonConversionError::InvalidStructure {
                message: "BOOL must be true/false".to_string(),
            }
        }),
        "NULL" => value
            .as_bool()
            .map(|_| AttributeValue::Null(true))
            .ok_or_else(|| JsonConversionError::InvalidStructure {
                message: "NULL must be true".to_string(),
            }),
        "L" => {
            let Value::Array(values) = value else {
                return Err(JsonConversionError::InvalidStructure {
                    message: "L must be an array".to_string(),
                });
            };
            let mut list = Vec::with_capacity(values.len());
            for value in values {
                list.push(from_dynamodb_json_value(value)?);
            }
            Ok(AttributeValue::L(list))
        }
        "M" => {
            let Value::Object(values) = value else {
                return Err(JsonConversionError::InvalidStructure {
                    message: "M must be an object".to_string(),
                });
            };
            let mut map = HashMap::with_capacity(values.len());
            for (key, value) in values {
                map.insert(key.clone(), from_dynamodb_json_value(value)?);
            }
            Ok(AttributeValue::M(map))
        }
        other => Err(JsonConversionError::UnsupportedType {
            attribute_type: other.to_string(),
        }),
    }
}

fn json_type(key: &str, value: Value) -> Value {
    let mut map = Map::with_capacity(1);
    map.insert(key.to_string(), value);
    Value::Object(map)
}

fn to_json_value(value: &AttributeValue) -> Result<Value> {
    match value {
        AttributeValue::Bool(bool_value) => Ok(Value::Bool(*bool_value)),
        AttributeValue::S(string_value) => Ok(Value::String(string_value.clone())),
        AttributeValue::N(number_value) => {
            let number =
                Number::from_str(number_value).map_err(|_| JsonConversionError::InvalidNumber {
                    value: number_value.clone(),
                })?;
            Ok(Value::Number(number))
        }
        AttributeValue::Null(_) => Ok(Value::Null),
        AttributeValue::L(list) => {
            let mut array = Vec::with_capacity(list.len());
            for element in list {
                array.push(to_json_value(element)?);
            }
            Ok(Value::Array(array))
        }
        AttributeValue::M(map) => {
            let mut object = Map::with_capacity(map.len());
            for (key, attribute_value) in map {
                object.insert(key.clone(), to_json_value(attribute_value)?);
            }
            Ok(Value::Object(object))
        }
        AttributeValue::B(_) => Err(JsonConversionError::UnsupportedType {
            attribute_type: "B".to_string(),
        }),
        AttributeValue::Bs(_) => Err(JsonConversionError::UnsupportedType {
            attribute_type: "BS".to_string(),
        }),
        AttributeValue::Ns(_) => Err(JsonConversionError::UnsupportedType {
            attribute_type: "NS".to_string(),
        }),
        AttributeValue::Ss(_) => Err(JsonConversionError::UnsupportedType {
            attribute_type: "SS".to_string(),
        }),
        _ => Err(JsonConversionError::UnsupportedType {
            attribute_type: "Unknown".to_string(),
        }),
    }
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
    fn converts_basic_scalar_values() {
        let item = attr_map(vec![
            ("string", AttributeValue::S("hello".to_string())),
            ("bool_true", AttributeValue::Bool(true)),
            ("bool_false", AttributeValue::Bool(false)),
            ("null", AttributeValue::Null(true)),
            ("int", AttributeValue::N("42".to_string())),
            ("float", AttributeValue::N(std::f64::consts::PI.to_string())),
        ]);

        let json = to_json(&item).expect("conversion succeeds");
        let obj = json.as_object().expect("expected JSON object");

        assert_eq!(
            obj.get("string").unwrap(),
            &Value::String("hello".to_string())
        );
        assert_eq!(obj.get("bool_true").unwrap(), &Value::Bool(true));
        assert_eq!(obj.get("bool_false").unwrap(), &Value::Bool(false));
        assert_eq!(obj.get("null").unwrap(), &Value::Null);
        assert_eq!(obj.get("int").unwrap(), &Value::Number(Number::from(42)));
        assert_eq!(
            obj.get("float").unwrap(),
            &Value::Number(Number::from_f64(std::f64::consts::PI).unwrap())
        );
    }

    #[test]
    fn converts_nested_structures() {
        let nested = AttributeValue::M(attr_map(vec![(
            "inner",
            AttributeValue::S("value".to_string()),
        )]));
        let item = attr_map(vec![
            (
                "list",
                AttributeValue::L(vec![
                    AttributeValue::Bool(false),
                    AttributeValue::N("1".to_string()),
                    AttributeValue::M(attr_map(vec![("nested", AttributeValue::Null(true))])),
                ]),
            ),
            ("map", nested),
        ]);

        let json = to_json(&item).expect("conversion succeeds");
        let obj = json.as_object().expect("expected JSON object");

        let list = obj.get("list").unwrap().as_array().expect("expected array");
        assert_eq!(list[0], Value::Bool(false));
        assert_eq!(list[1], Value::Number(Number::from(1)));
        assert_eq!(
            list[2]
                .as_object()
                .unwrap()
                .get("nested")
                .expect("nested key"),
            &Value::Null
        );

        let map = obj.get("map").unwrap().as_object().expect("expected map");
        assert_eq!(
            map.get("inner").unwrap(),
            &Value::String("value".to_string())
        );
    }

    #[test]
    fn number_conversion_error_is_reported() {
        let item = attr_map(vec![("bad_number", AttributeValue::N("nope".to_string()))]);

        let error = to_json(&item).unwrap_err();
        assert_eq!(
            error,
            JsonConversionError::InvalidNumber {
                value: "nope".to_string()
            }
        );
    }

    #[test]
    fn unsupported_types_return_error() {
        let item = attr_map(vec![("set", AttributeValue::Ss(vec!["a".to_string()]))]);

        let error = to_json(&item).unwrap_err();
        assert_eq!(
            error,
            JsonConversionError::UnsupportedType {
                attribute_type: "SS".to_string()
            }
        );
    }

    #[test]
    fn produces_pretty_json_string() {
        let item = attr_map(vec![
            ("value", AttributeValue::N("5".to_string())),
            ("text", AttributeValue::S("hi".to_string())),
        ]);

        let json_string = to_json_string(&item).expect("string conversion succeeds");
        assert!(json_string.contains("\"value\": 5"));
        assert!(json_string.contains("\"text\": \"hi\""));
    }
}
