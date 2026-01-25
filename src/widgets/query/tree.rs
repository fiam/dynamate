use aws_sdk_dynamodb::types::AttributeValue;
use serde_json::Value;

use dynamate::dynamodb::json;

pub fn item_to_lines(item: &std::collections::HashMap<String, AttributeValue>) -> Vec<String> {
    let value = match json::to_json(item) {
        Ok(value) => value,
        Err(err) => {
            return vec![format!("Failed to render item: {err}")];
        }
    };

    let mut lines = Vec::new();
    render_value(&value, 0, &mut lines);
    if lines.is_empty() {
        lines.push("(empty item)".to_string());
    }
    lines
}

fn render_value(value: &Value, indent: usize, lines: &mut Vec<String>) {
    match value {
        Value::Object(map) => {
            if map.is_empty() {
                lines.push(format!("{}{}", indent_prefix(indent), "{}"));
                return;
            }

            let mut keys: Vec<_> = map.keys().collect();
            keys.sort();
            for key in keys {
                let child = &map[key];
                if is_scalar(child) {
                    lines.push(format!(
                        "{}{}: {}",
                        indent_prefix(indent),
                        key,
                        scalar_text(child)
                    ));
                } else {
                    lines.push(format!("{}{}:", indent_prefix(indent), key));
                    render_value(child, indent + 2, lines);
                }
            }
        }
        Value::Array(values) => {
            if values.is_empty() {
                lines.push(format!("{}[]", indent_prefix(indent)));
                return;
            }

            for value in values {
                if is_scalar(value) {
                    lines.push(format!("{}- {}", indent_prefix(indent), scalar_text(value)));
                } else {
                    lines.push(format!("{}-", indent_prefix(indent)));
                    render_value(value, indent + 2, lines);
                }
            }
        }
        _ => {
            lines.push(format!("{}{}", indent_prefix(indent), scalar_text(value)));
        }
    }
}

fn is_scalar(value: &Value) -> bool {
    matches!(
        value,
        Value::String(_) | Value::Number(_) | Value::Bool(_) | Value::Null
    )
}

fn scalar_text(value: &Value) -> String {
    match value {
        Value::String(text) => format!("\"{}\"", text),
        Value::Number(number) => number.to_string(),
        Value::Bool(value) => value.to_string(),
        Value::Null => "null".to_string(),
        _ => "".to_string(),
    }
}

fn indent_prefix(indent: usize) -> String {
    " ".repeat(indent)
}
