use std::collections::HashSet;

use aws_sdk_dynamodb::types::AttributeValue;
use ratatui::{
    style::{Modifier, Style},
    text::{Line, Span},
};
use serde_json::Value;

use dynamate::dynamodb::json::{self, JsonConversionError};

use crate::widgets::theme::Theme;

pub fn item_to_lines(
    item: &std::collections::HashMap<String, AttributeValue>,
    theme: &Theme,
    key_order: Option<&[String]>,
) -> Vec<Line<'static>> {
    let value = match item_to_json_value(item) {
        Ok(value) => value,
        Err(err) => {
            return vec![Line::from(format!("Failed to render item: {err}"))];
        }
    };

    let mut lines = Vec::new();
    render_value(&value, 0, theme, &mut lines, key_order);
    if lines.is_empty() {
        lines.push(Line::from("(empty item)"));
    }
    lines
}

fn item_to_json_value(
    item: &std::collections::HashMap<String, AttributeValue>,
) -> json::Result<Value> {
    match json::to_json(item) {
        Ok(value) => Ok(value),
        Err(JsonConversionError::UnsupportedType { .. }) => json::to_dynamodb_json(item),
        Err(err) => Err(err),
    }
}

fn render_value(
    value: &Value,
    indent: usize,
    theme: &Theme,
    lines: &mut Vec<Line<'static>>,
    key_order: Option<&[String]>,
) {
    match value {
        Value::Object(map) => {
            if map.is_empty() {
                lines.push(Line::from(vec![
                    indent_span(indent, theme),
                    Span::styled("{}", Style::default().fg(theme.text_muted())),
                ]));
                return;
            }

            let mut keys: Vec<&str> = Vec::new();
            if let Some(order) = key_order {
                let mut seen = HashSet::new();
                for key in order {
                    if map.contains_key(key) {
                        keys.push(key.as_str());
                        seen.insert(key.as_str());
                    }
                }
                let mut remaining: Vec<&str> = map
                    .keys()
                    .map(|key| key.as_str())
                    .filter(|key| !seen.contains(key))
                    .collect();
                remaining.sort();
                keys.extend(remaining);
            } else {
                let mut sorted: Vec<&str> = map.keys().map(|key| key.as_str()).collect();
                sorted.sort();
                keys = sorted;
            }

            for key in keys {
                let Some(child) = map.get(key) else {
                    continue;
                };
                if is_scalar(child) {
                    let line = Line::from(vec![
                        indent_span(indent, theme),
                        Span::styled(
                            key.to_string(),
                            Style::default()
                                .fg(theme.accent())
                                .add_modifier(Modifier::BOLD),
                        ),
                        Span::styled(": ", Style::default().fg(theme.text_muted())),
                        scalar_span(child, theme),
                    ]);
                    lines.push(line);
                } else {
                    let line = Line::from(vec![
                        indent_span(indent, theme),
                        Span::styled(
                            key.to_string(),
                            Style::default()
                                .fg(theme.accent())
                                .add_modifier(Modifier::BOLD),
                        ),
                        Span::styled(":", Style::default().fg(theme.text_muted())),
                    ]);
                    lines.push(line);
                    render_value(child, indent + 2, theme, lines, None);
                }
            }
        }
        Value::Array(values) => {
            if values.is_empty() {
                lines.push(Line::from(vec![
                    indent_span(indent, theme),
                    Span::styled("[]", Style::default().fg(theme.text_muted())),
                ]));
                return;
            }

            for value in values {
                if is_scalar(value) {
                    let line = Line::from(vec![
                        indent_span(indent, theme),
                        Span::styled("- ", Style::default().fg(theme.text_muted())),
                        scalar_span(value, theme),
                    ]);
                    lines.push(line);
                } else {
                    let line = Line::from(vec![
                        indent_span(indent, theme),
                        Span::styled("-", Style::default().fg(theme.text_muted())),
                    ]);
                    lines.push(line);
                    render_value(value, indent + 2, theme, lines, None);
                }
            }
        }
        _ => {
            lines.push(Line::from(vec![
                indent_span(indent, theme),
                scalar_span(value, theme),
            ]));
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

fn scalar_span(value: &Value, theme: &Theme) -> Span<'static> {
    match value {
        Value::String(text) => Span::styled(
            format!("\"{}\"", text),
            Style::default().fg(theme.accent_alt()),
        ),
        Value::Number(number) => {
            Span::styled(number.to_string(), Style::default().fg(theme.warning()))
        }
        Value::Bool(value) => {
            Span::styled(value.to_string(), Style::default().fg(theme.text_muted()))
        }
        Value::Null => Span::styled("null", Style::default().fg(theme.text_muted())),
        _ => Span::styled(scalar_text(value), Style::default().fg(theme.text())),
    }
}

fn indent_span(indent: usize, theme: &Theme) -> Span<'static> {
    if indent == 0 {
        return Span::raw("");
    }
    Span::styled(" ".repeat(indent), Style::default().fg(theme.text_muted()))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use aws_sdk_dynamodb::types::AttributeValue;

    use super::item_to_lines;
    use crate::widgets::theme::Theme;

    #[test]
    fn falls_back_to_dynamodb_json_for_string_sets() {
        let item = HashMap::from([
            ("pk".to_string(), AttributeValue::S("item#1".to_string())),
            (
                "tags".to_string(),
                AttributeValue::Ss(vec!["alpha".to_string(), "beta".to_string()]),
            ),
        ]);
        let key_order = ["pk".to_string(), "tags".to_string()];

        let lines = item_to_lines(&item, &Theme::dark(), Some(&key_order));
        let rendered: Vec<String> = lines
            .into_iter()
            .map(|line| {
                line.spans
                    .into_iter()
                    .map(|span| span.content.to_string())
                    .collect()
            })
            .collect();

        assert_eq!(
            rendered,
            vec![
                "pk:".to_string(),
                "  S: \"item#1\"".to_string(),
                "tags:".to_string(),
                "  SS:".to_string(),
                "    - \"alpha\"".to_string(),
                "    - \"beta\"".to_string(),
            ]
        );
    }
}
