use aws_sdk_dynamodb::types::AttributeValue;
use ratatui::{
    style::{Modifier, Style},
    text::{Line, Span},
};
use serde_json::Value;

use dynamate::dynamodb::json;

use crate::widgets::theme::Theme;

pub fn item_to_lines(
    item: &std::collections::HashMap<String, AttributeValue>,
    theme: &Theme,
) -> Vec<Line<'static>> {
    let value = match json::to_json(item) {
        Ok(value) => value,
        Err(err) => {
            return vec![Line::from(format!("Failed to render item: {err}"))];
        }
    };

    let mut lines = Vec::new();
    render_value(&value, 0, theme, &mut lines);
    if lines.is_empty() {
        lines.push(Line::from("(empty item)"));
    }
    lines
}

fn render_value(value: &Value, indent: usize, theme: &Theme, lines: &mut Vec<Line<'static>>) {
    match value {
        Value::Object(map) => {
            if map.is_empty() {
                lines.push(Line::from(vec![
                    indent_span(indent, theme),
                    Span::styled("{}", Style::default().fg(theme.text_muted())),
                ]));
                return;
            }

            let mut keys: Vec<_> = map.keys().collect();
            keys.sort();
            for key in keys {
                let child = &map[key];
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
                    render_value(child, indent + 2, theme, lines);
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
                    render_value(value, indent + 2, theme, lines);
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
        Value::Bool(value) => Span::styled(
            value.to_string(),
            Style::default().fg(theme.text_muted()),
        ),
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
