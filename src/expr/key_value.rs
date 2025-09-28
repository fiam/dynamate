use super::ast::{KeyValue, Value};
use super::error::ParseError;
use std::collections::HashMap;

pub fn parse_expressions(input: &str) -> Result<Vec<KeyValue>, ParseError> {
    let mut result = Vec::new();
    let chars: Vec<char> = input.chars().collect();
    let mut i = 0;

    while i < chars.len() {
        // Skip whitespace
        while i < chars.len() && chars[i].is_whitespace() {
            i += 1;
        }

        if i >= chars.len() {
            break;
        }

        // Parse key=value pair
        let (key, value, new_pos) = parse_key_value(&chars, i)?;
        result.push(KeyValue { key, value });
        i = new_pos;
    }

    Ok(result)
}

fn parse_key_value(chars: &[char], start: usize) -> Result<(String, Value, usize), ParseError> {
    let i = start;

    // Parse key
    let (key, mut i) = parse_token_as_string(chars, i)?;

    // Expect '='
    if i >= chars.len() || chars[i] != '=' {
        return Err(ParseError::MissingValue {
            key: key.clone(),
            position: i,
        });
    }
    i += 1; // Skip '='

    // Parse value with type inference
    let (value, i) = parse_value(chars, i)?;

    Ok((key, value, i))
}

fn parse_value(chars: &[char], start: usize) -> Result<(Value, usize), ParseError> {
    let mut i = start;

    // Skip whitespace before value
    while i < chars.len() && chars[i].is_whitespace() {
        i += 1;
    }

    if i >= chars.len() {
        return Err(ParseError::InvalidSyntax {
            message: "Expected value after '='".to_string(),
            position: i,
        });
    }

    // Check if value starts with a quote (explicit string)
    if chars[i] == '"' || chars[i] == '\'' {
        let (string_val, new_pos) = parse_quoted_string(chars, i)?;
        Ok((Value::String(string_val), new_pos))
    } else {
        // Parse unquoted token and infer type
        let (token, new_pos) = parse_unquoted_token(chars, i)?;
        let value = infer_value_type(&token)?;
        Ok((value, new_pos))
    }
}

fn parse_token_as_string(chars: &[char], start: usize) -> Result<(String, usize), ParseError> {
    let mut i = start;
    let mut result = String::new();

    if i >= chars.len() {
        return Err(ParseError::InvalidSyntax {
            message: "Unexpected end of input".to_string(),
            position: i,
        });
    }

    // Check if token starts with a quote
    if chars[i] == '"' || chars[i] == '\'' {
        let (string_val, new_pos) = parse_quoted_string(chars, i)?;
        Ok((string_val, new_pos))
    } else {
        // Parse unquoted token
        let (token, new_pos) = parse_unquoted_token(chars, i)?;
        Ok((token, new_pos))
    }
}

fn parse_quoted_string(chars: &[char], start: usize) -> Result<(String, usize), ParseError> {
    let mut i = start;
    let quote_char = chars[i];
    let quote_start = i;
    i += 1; // Skip opening quote
    let mut result = String::new();

    while i < chars.len() && chars[i] != quote_char {
        if chars[i] == '\\' {
            i += 1; // Skip backslash
            if i >= chars.len() {
                return Err(ParseError::InvalidEscapeSequence { position: i - 1 });
            }

            // Handle escape sequences
            match chars[i] {
                '\\' => result.push('\\'),
                '"' => result.push('"'),
                '\'' => result.push('\''),
                'n' => result.push('\n'),
                'r' => result.push('\r'),
                't' => result.push('\t'),
                c => {
                    // For other characters, just include them literally
                    result.push(c);
                }
            }
        } else {
            result.push(chars[i]);
        }
        i += 1;
    }

    if i >= chars.len() {
        return Err(ParseError::UnterminatedQuote {
            position: quote_start,
            quote_char,
        });
    }

    i += 1; // Skip closing quote
    Ok((result, i))
}

fn parse_unquoted_token(chars: &[char], start: usize) -> Result<(String, usize), ParseError> {
    let mut i = start;
    let mut result = String::new();

    // Parse unquoted token
    while i < chars.len() && !chars[i].is_whitespace() && chars[i] != '=' {
        result.push(chars[i]);
        i += 1;
    }

    if result.is_empty() {
        return Err(ParseError::InvalidSyntax {
            message: "Empty token".to_string(),
            position: start,
        });
    }

    Ok((result, i))
}

fn infer_value_type(token: &str) -> Result<Value, ParseError> {
    // Try to parse as boolean first
    match token.to_lowercase().as_str() {
        "true" => return Ok(Value::Boolean(true)),
        "false" => return Ok(Value::Boolean(false)),
        "null" => return Ok(Value::Null),
        _ => {}
    }

    // Try to parse as number
    if let Ok(int_val) = token.parse::<i64>() {
        return Ok(Value::Number(int_val as f64));
    }

    if let Ok(float_val) = token.parse::<f64>() {
        return Ok(Value::Number(float_val));
    }

    // Default to string if not quoted but couldn't parse as other types
    Ok(Value::String(token.to_string()))
}

pub fn parse_to_map(input: &str) -> Result<HashMap<String, Value>, ParseError> {
    let pairs = parse_expressions(input)?;
    let mut map = HashMap::new();

    for pair in pairs {
        map.insert(pair.key, pair.value);
    }

    Ok(map)
}

// Legacy compatibility - convert back to old KeyValue struct
#[derive(Debug, Clone, PartialEq)]
pub struct LegacyKeyValue {
    pub key: String,
    pub value: String,
}

pub fn parse_expressions_legacy(input: &str) -> Result<Vec<LegacyKeyValue>, ParseError> {
    let new_pairs = parse_expressions(input)?;
    let legacy_pairs = new_pairs
        .into_iter()
        .map(|pair| LegacyKeyValue {
            key: pair.key,
            value: value_to_string(&pair.value),
        })
        .collect();
    Ok(legacy_pairs)
}

pub fn parse_to_map_legacy(input: &str) -> Result<HashMap<String, String>, ParseError> {
    let pairs = parse_expressions_legacy(input)?;
    let mut map = HashMap::new();

    for pair in pairs {
        map.insert(pair.key, pair.value);
    }

    Ok(map)
}

fn value_to_string(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        Value::Number(n) => {
            if n.fract() == 0.0 {
                format!("{}", *n as i64)
            } else {
                n.to_string()
            }
        }
        Value::Boolean(b) => b.to_string(),
        Value::Null => "null".to_string(),
    }
}
