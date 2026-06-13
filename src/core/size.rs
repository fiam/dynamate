//! Item size estimation over the neutral [`Value`] / [`Item`] model.
//!
//! The rules mirror DynamoDB's item-size accounting (it is an estimate; sets
//! are treated like lists). Backends that don't bill by size can ignore this.

use super::value::{Item, Value};

/// Estimate item size in bytes using DynamoDB item-size rules.
pub fn estimate_item_size_bytes(item: &Item) -> usize {
    item.iter()
        .map(|(name, value)| name.len() + estimate_value_size_bytes(value))
        .sum()
}

fn estimate_value_size_bytes(value: &Value) -> usize {
    match value {
        Value::Str(text) => text.len(),
        Value::Num(num) => number_size_bytes(num.as_str()),
        Value::Bytes(bytes) => bytes.len(),
        Value::Bool(_) | Value::Null => 1,
        Value::List(list) => {
            let values_size: usize = list.iter().map(estimate_value_size_bytes).sum();
            3 + list.len() + values_size
        }
        Value::Map(map) => {
            let mut size = 3 + map.len();
            for (name, value) in map {
                size += name.len();
                size += estimate_value_size_bytes(value);
            }
            size
        }
        Value::StringSet(set) => {
            let values_size: usize = set.iter().map(String::len).sum();
            3 + set.len() + values_size
        }
        Value::NumberSet(set) => {
            let values_size: usize = set.iter().map(|n| number_size_bytes(n.as_str())).sum();
            3 + set.len() + values_size
        }
        Value::BytesSet(set) => {
            let values_size: usize = set.iter().map(Vec::len).sum();
            3 + set.len() + values_size
        }
    }
}

fn number_size_bytes(num: &str) -> usize {
    let mut s = num.trim();
    if s.is_empty() {
        return 1;
    }
    if let Some(rest) = s.strip_prefix('-') {
        s = rest;
    }
    if let Some(rest) = s.strip_prefix('+') {
        s = rest;
    }

    let coeff = match s.find(['e', 'E']) {
        Some(idx) => &s[..idx],
        None => s,
    };
    let has_decimal = coeff.contains('.');
    let mut digits: String = coeff.chars().filter(|c| *c != '.').collect();
    digits = digits.trim_start_matches('0').to_string();
    if has_decimal {
        digits = digits.trim_end_matches('0').to_string();
    }
    let count = if digits.is_empty() { 1 } else { digits.len() };
    count.div_ceil(2) + 1
}
