use std::collections::HashMap;

use aws_sdk_dynamodb::types::AttributeValue;

/// Estimate item size in bytes using DynamoDB item size rules.
/// This is an estimate (sets are treated like lists).
pub fn estimate_item_size_bytes(item: &HashMap<String, AttributeValue>) -> usize {
    item.iter()
        .map(|(name, value)| name.as_bytes().len() + estimate_value_size_bytes(value))
        .sum()
}

fn estimate_value_size_bytes(value: &AttributeValue) -> usize {
    match value {
        AttributeValue::S(text) => text.as_bytes().len(),
        AttributeValue::N(num) => number_size_bytes(num),
        AttributeValue::B(bytes) => bytes.as_ref().len(),
        AttributeValue::Bool(_) => 1,
        AttributeValue::Null(_) => 1,
        AttributeValue::L(list) => {
            let values_size: usize = list.iter().map(estimate_value_size_bytes).sum();
            3 + list.len() + values_size
        }
        AttributeValue::M(map) => {
            let mut size = 3 + map.len();
            for (name, value) in map {
                size += name.as_bytes().len();
                size += estimate_value_size_bytes(value);
            }
            size
        }
        AttributeValue::Ss(set) => {
            let values_size: usize = set.iter().map(|s| s.as_bytes().len()).sum();
            3 + set.len() + values_size
        }
        AttributeValue::Ns(set) => {
            let values_size: usize = set.iter().map(|n| number_size_bytes(n)).sum();
            3 + set.len() + values_size
        }
        AttributeValue::Bs(set) => {
            let values_size: usize = set.iter().map(|b| b.as_ref().len()).sum();
            3 + set.len() + values_size
        }
        _ => 0,
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

    let coeff = match s.find(|c| c == 'e' || c == 'E') {
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
    (count + 1) / 2 + 1
}
