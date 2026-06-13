//! Item size estimation for DynamoDB items.
//!
//! Thin shim over the backend-neutral [`crate::core::size`]: the DynamoDB item
//! is converted to a neutral [`Item`](crate::core::value::Item) and sized there.
//! New code should prefer `core::size` directly.

use std::collections::HashMap;

use aws_sdk_dynamodb::types::AttributeValue;

use crate::core::size as core_size;
use crate::dynamodb::convert::item_from_attribute_map;

/// Estimate item size in bytes using DynamoDB item size rules.
pub fn estimate_item_size_bytes(item: &HashMap<String, AttributeValue>) -> usize {
    core_size::estimate_item_size_bytes(&item_from_attribute_map(item))
}
