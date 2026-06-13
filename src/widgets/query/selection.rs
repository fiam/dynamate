//! Row-selection model for the query results table.
//!
//! Selection is tracked either as an explicit set of item keys or as
//! "everything the query matched, minus an excluded set", so that selecting all
//! results does not require materializing every key. [`ItemKey`] is the stable
//! identity of a row (its primary key), independent of the loaded snapshot.

use std::collections::{HashMap, HashSet};

use aws_sdk_dynamodb::types::AttributeValue;
use aws_smithy_types::Blob;
use dynamate::core::schema::CollectionSchema;

use super::widget::extract_hash_range;

#[derive(Debug, Clone, Default)]
pub(super) enum SelectionMode {
    #[default]
    None,
    Explicit(HashSet<ItemKey>),
    Query {
        excluded: HashSet<ItemKey>,
    },
}

#[derive(Debug, Clone)]
pub(super) enum SelectionSnapshot {
    Explicit(HashSet<ItemKey>),
    Query { excluded: HashSet<ItemKey> },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) struct ItemKey {
    pub(super) hash_key: String,
    pub(super) hash_value: KeyValue,
    pub(super) range: Option<(String, KeyValue)>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) enum KeyValue {
    String(String),
    Number(String),
    Binary(Vec<u8>),
}

impl SelectionMode {
    pub(super) fn is_active(&self) -> bool {
        !matches!(self, Self::None)
    }

    pub(super) fn clear(&mut self) {
        *self = Self::None;
    }

    pub(super) fn snapshot(&self) -> Option<SelectionSnapshot> {
        match self {
            Self::None => None,
            Self::Explicit(keys) => Some(SelectionSnapshot::Explicit(keys.clone())),
            Self::Query { excluded } => Some(SelectionSnapshot::Query {
                excluded: excluded.clone(),
            }),
        }
    }

    pub(super) fn remove_key(&mut self, key: &ItemKey) {
        match self {
            Self::None => {}
            Self::Explicit(keys) => {
                keys.remove(key);
                if keys.is_empty() {
                    *self = Self::None;
                }
            }
            Self::Query { excluded } => {
                excluded.remove(key);
            }
        }
    }

    /// Flip the selected state of every `loaded_keys` entry, leaving any
    /// not-yet-loaded rows untouched. With no selection this selects all
    /// loaded rows; in `Explicit` it toggles membership; in `Query` it
    /// toggles each key's exclusion.
    pub(super) fn invert_loaded(&mut self, loaded_keys: impl IntoIterator<Item = ItemKey>) {
        match self {
            Self::None => {
                let keys: HashSet<ItemKey> = loaded_keys.into_iter().collect();
                if !keys.is_empty() {
                    *self = Self::Explicit(keys);
                }
            }
            Self::Explicit(keys) => {
                for key in loaded_keys {
                    if !keys.remove(&key) {
                        keys.insert(key);
                    }
                }
                if keys.is_empty() {
                    *self = Self::None;
                }
            }
            Self::Query { excluded } => {
                for key in loaded_keys {
                    if !excluded.remove(&key) {
                        excluded.insert(key);
                    }
                }
            }
        }
    }
}

impl SelectionSnapshot {
    pub(super) fn is_selected(&self, key: &ItemKey) -> bool {
        match self {
            Self::Explicit(keys) => keys.contains(key),
            Self::Query { excluded } => !excluded.contains(key),
        }
    }
}

impl ItemKey {
    pub(super) fn from_item(
        item: &HashMap<String, AttributeValue>,
        schema: &CollectionSchema,
    ) -> Result<Self, String> {
        let (hash_key, range_key) = extract_hash_range(schema);
        let Some(hash_key) = hash_key else {
            return Err("Table is missing a partition key".to_string());
        };
        let hash_value = item
            .get(&hash_key)
            .ok_or_else(|| format!("Item is missing {hash_key}"))?;
        let hash_value = KeyValue::from_attr(hash_value)?;
        let range = match range_key {
            Some(range_key) => {
                let range_value = item
                    .get(&range_key)
                    .ok_or_else(|| format!("Item is missing {range_key}"))?;
                Some((range_key, KeyValue::from_attr(range_value)?))
            }
            None => None,
        };
        Ok(Self {
            hash_key,
            hash_value,
            range,
        })
    }

    pub(super) fn to_key_map(&self) -> HashMap<String, AttributeValue> {
        let mut key = HashMap::with_capacity(2);
        key.insert(self.hash_key.clone(), self.hash_value.to_attr());
        if let Some((range_key, range_value)) = self.range.as_ref() {
            key.insert(range_key.clone(), range_value.to_attr());
        }
        key
    }

    pub(super) fn summary_line(&self) -> String {
        let mut parts = vec![format!("{}={}", self.hash_key, self.hash_value.display())];
        if let Some((range_key, range_value)) = self.range.as_ref() {
            parts.push(format!("{range_key}={}", range_value.display()));
        }
        parts.join(" · ")
    }
}

impl KeyValue {
    pub(super) fn from_attr(value: &AttributeValue) -> Result<Self, String> {
        if let Ok(value) = value.as_s() {
            return Ok(Self::String(value.clone()));
        }
        if let Ok(value) = value.as_n() {
            return Ok(Self::Number(value.clone()));
        }
        if let Ok(value) = value.as_b() {
            return Ok(Self::Binary(value.as_ref().to_vec()));
        }
        Err("Primary key values must be scalar string, number, or binary".to_string())
    }

    pub(super) fn to_attr(&self) -> AttributeValue {
        match self {
            Self::String(value) => AttributeValue::S(value.clone()),
            Self::Number(value) => AttributeValue::N(value.clone()),
            Self::Binary(value) => AttributeValue::B(Blob::new(value.clone())),
        }
    }

    pub(super) fn display(&self) -> String {
        match self {
            Self::String(value) => value.clone(),
            Self::Number(value) => value.clone(),
            Self::Binary(value) => format!("<binary:{}>", value.len()),
        }
    }
}
