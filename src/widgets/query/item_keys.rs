use std::collections::HashSet;

use aws_sdk_dynamodb::types::{KeySchemaElement, KeyType, TableDescription};

#[derive(Debug, Default, Clone)]
pub struct ItemKeys {
    set: HashSet<String>,
    hidden: HashSet<String>,
    sorted: Vec<String>,
    visible: Vec<String>,
}

impl ItemKeys {
    /// Insert many keys and rebuild the cached order:
    ///  - HASH key first, RANGE key second, others alphabetical.
    pub fn extend<I>(&mut self, keys: I, table: &TableDescription)
    where
        I: IntoIterator<Item = String>,
    {
        self.set.extend(keys);
        self.rebuild_with_schema(table);
    }

    /// Insert many keys and rebuild the cached order without table schema info.
    /// This keeps things responsive when DescribeTable is slow or unavailable.
    pub fn extend_unordered<I>(&mut self, keys: I)
    where
        I: IntoIterator<Item = String>,
    {
        self.set.extend(keys);
        self.rebuild_unordered();
    }

    /// Rebuild ordering using the table schema (HASH first, RANGE second).
    pub fn rebuild_with_schema(&mut self, table: &TableDescription) {
        let mut keys: Vec<String> = self.set.iter().cloned().collect();
        let (hash_name, range_name) = extract_hash_range(table);
        keys.sort_by(|a, b| {
            rank(a, &hash_name, &range_name)
                .cmp(&rank(b, &hash_name, &range_name))
                .then_with(|| a.cmp(b))
        });
        self.sorted = keys;
        self.update_visible();
    }

    /// Rebuild ordering alphabetically (no schema).
    pub fn rebuild_unordered(&mut self) {
        let mut keys: Vec<String> = self.set.iter().cloned().collect();
        keys.sort();
        self.sorted = keys;
        self.update_visible();
    }

    /// Sorted keys including hidden fields.
    pub fn sorted(&self) -> &[String] {
        &self.sorted
    }

    /// Sorted keys with hidden fields filtered out.
    pub fn visible(&self) -> &[String] {
        &self.visible
    }

    pub fn hide(&mut self, key: &str) {
        self.hidden.insert(key.to_string());
        self.update_visible();
    }

    pub fn unhide(&mut self, key: &str) {
        self.hidden.remove(key);
        self.update_visible();
    }

    pub fn is_hidden(&self, key: &str) -> bool {
        self.hidden.contains(key)
    }

    pub fn clear(&mut self) {
        self.set.clear();
        self.hidden.clear();
        self.sorted.clear();
        self.visible.clear();
    }

    fn update_visible(&mut self) {
        self.visible = self
            .sorted
            .iter()
            .filter(|k| !self.hidden.contains(*k))
            .cloned()
            .collect();
    }
}

/// Return (HASH_name, RANGE_name) from the table (None if absent).
fn extract_hash_range(table: &TableDescription) -> (Option<String>, Option<String>) {
    let mut hash = None;
    let mut range = None;
    for KeySchemaElement {
        attribute_name,
        key_type,
        ..
    } in table.key_schema()
    {
        match key_type {
            KeyType::Hash => hash = Some(attribute_name.clone()),
            KeyType::Range => range = Some(attribute_name.clone()),
            _ => {}
        }
    }
    (hash, range)
}

/// Rank: 0 = HASH, 1 = RANGE, 2 = others
fn rank(name: &str, hash: &Option<String>, range: &Option<String>) -> u8 {
    if hash.as_deref() == Some(name) {
        0
    } else if range.as_deref() == Some(name) {
        1
    } else {
        2
    }
}
