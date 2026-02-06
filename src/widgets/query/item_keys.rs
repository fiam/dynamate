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
    ///  - Table HASH, Table RANGE
    ///  - GSI HASH, GSI RANGE
    ///  - LSI HASH, LSI RANGE
    ///  - Others alphabetical
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

    /// Rebuild ordering using the table schema.
    pub fn rebuild_with_schema(&mut self, table: &TableDescription) {
        let mut keys: Vec<String> = self.set.iter().cloned().collect();
        let ordering = extract_key_ordering(table);
        keys.sort_by(|a, b| {
            rank(a, &ordering)
                .cmp(&rank(b, &ordering))
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

#[derive(Debug, Default)]
struct KeyOrdering {
    table_hash: Option<String>,
    table_range: Option<String>,
    gsi_hash: HashSet<String>,
    gsi_range: HashSet<String>,
    lsi_hash: HashSet<String>,
    lsi_range: HashSet<String>,
}

fn extract_key_ordering(table: &TableDescription) -> KeyOrdering {
    let (table_hash, table_range) = extract_hash_range_from_schema(table.key_schema());
    let mut ordering = KeyOrdering {
        table_hash,
        table_range,
        ..KeyOrdering::default()
    };

    for gsi in table.global_secondary_indexes() {
        let (hash, range) = extract_hash_range_from_schema(gsi.key_schema());
        if let Some(hash) = hash {
            ordering.gsi_hash.insert(hash);
        }
        if let Some(range) = range {
            ordering.gsi_range.insert(range);
        }
    }

    for lsi in table.local_secondary_indexes() {
        let (hash, range) = extract_hash_range_from_schema(lsi.key_schema());
        if let Some(hash) = hash {
            ordering.lsi_hash.insert(hash);
        }
        if let Some(range) = range {
            ordering.lsi_range.insert(range);
        }
    }

    ordering
}

/// Return (HASH_name, RANGE_name) from a key schema (None if absent).
fn extract_hash_range_from_schema(
    schema: &[KeySchemaElement],
) -> (Option<String>, Option<String>) {
    let mut hash = None;
    let mut range = None;
    for KeySchemaElement {
        attribute_name,
        key_type,
        ..
    } in schema
    {
        match key_type {
            KeyType::Hash => hash = Some(attribute_name.clone()),
            KeyType::Range => range = Some(attribute_name.clone()),
            _ => {}
        }
    }
    (hash, range)
}

/// Rank:
/// 0 = table HASH, 1 = table RANGE
/// 2 = GSI HASH, 3 = GSI RANGE
/// 4 = LSI HASH, 5 = LSI RANGE
/// 6 = others
fn rank(name: &str, ordering: &KeyOrdering) -> u8 {
    if ordering.table_hash.as_deref() == Some(name) {
        0
    } else if ordering.table_range.as_deref() == Some(name) {
        1
    } else if ordering.gsi_hash.contains(name) {
        2
    } else if ordering.gsi_range.contains(name) {
        3
    } else if ordering.lsi_hash.contains(name) {
        4
    } else if ordering.lsi_range.contains(name) {
        5
    } else {
        6
    }
}
