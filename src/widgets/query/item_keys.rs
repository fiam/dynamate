use std::collections::HashSet;
use std::sync::{Arc, RwLock, RwLockReadGuard};

use aws_sdk_dynamodb::types::{KeySchemaElement, KeyType, TableDescription};

#[derive(Debug, Default)]
struct Inner {
    set: HashSet<String>,    // canonical keys (unique)
    hidden: HashSet<String>, // hidden keys
    sorted: Vec<String>,     // cached, sorted snapshot
    visible: Vec<String>,    // visible keys
}

#[derive(Debug, Default, Clone)]
pub struct ItemKeys {
    inner: Arc<RwLock<Inner>>,
}

/// Read guard that exposes a borrowed slice of the sorted keys (stable Rust).
pub struct SortedKeysGuard<'a> {
    guard: RwLockReadGuard<'a, Inner>,
}
impl<'a> SortedKeysGuard<'a> {
    pub fn as_slice(&self) -> &[String] {
        &self.guard.visible
    }
}
impl<'a> std::ops::Deref for SortedKeysGuard<'a> {
    type Target = [String];
    fn deref(&self) -> &Self::Target {
        &self.guard.sorted
    }
}

impl ItemKeys {
    /// Insert many keys and rebuild the cached order:
    ///  - HASH key first, RANGE key second, others alphabetical.
    pub fn extend<I>(&self, keys: I, table: &TableDescription)
    where
        I: IntoIterator<Item = String>,
    {
        let mut inner = self.inner.write().unwrap();

        // 1) Insert into the set (deduplicated)
        inner.set.extend(keys);

        // 2) Rebuild the sorted snapshot from the set
        let keys: Vec<String> = inner.set.iter().cloned().collect();
        inner.sorted.clear();
        inner.sorted.extend(keys);

        // 3) Sort: HASH first, then RANGE, then everything else alphabetically
        let (hash_name, range_name) = extract_hash_range(table);
        inner.sorted.sort_by(|a, b| {
            rank(a, &hash_name, &range_name)
                .cmp(&rank(b, &hash_name, &range_name))
                .then_with(|| a.cmp(b)) // alpha tie-break for non-key attrs
        });

        // 4) Update the visible keys
        self.update_visible(&mut inner);
    }

    /// Insert many keys and rebuild the cached order without table schema info.
    /// This keeps things responsive when DescribeTable is slow or unavailable.
    pub fn extend_unordered<I>(&self, keys: I)
    where
        I: IntoIterator<Item = String>,
    {
        let mut inner = self.inner.write().unwrap();
        inner.set.extend(keys);
        let mut keys: Vec<String> = inner.set.iter().cloned().collect();
        keys.sort();
        inner.sorted = keys;
        self.update_visible(&mut inner);
    }

    /// Borrow the current sorted view (no clone). Keep guard alive while using the slice.
    pub fn sorted(&self) -> SortedKeysGuard<'_> {
        SortedKeysGuard {
            guard: self.inner.read().unwrap(),
        }
    }

    pub fn hide(&self, key: &str) {
        let mut inner = self.inner.write().unwrap();
        inner.hidden.insert(key.to_string());
        self.update_visible(&mut inner);
    }

    pub fn unhide(&self, key: &str) {
        let mut inner = self.inner.write().unwrap();
        inner.hidden.remove(key);
        self.update_visible(&mut inner);
    }

    pub fn is_hidden(&self, key: &str) -> bool {
        let inner = self.inner.read().unwrap();
        inner.hidden.contains(key)
    }

    pub fn clear(&self) {
        let mut inner = self.inner.write().unwrap();
        inner.set.clear();
        inner.hidden.clear();
        inner.sorted.clear();
        inner.visible.clear();
    }

    fn update_visible(&self, inner: &mut Inner) {
        inner.visible = inner
            .sorted
            .iter()
            .filter(|k| !inner.hidden.contains(*k))
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
