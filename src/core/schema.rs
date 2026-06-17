//! Backend-neutral description of a collection's shape (keys + indexes).
//!
//! This generalizes DynamoDB's `TableDescription`: a [`KeySchema`] holds an
//! ordered list of key fields (one for a Mongo `_id`, two for a DynamoDB
//! partition+sort key, potentially more elsewhere), and [`IndexSchema`] unifies
//! GSIs/LSIs/Mongo indexes/Firestore composite indexes.

/// The scalar type of a key field.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScalarType {
    String,
    Number,
    Binary,
}

/// The role a key field plays.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyRole {
    /// Partition / hash key — distributes data.
    Partition,
    /// Sort / range key — orders within a partition.
    Sort,
}

/// A single key field.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyField {
    pub name: String,
    pub role: KeyRole,
    pub ty: ScalarType,
}

/// An ordered set of key fields.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct KeySchema {
    pub fields: Vec<KeyField>,
}

impl KeySchema {
    /// The partition-key field name, if any.
    pub fn partition_key(&self) -> Option<&str> {
        self.fields
            .iter()
            .find(|f| f.role == KeyRole::Partition)
            .map(|f| f.name.as_str())
    }

    /// The sort-key field name, if any.
    pub fn sort_key(&self) -> Option<&str> {
        self.fields
            .iter()
            .find(|f| f.role == KeyRole::Sort)
            .map(|f| f.name.as_str())
    }
}

/// What an index projects into its own storage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Projection {
    All,
    KeysOnly,
    Include(Vec<String>),
}

/// The kind of secondary index, spanning backend vocabularies.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexKind {
    /// DynamoDB global secondary index (independent partition key).
    GlobalSecondary,
    /// DynamoDB local secondary index (shares the table partition key).
    LocalSecondary,
    /// A generic secondary index (e.g. MongoDB).
    Secondary,
    /// A composite index (e.g. Firestore).
    Composite,
}

/// A secondary index on a collection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexSchema {
    pub name: String,
    pub kind: IndexKind,
    pub key: KeySchema,
    pub projection: Projection,
}

/// A column in a tabular collection. Populated by SQL backends, where a row has
/// a fixed shape; schemaless backends (DynamoDB, MongoDB) leave the collection's
/// `columns` list empty.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnSchema {
    pub name: String,
    /// The backend's own type name (e.g. `integer`, `text`, `jsonb`).
    pub data_type: String,
    pub nullable: bool,
}

/// Autocompletion hints for the database-level (SQL) query view: the table names
/// and each table's columns, so completion can be context-aware (offer tables
/// after `FROM`/`JOIN`, and only the referenced tables' columns elsewhere).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SchemaHints {
    pub tables: Vec<String>,
    /// `(table_name, column_names)` for every table.
    pub columns: Vec<(String, Vec<String>)>,
}

impl SchemaHints {
    /// Columns belonging to any of `tables` (case-insensitive). When `tables` is
    /// empty (or matches nothing), returns every column across all tables.
    pub fn columns_for(&self, tables: &[String]) -> Vec<String> {
        let mut out = Vec::new();
        let referenced: Vec<String> = tables.iter().map(|t| t.to_ascii_lowercase()).collect();
        let mut matched_any = false;
        for (table, cols) in &self.columns {
            if referenced.is_empty() || referenced.contains(&table.to_ascii_lowercase()) {
                if !referenced.is_empty() {
                    matched_any = true;
                }
                for col in cols {
                    if !out.contains(col) {
                        out.push(col.clone());
                    }
                }
            }
        }
        if !referenced.is_empty() && !matched_any {
            // Referenced tables we don't know about: fall back to all columns.
            for (_, cols) in &self.columns {
                for col in cols {
                    if !out.contains(col) {
                        out.push(col.clone());
                    }
                }
            }
        }
        out
    }
}

/// The full neutral schema for a collection.
///
/// Alongside the structural fields it carries a few optional runtime stats
/// (status/counts) that the collection picker surfaces; backends that don't
/// expose them leave them `None`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CollectionSchema {
    pub name: String,
    pub key: KeySchema,
    pub indexes: Vec<IndexSchema>,
    /// Column shape for tabular backends (SQL); empty for schemaless backends.
    pub columns: Vec<ColumnSchema>,
    pub ttl_attribute: Option<String>,
    /// Backend status string (e.g. DynamoDB "ACTIVE"), if any.
    pub status: Option<String>,
    /// Approximate item count, if the backend reports one.
    pub item_count: Option<i64>,
    /// Approximate size in bytes, if the backend reports one.
    pub size_bytes: Option<i64>,
}

impl CollectionSchema {
    /// Number of global secondary indexes.
    pub fn global_secondary_index_count(&self) -> usize {
        self.indexes
            .iter()
            .filter(|index| index.kind == IndexKind::GlobalSecondary)
            .count()
    }

    /// Number of local secondary indexes.
    pub fn local_secondary_index_count(&self) -> usize {
        self.indexes
            .iter()
            .filter(|index| index.kind == IndexKind::LocalSecondary)
            .count()
    }
}
