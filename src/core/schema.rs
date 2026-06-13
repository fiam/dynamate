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
