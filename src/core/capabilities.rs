//! What a backend can do, so the UI can hide or disable unsupported features.

/// How a backend models secondary indexes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SecondaryIndexSupport {
    /// No secondary indexes.
    None,
    /// DynamoDB-style global and local secondary indexes.
    DynamoStyleGsiLsi,
    /// Arbitrary single-/multi-field indexes (e.g. MongoDB).
    Arbitrary,
    /// Composite indexes (e.g. Firestore).
    Composite,
}

/// A flat, cheap-to-read description of a backend's feature set. Read on the
/// render/event hot path, so it is plain data (no async, no allocation).
#[derive(Debug, Clone)]
pub struct Capabilities {
    pub backend_label: &'static str,
    /// Supports DynamoDB-style set types (SS/NS/BS).
    pub set_types: bool,
    /// Supports binary attribute values.
    pub binary_type: bool,
    pub secondary_indexes: SecondaryIndexSupport,
    pub create_collection: bool,
    pub drop_collection: bool,
    /// Supports bulk delete of arbitrary keys (used to delete a multi-selection).
    pub batch_delete: bool,
    /// Offers the "purge" action (delete every item in a collection). Backends
    /// where a native bulk operation is the right tool (SQL `TRUNCATE`/`DELETE`)
    /// leave this off so the action is hidden.
    pub purge: bool,
    /// Supports querying by selecting a named index + key value (the index
    /// picker). Meaningful for key/document stores; SQL filters with `WHERE`
    /// instead, so it leaves this off.
    pub index_query: bool,
    pub ttl: bool,
    /// Reports an examined/scanned count distinct from the returned count.
    pub scanned_count: bool,
    /// Reports query cost (e.g. consumed capacity).
    pub consumed_capacity: bool,
    /// Supports a free-form database-level query (SQL `SELECT …` across tables).
    /// Drives the table picker's query view.
    pub raw_query: bool,
}
