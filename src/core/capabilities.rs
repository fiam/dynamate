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
    /// Supports bulk delete (the "purge" action).
    pub batch_delete: bool,
    pub ttl: bool,
    /// Reports an examined/scanned count distinct from the returned count.
    pub scanned_count: bool,
    /// Reports query cost (e.g. consumed capacity).
    pub consumed_capacity: bool,
}
