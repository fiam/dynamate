//! Backend-neutral error type for storage operations.

use std::fmt;

/// An error returned by a [`Datastore`](super::datastore::Datastore) operation.
#[derive(Debug, Clone)]
pub enum DbError {
    /// The requested collection/item did not exist.
    NotFound(String),
    /// The operation isn't supported by this backend.
    Unsupported(&'static str),
    /// A mutating operation was rejected because the datastore is read-only.
    ReadOnly,
    /// A backend-specific failure, already formatted for display.
    Backend(String),
}

impl DbError {
    /// The standard message shown when a write is rejected in read-only mode.
    pub const READ_ONLY_MESSAGE: &'static str = "Read-only mode: write operations are disabled";
}

impl fmt::Display for DbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DbError::NotFound(what) => write!(f, "not found: {what}"),
            DbError::Unsupported(what) => write!(f, "unsupported operation: {what}"),
            DbError::ReadOnly => f.write_str(DbError::READ_ONLY_MESSAGE),
            DbError::Backend(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for DbError {}

pub type Result<T> = std::result::Result<T, DbError>;
