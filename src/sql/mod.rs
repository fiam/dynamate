//! SQL backends (PostgreSQL + MySQL) behind the neutral
//! [`Datastore`](crate::core::datastore::Datastore) trait, over a `sqlx` pool.
//! A table is a collection, a row is a neutral item, and the query box is either
//! a per-table `WHERE` predicate or a database-level free-form `SELECT`.

pub mod backend;
pub mod connect;
pub mod convert;
pub mod dialect;
pub mod language;

pub use backend::SqlBackend;
pub use dialect::SqlDialectKind;
