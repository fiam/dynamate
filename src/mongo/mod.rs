//! The MongoDB backend: a [`Datastore`](crate::core::datastore::Datastore)
//! implementation plus its BSON conversion and JSON-filter query language.

pub mod backend;
pub mod connect;
pub mod convert;
pub mod language;

pub use backend::MongoBackend;
