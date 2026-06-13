//! Backend-neutral storage abstraction.
//!
//! This module is the seam that lets Dynamate target stores other than
//! DynamoDB (MongoDB, Firestore, …). It deliberately contains **no**
//! `aws_sdk_dynamodb` types — each backend converts its native types to and
//! from the neutral types defined here at its own boundary.
//!
//! The submodules are introduced incrementally (see the approved plan):
//! - [`value`] — the neutral [`value::Value`] / [`value::Item`] data model.
//! - `datastore`, `schema`, `query`, `dialect`, `capabilities`, `connect`
//!   are added in later phases.

pub mod capabilities;
pub mod connect;
pub mod datastore;
pub mod error;
pub mod json;
pub mod query;
pub mod schema;
pub mod size;
pub mod value;
