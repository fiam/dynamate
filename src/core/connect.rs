//! Backend selection and connection — the factory that turns a chosen backend
//! plus connection options into an `Arc<dyn Datastore>`.
//!
//! Adding a new backend means adding a [`BackendKind`] variant, the matching
//! [`ConnOptions`] data, and a `match` arm here — nothing else in the app needs
//! to change.

use std::sync::Arc;

use super::datastore::Datastore;
use super::error::{DbError, Result};

/// The set of supported backends. Selected with `--backend` on the CLI.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, clap::ValueEnum)]
pub enum BackendKind {
    #[default]
    Dynamodb,
    Mongodb,
    Firestore,
}

/// Per-backend connection parameters.
#[derive(Debug, Clone)]
pub enum ConnOptions {
    Dynamo { endpoint_url: Option<String> },
}

/// Open a datastore for the given backend.
pub async fn open(
    kind: BackendKind,
    options: &ConnOptions,
    read_only: bool,
) -> Result<Arc<dyn Datastore>> {
    match kind {
        BackendKind::Dynamodb => {
            let ConnOptions::Dynamo { endpoint_url } = options;
            let client = crate::dynamodb::connect::new_client(endpoint_url.as_deref())
                .await
                .map_err(DbError::Backend)?;
            Ok(Arc::new(crate::dynamodb::DynamoBackend::new(
                client, read_only,
            )))
        }
        BackendKind::Mongodb => Err(DbError::Unsupported(
            "the MongoDB backend is not yet implemented",
        )),
        BackendKind::Firestore => Err(DbError::Unsupported(
            "the Firestore backend is not yet implemented",
        )),
    }
}
