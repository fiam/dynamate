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
    Mongo { uri: String },
}

/// Choose a backend from the connection arguments by URI scheme: a
/// `mongodb://` / `mongodb+srv://` target selects MongoDB; anything else (an
/// AWS endpoint, or nothing) selects DynamoDB. An explicit `--backend` overrides
/// this.
pub fn detect_backend(target: Option<&str>, endpoint_url: Option<&str>) -> BackendKind {
    for candidate in [target, endpoint_url].into_iter().flatten() {
        let lower = candidate.trim().to_ascii_lowercase();
        if lower.starts_with("mongodb://") || lower.starts_with("mongodb+srv://") {
            return BackendKind::Mongodb;
        }
    }
    BackendKind::Dynamodb
}

/// Open a datastore for the given backend.
pub async fn open(
    kind: BackendKind,
    options: &ConnOptions,
    read_only: bool,
) -> Result<Arc<dyn Datastore>> {
    match (kind, options) {
        (BackendKind::Dynamodb, ConnOptions::Dynamo { endpoint_url }) => {
            let client = crate::dynamodb::connect::new_client(endpoint_url.as_deref())
                .await
                .map_err(DbError::Backend)?;
            Ok(Arc::new(crate::dynamodb::DynamoBackend::new(
                client, read_only,
            )))
        }
        (BackendKind::Mongodb, ConnOptions::Mongo { uri }) => {
            let backend = crate::mongo::connect::connect(uri, read_only)
                .await
                .map_err(DbError::Backend)?;
            Ok(Arc::new(backend))
        }
        (BackendKind::Firestore, _) => Err(DbError::Unsupported(
            "the Firestore backend is not yet implemented",
        )),
        (kind, _) => Err(DbError::Backend(format!(
            "connection options do not match the {kind:?} backend"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::{BackendKind, detect_backend};

    #[test]
    fn detects_mongo_from_target_scheme() {
        assert_eq!(
            detect_backend(Some("mongodb://localhost:27017/db"), None),
            BackendKind::Mongodb
        );
        assert_eq!(
            detect_backend(Some("mongodb+srv://cluster/db"), None),
            BackendKind::Mongodb
        );
    }

    #[test]
    fn detects_mongo_from_endpoint_url() {
        assert_eq!(
            detect_backend(None, Some("mongodb://localhost/db")),
            BackendKind::Mongodb
        );
    }

    #[test]
    fn defaults_to_dynamodb() {
        assert_eq!(detect_backend(None, None), BackendKind::Dynamodb);
        assert_eq!(
            detect_backend(Some("https://127.0.0.1:8000"), None),
            BackendKind::Dynamodb
        );
    }
}
