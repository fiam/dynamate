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
    Postgres,
    Mysql,
    Firestore,
}

/// Per-backend connection parameters.
#[derive(Debug, Clone)]
pub enum ConnOptions {
    Dynamo { endpoint_url: Option<String> },
    Mongo { uri: String },
    Sql { url: String },
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
        if lower.starts_with("postgres://") || lower.starts_with("postgresql://") {
            return BackendKind::Postgres;
        }
        if lower.starts_with("mysql://") {
            return BackendKind::Mysql;
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
        (kind @ (BackendKind::Postgres | BackendKind::Mysql), ConnOptions::Sql { url }) => {
            let dialect = if matches!(kind, BackendKind::Postgres) {
                crate::sql::SqlDialectKind::Postgres
            } else {
                crate::sql::SqlDialectKind::Mysql
            };
            let backend = crate::sql::SqlBackend::connect(url, dialect, read_only)
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

    #[test]
    fn detects_sql_engines_from_scheme() {
        assert_eq!(
            detect_backend(Some("postgres://u:p@host/db"), None),
            BackendKind::Postgres
        );
        assert_eq!(
            detect_backend(Some("postgresql://host/db"), None),
            BackendKind::Postgres
        );
        assert_eq!(
            detect_backend(Some("mysql://host:3306/db"), None),
            BackendKind::Mysql
        );
    }
}
