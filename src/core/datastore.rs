//! The backend-neutral storage trait.
//!
//! One `Arc<dyn Datastore>` is chosen at startup and shared across all widgets.
//! Each backend converts its native types to and from the neutral types in this
//! module at its own boundary, compiles the [`QueryPlan`] to its own query
//! language, and enforces read-only mode inside its mutating methods.

use async_trait::async_trait;

use crate::expr::builtins::Dialect;

use super::capabilities::Capabilities;
use super::error::Result;
use super::query::{
    BatchDeleteOutcome, CreateCollectionSpec, Key, Page, PlanExplanation, QueryPlan, QueryResult,
};
use super::schema::CollectionSchema;
use super::value::Item;

#[async_trait]
pub trait Datastore: Send + Sync {
    /// Static description of what this backend supports.
    fn capabilities(&self) -> &Capabilities;

    /// The query-language dialect (function set + type codes) for this backend.
    /// Consulted by the parser and autocompletion engine.
    fn dialect(&self) -> &Dialect;

    /// A short human label for the backend (e.g. "DynamoDB").
    fn label(&self) -> &str {
        self.capabilities().backend_label
    }

    /// Whether mutating operations are disabled. The backend also enforces this
    /// internally (mutating methods return [`DbError::ReadOnly`]); this accessor
    /// lets the UI gate affordances up front.
    ///
    /// [`DbError::ReadOnly`]: super::error::DbError::ReadOnly
    fn is_read_only(&self) -> bool;

    /// Downcasting hook used transitionally while UI widgets are migrated off the
    /// raw SDK client. Will be removed once no widget needs the concrete backend.
    fn as_any(&self) -> &dyn std::any::Any;

    /// Verify connectivity/credentials. Called once at startup.
    async fn validate(&self) -> Result<()>;

    /// List the collection (table) names.
    async fn list_collections(&self) -> Result<Vec<String>>;

    /// Describe a collection's neutral schema.
    async fn describe_collection(&self, name: &str) -> Result<CollectionSchema>;

    /// Run a query, returning one page of results. The backend compiles the
    /// plan to its dialect and paginates itself.
    async fn query(&self, name: &str, plan: &QueryPlan, page: Page) -> Result<QueryResult>;

    /// Create or replace a single item.
    async fn put_item(&self, name: &str, item: Item) -> Result<()>;

    /// Delete a single item by key.
    async fn delete_item(&self, name: &str, key: Key) -> Result<()>;

    /// Delete many items by key.
    async fn batch_delete(&self, name: &str, keys: Vec<Key>) -> Result<BatchDeleteOutcome>;

    /// Create a collection.
    async fn create_collection(&self, spec: &CreateCollectionSpec) -> Result<()>;

    /// Drop a collection.
    async fn drop_collection(&self, name: &str) -> Result<()>;

    /// The TTL attribute for a collection, if TTL is configured.
    async fn describe_ttl(&self, _name: &str) -> Result<Option<String>> {
        Ok(None)
    }

    /// Predict how a query would run, when the backend can. Defaults to unknown.
    async fn explain(&self, _name: &str, _plan: &QueryPlan) -> PlanExplanation {
        PlanExplanation::Unknown
    }
}
