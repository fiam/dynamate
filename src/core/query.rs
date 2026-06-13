//! Backend-neutral query plan, pagination, and result types.

use crate::expr::DynamoExpression;

use super::schema::{IndexSchema, KeySchema};
use super::value::{Item, Value};

/// A filter expression handed to a backend to compile to its own dialect.
///
/// This is the (already backend-neutral) parsed query AST. It is aliased here so
/// the rest of the abstraction doesn't name the DynamoDB-era `DynamoExpression`
/// directly; the alias will become the canonical name in a later cleanup.
pub type FilterExpr = DynamoExpression;

/// Which index, if any, the user explicitly asked to run against.
#[derive(Debug, Clone)]
pub enum IndexHint {
    /// Use the collection's primary key.
    Primary,
    /// Use the named secondary index.
    Named(String),
}

/// An exact equality on a key attribute, preserving the precise value.
///
/// Used for index/primary lookups built programmatically from a selected item,
/// where the key value may be a number or binary that the text-filter AST
/// ([`FilterExpr`]) can't carry losslessly.
#[derive(Debug, Clone)]
pub struct KeyEquals {
    pub attribute: String,
    pub value: Value,
}

/// A backend-neutral query: an optional text filter, an optional index hint, and
/// an optional exact key equality.
///
/// The backend is responsible for compiling this to its native query language
/// and deciding how to execute it (an indexed lookup vs. a full scan).
#[derive(Debug, Clone, Default)]
pub struct QueryPlan {
    pub filter: Option<FilterExpr>,
    pub index_hint: Option<IndexHint>,
    pub key_equals: Option<KeyEquals>,
}

impl QueryPlan {
    /// A text-filter query (no explicit index).
    pub fn new(filter: Option<FilterExpr>, index_hint: Option<IndexHint>) -> Self {
        Self {
            filter,
            index_hint,
            key_equals: None,
        }
    }

    /// An exact key-equality lookup against the given index target.
    pub fn key_lookup(attribute: String, value: Value, index_hint: IndexHint) -> Self {
        Self {
            filter: None,
            index_hint: Some(index_hint),
            key_equals: Some(KeyEquals { attribute, value }),
        }
    }
}

/// An opaque pagination token. The backend defines its contents; callers only
/// store it and pass it back to fetch the next page.
#[derive(Debug, Clone, PartialEq)]
pub struct Cursor(pub Item);

/// A page request: where to resume from and how many items to fetch.
#[derive(Debug, Clone, Default)]
pub struct Page {
    pub cursor: Option<Cursor>,
    pub limit: Option<u32>,
}

/// How a query was actually served (reported after execution).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlanKind {
    /// A full collection scan.
    Scan,
    /// An indexed query; `index` is `None` for the primary key.
    IndexedQuery { index: Option<String> },
}

/// Backend-neutral cost accounting (DynamoDB consumed-capacity, etc.).
#[derive(Debug, Clone, Default)]
pub struct QueryCost {
    /// Capacity units consumed, if the backend reports them.
    pub capacity_units: Option<f64>,
}

/// The result of a [`query`](super::datastore::Datastore::query).
#[derive(Debug, Clone)]
pub struct QueryResult {
    pub items: Vec<Item>,
    pub count: u64,
    /// Items examined before filtering (DynamoDB-specific; `None` elsewhere).
    pub scanned_count: Option<u64>,
    /// Pagination token for the next page, if more results remain.
    pub next: Option<Cursor>,
    pub plan_kind: PlanKind,
    pub cost: Option<QueryCost>,
}

/// A primary-key projection identifying a single item.
#[derive(Debug, Clone, PartialEq)]
pub struct Key(pub Item);

/// The outcome of a [`batch_delete`](super::datastore::Datastore::batch_delete).
#[derive(Debug, Clone, Default)]
pub struct BatchDeleteOutcome {
    pub deleted: u64,
}

/// A pre-flight estimate of how a query would run, when a backend can provide
/// one. Lets the UI warn before an expensive full scan.
#[derive(Debug, Clone)]
pub enum PlanExplanation {
    /// The backend can't predict the plan.
    Unknown,
    /// The backend predicts this plan kind.
    Predicted(PlanKind),
}

/// A backend-neutral request to create a collection.
#[derive(Debug, Clone)]
pub struct CreateCollectionSpec {
    pub name: String,
    pub key: KeySchema,
    pub indexes: Vec<IndexSchema>,
}
