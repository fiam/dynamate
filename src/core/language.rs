//! The per-backend query language.
//!
//! Each backend owns its own query syntax, autocompletion, validation and
//! compilation — a DynamoDB user writes filter expressions (`begins_with(SK, …)`)
//! while a MongoDB user writes filter documents (`{ age: { $gt: 21 } }`). The UI
//! never parses the query itself; it asks the active datastore's
//! [`QueryLanguage`] (via [`Datastore::query_language`]) to validate the text for
//! the hint line, produce autocompletion, summarize it, and supply the reference
//! popup. The raw text travels to the backend in
//! [`QueryPlan::filter`](super::query::QueryPlan), which the backend parses.
//!
//! [`Datastore::query_language`]: super::datastore::Datastore::query_language

use super::query::PlanKind;
use super::schema::{CollectionSchema, SchemaHints};

/// Byte offsets into the input string delimiting the token under the cursor
/// (the span a chosen suggestion replaces).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TokenSpan {
    pub start: usize,
    pub end: usize,
}

/// The category of a completion suggestion (drives its icon/colour).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SuggestionKind {
    Function,
    Keyword,
    Operator,
    /// A field / attribute name.
    Field,
    /// A literal value (or one chunk of one).
    Value,
}

/// A single completion suggestion.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Suggestion {
    /// Text inserted when the suggestion is accepted.
    pub text: String,
    pub kind: SuggestionKind,
    /// Short description shown dimmed next to the suggestion.
    pub detail: String,
}

/// The result of [`QueryLanguage::complete`]: the token span under the cursor and
/// the ranked suggestions for it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Completion {
    pub span: TokenSpan,
    pub suggestions: Vec<Suggestion>,
}

/// Validity of the current query text, for the live hint line under the box.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryStatus {
    /// Empty input — the UI shows the placeholder.
    Empty,
    /// Parseable but unfinished — don't show an error yet.
    Incomplete,
    /// Invalid, with a human-readable reason.
    Invalid(String),
    /// Valid; `plan_kind` previews how it would run (scan vs indexed).
    Valid { plan_kind: PlanKind },
}

/// Inputs to [`QueryLanguage::complete`].
pub struct CompletionRequest<'a> {
    pub text: &'a str,
    pub cursor: usize,
    /// Field/attribute names observed in the loaded items.
    pub attributes: &'a [String],
    /// Observed values for a given field, used to complete literals.
    pub value_lookup: &'a dyn Fn(&str) -> Vec<String>,
    /// The collection schema, when known.
    pub schema: Option<&'a CollectionSchema>,
    /// Database-level table/column hints, for the free-form SQL query view.
    /// `None` outside that view.
    pub sql_hints: Option<&'a SchemaHints>,
}

/// A section of the in-app query reference popup.
#[derive(Debug, Clone)]
pub struct ReferenceSection {
    pub heading: String,
    /// `(syntax, description)` rows.
    pub entries: Vec<(String, String)>,
}

/// A backend's query language: parsing/validation, autocompletion, summarizing,
/// and reference docs. All methods are synchronous and pure (no I/O) so the UI
/// can call them on the render/keystroke path.
pub trait QueryLanguage: Send + Sync {
    /// One-line example/placeholder shown when the query box is empty.
    fn placeholder(&self, schema: Option<&CollectionSchema>) -> String;

    /// Validate the query text for the live hint line.
    fn validate(&self, text: &str, schema: Option<&CollectionSchema>) -> QueryStatus;

    /// Autocompletion suggestions for the token under the cursor.
    fn complete(&self, req: &CompletionRequest<'_>) -> Completion;

    /// A compact, normalized rendering of the query (for footers / filenames).
    fn summarize(&self, text: &str, schema: Option<&CollectionSchema>) -> Option<String>;

    /// Content for the in-app reference popup.
    fn reference(&self) -> Vec<ReferenceSection>;
}
