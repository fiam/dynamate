//! The backend-neutral value model.
//!
//! [`Value`] is a superset of every DynamoDB `AttributeValue` variant and maps
//! cleanly onto MongoDB BSON and Firestore value types. It intentionally does
//! **not** reuse `serde_json::Value`, which cannot represent binary data, the
//! three set types, or arbitrary-precision numbers — all of which Dynamate
//! already relies on.
//!
//! Numbers are stored as their canonical decimal string (see [`Number`]) so
//! that DynamoDB's up-to-38-digit precision survives a round trip; parsing to
//! `i64`/`f64` happens only at the edges that need it.

use std::fmt;

use indexmap::IndexMap;

/// An ordered collection of named attributes — the neutral representation of a
/// single record/item/document.
///
/// Order is preserved (unlike the `HashMap<String, AttributeValue>` used at the
/// SDK boundary today) so JSON-editor round-trips stay diff-stable.
pub type Item = IndexMap<String, Value>;

/// A backend-neutral attribute value.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    Str(String),
    Num(Number),
    Bytes(Vec<u8>),
    List(Vec<Value>),
    Map(Item),
    /// String set. Backends without set types simply never produce this.
    StringSet(Vec<String>),
    /// Number set.
    NumberSet(Vec<Number>),
    /// Binary set.
    BytesSet(Vec<Vec<u8>>),
}

/// A number preserved verbatim as its decimal string.
///
/// Keeping the original text (rather than an `f64`) preserves precision for
/// DynamoDB's arbitrary-precision `N` type. Use [`Number::as_i64`] /
/// [`Number::as_f64`] when a parsed value is actually needed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Number(String);

impl Number {
    /// Wrap an existing decimal string without validating it.
    pub fn new(text: impl Into<String>) -> Self {
        Self(text.into())
    }

    /// The canonical decimal string.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Parse as an `i64`, if it fits and is integral.
    pub fn as_i64(&self) -> Option<i64> {
        self.0.parse().ok()
    }

    /// Parse as an `f64` (may lose precision).
    pub fn as_f64(&self) -> Option<f64> {
        self.0.parse().ok()
    }

    /// Consume into the underlying string.
    pub fn into_string(self) -> String {
        self.0
    }
}

impl fmt::Display for Number {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<i64> for Number {
    fn from(value: i64) -> Self {
        Self(value.to_string())
    }
}

impl From<i64> for Value {
    fn from(value: i64) -> Self {
        Value::Num(Number::from(value))
    }
}

impl From<bool> for Value {
    fn from(value: bool) -> Self {
        Value::Bool(value)
    }
}

impl From<String> for Value {
    fn from(value: String) -> Self {
        Value::Str(value)
    }
}

impl From<&str> for Value {
    fn from(value: &str) -> Self {
        Value::Str(value.to_string())
    }
}

impl Value {
    /// Borrow the inner string, if this is a [`Value::Str`].
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::Str(s) => Some(s),
            _ => None,
        }
    }

    /// Borrow the inner number, if this is a [`Value::Num`].
    pub fn as_number(&self) -> Option<&Number> {
        match self {
            Value::Num(n) => Some(n),
            _ => None,
        }
    }
}
