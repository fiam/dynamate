//! The DynamoDB query language behind [`QueryLanguage`].
//!
//! Wraps the existing `expr` parser, the [`completion`] engine and the
//! `builtins` dialect, with no behavior change for DynamoDB.

use crate::core::language::{
    Completion, CompletionRequest, QueryLanguage, QueryStatus, ReferenceSection,
};
use crate::core::query::PlanKind;
use crate::core::schema::CollectionSchema;
use crate::expr::error::ParseError;
use crate::expr::{
    Comparator, DynamoExpression, Operand, format, parse_dynamo_expression,
    parse_single_value_token,
};

use super::completion;
use super::table_analyzer::{QueryType, TableInfo};

/// The DynamoDB filter-expression language.
pub struct DynamoLanguage;

impl QueryLanguage for DynamoLanguage {
    fn placeholder(&self, schema: Option<&CollectionSchema>) -> String {
        let hash_key = schema
            .and_then(|s| s.key.partition_key())
            .unwrap_or("key")
            .to_string();
        format!(
            "{hash_key} = \"USER#123\"   ·   AND / OR / NOT / BETWEEN / IN   ·   ^g for functions & full reference"
        )
    }

    fn validate(&self, text: &str, schema: Option<&CollectionSchema>) -> QueryStatus {
        if text.trim().is_empty() {
            return QueryStatus::Empty;
        }
        match parse_query_classified(text, hash_key(schema)) {
            Ok(expr) => QueryStatus::Valid {
                plan_kind: predict_plan_kind(&expr, schema),
            },
            Err(ParseErrorKind::Incomplete) => QueryStatus::Incomplete,
            Err(ParseErrorKind::Invalid(message)) => QueryStatus::Invalid(message),
        }
    }

    fn complete(&self, req: &CompletionRequest<'_>) -> Completion {
        completion::suggest(req, crate::expr::builtins::default_dialect())
    }

    fn summarize(&self, text: &str, schema: Option<&CollectionSchema>) -> Option<String> {
        let trimmed = text.trim();
        if trimmed.is_empty() {
            return None;
        }
        match parse_query_text(trimmed, hash_key(schema)) {
            Ok(expr) => Some(format::format_query_summary(&expr)),
            Err(_) => Some(trimmed.to_string()),
        }
    }

    fn reference(&self) -> Vec<ReferenceSection> {
        let dialect = crate::expr::builtins::default_dialect();
        vec![
            ReferenceSection {
                heading: "Functions".to_string(),
                entries: dialect
                    .functions
                    .iter()
                    .map(|f| (f.signature.to_string(), f.summary.to_string()))
                    .collect(),
            },
            ReferenceSection {
                heading: "Operators".to_string(),
                entries: dialect
                    .operators
                    .iter()
                    .map(|o| (o.symbols.to_string(), o.summary.to_string()))
                    .collect(),
            },
            ReferenceSection {
                heading: "Keywords".to_string(),
                entries: dialect
                    .keywords
                    .iter()
                    .map(|k| (k.word.to_string(), k.summary.to_string()))
                    .collect(),
            },
            ReferenceSection {
                heading: "Value forms".to_string(),
                entries: dialect
                    .value_forms
                    .iter()
                    .map(|(form, desc)| ((*form).to_string(), (*desc).to_string()))
                    .collect(),
            },
            ReferenceSection {
                heading: "Single-token shortcut".to_string(),
                entries: dialect
                    .pk_shortcut
                    .iter()
                    .map(|(input, expands)| ((*input).to_string(), (*expands).to_string()))
                    .collect(),
            },
        ]
    }
}

fn hash_key(schema: Option<&CollectionSchema>) -> Option<&str> {
    schema.and_then(|s| s.key.partition_key())
}

/// Classification of a parse failure for the hint line.
enum ParseErrorKind {
    Incomplete,
    Invalid(String),
}

/// Parse query text into a [`DynamoExpression`], applying the single-token
/// partition-key shortcut (`foo` → `<hash_key> = "foo"`) on failure. Shared by
/// the backend's `query()` and this language's `validate`/`summarize`.
pub fn parse_query_text(text: &str, hash_key: Option<&str>) -> Result<DynamoExpression, String> {
    parse_query_classified(text, hash_key).map_err(|err| match err {
        ParseErrorKind::Incomplete => "incomplete query".to_string(),
        ParseErrorKind::Invalid(message) => message,
    })
}

fn parse_query_classified(
    text: &str,
    hash_key: Option<&str>,
) -> Result<DynamoExpression, ParseErrorKind> {
    match parse_dynamo_expression(text) {
        Ok(expr) => Ok(expr),
        Err(parse_error) => {
            // Single bare token → partition-key equality shortcut.
            if let Ok(value) = parse_single_value_token(text)
                && let Some(hash_key) = hash_key
            {
                return Ok(DynamoExpression::Comparison {
                    left: Operand::Path(hash_key.to_string()),
                    operator: Comparator::Equal,
                    right: value,
                });
            }
            if parse_error_is_incomplete(&parse_error) {
                Err(ParseErrorKind::Incomplete)
            } else {
                Err(ParseErrorKind::Invalid(parse_error.to_string()))
            }
        }
    }
}

fn parse_error_is_incomplete(err: &ParseError) -> bool {
    match err {
        ParseError::UnexpectedEndOfInput { .. } | ParseError::UnterminatedQuote { .. } => true,
        ParseError::UnexpectedToken { token, .. } => token == "EOF",
        _ => false,
    }
}

/// Predict whether the parsed query runs as an indexed Query or a full Scan.
pub fn predict_plan_kind(expr: &DynamoExpression, schema: Option<&CollectionSchema>) -> PlanKind {
    let Some(schema) = schema else {
        return PlanKind::Scan;
    };
    let table_info = TableInfo::from_collection_schema(schema);
    match table_info.analyze_query_type(expr) {
        QueryType::TableQuery { .. } => PlanKind::IndexedQuery { index: None },
        QueryType::GlobalSecondaryIndexQuery { index_name, .. }
        | QueryType::LocalSecondaryIndexQuery { index_name, .. } => PlanKind::IndexedQuery {
            index: Some(index_name),
        },
        QueryType::TableScan => PlanKind::Scan,
    }
}
