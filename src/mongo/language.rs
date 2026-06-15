//! The MongoDB query language: the query box is a JSON / MongoDB-extended-JSON
//! filter document (`{ "age": { "$gt": 21 } }`).

use mongodb::bson::{Bson, Document};

use crate::core::language::{
    Completion, CompletionRequest, QueryLanguage, QueryStatus, ReferenceSection, Suggestion,
    SuggestionKind, TokenSpan,
};
use crate::core::query::PlanKind;
use crate::core::schema::CollectionSchema;

/// MongoDB query operators offered by autocompletion / the reference popup.
const OPERATORS: &[(&str, &str)] = &[
    ("$eq", "Matches values equal to a value"),
    ("$ne", "Matches values not equal to a value"),
    ("$gt", "Greater than"),
    ("$gte", "Greater than or equal"),
    ("$lt", "Less than"),
    ("$lte", "Less than or equal"),
    ("$in", "Matches any value in an array"),
    ("$nin", "Matches none of the values in an array"),
    ("$exists", "Matches documents that have the field"),
    ("$regex", "Matches a regular expression"),
    ("$and", "Joins clauses with a logical AND"),
    ("$or", "Joins clauses with a logical OR"),
    ("$not", "Inverts the effect of a query expression"),
    ("$type", "Matches a BSON type"),
];

/// The MongoDB filter-document language.
pub struct MongoLanguage;

impl QueryLanguage for MongoLanguage {
    fn placeholder(&self, _schema: Option<&CollectionSchema>) -> String {
        "{ \"field\": value }   ·   $gt $gte $lt $in $regex $exists   ·   ^g for the operator reference".to_string()
    }

    fn validate(&self, text: &str, schema: Option<&CollectionSchema>) -> QueryStatus {
        let trimmed = text.trim();
        if trimmed.is_empty() {
            return QueryStatus::Empty;
        }
        match serde_json::from_str::<serde_json::Value>(trimmed) {
            Ok(serde_json::Value::Object(map)) => QueryStatus::Valid {
                plan_kind: plan_kind_for(&map, schema),
            },
            Ok(_) => QueryStatus::Invalid("filter must be a JSON object".to_string()),
            Err(err) => {
                // A document still being typed isn't an error yet.
                if trimmed.ends_with('}') {
                    QueryStatus::Invalid(err.to_string())
                } else {
                    QueryStatus::Incomplete
                }
            }
        }
    }

    fn complete(&self, req: &CompletionRequest<'_>) -> Completion {
        let span = token_span(req.text, req.cursor);
        let prefix = &req.text[span.start..req.cursor.min(span.end).max(span.start)];

        let mut suggestions = Vec::new();
        // Operators (when typing a `$...` key). Skip on an empty token to avoid
        // dumping the whole list before the user has typed anything.
        if !prefix.is_empty() {
            for (op, detail) in OPERATORS {
                if op.starts_with(prefix) {
                    suggestions.push(Suggestion {
                        text: (*op).to_string(),
                        kind: SuggestionKind::Operator,
                        detail: (*detail).to_string(),
                    });
                }
            }
        }
        // Field names from the observed documents.
        if !prefix.starts_with('$') {
            for field in req.attributes {
                if prefix.is_empty() || field.starts_with(prefix) {
                    suggestions.push(Suggestion {
                        text: field.clone(),
                        kind: SuggestionKind::Field,
                        detail: "field".to_string(),
                    });
                }
            }
        }
        suggestions.truncate(8);
        Completion { span, suggestions }
    }

    fn summarize(&self, text: &str, _schema: Option<&CollectionSchema>) -> Option<String> {
        let trimmed = text.trim();
        if trimmed.is_empty() {
            return None;
        }
        match serde_json::from_str::<serde_json::Value>(trimmed) {
            Ok(value) => Some(value.to_string()),
            Err(_) => Some(trimmed.to_string()),
        }
    }

    fn reference(&self) -> Vec<ReferenceSection> {
        vec![
            ReferenceSection {
                heading: "Filter".to_string(),
                entries: vec![
                    ("{ field: value }".to_string(), "Equality match".to_string()),
                    (
                        "{ field: { $gt: 1 } }".to_string(),
                        "Operator match".to_string(),
                    ),
                    (
                        "{ $or: [ {..}, {..} ] }".to_string(),
                        "Logical combination".to_string(),
                    ),
                    (
                        "{ }  (or blank)".to_string(),
                        "Match all (full scan)".to_string(),
                    ),
                ],
            },
            ReferenceSection {
                heading: "Operators".to_string(),
                entries: OPERATORS
                    .iter()
                    .map(|(op, desc)| ((*op).to_string(), (*desc).to_string()))
                    .collect(),
            },
        ]
    }
}

/// Parse the query-box text into a MongoDB filter document (extended JSON
/// supported). Empty text matches all. Used by the backend's `query()`.
pub fn parse_filter(text: &str) -> Result<Document, String> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Ok(Document::new());
    }
    let json: serde_json::Value = serde_json::from_str(trimmed).map_err(|err| err.to_string())?;
    match Bson::try_from(json).map_err(|err| err.to_string())? {
        Bson::Document(doc) => Ok(doc),
        _ => Err("filter must be a JSON object".to_string()),
    }
}

fn plan_kind_for(
    map: &serde_json::Map<String, serde_json::Value>,
    schema: Option<&CollectionSchema>,
) -> PlanKind {
    if map.contains_key("_id") {
        return PlanKind::IndexedQuery { index: None };
    }
    if let Some(schema) = schema {
        for index in &schema.indexes {
            if let Some(field) = index.key.partition_key()
                && map.contains_key(field)
            {
                return PlanKind::IndexedQuery {
                    index: Some(index.name.clone()),
                };
            }
        }
    }
    PlanKind::Scan
}

/// Token characters in a Mongo filter: field/operator name chars.
fn is_token_char(c: char) -> bool {
    c.is_alphanumeric() || matches!(c, '_' | '$' | '.')
}

fn token_span(input: &str, cursor: usize) -> TokenSpan {
    let cursor = cursor.min(input.len());
    let mut start = cursor;
    for (idx, ch) in input[..cursor].char_indices().rev() {
        if is_token_char(ch) {
            start = idx;
        } else {
            break;
        }
    }
    let mut end = cursor;
    for (idx, ch) in input[cursor..].char_indices() {
        if is_token_char(ch) {
            end = cursor + idx + ch.len_utf8();
        } else {
            break;
        }
    }
    TokenSpan { start, end }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn req<'a>(
        text: &'a str,
        attrs: &'a [String],
        lookup: &'a dyn Fn(&str) -> Vec<String>,
    ) -> CompletionRequest<'a> {
        CompletionRequest {
            text,
            cursor: text.len(),
            attributes: attrs,
            value_lookup: lookup,
            schema: None,
        }
    }

    #[test]
    fn empty_is_empty_status() {
        assert!(matches!(
            MongoLanguage.validate("  ", None),
            QueryStatus::Empty
        ));
    }

    #[test]
    fn valid_object_is_valid() {
        let status = MongoLanguage.validate(r#"{ "age": { "$gt": 21 } }"#, None);
        assert!(matches!(status, QueryStatus::Valid { .. }));
    }

    #[test]
    fn unfinished_is_incomplete_not_invalid() {
        assert!(matches!(
            MongoLanguage.validate(r#"{ "age": "#, None),
            QueryStatus::Incomplete
        ));
    }

    #[test]
    fn non_object_is_invalid() {
        assert!(matches!(
            MongoLanguage.validate("42", None),
            QueryStatus::Invalid(_)
        ));
    }

    #[test]
    fn id_filter_is_indexed() {
        let status = MongoLanguage.validate(r#"{ "_id": 1 }"#, None);
        assert!(matches!(
            status,
            QueryStatus::Valid {
                plan_kind: PlanKind::IndexedQuery { index: None }
            }
        ));
    }

    #[test]
    fn match_all_is_scan() {
        let status = MongoLanguage.validate("{}", None);
        assert_eq!(
            status,
            QueryStatus::Valid {
                plan_kind: PlanKind::Scan
            }
        );
    }

    #[test]
    fn completion_offers_operators_for_dollar_prefix() {
        let lookup = |_: &str| Vec::new();
        let attrs = vec!["name".to_string()];
        let completion = MongoLanguage.complete(&req(r#"{ "age": { "$g"#, &attrs, &lookup));
        assert!(completion.suggestions.iter().any(|s| s.text == "$gt"));
    }

    #[test]
    fn completion_offers_fields() {
        let lookup = |_: &str| Vec::new();
        let attrs = vec!["name".to_string(), "age".to_string()];
        let completion = MongoLanguage.complete(&req(r#"{ "na"#, &attrs, &lookup));
        assert!(completion.suggestions.iter().any(|s| s.text == "name"));
    }

    #[test]
    fn parse_filter_handles_empty_and_object() {
        assert!(parse_filter("  ").unwrap().is_empty());
        let doc = parse_filter(r#"{ "x": 1 }"#).unwrap();
        assert_eq!(
            doc.get_i32("x")
                .ok()
                .or_else(|| doc.get_i64("x").ok().map(|v| v as i32)),
            Some(1)
        );
    }
}
