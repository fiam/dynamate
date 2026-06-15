//! Autocompletion engine for the DynamoDB query language.
//!
//! Pure logic (no UI): given the input text, the cursor byte offset, the known
//! field names and a value-lookup, it returns the token span under the cursor
//! and a ranked list of suggestions (functions, keywords, operators, fields,
//! values). Driven by the DynamoDB [`Dialect`]. Moved out of the UI layer so it
//! can sit behind [`QueryLanguage`](crate::core::language::QueryLanguage).

use std::collections::HashSet;

use crate::core::language::{Completion, CompletionRequest, Suggestion, SuggestionKind, TokenSpan};
use crate::expr::builtins::Dialect;

/// Entry point used by the DynamoDB [`QueryLanguage`](super::language::DynamoLanguage).
pub fn suggest(req: &CompletionRequest<'_>, dialect: &Dialect) -> Completion {
    let (span, suggestions) = suggestions(
        req.text,
        req.cursor,
        req.attributes,
        dialect,
        req.value_lookup,
    );
    Completion { span, suggestions }
}

const OPERATORS: &[&str] = &["=", "!=", "<", "<=", ">", ">="];
const MAX_SUGGESTIONS: usize = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Context {
    PathStart,
    ValueStart,
    AfterPath,
    AfterValue,
    Unknown,
}

/// Mirror of the lexer's bare-token rule (`expr::lexer::is_bare_token_char`).
fn is_bare_token_char(c: char) -> bool {
    !c.is_whitespace()
        && !matches!(
            c,
            '(' | ')' | ',' | '=' | '!' | '<' | '>' | '"' | '\'' | '`'
        )
}

/// Find the run of bare-token characters spanning the cursor.
pub fn token_under_cursor(input: &str, cursor: usize) -> TokenSpan {
    let cursor = cursor.min(input.len());
    let mut start = cursor;
    for (idx, ch) in input[..cursor].char_indices().rev() {
        if is_bare_token_char(ch) {
            start = idx;
        } else {
            break;
        }
    }
    let mut end = cursor;
    for (idx, ch) in input[cursor..].char_indices() {
        if is_bare_token_char(ch) {
            end = cursor + idx + ch.len_utf8();
        } else {
            break;
        }
    }
    TokenSpan { start, end }
}

fn last_bare_token(s: &str) -> Option<&str> {
    let s = s.trim_end();
    if s.is_empty() {
        return None;
    }
    let mut start = s.len();
    for (idx, ch) in s.char_indices().rev() {
        if is_bare_token_char(ch) {
            start = idx;
        } else {
            break;
        }
    }
    if start == s.len() {
        None
    } else {
        Some(&s[start..])
    }
}

fn last_token_and_rest(s: &str) -> Option<(&str, &str)> {
    let s = s.trim_end();
    let mut start = s.len();
    for (i, ch) in s.char_indices().rev() {
        if is_bare_token_char(ch) {
            start = i;
        } else {
            break;
        }
    }
    if start == s.len() {
        None
    } else {
        Some((&s[start..], &s[..start]))
    }
}

fn detect_context(before: &str, dialect: &Dialect) -> Context {
    let before = before.trim_end();
    let Some(last) = before.chars().last() else {
        return Context::PathStart;
    };
    match last {
        '(' => Context::PathStart,
        ',' | '=' | '!' | '<' | '>' => Context::ValueStart,
        '"' | '\'' | ')' => Context::AfterValue,
        _ => match last_token_and_rest(before) {
            Some((tok, rest)) => match tok.to_ascii_uppercase().as_str() {
                "AND" | "OR" | "NOT" => Context::PathStart,
                "BETWEEN" | "IN" => Context::ValueStart,
                _ if dialect.is_function_name(tok) => Context::PathStart,
                _ => {
                    let rest = rest.trim_end();
                    if rest.ends_with(['=', '!', '<', '>']) || rest.ends_with(')') {
                        Context::AfterValue
                    } else {
                        Context::AfterPath
                    }
                }
            },
            None => Context::Unknown,
        },
    }
}

fn prefix_matches(candidate: &str, prefix: &str) -> bool {
    prefix.is_empty()
        || candidate
            .to_ascii_lowercase()
            .starts_with(&prefix.to_ascii_lowercase())
}

fn strip_trailing_quote(before: &str) -> &str {
    before
        .strip_suffix('"')
        .or_else(|| before.strip_suffix('\''))
        .unwrap_or(before)
        .trim_end()
}

fn comparison_value_path(before: &str) -> Option<String> {
    let s = strip_trailing_quote(before);
    let without_op = s.trim_end_matches(['=', '!', '<', '>']);
    if without_op.len() == s.len() {
        return None;
    }
    last_bare_token(without_op).map(std::string::ToString::to_string)
}

struct FuncArg {
    takes_type_code: bool,
    past_first: bool,
    first_path: Option<String>,
}

fn function_arg(before: &str, dialect: &Dialect) -> Option<FuncArg> {
    let s = strip_trailing_quote(before);
    let mut depth = 0i32;
    let mut open = None;
    for (i, c) in s.char_indices().rev() {
        match c {
            ')' => depth += 1,
            '(' => {
                if depth == 0 {
                    open = Some(i);
                    break;
                }
                depth -= 1;
            }
            _ => {}
        }
    }
    let open = open?;
    let func = last_bare_token(s[..open].trim_end())?;
    let doc = dialect.function_by_name(func)?;
    let inner = &s[open + 1..];
    let (past_first, first_path) = match inner.find(',') {
        Some(c) => {
            let arg = inner[..c].trim().trim_matches('`').trim();
            (true, (!arg.is_empty()).then(|| arg.to_string()))
        }
        None => (false, None),
    };
    Some(FuncArg {
        takes_type_code: doc.takes_type_code,
        past_first,
        first_path,
    })
}

fn push_value_chunks(out: &mut Vec<Suggestion>, values: &[String], prefix: &str) {
    let lower_prefix = prefix.to_ascii_lowercase();
    let mut seen: HashSet<String> = HashSet::new();
    for v in values {
        if !v.to_ascii_lowercase().starts_with(&lower_prefix) {
            continue;
        }
        let rest_start = prefix.len();
        if !v.is_char_boundary(rest_start) {
            continue;
        }
        let chunk_end = match v[rest_start..].find('#') {
            Some(i) => rest_start + i + 1,
            None => v.len(),
        };
        if chunk_end <= prefix.len() {
            continue;
        }
        let text = v[..chunk_end].to_string();
        if !seen.insert(text.clone()) {
            continue;
        }
        out.push(Suggestion {
            text,
            kind: SuggestionKind::Value,
            detail: "value".to_string(),
        });
        if seen.len() >= MAX_SUGGESTIONS {
            break;
        }
    }
}

/// Compute the token span under the cursor and the ranked suggestion list.
pub fn suggestions(
    input: &str,
    cursor: usize,
    attrs: &[String],
    dialect: &Dialect,
    value_lookup: impl Fn(&str) -> Vec<String>,
) -> (TokenSpan, Vec<Suggestion>) {
    let span = token_under_cursor(input, cursor);
    let prefix = &input[span.start..cursor.min(span.end).max(span.start)];
    let before = &input[..span.start];

    let mut out: Vec<Suggestion> = Vec::new();

    let push_attrs = |out: &mut Vec<Suggestion>| {
        for name in attrs {
            if prefix_matches(name, prefix) {
                out.push(Suggestion {
                    text: name.clone(),
                    kind: SuggestionKind::Field,
                    detail: "field".to_string(),
                });
            }
        }
    };
    let push_functions = |out: &mut Vec<Suggestion>| {
        for f in dialect.functions {
            if prefix_matches(f.name, prefix) {
                out.push(Suggestion {
                    text: format!("{}(", f.name),
                    kind: SuggestionKind::Function,
                    detail: f.signature.to_string(),
                });
            }
        }
    };
    let push_keywords = |out: &mut Vec<Suggestion>, words: &[&str]| {
        for w in words {
            if prefix_matches(w, prefix) {
                out.push(Suggestion {
                    text: w.to_string(),
                    kind: SuggestionKind::Keyword,
                    detail: "keyword".to_string(),
                });
            }
        }
    };
    let push_operators = |out: &mut Vec<Suggestion>| {
        for op in OPERATORS {
            if prefix_matches(op, prefix) {
                out.push(Suggestion {
                    text: op.to_string(),
                    kind: SuggestionKind::Operator,
                    detail: "operator".to_string(),
                });
            }
        }
    };
    let push_value_chunks_for = |out: &mut Vec<Suggestion>, path: &str| {
        let values = value_lookup(path);
        push_value_chunks(out, &values, prefix);
    };

    if let Some(func_arg) = function_arg(before, dialect) {
        if !func_arg.past_first {
            push_attrs(&mut out);
        } else if func_arg.takes_type_code {
            for ty in dialect.type_codes {
                if prefix_matches(ty, prefix) {
                    out.push(Suggestion {
                        text: (*ty).to_string(),
                        kind: SuggestionKind::Value,
                        detail: "type".to_string(),
                    });
                }
            }
        } else if let Some(path) = func_arg.first_path {
            push_value_chunks_for(&mut out, &path);
        }
        out.truncate(MAX_SUGGESTIONS);
        return (span, out);
    }

    if let Some(path) = comparison_value_path(before) {
        push_value_chunks_for(&mut out, &path);
        if !prefix.is_empty() {
            push_keywords(&mut out, &["true", "false", "null"]);
        }
        out.truncate(MAX_SUGGESTIONS);
        return (span, out);
    }

    match detect_context(before, dialect) {
        Context::AfterPath => {
            push_operators(&mut out);
            push_keywords(&mut out, &["BETWEEN", "IN"]);
        }
        Context::AfterValue => {
            push_keywords(&mut out, &["AND", "OR"]);
        }
        Context::ValueStart => {
            if !prefix.is_empty() {
                push_keywords(&mut out, &["true", "false", "null"]);
            }
        }
        Context::PathStart | Context::Unknown => {
            if !prefix.is_empty() {
                push_attrs(&mut out);
                push_functions(&mut out);
                push_keywords(&mut out, &["NOT"]);
            }
        }
    }

    out.truncate(MAX_SUGGESTIONS);
    (span, out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dialect() -> &'static Dialect {
        crate::expr::builtins::default_dialect()
    }

    fn attrs() -> Vec<String> {
        vec![
            "PK".to_string(),
            "SK".to_string(),
            "status".to_string(),
            "started_at".to_string(),
        ]
    }

    fn no_values(_: &str) -> Vec<String> {
        Vec::new()
    }

    fn pk_values(path: &str) -> Vec<String> {
        if path == "PK" || path == "SK" {
            vec![
                "USAGETHRESHOLDS#hilti".to_string(),
                "USAGETHRESHOLDS#acme".to_string(),
                "USER#1".to_string(),
            ]
        } else {
            Vec::new()
        }
    }

    #[test]
    fn token_span_basic() {
        let input = "status = active";
        let span = token_under_cursor(input, 3);
        assert_eq!(&input[span.start..span.end], "status");
    }

    #[test]
    fn token_span_multibyte() {
        let input = "café = 1";
        let cursor = "café".len();
        let span = token_under_cursor(input, cursor);
        assert_eq!(&input[span.start..span.end], "café");
    }

    #[test]
    fn token_span_on_whitespace_is_empty() {
        let input = "status ";
        let span = token_under_cursor(input, input.len());
        assert_eq!(span.start, span.end);
    }

    #[test]
    fn suggests_functions_and_attrs_by_prefix() {
        let (_, s) = suggestions("sta", 3, &attrs(), dialect(), no_values);
        let texts: Vec<&str> = s.iter().map(|x| x.text.as_str()).collect();
        assert!(texts.contains(&"status"));
        assert!(texts.contains(&"started_at"));
    }

    #[test]
    fn suggests_function_prefix() {
        let (_, s) = suggestions("beg", 3, &attrs(), dialect(), no_values);
        assert!(s.iter().any(|x| x.text == "begins_with("));
    }

    #[test]
    fn inside_function_suggests_attributes() {
        let input = "begins_with(S";
        let (span, s) = suggestions(input, input.len(), &attrs(), dialect(), no_values);
        assert_eq!(&input[span.start..span.end], "S");
        let texts: Vec<&str> = s.iter().map(|x| x.text.as_str()).collect();
        assert!(texts.contains(&"SK"));
        assert!(s.iter().all(|x| x.kind != SuggestionKind::Function));
    }

    #[test]
    fn after_path_suggests_operators() {
        let (_, s) = suggestions("status ", "status ".len(), &attrs(), dialect(), no_values);
        assert!(
            s.iter()
                .any(|x| x.text == "=" && x.kind == SuggestionKind::Operator)
        );
        assert!(s.iter().any(|x| x.text == "BETWEEN"));
        assert!(s.iter().all(|x| x.text != "AND"));
    }

    #[test]
    fn after_completed_comparison_suggests_connectors() {
        let input = "PK=ACCOUNT#minervaproject ";
        let (_, s) = suggestions(input, input.len(), &attrs(), dialect(), pk_values);
        let texts: Vec<&str> = s.iter().map(|x| x.text.as_str()).collect();
        assert!(texts.contains(&"AND"));
        assert!(texts.contains(&"OR"));
        assert!(s.iter().all(|x| x.kind != SuggestionKind::Operator));
    }

    #[test]
    fn after_keyword_suggests_operands() {
        let input = "verified = true AND sta";
        let (_, s) = suggestions(input, input.len(), &attrs(), dialect(), no_values);
        let texts: Vec<&str> = s.iter().map(|x| x.text.as_str()).collect();
        assert!(texts.contains(&"status"));
        assert!(texts.contains(&"started_at"));
    }

    #[test]
    fn value_position_offers_no_attributes() {
        let (_, s) = suggestions("PK=AC", "PK=AC".len(), &attrs(), dialect(), no_values);
        assert!(s.iter().all(|x| x.kind != SuggestionKind::Field));
    }

    #[test]
    fn value_position_offers_literals_by_prefix() {
        let input = "verified = tr";
        let (_, s) = suggestions(input, input.len(), &attrs(), dialect(), no_values);
        assert!(s.iter().any(|x| x.text == "true"));
        assert!(s.iter().all(|x| x.kind != SuggestionKind::Field));
    }

    #[test]
    fn value_chunks_advance_by_hash() {
        let input = "PK = US";
        let (_, s) = suggestions(input, input.len(), &attrs(), dialect(), pk_values);
        let texts: Vec<&str> = s.iter().map(|x| x.text.as_str()).collect();
        assert!(texts.contains(&"USAGETHRESHOLDS#"));
        assert!(texts.contains(&"USER#"));
        assert_eq!(
            texts.iter().filter(|t| **t == "USAGETHRESHOLDS#").count(),
            1
        );
        assert!(s.iter().all(|x| x.kind != SuggestionKind::Field));
    }

    #[test]
    fn value_chunks_complete_next_segment() {
        let input = "PK = USAGETHRESHOLDS#";
        let (_, s) = suggestions(input, input.len(), &attrs(), dialect(), pk_values);
        let texts: Vec<&str> = s.iter().map(|x| x.text.as_str()).collect();
        assert!(texts.contains(&"USAGETHRESHOLDS#hilti"));
        assert!(texts.contains(&"USAGETHRESHOLDS#acme"));
        assert!(!texts.contains(&"USER#1"));
    }

    #[test]
    fn value_chunks_empty_prefix_offers_first_segments() {
        let input = "PK = ";
        let (_, s) = suggestions(input, input.len(), &attrs(), dialect(), pk_values);
        let texts: Vec<&str> = s.iter().map(|x| x.text.as_str()).collect();
        assert!(texts.contains(&"USAGETHRESHOLDS#"));
        assert!(texts.contains(&"USER#"));
    }

    #[test]
    fn begins_with_value_arg_offers_value_chunks() {
        let input = "begins_with(SK, \"US";
        let (_, s) = suggestions(input, input.len(), &attrs(), dialect(), pk_values);
        let texts: Vec<&str> = s.iter().map(|x| x.text.as_str()).collect();
        assert!(texts.contains(&"USAGETHRESHOLDS#"));
        assert!(texts.contains(&"USER#"));
        assert!(
            s.iter()
                .all(|x| x.kind != SuggestionKind::Field && x.kind != SuggestionKind::Function)
        );
    }

    #[test]
    fn begins_with_first_arg_offers_attributes() {
        let input = "begins_with(S";
        let (_, s) = suggestions(input, input.len(), &attrs(), dialect(), pk_values);
        let texts: Vec<&str> = s.iter().map(|x| x.text.as_str()).collect();
        assert!(texts.contains(&"SK"));
        assert!(s.iter().all(|x| x.kind != SuggestionKind::Function));
    }

    #[test]
    fn combined_with_and_completes_function_value() {
        let input = "PK = \"USER#1\" AND begins_with(SK, \"USAGETHRESHOLDS#";
        let (_, s) = suggestions(input, input.len(), &attrs(), dialect(), pk_values);
        let texts: Vec<&str> = s.iter().map(|x| x.text.as_str()).collect();
        assert!(texts.contains(&"USAGETHRESHOLDS#hilti"));
        assert!(texts.contains(&"USAGETHRESHOLDS#acme"));
    }

    #[test]
    fn attribute_type_offers_type_codes() {
        let input = "attribute_type(age, \"N";
        let (_, s) = suggestions(input, input.len(), &attrs(), dialect(), no_values);
        let texts: Vec<&str> = s.iter().map(|x| x.text.as_str()).collect();
        assert!(texts.contains(&"N"));
        assert!(texts.contains(&"NULL"));
        assert!(s.iter().all(|x| x.kind != SuggestionKind::Field));
    }

    #[test]
    fn quoted_comparison_value_offers_chunks() {
        let input = "PK = \"US";
        let (_, s) = suggestions(input, input.len(), &attrs(), dialect(), pk_values);
        let texts: Vec<&str> = s.iter().map(|x| x.text.as_str()).collect();
        assert!(texts.contains(&"USAGETHRESHOLDS#"));
    }

    #[test]
    fn empty_input_no_suggestions() {
        let (_, s) = suggestions("", 0, &attrs(), dialect(), no_values);
        assert!(s.is_empty());
    }

    #[test]
    fn empty_attrs_still_offers_functions() {
        let (_, s) = suggestions("att", 3, &[], dialect(), no_values);
        assert!(s.iter().any(|x| x.text == "attribute_exists("));
    }
}
