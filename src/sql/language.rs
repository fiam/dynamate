//! The SQL query language(s): a per-table `WHERE` predicate, and a database-level
//! free-form `SELECT`. One struct with a mode flag backs both.

use crate::core::language::{
    Completion, CompletionRequest, QueryLanguage, QueryStatus, ReferenceSection, Suggestion,
    SuggestionKind, TokenSpan,
};
use crate::core::query::PlanKind;
use crate::core::schema::CollectionSchema;

/// Which surface a [`SqlLanguage`] drives.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SqlLangMode {
    /// Per-table view: a `WHERE` predicate over one table.
    Filter,
    /// Database-level view: a free-form `SELECT … FROM …`.
    Query,
}

pub struct SqlLanguage {
    pub mode: SqlLangMode,
}

/// What the cursor position expects next, for context-aware SQL completion.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SqlContext {
    /// Naming a table (after `FROM` / `JOIN`).
    Table,
    /// Naming a column / starting an operand (after `SELECT`, `WHERE`, `AND`, an
    /// operator, `(`, …).
    Column,
    /// Right after `IS` / `IS NOT`: `NULL` / `TRUE` / `FALSE` / `NOT` / `DISTINCT`.
    Value,
    /// After an operand: clause keywords / operators come next.
    Keyword,
}

impl SqlLanguage {
    /// Per-table `WHERE` predicate completion: the row's columns + filter
    /// operators, on a non-empty prefix.
    fn complete_filter(&self, req: &CompletionRequest<'_>, prefix: &str) -> Vec<Suggestion> {
        let mut suggestions = Vec::new();
        if !prefix.is_empty() {
            for ident in req.attributes {
                if starts_with_ci(ident, prefix) {
                    suggestions.push(field(ident.clone(), "column"));
                }
            }
            for kw in FILTER_KEYWORDS {
                if starts_with_ci(kw, prefix) {
                    suggestions.push(keyword(kw));
                }
            }
        }
        suggestions.truncate(8);
        suggestions
    }

    /// Database-level SQL completion: offer tables after `FROM`/`JOIN`, the
    /// referenced tables' columns where an operand is expected, and clause
    /// keywords/operators after an operand.
    fn complete_query(
        &self,
        req: &CompletionRequest<'_>,
        prefix: &str,
        token_start: usize,
    ) -> Vec<Suggestion> {
        let hints = req.sql_hints;
        let mut suggestions = Vec::new();
        match classify(req.text, token_start) {
            SqlContext::Table => {
                if let Some(hints) = hints {
                    for table in &hints.tables {
                        if prefix.is_empty() || starts_with_ci(table, prefix) {
                            suggestions.push(field(table.clone(), "table"));
                        }
                    }
                }
            }
            SqlContext::Column => {
                if let Some(hints) = hints {
                    let referenced = referenced_tables(req.text);
                    for column in hints.columns_for(&referenced) {
                        if prefix.is_empty() || starts_with_ci(&column, prefix) {
                            suggestions.push(field(column, "column"));
                        }
                    }
                }
                // Literals (NULL/TRUE/FALSE) are valid operands too.
                for kw in VALUE_KEYWORDS {
                    if !prefix.is_empty() && starts_with_ci(kw, prefix) {
                        suggestions.push(keyword(kw));
                    }
                }
            }
            SqlContext::Value => {
                let tokens = tokenize(&req.text[..token_start.min(req.text.len())]);
                // After `IS NOT`, `NOT`/`DISTINCT` are no longer valid.
                let candidates: &[&str] = if tokens.last().map(String::as_str) == Some("not") {
                    &["NULL", "TRUE", "FALSE"]
                } else {
                    AFTER_IS_KEYWORDS
                };
                for kw in candidates {
                    if prefix.is_empty() || starts_with_ci(kw, prefix) {
                        suggestions.push(keyword(kw));
                    }
                }
            }
            SqlContext::Keyword => {
                // Only the keywords that can legally follow the current clause
                // (e.g. after `SELECT …` the next keyword is `FROM`).
                let tokens = tokenize(&req.text[..token_start.min(req.text.len())]);
                let prev = tokens.last().map(String::as_str);
                let clause = current_clause(req.text, token_start);
                for kw in next_keywords(clause.as_deref(), prev) {
                    if prefix.is_empty() || starts_with_ci(kw, prefix) {
                        suggestions.push(keyword(kw));
                    }
                }
            }
        }
        suggestions.truncate(10);
        suggestions
    }
}

fn field(text: String, detail: &str) -> Suggestion {
    Suggestion {
        text,
        kind: SuggestionKind::Field,
        detail: detail.to_string(),
    }
}

fn keyword(kw: &str) -> Suggestion {
    Suggestion {
        text: kw.to_string(),
        kind: SuggestionKind::Keyword,
        detail: "keyword".to_string(),
    }
}

const KEYWORDS: &[&str] = &[
    "SELECT", "FROM", "WHERE", "JOIN", "LEFT", "RIGHT", "INNER", "OUTER", "ON", "AND", "OR", "NOT",
    "IN", "IS", "NULL", "LIKE", "ILIKE", "BETWEEN", "GROUP", "BY", "ORDER", "HAVING", "LIMIT",
    "OFFSET", "AS", "DISTINCT", "COUNT", "SUM", "AVG", "MIN", "MAX", "ASC", "DESC", "TRUE",
    "FALSE",
];

/// Operators offered in the per-table filter.
const FILTER_KEYWORDS: &[&str] = &[
    "AND", "OR", "NOT", "IN", "IS", "NULL", "LIKE", "ILIKE", "BETWEEN", "TRUE", "FALSE",
];

/// Keywords that can legally follow an operand, given the clause the cursor is
/// in and the previous token. Keeps the suggestions valid for the position
/// (e.g. after `SELECT *` only `FROM`/`AS`; before a table only join/where/…).
fn next_keywords(clause: Option<&str>, prev: Option<&str>) -> &'static [&'static str] {
    // `GROUP`/`ORDER` must be followed by `BY`.
    if matches!(prev, Some("group" | "order")) {
        return &["BY"];
    }
    match clause {
        // No clause yet — the statement starts with SELECT.
        None => &["SELECT"],
        Some("select") => &["FROM", "AS"],
        Some("from" | "join") => &[
            "JOIN", "LEFT", "RIGHT", "INNER", "OUTER", "ON", "WHERE", "GROUP", "ORDER", "LIMIT",
            "AS",
        ],
        Some("where" | "on" | "having") => &[
            "AND", "OR", "NOT", "IN", "IS", "LIKE", "ILIKE", "BETWEEN", "GROUP", "ORDER", "LIMIT",
        ],
        Some("group") => &["ORDER", "HAVING", "LIMIT"],
        Some("order") => &["ASC", "DESC", "LIMIT", "OFFSET"],
        Some("set") => &["WHERE"],
        Some(_) => &["WHERE", "JOIN", "GROUP", "ORDER", "LIMIT"],
    }
}

/// Words that begin a clause (used to find the clause the cursor sits in).
const CLAUSE_KEYWORDS: &[&str] = &[
    "select", "from", "where", "group", "order", "having", "on", "set", "join",
];

/// Literal keywords valid wherever an operand/value is expected.
const VALUE_KEYWORDS: &[&str] = &["NULL", "TRUE", "FALSE"];

/// Keywords valid right after `IS` (e.g. `IS NULL`, `IS NOT NULL`).
const AFTER_IS_KEYWORDS: &[&str] = &["NULL", "NOT", "TRUE", "FALSE", "DISTINCT"];

impl QueryLanguage for SqlLanguage {
    fn placeholder(&self, _schema: Option<&CollectionSchema>) -> String {
        match self.mode {
            SqlLangMode::Filter => {
                "age > 21 AND status = 'active'   ·   = <> < > LIKE IN IS NULL   ·   ^g for reference"
                    .to_string()
            }
            SqlLangMode::Query => {
                "SELECT * FROM table WHERE …   ·   JOIN / GROUP BY / ORDER BY   ·   ^g for reference"
                    .to_string()
            }
        }
    }

    fn validate(&self, text: &str, _schema: Option<&CollectionSchema>) -> QueryStatus {
        let trimmed = text.trim();
        if trimmed.is_empty() {
            return QueryStatus::Empty;
        }
        if !balanced(trimmed) {
            return QueryStatus::Incomplete;
        }
        // SQL plan prediction needs the server (EXPLAIN); report a scan.
        QueryStatus::Valid {
            plan_kind: PlanKind::Scan,
        }
    }

    fn complete(&self, req: &CompletionRequest<'_>) -> Completion {
        let span = token_span(req.text, req.cursor);
        let prefix = &req.text[span.start..req.cursor.min(span.end).max(span.start)];
        let suggestions = match self.mode {
            SqlLangMode::Filter => self.complete_filter(req, prefix),
            SqlLangMode::Query => self.complete_query(req, prefix, span.start),
        };
        Completion { span, suggestions }
    }

    fn summarize(&self, text: &str, _schema: Option<&CollectionSchema>) -> Option<String> {
        let trimmed = text.trim();
        if trimmed.is_empty() {
            return None;
        }
        Some(trimmed.split_whitespace().collect::<Vec<_>>().join(" "))
    }

    fn reference(&self) -> Vec<ReferenceSection> {
        match self.mode {
            SqlLangMode::Filter => vec![ReferenceSection {
                heading: "Operators".to_string(),
                entries: vec![
                    ("=  <>  !=".to_string(), "Equality / inequality".to_string()),
                    ("<  <=  >  >=".to_string(), "Comparison".to_string()),
                    ("LIKE  ILIKE".to_string(), "Pattern match".to_string()),
                    ("IN (…)".to_string(), "Membership".to_string()),
                    (
                        "IS NULL / IS NOT NULL".to_string(),
                        "Null tests".to_string(),
                    ),
                    ("BETWEEN a AND b".to_string(), "Range".to_string()),
                    ("AND  OR  NOT".to_string(), "Combine predicates".to_string()),
                ],
            }],
            SqlLangMode::Query => vec![ReferenceSection {
                heading: "SQL".to_string(),
                entries: vec![
                    ("SELECT … FROM t".to_string(), "Project rows".to_string()),
                    (
                        "JOIN t2 ON t.a = t2.b".to_string(),
                        "Combine tables".to_string(),
                    ),
                    ("WHERE <predicate>".to_string(), "Filter".to_string()),
                    ("GROUP BY / HAVING".to_string(), "Aggregate".to_string()),
                    ("ORDER BY … LIMIT n".to_string(), "Sort / page".to_string()),
                ],
            }],
        }
    }
}

fn starts_with_ci(candidate: &str, prefix: &str) -> bool {
    candidate
        .to_ascii_lowercase()
        .starts_with(&prefix.to_ascii_lowercase())
}

/// Whether quotes and parentheses are balanced (used to distinguish a
/// still-being-typed query from a malformed one).
fn balanced(text: &str) -> bool {
    let mut depth = 0i32;
    let mut in_single = false;
    let mut in_double = false;
    for ch in text.chars() {
        match ch {
            '\'' if !in_double => in_single = !in_single,
            '"' if !in_single => in_double = !in_double,
            '(' if !in_single && !in_double => depth += 1,
            ')' if !in_single && !in_double => depth -= 1,
            _ => {}
        }
        if depth < 0 {
            return false;
        }
    }
    depth == 0 && !in_single && !in_double
}

fn is_token_char(c: char) -> bool {
    c.is_alphanumeric() || matches!(c, '_' | '.')
}

/// All keywords (lowercased) — used to tell a keyword from a table/column name.
fn is_keyword(word: &str) -> bool {
    let lower = word.to_ascii_lowercase();
    KEYWORDS.iter().any(|k| k.eq_ignore_ascii_case(&lower))
}

/// The whitespace-separated word tokens of `text`, in order.
fn word_tokens(text: &str) -> Vec<&str> {
    text.split(|c: char| !is_token_char(c))
        .filter(|t| !t.is_empty())
        .collect()
}

/// Split into tokens: word runs and punctuation clusters, lowercased, in order.
/// Whitespace separates and is dropped.
fn tokenize(text: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut cur = String::new();
    let mut cur_word = false;
    for ch in text.chars() {
        if ch.is_whitespace() {
            if !cur.is_empty() {
                tokens.push(std::mem::take(&mut cur));
            }
            continue;
        }
        let is_word = is_token_char(ch);
        if !cur.is_empty() && is_word != cur_word {
            tokens.push(std::mem::take(&mut cur));
        }
        cur_word = is_word;
        cur.push(ch.to_ascii_lowercase());
    }
    if !cur.is_empty() {
        tokens.push(cur);
    }
    tokens
}

/// The clause keyword the cursor currently sits under (the last clause word
/// before `upto`).
fn current_clause(text: &str, upto: usize) -> Option<String> {
    word_tokens(&text[..upto.min(text.len())])
        .into_iter()
        .rev()
        .map(str::to_ascii_lowercase)
        .find(|w| CLAUSE_KEYWORDS.contains(&w.as_str()))
}

/// Table names referenced by `FROM` / `JOIN` clauses anywhere in `text`.
fn referenced_tables(text: &str) -> Vec<String> {
    let mut tables = Vec::new();
    let mut expect = false;
    for word in word_tokens(text) {
        if expect {
            if !is_keyword(word) {
                tables.push(word.to_string());
            }
            expect = false;
        }
        let lower = word.to_ascii_lowercase();
        if lower == "from" || lower == "join" {
            expect = true;
        }
    }
    tables
}

fn is_operator_token(token: &str) -> bool {
    !token.is_empty() && token.chars().all(|c| matches!(c, '=' | '<' | '>' | '!'))
}

/// Classify what the cursor expects next, for SQL completion.
fn classify(text: &str, token_start: usize) -> SqlContext {
    let tokens = tokenize(&text[..token_start.min(text.len())]);
    let Some(prev) = tokens.last().map(String::as_str) else {
        // Start of the query: a keyword (SELECT) comes first.
        return SqlContext::Keyword;
    };
    let prev2 = tokens.len().checked_sub(2).map(|i| tokens[i].as_str());
    match prev {
        "from" | "join" | "update" | "into" => SqlContext::Table,
        // `IS …` / `IS NOT …` expect NULL / TRUE / FALSE / DISTINCT.
        "is" => SqlContext::Value,
        "not" if prev2 == Some("is") => SqlContext::Value,
        "," => {
            if current_clause(text, token_start).as_deref() == Some("from") {
                SqlContext::Table
            } else {
                SqlContext::Column
            }
        }
        "select" | "where" | "and" | "or" | "on" | "not" | "having" | "by" | "set" | "(" => {
            SqlContext::Column
        }
        other if is_operator_token(other) => SqlContext::Column,
        _ => SqlContext::Keyword,
    }
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

    fn lang() -> SqlLanguage {
        SqlLanguage {
            mode: SqlLangMode::Query,
        }
    }

    fn hints() -> crate::core::schema::SchemaHints {
        crate::core::schema::SchemaHints {
            tables: vec!["users".to_string(), "orders".to_string()],
            columns: vec![
                (
                    "users".to_string(),
                    vec!["user_id".to_string(), "name".to_string()],
                ),
                (
                    "orders".to_string(),
                    vec!["order_id".to_string(), "total".to_string()],
                ),
            ],
        }
    }

    fn complete(text: &str) -> Vec<String> {
        let lookup = |_: &str| Vec::new();
        let hints = hints();
        let req = CompletionRequest {
            text,
            cursor: text.len(),
            attributes: &[],
            value_lookup: &lookup,
            schema: None,
            sql_hints: Some(&hints),
        };
        lang()
            .complete(&req)
            .suggestions
            .into_iter()
            .map(|s| s.text)
            .collect()
    }

    #[test]
    fn empty_is_empty() {
        assert!(matches!(lang().validate("  ", None), QueryStatus::Empty));
    }

    #[test]
    fn balanced_select_is_valid() {
        assert!(matches!(
            lang().validate("SELECT * FROM t WHERE a = 1", None),
            QueryStatus::Valid { .. }
        ));
    }

    #[test]
    fn open_paren_is_incomplete() {
        assert!(matches!(
            lang().validate("SELECT * FROM t WHERE a IN (1, 2", None),
            QueryStatus::Incomplete
        ));
    }

    #[test]
    fn after_from_suggests_tables_even_with_empty_prefix() {
        let texts = complete("SELECT * FROM ");
        assert!(texts.contains(&"users".to_string()));
        assert!(texts.contains(&"orders".to_string()));
        // Not columns, not keywords.
        assert!(!texts.contains(&"user_id".to_string()));
        assert!(!texts.iter().any(|t| t == "SELECT"));
    }

    #[test]
    fn from_prefix_suggests_only_matching_tables() {
        let texts = complete("SELECT * FROM us");
        assert!(texts.contains(&"users".to_string()));
        assert!(!texts.contains(&"orders".to_string()));
        // user_id is a column, not a table — must not appear in a table position.
        assert!(!texts.contains(&"user_id".to_string()));
    }

    #[test]
    fn where_suggests_only_referenced_table_columns_no_keywords() {
        let texts = complete("SELECT * FROM users WHERE ");
        assert!(texts.contains(&"user_id".to_string()));
        assert!(texts.contains(&"name".to_string()));
        // orders' columns are not in scope, and no keywords here.
        assert!(!texts.contains(&"order_id".to_string()));
        assert!(!texts.iter().any(|t| t == "IN" || t == "INNER" || t == "IS"));
    }

    #[test]
    fn is_not_suggests_null() {
        let texts = complete("SELECT * FROM customers WHERE id IS NOT N");
        assert!(texts.contains(&"NULL".to_string()));
        // A column is not valid right after IS NOT, and NOT can't repeat.
        assert!(!texts.contains(&"name".to_string()));
        assert!(!texts.contains(&"NOT".to_string()));
    }

    #[test]
    fn is_suggests_null_and_not() {
        let texts = complete("SELECT * FROM customers WHERE id IS ");
        assert!(texts.contains(&"NULL".to_string()));
        assert!(texts.contains(&"NOT".to_string()));
    }

    #[test]
    fn after_operand_suggests_keywords() {
        let texts = complete("SELECT * FROM users ");
        assert!(texts.iter().any(|t| t == "WHERE" || t == "JOIN"));
    }

    #[test]
    fn select_projection_only_offers_from() {
        let texts = complete("SELECT * ");
        assert!(texts.contains(&"FROM".to_string()));
        // WHERE/JOIN/GROUP are not valid before FROM.
        assert!(
            !texts
                .iter()
                .any(|t| t == "WHERE" || t == "JOIN" || t == "GROUP")
        );
    }

    #[test]
    fn start_completes_select_keyword() {
        assert!(complete("SEL").iter().any(|t| t == "SELECT"));
    }
}
