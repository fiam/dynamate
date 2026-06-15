//! The query-language dialect: the set of functions, keywords, operators and
//! value forms a backend understands.
//!
//! A [`Dialect`] is the unit of generalization for the query layer. The parser
//! (`super::parser`), the autocompletion engine
//! (`crate::widgets::query::completion`) and the in-app reference popup
//! (`crate::widgets::query::reference_popup`) all consult a `Dialect` rather
//! than a hardcoded list, so a new backend (MongoDB, Firestore, …) can offer a
//! different function set without touching those consumers.
//!
//! This module also defines [`DYNAMODB_DIALECT`], the dialect for the built-in
//! DynamoDB backend (the only one today). The grammar itself — paths, the six
//! comparison operators, `AND`/`OR`/`NOT`/`BETWEEN`/`IN`, literals — is
//! universal and lives in the lexer/parser; only the function set and type
//! codes are dialect-specific.

use super::ast::FunctionName;

/// A built-in function available in query expressions.
pub struct FunctionDoc {
    /// Canonical (lowercase) name as written in a query.
    pub name: &'static str,
    /// The semantic kind this function maps to in the AST. A backend's compiler
    /// turns this kind into its native query operator.
    pub func: FunctionName,
    /// Human-readable signature, e.g. `begins_with(path, value)`.
    pub signature: &'static str,
    /// One-line summary of what the function does.
    pub summary: &'static str,
    /// A short, copy-pasteable example.
    pub example: &'static str,
    /// Whether an argument is a type code (e.g. `attribute_type(path, "N")`),
    /// which completion offers from [`Dialect::type_codes`] rather than data.
    pub takes_type_code: bool,
}

/// A reserved keyword (logical operators and literals).
pub struct KeywordDoc {
    pub word: &'static str,
    pub summary: &'static str,
    pub example: &'static str,
}

/// A comparison operator.
pub struct OperatorDoc {
    pub symbols: &'static str,
    pub summary: &'static str,
}

pub static FUNCTIONS: &[FunctionDoc] = &[
    FunctionDoc {
        name: "attribute_exists",
        func: FunctionName::AttributeExists,
        signature: "attribute_exists(path)",
        summary: "True when the attribute is present on the item.",
        example: "attribute_exists(email)",
        takes_type_code: false,
    },
    FunctionDoc {
        name: "attribute_not_exists",
        func: FunctionName::AttributeNotExists,
        signature: "attribute_not_exists(path)",
        summary: "True when the attribute is absent from the item.",
        example: "attribute_not_exists(deleted_at)",
        takes_type_code: false,
    },
    FunctionDoc {
        name: "attribute_type",
        func: FunctionName::AttributeType,
        signature: "attribute_type(path, type)",
        summary: "True when the attribute has the given DynamoDB type (S, N, B, BOOL, M, L, SS, NS, BS, NULL).",
        example: "attribute_type(age, \"N\")",
        takes_type_code: true,
    },
    FunctionDoc {
        name: "begins_with",
        func: FunctionName::BeginsWith,
        signature: "begins_with(path, prefix)",
        summary: "True when the string attribute starts with the given prefix.",
        example: "begins_with(SK, \"ORDER#\")",
        takes_type_code: false,
    },
    FunctionDoc {
        name: "contains",
        func: FunctionName::Contains,
        signature: "contains(path, value)",
        summary: "True when a string contains the substring, or a set/list contains the value.",
        example: "contains(tags, \"urgent\")",
        takes_type_code: false,
    },
    FunctionDoc {
        name: "size",
        func: FunctionName::Size,
        signature: "size(path)",
        summary: "The size of the attribute (string length, or element count of a list/map/set). Use in a comparison.",
        example: "size(items) > 0",
        takes_type_code: false,
    },
];

pub static KEYWORDS: &[KeywordDoc] = &[
    KeywordDoc {
        word: "AND",
        summary: "Both sides must match.",
        example: "age >= 21 AND verified = true",
    },
    KeywordDoc {
        word: "OR",
        summary: "Either side may match.",
        example: "status = active OR status = pending",
    },
    KeywordDoc {
        word: "NOT",
        summary: "Negates the following expression.",
        example: "NOT attribute_exists(archived)",
    },
    KeywordDoc {
        word: "BETWEEN",
        summary: "Inclusive range test: path BETWEEN low AND high.",
        example: "age BETWEEN 18 AND 65",
    },
    KeywordDoc {
        word: "IN",
        summary: "Membership test against a list of values.",
        example: "status IN (active, pending, hold)",
    },
    KeywordDoc {
        word: "true / false",
        summary: "Boolean literals.",
        example: "verified = true",
    },
    KeywordDoc {
        word: "null",
        summary: "Null literal.",
        example: "middle_name = null",
    },
];

pub static OPERATORS: &[OperatorDoc] = &[
    OperatorDoc {
        symbols: "=",
        summary: "Equal",
    },
    OperatorDoc {
        symbols: "!=  <>",
        summary: "Not equal",
    },
    OperatorDoc {
        symbols: "<",
        summary: "Less than",
    },
    OperatorDoc {
        symbols: "<=",
        summary: "Less than or equal",
    },
    OperatorDoc {
        symbols: ">",
        summary: "Greater than",
    },
    OperatorDoc {
        symbols: ">=",
        summary: "Greater than or equal",
    },
];

/// Accepted value forms, for the reference popup.
pub static VALUE_FORMS: &[(&str, &str)] = &[
    ("\"text\"  'text'", "Quoted string"),
    ("42  3.14  -7  1e6", "Number"),
    ("true  false", "Boolean"),
    ("null", "Null"),
    ("active  USER_123", "Unquoted identifier (inferred type)"),
    (
        "`attr name`",
        "Backtick path for names with spaces/punctuation",
    ),
];

/// Notes about the single-token partition-key shortcut, for the reference popup.
pub static PK_SHORTCUT: &[(&str, &str)] = &[
    ("foo", "<hash_key> = \"foo\""),
    ("\"foo bar\"", "<hash_key> = \"foo bar\""),
    ("123", "<hash_key> = 123"),
];

/// DynamoDB attribute type codes accepted by `attribute_type(path, type)`.
pub static ATTR_TYPES: &[&str] = &["S", "N", "B", "BOOL", "M", "L", "SS", "NS", "BS", "NULL"];

/// A query-language dialect: the function set, type codes and reference data a
/// backend offers. The DynamoDB query language uses it to keep the parser and
/// completion engine off hardcoded function lists.
pub struct Dialect {
    pub functions: &'static [FunctionDoc],
    pub keywords: &'static [KeywordDoc],
    pub operators: &'static [OperatorDoc],
    /// Type codes for type-predicate functions (see [`FunctionDoc::takes_type_code`]).
    pub type_codes: &'static [&'static str],
    pub value_forms: &'static [(&'static str, &'static str)],
    pub pk_shortcut: &'static [(&'static str, &'static str)],
}

impl Dialect {
    /// Look up a function by name, case-insensitively.
    pub fn function_by_name(&self, name: &str) -> Option<&'static FunctionDoc> {
        self.functions
            .iter()
            .find(|f| f.name.eq_ignore_ascii_case(name))
    }

    /// Whether `name` is a known function in this dialect (case-insensitive).
    pub fn is_function_name(&self, name: &str) -> bool {
        self.function_by_name(name).is_some()
    }
}

/// The dialect for the built-in DynamoDB backend.
pub static DYNAMODB_DIALECT: Dialect = Dialect {
    functions: FUNCTIONS,
    keywords: KEYWORDS,
    operators: OPERATORS,
    type_codes: ATTR_TYPES,
    value_forms: VALUE_FORMS,
    pk_shortcut: PK_SHORTCUT,
};

/// The default dialect used by entry points that don't take an explicit one
/// (today this is the only backend's dialect).
pub fn default_dialect() -> &'static Dialect {
    &DYNAMODB_DIALECT
}

impl FunctionName {
    /// The canonical lowercase spelling, backed by the registry.
    pub fn as_str(&self) -> &'static str {
        FUNCTIONS
            .iter()
            .find(|f| &f.func == self)
            .map_or("", |f| f.name)
    }
}
