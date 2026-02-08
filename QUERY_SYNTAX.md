# DynamoDB Query Syntax Reference

This document describes the query expression syntax accepted by Dynamate's
parser ([`src/expr/parser.rs`](src/expr/parser.rs)) and how those expressions
are translated into DynamoDB Query/Scan requests.

## Quick Start

```text
status = active
PK = "USER#123"
age >= 21 AND verified = true
begins_with(SK, "ORDER#")
```

A blank query runs a full table scan.

## Single-Token PK Shortcut

When the input is exactly one scalar token and is not a full expression, Dynamate treats it as:

```text
<table_hash_key> = <token>
```

Examples:

```text
foo              -> PK = "foo"     (string)
"foo bar"        -> PK = "foo bar" (string)
123              -> PK = 123        (number)
true             -> PK = true       (boolean)
null             -> PK = null       (null)
```

Rules:

1. The shortcut only applies to a single token.
2. Backtick path tokens are excluded (for example `` `other field` `` does not trigger the shortcut).
3. If normal expression parsing succeeds, normal parsing always wins.
4. The shortcut always targets the table primary hash key (not GSIs/LSIs).

## Supported Grammar

## Comparisons

```text
path = value
path <> value
path != value
path < value
path <= value
path > value
path >= value
path BETWEEN value AND value
path IN (value, value, ...)
```

## Logical operators

```text
expr AND expr
expr OR expr
NOT expr
(expr)
```

Operator precedence:

1. Parentheses
2. `NOT`
3. `AND`
4. `OR`

## Functions

Supported function names:

1. `attribute_exists(path)`
2. `attribute_not_exists(path)`
3. `attribute_type(path, value)`
4. `begins_with(path, value)`
5. `contains(path, value)`
6. `size(path)`

## Paths and Values

## Path operands (attribute references)

1. Bare identifier: `status`, `PK`, `created_at`
2. Backtick path/name: `` `other field` ``

Bare identifiers use `[A-Za-z_][A-Za-z0-9_]*`.

## Value operands

Accepted value forms:

1. Quoted strings: `"text"`, `'text'`
2. Numbers: `42`, `3.14`, `-7`, `1e6`
3. Booleans: `true`, `false`
4. Null: `null`
5. Unquoted identifiers (inferred): `active`, `USER_123`

Unquoted identifier inference:

1. `true`/`false` -> boolean
2. `null` -> null
3. Numeric-looking token -> number
4. Otherwise -> string

## Query vs Scan Behavior

Dynamate analyzes the parsed expression against the table schema:

1. If partition-key equality is present (and optional compatible sort-key condition), it builds a `Query`.
2. Otherwise, it builds a `Scan` with filter expression.

Current behavior note:

1. For key-based queries, Dynamate does not currently extract extra non-key
   predicates into a separate filter expression.
2. If the expression cannot be represented as a key query pattern, it falls back to `Scan`.

## Not Supported

These are DynamoDB-native concepts but are not part of Dynamate's input syntax:

1. `#name` expression attribute aliases
2. `:value` placeholders
3. Projection expression input
4. Update-expression input (`SET`, `ADD`, `REMOVE`, `DELETE`)
5. Condition-expression input for write APIs

Also note:

1. Attribute names containing spaces or punctuation require backticks.
2. A multi-token input without operators (for example `foo bar`) is invalid.
