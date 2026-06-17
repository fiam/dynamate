//! Per-engine SQL differences. The rest of the SQL backend is dialect-agnostic
//! and goes through these helpers.

/// Which SQL engine a [`SqlBackend`](super::SqlBackend) talks to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SqlDialectKind {
    Postgres,
    Mysql,
}

/// Whether a Postgres `information_schema` type name can be used directly as a
/// `::type` cast target. `ARRAY` and `USER-DEFINED` are reported names that
/// aren't valid cast targets, so values for those columns bind without a cast.
fn is_castable(data_type: &str) -> bool {
    let lower = data_type.to_ascii_lowercase();
    !lower.is_empty() && !matches!(lower.as_str(), "array" | "user-defined")
}

impl SqlDialectKind {
    pub const fn label(self) -> &'static str {
        match self {
            SqlDialectKind::Postgres => "PostgreSQL",
            SqlDialectKind::Mysql => "MySQL",
        }
    }

    /// Quote an identifier (table/column name).
    pub fn quote_ident(self, name: &str) -> String {
        match self {
            SqlDialectKind::Postgres => format!("\"{}\"", name.replace('"', "\"\"")),
            SqlDialectKind::Mysql => format!("`{}`", name.replace('`', "``")),
        }
    }

    /// A bind placeholder for the 1-based parameter index.
    pub fn placeholder(self, index: usize) -> String {
        match self {
            SqlDialectKind::Postgres => format!("${index}"),
            SqlDialectKind::Mysql => "?".to_string(),
        }
    }

    /// A bind placeholder coerced to a column's declared type. Postgres won't
    /// implicitly cast bound text into `uuid`/`jsonb`/`timestamptz`/… columns, so
    /// values read back as strings need an explicit `::type` cast on write;
    /// MySQL coerces implicitly, so its placeholder is unchanged.
    pub fn placeholder_for(self, index: usize, data_type: Option<&str>) -> String {
        match self {
            SqlDialectKind::Postgres => match data_type.map(str::trim).filter(|t| is_castable(t)) {
                Some(ty) => format!("${index}::{ty}"),
                None => format!("${index}"),
            },
            SqlDialectKind::Mysql => "?".to_string(),
        }
    }

    /// Statement run on each new connection to make the session read-only.
    pub fn read_only_session_sql(self) -> &'static str {
        match self {
            SqlDialectKind::Postgres => "SET default_transaction_read_only = on",
            SqlDialectKind::Mysql => "SET SESSION transaction_read_only = ON",
        }
    }

    /// List base tables in the connected database/schema.
    pub fn list_tables_sql(self) -> &'static str {
        match self {
            SqlDialectKind::Postgres => {
                "SELECT table_name FROM information_schema.tables \
                 WHERE table_schema = current_schema() AND table_type = 'BASE TABLE' \
                 ORDER BY table_name"
            }
            SqlDialectKind::Mysql => {
                "SELECT table_name FROM information_schema.tables \
                 WHERE table_schema = DATABASE() AND table_type = 'BASE TABLE' \
                 ORDER BY table_name"
            }
        }
    }

    /// Columns of a table — name, type, nullability — in ordinal order (one
    /// bound table-name param).
    pub fn columns_sql(self) -> &'static str {
        match self {
            SqlDialectKind::Postgres => {
                "SELECT column_name, data_type, is_nullable FROM information_schema.columns \
                 WHERE table_schema = current_schema() AND table_name = $1 \
                 ORDER BY ordinal_position"
            }
            SqlDialectKind::Mysql => {
                "SELECT column_name, data_type, is_nullable FROM information_schema.columns \
                 WHERE table_schema = DATABASE() AND table_name = ? \
                 ORDER BY ordinal_position"
            }
        }
    }

    /// Primary-key column names of a table, in key order (one bound table-name param).
    pub fn primary_key_sql(self) -> &'static str {
        match self {
            SqlDialectKind::Postgres => {
                "SELECT kcu.column_name FROM information_schema.table_constraints tc \
                 JOIN information_schema.key_column_usage kcu \
                   ON tc.constraint_name = kcu.constraint_name \
                  AND tc.table_schema = kcu.table_schema \
                 WHERE tc.table_schema = current_schema() \
                   AND tc.table_name = $1 AND tc.constraint_type = 'PRIMARY KEY' \
                 ORDER BY kcu.ordinal_position"
            }
            SqlDialectKind::Mysql => {
                "SELECT kcu.column_name FROM information_schema.table_constraints tc \
                 JOIN information_schema.key_column_usage kcu \
                   ON tc.constraint_name = kcu.constraint_name \
                  AND tc.table_schema = kcu.table_schema \
                  AND tc.table_name = kcu.table_name \
                 WHERE tc.table_schema = DATABASE() \
                   AND tc.table_name = ? AND tc.constraint_type = 'PRIMARY KEY' \
                 ORDER BY kcu.ordinal_position"
            }
        }
    }

    /// Distinct secondary-index names on a table (one bound table-name param).
    pub fn indexes_sql(self) -> &'static str {
        match self {
            SqlDialectKind::Postgres => {
                "SELECT indexname AS index_name FROM pg_indexes \
                 WHERE schemaname = current_schema() AND tablename = $1 \
                 ORDER BY indexname"
            }
            SqlDialectKind::Mysql => {
                "SELECT DISTINCT index_name FROM information_schema.statistics \
                 WHERE table_schema = DATABASE() AND table_name = ? AND index_name <> 'PRIMARY' \
                 ORDER BY index_name"
            }
        }
    }

    /// `(table_name, column_name)` pairs for the whole database, for autocompletion.
    pub fn schema_hints_sql(self) -> &'static str {
        match self {
            SqlDialectKind::Postgres => {
                "SELECT table_name, column_name FROM information_schema.columns \
                 WHERE table_schema = current_schema() ORDER BY table_name, ordinal_position"
            }
            SqlDialectKind::Mysql => {
                "SELECT table_name, column_name FROM information_schema.columns \
                 WHERE table_schema = DATABASE() ORDER BY table_name, ordinal_position"
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::SqlDialectKind::{Mysql, Postgres};

    #[test]
    fn placeholders_differ_by_engine() {
        assert_eq!(Postgres.placeholder(1), "$1");
        assert_eq!(Postgres.placeholder(3), "$3");
        assert_eq!(Mysql.placeholder(1), "?");
        assert_eq!(Mysql.placeholder(3), "?");
    }

    #[test]
    fn identifiers_are_quoted_and_escaped() {
        assert_eq!(Postgres.quote_ident("name"), "\"name\"");
        assert_eq!(Postgres.quote_ident("a\"b"), "\"a\"\"b\"");
        assert_eq!(Mysql.quote_ident("name"), "`name`");
        assert_eq!(Mysql.quote_ident("a`b"), "`a``b`");
    }

    #[test]
    fn read_only_session_statement_per_engine() {
        assert!(
            Postgres
                .read_only_session_sql()
                .contains("default_transaction_read_only")
        );
        assert!(
            Mysql
                .read_only_session_sql()
                .contains("transaction_read_only")
        );
    }
}
