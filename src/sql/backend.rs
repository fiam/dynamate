//! The PostgreSQL / MySQL implementation of the neutral [`Datastore`] trait,
//! over a `sqlx` pool. Engine differences live in [`SqlDialectKind`].

use std::collections::HashMap;

use async_trait::async_trait;
use sqlx::mysql::MySqlPool;
use sqlx::postgres::PgPool;

use crate::core::capabilities::{Capabilities, SecondaryIndexSupport};
use crate::core::datastore::Datastore;
use crate::core::error::{DbError, Result};
use crate::core::language::QueryLanguage;
use crate::core::query::{
    BatchDeleteOutcome, CreateCollectionSpec, Cursor, Key, Page, PlanKind, QueryPlan, QueryResult,
};
use crate::core::schema::{
    CollectionSchema, ColumnSchema, IndexKind, IndexSchema, KeyField, KeyRole, KeySchema,
    Projection, ScalarType, SchemaHints,
};
use crate::core::value::{Item, Number, Value};

use super::convert::{bind_mysql, bind_pg, mysql_row_to_item, pg_row_to_item};
use super::dialect::SqlDialectKind;
use super::language::{SqlLangMode, SqlLanguage};

/// The active connection pool.
pub enum SqlPool {
    Pg(PgPool),
    MySql(MySqlPool),
}

/// Cursor key carrying the running LIMIT/OFFSET offset.
const OFFSET_KEY: &str = "__offset";
/// Rows per `WHERE … IN (…)` chunk in a batch delete.
const BATCH_DELETE_CHUNK: usize = 500;

pub struct SqlBackend {
    pool: SqlPool,
    dialect: SqlDialectKind,
    #[allow(dead_code)]
    database: String,
    read_only: bool,
}

impl SqlBackend {
    pub fn new(pool: SqlPool, dialect: SqlDialectKind, database: String, read_only: bool) -> Self {
        Self {
            pool,
            dialect,
            database,
            read_only,
        }
    }

    pub async fn connect(
        url: &str,
        dialect: SqlDialectKind,
        read_only: bool,
    ) -> std::result::Result<Self, String> {
        super::connect::connect(url, dialect, read_only).await
    }

    fn ensure_writable(&self) -> Result<()> {
        if self.read_only {
            Err(DbError::ReadOnly)
        } else {
            Ok(())
        }
    }

    /// Run a query (optionally with bound params) and decode rows to items.
    async fn fetch_items(&self, sql: &str, binds: &[Value]) -> Result<Vec<Item>> {
        match &self.pool {
            SqlPool::Pg(pool) => {
                let mut query = sqlx::query(sql);
                for value in binds {
                    query = bind_pg(query, value);
                }
                let rows = query
                    .fetch_all(pool)
                    .await
                    .map_err(|e| DbError::Backend(e.to_string()))?;
                Ok(rows.iter().map(pg_row_to_item).collect())
            }
            SqlPool::MySql(pool) => {
                let mut query = sqlx::query(sql);
                for value in binds {
                    query = bind_mysql(query, value);
                }
                let rows = query
                    .fetch_all(pool)
                    .await
                    .map_err(|e| DbError::Backend(e.to_string()))?;
                Ok(rows.iter().map(mysql_row_to_item).collect())
            }
        }
    }

    /// Run a write statement, returning the affected row count.
    async fn execute(&self, sql: &str, binds: &[Value]) -> Result<u64> {
        match &self.pool {
            SqlPool::Pg(pool) => {
                let mut query = sqlx::query(sql);
                for value in binds {
                    query = bind_pg(query, value);
                }
                query
                    .execute(pool)
                    .await
                    .map(|r| r.rows_affected())
                    .map_err(|e| DbError::Backend(e.to_string()))
            }
            SqlPool::MySql(pool) => {
                let mut query = sqlx::query(sql);
                for value in binds {
                    query = bind_mysql(query, value);
                }
                query
                    .execute(pool)
                    .await
                    .map(|r| r.rows_affected())
                    .map_err(|e| DbError::Backend(e.to_string()))
            }
        }
    }

    fn quote(&self, ident: &str) -> String {
        self.dialect.quote_ident(ident)
    }

    /// Primary-key column names of a table, in key order.
    async fn pk_columns(&self, table: &str) -> Result<Vec<String>> {
        let rows = self
            .fetch_items(
                self.dialect.primary_key_sql(),
                &[Value::Str(table.to_string())],
            )
            .await?;
        Ok(rows
            .iter()
            .filter_map(|r| column_str(r, "column_name"))
            .collect())
    }

    /// Map of column name → declared type, for casting bound parameters on write.
    async fn column_types(&self, table: &str) -> Result<HashMap<String, String>> {
        let rows = self
            .fetch_items(self.dialect.columns_sql(), &[Value::Str(table.to_string())])
            .await?;
        Ok(rows
            .iter()
            .filter_map(|r| Some((column_str(r, "column_name")?, column_str(r, "data_type")?)))
            .collect())
    }

    /// Placeholder for `column`, cast to its declared type when known.
    fn typed_placeholder(
        &self,
        index: usize,
        column: &str,
        types: &HashMap<String, String>,
    ) -> String {
        self.dialect
            .placeholder_for(index, types.get(column).map(String::as_str))
    }
}

const fn capabilities(dialect: SqlDialectKind) -> Capabilities {
    Capabilities {
        backend_label: dialect.label(),
        set_types: false,
        binary_type: true,
        secondary_indexes: SecondaryIndexSupport::Arbitrary,
        create_collection: false,
        drop_collection: true,
        batch_delete: true,
        purge: false,
        index_query: false,
        ttl: false,
        scanned_count: false,
        consumed_capacity: false,
        raw_query: true,
    }
}

#[async_trait]
impl Datastore for SqlBackend {
    fn capabilities(&self) -> &Capabilities {
        // Two statics so the &'static borrow is valid for either dialect.
        const PG: Capabilities = capabilities(SqlDialectKind::Postgres);
        const MY: Capabilities = capabilities(SqlDialectKind::Mysql);
        match self.dialect {
            SqlDialectKind::Postgres => &PG,
            SqlDialectKind::Mysql => &MY,
        }
    }

    fn query_language(&self) -> &dyn QueryLanguage {
        static FILTER: SqlLanguage = SqlLanguage {
            mode: SqlLangMode::Filter,
        };
        &FILTER
    }

    fn raw_query_language(&self) -> Option<&dyn QueryLanguage> {
        static QUERY: SqlLanguage = SqlLanguage {
            mode: SqlLangMode::Query,
        };
        Some(&QUERY)
    }

    fn is_read_only(&self) -> bool {
        self.read_only
    }

    async fn validate(&self) -> Result<()> {
        self.fetch_items("SELECT 1", &[]).await.map(|_| ())
    }

    async fn list_collections(&self) -> Result<Vec<String>> {
        let rows = self
            .fetch_items(self.dialect.list_tables_sql(), &[])
            .await?;
        Ok(rows
            .iter()
            .filter_map(|r| column_str(r, "table_name"))
            .collect())
    }

    async fn describe_collection(&self, name: &str) -> Result<CollectionSchema> {
        let pk = self.pk_columns(name).await?;
        let key = KeySchema {
            fields: pk
                .iter()
                .enumerate()
                .map(|(idx, col)| KeyField {
                    name: col.clone(),
                    role: if idx == 0 {
                        KeyRole::Partition
                    } else {
                        KeyRole::Sort
                    },
                    ty: ScalarType::String,
                })
                .collect(),
        };
        let index_rows = self
            .fetch_items(self.dialect.indexes_sql(), &[Value::Str(name.to_string())])
            .await?;
        let indexes = index_rows
            .iter()
            .filter_map(|r| column_str(r, "index_name"))
            .map(|index_name| IndexSchema {
                name: index_name,
                kind: IndexKind::Secondary,
                key: KeySchema::default(),
                projection: Projection::All,
            })
            .collect();
        let column_rows = self
            .fetch_items(self.dialect.columns_sql(), &[Value::Str(name.to_string())])
            .await?;
        let columns = column_rows
            .iter()
            .filter_map(|r| {
                let name = column_str(r, "column_name")?;
                Some(ColumnSchema {
                    name,
                    data_type: column_str(r, "data_type").unwrap_or_default(),
                    nullable: column_str(r, "is_nullable")
                        .is_some_and(|n| n.eq_ignore_ascii_case("yes")),
                })
            })
            .collect();
        Ok(CollectionSchema {
            name: name.to_string(),
            key,
            indexes,
            columns,
            ttl_attribute: None,
            status: None,
            item_count: None,
            size_bytes: None,
        })
    }

    async fn query(&self, name: &str, plan: &QueryPlan, page: Page) -> Result<QueryResult> {
        let mut sql = format!("SELECT * FROM {}", self.quote(name));
        let mut binds = Vec::new();
        let plan_kind = if let Some(key_equals) = plan.key_equals.as_ref() {
            // Cast the bound value to the key column's type (e.g. uuid).
            let types = self.column_types(name).await?;
            sql.push_str(&format!(
                " WHERE {} = {}",
                self.quote(&key_equals.attribute),
                self.typed_placeholder(1, &key_equals.attribute, &types)
            ));
            binds.push(key_equals.value.clone());
            PlanKind::IndexedQuery { index: None }
        } else if let Some(text) = plan
            .filter
            .as_deref()
            .map(str::trim)
            .filter(|t| !t.is_empty())
        {
            sql.push_str(&format!(" WHERE {text}"));
            PlanKind::Scan
        } else {
            PlanKind::Scan
        };
        sql.push_str(" ORDER BY 1");
        let offset = offset_from_cursor(page.cursor.as_ref());
        if let Some(limit) = page.limit {
            sql.push_str(&format!(" LIMIT {limit} OFFSET {offset}"));
        }
        let items = self.fetch_items(&sql, &binds).await?;
        Ok(paged_result(items, page.limit, offset, plan_kind))
    }

    async fn raw_query(&self, query: &str, page: Page) -> Result<QueryResult> {
        let inner = query.trim().trim_end_matches(';');
        let offset = offset_from_cursor(page.cursor.as_ref());
        let mut sql = format!("SELECT * FROM ( {inner} ) AS _dynamate_q");
        if let Some(limit) = page.limit {
            sql.push_str(&format!(" LIMIT {limit} OFFSET {offset}"));
        }
        let items = self.fetch_items(&sql, &[]).await?;
        Ok(paged_result(items, page.limit, offset, PlanKind::Scan))
    }

    async fn schema_hints(&self) -> Result<SchemaHints> {
        // Rows arrive ordered by (table_name, ordinal_position).
        let rows = self
            .fetch_items(self.dialect.schema_hints_sql(), &[])
            .await?;
        let mut hints = SchemaHints::default();
        for row in &rows {
            let Some(table) = column_str(row, "table_name") else {
                continue;
            };
            push_unique(&mut hints.tables, table.clone());
            if hints.columns.iter().all(|(t, _)| *t != table) {
                hints.columns.push((table.clone(), Vec::new()));
            }
            let entry = hints
                .columns
                .iter_mut()
                .find(|(t, _)| *t == table)
                .expect("table row just ensured");
            if let Some(column) = column_str(row, "column_name") {
                push_unique(&mut entry.1, column);
            }
        }
        Ok(hints)
    }

    async fn put_item(&self, name: &str, item: Item) -> Result<()> {
        self.ensure_writable()?;
        let cols: Vec<String> = item.keys().cloned().collect();
        if cols.is_empty() {
            return Err(DbError::Backend("cannot insert an empty row".to_string()));
        }
        let pk = self.pk_columns(name).await?;
        let types = self.column_types(name).await?;
        let quoted_cols: Vec<String> = cols.iter().map(|c| self.quote(c)).collect();
        let placeholders: Vec<String> = cols
            .iter()
            .enumerate()
            .map(|(i, c)| self.typed_placeholder(i + 1, c, &types))
            .collect();
        let mut sql = format!(
            "INSERT INTO {} ({}) VALUES ({})",
            self.quote(name),
            quoted_cols.join(", "),
            placeholders.join(", ")
        );
        sql.push_str(&self.upsert_clause(&cols, &pk));
        let binds: Vec<Value> = cols
            .iter()
            .map(|c| item.get(c).cloned().unwrap_or(Value::Null))
            .collect();
        self.execute(&sql, &binds).await.map(|_| ())
    }

    async fn delete_item(&self, name: &str, key: Key) -> Result<()> {
        self.ensure_writable()?;
        let types = self.column_types(name).await?;
        let (clause, binds) = self.equality_clause(&key.0, 1, &types);
        if binds.is_empty() {
            return Err(DbError::Backend("delete requires a key".to_string()));
        }
        let sql = format!("DELETE FROM {} WHERE {clause}", self.quote(name));
        self.execute(&sql, &binds).await.map(|_| ())
    }

    async fn batch_delete(&self, name: &str, keys: Vec<Key>) -> Result<BatchDeleteOutcome> {
        self.ensure_writable()?;
        let types = self.column_types(name).await?;
        let mut deleted = 0_u64;
        for chunk in keys.chunks(BATCH_DELETE_CHUNK) {
            let mut clauses = Vec::new();
            let mut binds = Vec::new();
            for key in chunk {
                let (clause, mut row_binds) = self.equality_clause(&key.0, binds.len() + 1, &types);
                if clause.is_empty() {
                    continue;
                }
                clauses.push(format!("({clause})"));
                binds.append(&mut row_binds);
            }
            if clauses.is_empty() {
                continue;
            }
            let sql = format!(
                "DELETE FROM {} WHERE {}",
                self.quote(name),
                clauses.join(" OR ")
            );
            deleted += self.execute(&sql, &binds).await?;
        }
        Ok(BatchDeleteOutcome { deleted })
    }

    async fn create_collection(&self, _spec: &CreateCollectionSpec) -> Result<()> {
        Err(DbError::Unsupported(
            "creating tables is not supported for SQL backends",
        ))
    }

    async fn drop_collection(&self, name: &str) -> Result<()> {
        self.ensure_writable()?;
        let sql = format!("DROP TABLE {}", self.quote(name));
        self.execute(&sql, &[]).await.map(|_| ())
    }
}

impl SqlBackend {
    /// `col1 = $a AND col2 = $b`, with binds, starting at placeholder `start`.
    /// Placeholders are cast to the column types so non-text keys (e.g. `uuid`)
    /// match.
    fn equality_clause(
        &self,
        item: &Item,
        start: usize,
        types: &HashMap<String, String>,
    ) -> (String, Vec<Value>) {
        let mut parts = Vec::new();
        let mut binds = Vec::new();
        for (col, value) in item {
            parts.push(format!(
                "{} = {}",
                self.quote(col),
                self.typed_placeholder(start + binds.len(), col, types)
            ));
            binds.push(value.clone());
        }
        (parts.join(" AND "), binds)
    }

    /// The dialect-specific upsert tail for an INSERT over `cols` with PK `pk`.
    fn upsert_clause(&self, cols: &[String], pk: &[String]) -> String {
        if pk.is_empty() {
            return String::new();
        }
        let non_pk: Vec<&String> = cols.iter().filter(|c| !pk.contains(c)).collect();
        match self.dialect {
            SqlDialectKind::Postgres => {
                let target = pk
                    .iter()
                    .map(|c| self.quote(c))
                    .collect::<Vec<_>>()
                    .join(", ");
                if non_pk.is_empty() {
                    format!(" ON CONFLICT ({target}) DO NOTHING")
                } else {
                    let sets = non_pk
                        .iter()
                        .map(|c| format!("{0} = EXCLUDED.{0}", self.quote(c)))
                        .collect::<Vec<_>>()
                        .join(", ");
                    format!(" ON CONFLICT ({target}) DO UPDATE SET {sets}")
                }
            }
            SqlDialectKind::Mysql => {
                let assignments = if non_pk.is_empty() {
                    let c = self.quote(&pk[0]);
                    format!("{c} = {c}")
                } else {
                    non_pk
                        .iter()
                        .map(|c| format!("{0} = VALUES({0})", self.quote(c)))
                        .collect::<Vec<_>>()
                        .join(", ")
                };
                format!(" ON DUPLICATE KEY UPDATE {assignments}")
            }
        }
    }
}

/// Read a string column by name, case-insensitively — MySQL returns
/// `information_schema` column labels uppercased (`TABLE_NAME`), Postgres lower.
/// MySQL also reports these catalog columns with a binary collation, so they
/// arrive as [`Value::Bytes`]; decode those as UTF-8.
fn column_str(item: &Item, key: &str) -> Option<String> {
    item.iter()
        .find(|(k, _)| k.eq_ignore_ascii_case(key))
        .and_then(|(_, v)| match v {
            Value::Str(s) => Some(s.clone()),
            Value::Bytes(b) => String::from_utf8(b.clone()).ok(),
            _ => None,
        })
}

fn offset_from_cursor(cursor: Option<&Cursor>) -> u64 {
    cursor
        .and_then(|c| c.0.get(OFFSET_KEY))
        .and_then(Value::as_number)
        .and_then(Number::as_i64)
        .unwrap_or(0)
        .max(0) as u64
}

fn paged_result(
    items: Vec<Item>,
    limit: Option<u32>,
    offset: u64,
    plan_kind: PlanKind,
) -> QueryResult {
    let count = items.len() as u64;
    let next = match limit {
        Some(limit) if count == u64::from(limit) => {
            let mut cursor = Item::new();
            cursor.insert(
                OFFSET_KEY.to_string(),
                Value::Num(Number::new((offset + u64::from(limit)).to_string())),
            );
            Some(Cursor(cursor))
        }
        _ => None,
    };
    QueryResult {
        items,
        count,
        scanned_count: None,
        next,
        plan_kind,
        cost: None,
    }
}

fn push_unique(out: &mut Vec<String>, value: String) {
    if !out.contains(&value) {
        out.push(value);
    }
}
