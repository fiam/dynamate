//! Integration tests for the PostgreSQL [`Datastore`] implementation, exercising
//! the neutral `Value`/`Item`/`Cursor` boundary, the per-table `WHERE` filter,
//! the database-level free-form `raw_query` (incl. a JOIN), and both layers of
//! `--readonly` against a real `postgres:16` container.

use std::time::Duration;

use color_eyre::Result;
use sqlx::postgres::PgPool;
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt,
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
};

use dynamate::core::datastore::Datastore;
use dynamate::core::error::DbError;
use dynamate::core::query::{IndexHint, Key, Page, PlanKind, QueryPlan};
use dynamate::core::value::{Item, Number, Value};
use dynamate::sql::{SqlBackend, SqlDialectKind};

#[allow(dead_code)]
struct PgEnv {
    container: ContainerAsync<GenericImage>,
    url: String,
}

async fn new_pg_env() -> Result<PgEnv> {
    let container = GenericImage::new("postgres", "16")
        .with_exposed_port(5432.tcp())
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_USER", "dynamate")
        .with_env_var("POSTGRES_PASSWORD", "secret")
        .with_env_var("POSTGRES_DB", "testdb")
        .start()
        .await
        .expect("Failed to start PostgreSQL");
    let port = container.get_host_port_ipv4(5432).await?;
    Ok(PgEnv {
        container,
        url: format!("postgres://dynamate:secret@127.0.0.1:{port}/testdb"),
    })
}

/// Connect a backend, retrying while the server finishes warming up.
async fn new_backend(url: &str, read_only: bool) -> SqlBackend {
    for attempt in 1..=15 {
        if let Ok(backend) = SqlBackend::connect(url, SqlDialectKind::Postgres, read_only).await
            && backend.validate().await.is_ok()
        {
            return backend;
        }
        tokio::time::sleep(Duration::from_millis(300 * attempt)).await;
    }
    panic!("PostgreSQL never became ready");
}

/// Create two related tables and seed them. Returns once committed.
async fn seed(url: &str) {
    let pool = PgPool::connect(url).await.expect("seed pool");
    sqlx::query("CREATE TABLE authors (id INT PRIMARY KEY, name TEXT NOT NULL, age INT NOT NULL)")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("CREATE INDEX authors_age_idx ON authors (age)")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(
        "CREATE TABLE books (id INT PRIMARY KEY, author_id INT NOT NULL REFERENCES authors(id), \
         title TEXT NOT NULL)",
    )
    .execute(&pool)
    .await
    .unwrap();
    for i in 1..=5 {
        sqlx::query("INSERT INTO authors (id, name, age) VALUES ($1, $2, $3)")
            .bind(i)
            .bind(format!("Person {i}"))
            .bind(20 + i)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO books (id, author_id, title) VALUES ($1, $2, $3)")
            .bind(100 + i)
            .bind(i)
            .bind(format!("Book {i}"))
            .execute(&pool)
            .await
            .unwrap();
    }
    pool.close().await;
}

fn item(entries: Vec<(&str, Value)>) -> Item {
    entries
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect()
}

async fn scan_count(backend: &SqlBackend, table: &str) -> u64 {
    backend
        .query(table, &QueryPlan::default(), Page::default())
        .await
        .unwrap()
        .count
}

#[tokio::test]
async fn postgres_round_trips_through_neutral_values() {
    let env = new_pg_env().await.unwrap();
    let backend = new_backend(&env.url, false).await;
    seed(&env.url).await;

    // list_collections reports both base tables.
    let tables = backend.list_collections().await.unwrap();
    assert!(tables.contains(&"authors".to_string()));
    assert!(tables.contains(&"books".to_string()));

    // describe_collection reports the PK and the secondary index.
    let schema = backend.describe_collection("authors").await.unwrap();
    assert_eq!(schema.key.partition_key(), Some("id"));
    assert!(schema.indexes.iter().any(|i| i.name == "authors_age_idx"));

    assert_eq!(scan_count(&backend, "authors").await, 5);

    // Per-table WHERE filter (raw SQL predicate).
    let by_name = backend
        .query(
            "authors",
            &QueryPlan::new(Some("name = 'Person 1'".to_string()), None),
            Page::default(),
        )
        .await
        .unwrap();
    assert_eq!(by_name.count, 1);
    assert_eq!(
        by_name.items[0].get("age"),
        Some(&Value::Num(Number::from(21)))
    );

    // Operator predicate.
    let older = backend
        .query(
            "authors",
            &QueryPlan::new(Some("age > 22".to_string()), None),
            Page::default(),
        )
        .await
        .unwrap();
    assert_eq!(older.count, 3); // ages 23, 24, 25

    // A no-filter query scans.
    let scan = backend
        .query("authors", &QueryPlan::default(), Page::default())
        .await
        .unwrap();
    assert_eq!(scan.plan_kind, PlanKind::Scan);

    // PK key lookup (the index-picker path) reports an indexed query.
    let one = backend
        .query(
            "authors",
            &QueryPlan::key_lookup(
                "id".to_string(),
                Value::Num(Number::from(1)),
                IndexHint::Primary,
            ),
            Page::default(),
        )
        .await
        .unwrap();
    assert_eq!(one.count, 1);
    assert_eq!(one.plan_kind, PlanKind::IndexedQuery { index: None });

    // Pagination: limit 2, follow the cursor, see every row exactly once.
    let mut seen = std::collections::HashSet::new();
    let mut cursor = None;
    loop {
        let page = backend
            .query(
                "authors",
                &QueryPlan::default(),
                Page {
                    cursor,
                    limit: Some(2),
                },
            )
            .await
            .unwrap();
        for row in &page.items {
            if let Some(Value::Num(id)) = row.get("id") {
                assert!(seen.insert(id.as_str().to_string()), "duplicate id {id:?}");
            }
        }
        cursor = page.next;
        if cursor.is_none() {
            break;
        }
    }
    assert_eq!(seen.len(), 5);

    // Database-level raw_query: a JOIN across both tables.
    let joined = backend
        .raw_query(
            "SELECT a.name AS author, b.title AS title \
             FROM authors a JOIN books b ON b.author_id = a.id ORDER BY b.id",
            Page::default(),
        )
        .await
        .unwrap();
    assert_eq!(joined.count, 5);
    assert_eq!(
        joined.items[0].get("author"),
        Some(&Value::Str("Person 1".to_string()))
    );
    assert_eq!(
        joined.items[0].get("title"),
        Some(&Value::Str("Book 1".to_string()))
    );

    // raw_query is paginated too.
    let mut raw_seen = std::collections::HashSet::new();
    let mut cursor = None;
    loop {
        let page = backend
            .raw_query(
                "SELECT b.id AS id FROM books b ORDER BY b.id",
                Page {
                    cursor,
                    limit: Some(2),
                },
            )
            .await
            .unwrap();
        for row in &page.items {
            if let Some(Value::Num(id)) = row.get("id") {
                assert!(raw_seen.insert(id.as_str().to_string()));
            }
        }
        cursor = page.next;
        if cursor.is_none() {
            break;
        }
    }
    assert_eq!(raw_seen.len(), 5);

    // schema_hints lists tables, and each table's columns, for autocompletion.
    let hints = backend.schema_hints().await.unwrap();
    assert!(hints.tables.contains(&"authors".to_string()));
    assert!(hints.tables.contains(&"books".to_string()));
    let author_cols = hints.columns_for(&["authors".to_string()]);
    for expected in ["id", "name", "age"] {
        assert!(
            author_cols.contains(&expected.to_string()),
            "missing column {expected}"
        );
    }
    // Columns are scoped per table: a books-only column isn't in authors.
    assert!(!author_cols.contains(&"title".to_string()));

    // put_item upsert: insert a new row, then update it in place.
    backend
        .put_item(
            "authors",
            item(vec![
                ("id", Value::Num(Number::from(6))),
                ("name", Value::Str("New".to_string())),
                ("age", Value::Num(Number::from(40))),
            ]),
        )
        .await
        .unwrap();
    assert_eq!(scan_count(&backend, "authors").await, 6);
    backend
        .put_item(
            "authors",
            item(vec![
                ("id", Value::Num(Number::from(6))),
                ("name", Value::Str("Renamed".to_string())),
                ("age", Value::Num(Number::from(41))),
            ]),
        )
        .await
        .unwrap();
    assert_eq!(scan_count(&backend, "authors").await, 6); // updated, not added
    let renamed = backend
        .query(
            "authors",
            &QueryPlan::key_lookup(
                "id".to_string(),
                Value::Num(Number::from(6)),
                IndexHint::Primary,
            ),
            Page::default(),
        )
        .await
        .unwrap();
    assert_eq!(
        renamed.items[0].get("name"),
        Some(&Value::Str("Renamed".to_string()))
    );

    // delete_item.
    backend
        .delete_item(
            "authors",
            Key(item(vec![("id", Value::Num(Number::from(6)))])),
        )
        .await
        .unwrap();
    assert_eq!(scan_count(&backend, "authors").await, 5);

    // batch_delete (delete the books first to satisfy the FK).
    let outcome = backend
        .batch_delete(
            "books",
            vec![
                Key(item(vec![("id", Value::Num(Number::from(101)))])),
                Key(item(vec![("id", Value::Num(Number::from(102)))])),
            ],
        )
        .await
        .unwrap();
    assert_eq!(outcome.deleted, 2);
    assert_eq!(scan_count(&backend, "books").await, 3);

    // drop_collection.
    backend.drop_collection("books").await.unwrap();
    assert!(
        !backend
            .list_collections()
            .await
            .unwrap()
            .contains(&"books".to_string())
    );

    // create_collection is unsupported for SQL backends.
    assert!(!backend.capabilities().create_collection);
}

#[tokio::test]
async fn read_only_enforced_at_both_layers() {
    let env = new_pg_env().await.unwrap();
    let writer = new_backend(&env.url, false).await;
    seed(&env.url).await;

    let reader = new_backend(&env.url, true).await;
    assert!(reader.is_read_only());

    // Layer 1: the backend rejects writes before they reach the server.
    let err = reader
        .put_item(
            "authors",
            item(vec![
                ("id", Value::Num(Number::from(99))),
                ("name", Value::Str("Mallory".to_string())),
                ("age", Value::Num(Number::from(1))),
            ]),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, DbError::ReadOnly));

    // Layer 2: the connection itself is a read-only session, so a write smuggled
    // through the free-form query box is refused by the server. Confirm the
    // session setting is actually on for the reader and off for the writer.
    let reader_ro = reader
        .raw_query(
            "SELECT current_setting('default_transaction_read_only') AS ro",
            Page::default(),
        )
        .await
        .unwrap();
    assert_eq!(
        reader_ro.items[0].get("ro"),
        Some(&Value::Str("on".to_string()))
    );

    let writer_ro = writer
        .raw_query(
            "SELECT current_setting('default_transaction_read_only') AS ro",
            Page::default(),
        )
        .await
        .unwrap();
    assert_eq!(
        writer_ro.items[0].get("ro"),
        Some(&Value::Str("off".to_string()))
    );

    // Reads still work on the read-only backend.
    assert_eq!(scan_count(&reader, "authors").await, 5);
}

/// Round-tripping a row with non-text column types (uuid, jsonb, timestamptz,
/// numeric, bool) back through `put_item` must succeed — this is what the
/// edit/create UI does. Postgres won't implicitly cast text → those types, so
/// the backend must cast bound parameters to the column types.
#[tokio::test]
async fn put_item_round_trips_typed_columns() {
    let env = new_pg_env().await.unwrap();
    let backend = new_backend(&env.url, false).await;
    let pool = PgPool::connect(&env.url).await.expect("seed pool");
    sqlx::query(
        "CREATE TABLE events (\
           id UUID PRIMARY KEY, \
           label TEXT NOT NULL, \
           payload JSONB, \
           amount NUMERIC(10,2), \
           active BOOLEAN NOT NULL, \
           at TIMESTAMPTZ NOT NULL)",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO events (id, label, payload, amount, active, at) VALUES \
         ('11111111-1111-1111-1111-111111111111', 'first', '{\"k\": 1}', 12.50, true, \
          '2024-06-01T10:00:00Z')",
    )
    .execute(&pool)
    .await
    .unwrap();
    pool.close().await;

    // Read the row back as neutral items (what the browse view shows).
    let page = backend
        .query("events", &QueryPlan::default(), Page::default())
        .await
        .unwrap();
    assert_eq!(page.count, 1);
    let row = page.items[0].clone();

    // Edit a field and write the whole row back (the edit path).
    let mut edited = row.clone();
    edited.insert("label".to_string(), Value::Str("edited".to_string()));
    backend.put_item("events", edited).await.unwrap();

    // The label changed and the typed columns survived the round-trip.
    let after = backend
        .query("events", &QueryPlan::default(), Page::default())
        .await
        .unwrap();
    assert_eq!(after.count, 1);
    assert_eq!(
        after.items[0].get("label"),
        Some(&Value::Str("edited".to_string()))
    );

    // delete_item by the uuid primary key must also work.
    backend
        .delete_item(
            "events",
            Key(item(vec![(
                "id",
                Value::Str("11111111-1111-1111-1111-111111111111".to_string()),
            )])),
        )
        .await
        .unwrap();
    assert_eq!(scan_count(&backend, "events").await, 0);
}
