//! Integration tests for the MySQL [`Datastore`] implementation, mirroring the
//! PostgreSQL suite: the neutral `Value`/`Item`/`Cursor` boundary, per-table
//! `WHERE` filter, database-level free-form `raw_query` (incl. a JOIN), and both
//! layers of `--readonly` against a real `mysql:8` container.

use std::time::Duration;

use color_eyre::Result;
use sqlx::mysql::MySqlPool;
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
struct MysqlEnv {
    container: ContainerAsync<GenericImage>,
    url: String,
}

async fn new_mysql_env() -> Result<MysqlEnv> {
    let container = GenericImage::new("mysql", "8")
        .with_exposed_port(3306.tcp())
        .with_wait_for(WaitFor::message_on_stderr("ready for connections"))
        .with_env_var("MYSQL_ROOT_PASSWORD", "rootpw")
        .with_env_var("MYSQL_DATABASE", "testdb")
        .with_env_var("MYSQL_USER", "dynamate")
        .with_env_var("MYSQL_PASSWORD", "secret")
        .start()
        .await
        .expect("Failed to start MySQL");
    let port = container.get_host_port_ipv4(3306).await?;
    Ok(MysqlEnv {
        container,
        url: format!("mysql://dynamate:secret@127.0.0.1:{port}/testdb"),
    })
}

/// Connect a backend, retrying while the server finishes warming up.
async fn new_backend(url: &str, read_only: bool) -> SqlBackend {
    for attempt in 1..=20 {
        if let Ok(backend) = SqlBackend::connect(url, SqlDialectKind::Mysql, read_only).await
            && backend.validate().await.is_ok()
        {
            return backend;
        }
        tokio::time::sleep(Duration::from_millis(400 * attempt)).await;
    }
    panic!("MySQL never became ready");
}

/// Create two related tables and seed them. Returns once committed.
async fn seed(url: &str) {
    let pool = MySqlPool::connect(url).await.expect("seed pool");
    sqlx::query(
        "CREATE TABLE authors (id INT PRIMARY KEY, name VARCHAR(255) NOT NULL, age INT NOT NULL)",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("CREATE INDEX authors_age_idx ON authors (age)")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(
        "CREATE TABLE books (id INT PRIMARY KEY, author_id INT NOT NULL, title VARCHAR(255) NOT NULL, \
         FOREIGN KEY (author_id) REFERENCES authors(id))",
    )
    .execute(&pool)
    .await
    .unwrap();
    for i in 1..=5 {
        sqlx::query("INSERT INTO authors (id, name, age) VALUES (?, ?, ?)")
            .bind(i)
            .bind(format!("Person {i}"))
            .bind(20 + i)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO books (id, author_id, title) VALUES (?, ?, ?)")
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
async fn mysql_round_trips_through_neutral_values() {
    let env = new_mysql_env().await.unwrap();
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
    let env = new_mysql_env().await.unwrap();
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

    // Layer 2: the connection itself is a read-only session. Confirm the session
    // setting is on for the reader and off for the writer.
    let reader_ro = reader
        .raw_query(
            "SELECT @@session.transaction_read_only AS ro",
            Page::default(),
        )
        .await
        .unwrap();
    assert_eq!(
        reader_ro.items[0].get("ro"),
        Some(&Value::Num(Number::from(1)))
    );

    let writer_ro = writer
        .raw_query(
            "SELECT @@session.transaction_read_only AS ro",
            Page::default(),
        )
        .await
        .unwrap();
    assert_eq!(
        writer_ro.items[0].get("ro"),
        Some(&Value::Num(Number::from(0)))
    );

    // Reads still work on the read-only backend.
    assert_eq!(scan_count(&reader, "authors").await, 5);
}
