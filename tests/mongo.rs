//! Integration tests for the MongoDB [`Datastore`] implementation, exercising
//! the neutral `Value`/`Item`/`Cursor` boundary and the JSON filter language
//! end-to-end against a real `mongo` container.

use std::time::Duration;

use color_eyre::Result;
use testcontainers::{
    ContainerAsync, GenericImage,
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
};

use dynamate::core::datastore::Datastore;
use dynamate::core::query::{CreateCollectionSpec, IndexHint, Key, Page, PlanKind, QueryPlan};
use dynamate::core::schema::{
    IndexKind, IndexSchema, KeyField, KeyRole, KeySchema, Projection, ScalarType,
};
use dynamate::core::value::{Item, Number, Value};
use dynamate::mongo::MongoBackend;

#[allow(dead_code)]
struct MongoEnv {
    container: ContainerAsync<GenericImage>,
    uri: String,
}

async fn new_mongo_env() -> Result<MongoEnv> {
    let container = GenericImage::new("mongo", "7")
        .with_exposed_port(27017.tcp())
        .with_wait_for(WaitFor::message_on_stdout("Waiting for connections"))
        .start()
        .await
        .expect("Failed to start MongoDB");
    let port = container.get_host_port_ipv4(27017).await?;
    Ok(MongoEnv {
        container,
        uri: format!("mongodb://127.0.0.1:{port}/testdb"),
    })
}

async fn new_backend(uri: &str, read_only: bool) -> MongoBackend {
    // Validate with a few retries while the server finishes warming up.
    for attempt in 1..=10 {
        let backend = dynamate::mongo::connect::connect(uri, read_only)
            .await
            .expect("connect");
        if backend.validate().await.is_ok() {
            return backend;
        }
        tokio::time::sleep(Duration::from_millis(200 * attempt)).await;
    }
    panic!("MongoDB never became ready");
}

fn item(entries: Vec<(&str, Value)>) -> Item {
    entries
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect()
}

fn person(id: &str, name: &str, age: i64) -> Item {
    item(vec![
        ("_id", Value::Str(id.to_string())),
        ("name", Value::Str(name.to_string())),
        ("age", Value::Num(Number::from(age))),
    ])
}

fn demo_spec() -> CreateCollectionSpec {
    CreateCollectionSpec {
        name: "people".to_string(),
        key: KeySchema {
            fields: vec![KeyField {
                name: "_id".to_string(),
                role: KeyRole::Partition,
                ty: ScalarType::String,
            }],
        },
        indexes: vec![IndexSchema {
            name: "age_idx".to_string(),
            kind: IndexKind::Secondary,
            key: KeySchema {
                fields: vec![KeyField {
                    name: "age".to_string(),
                    role: KeyRole::Partition,
                    ty: ScalarType::Number,
                }],
            },
            projection: Projection::All,
        }],
    }
}

async fn scan_count(backend: &MongoBackend) -> u64 {
    backend
        .query("people", &QueryPlan::default(), Page::default())
        .await
        .unwrap()
        .count
}

#[tokio::test]
async fn mongo_round_trips_through_neutral_values() {
    let env = new_mongo_env().await.unwrap();
    let backend = new_backend(&env.uri, false).await;

    backend.create_collection(&demo_spec()).await.unwrap();

    // describe_collection reports the _id key and the created index.
    let schema = backend.describe_collection("people").await.unwrap();
    assert_eq!(schema.key.partition_key(), Some("_id"));
    assert!(schema.indexes.iter().any(|i| i.name == "age_idx"));

    // Insert five people (ages 21..=25).
    for i in 1..=5 {
        backend
            .put_item(
                "people",
                person(&format!("u{i}"), &format!("Person {i}"), 20 + i),
            )
            .await
            .unwrap();
    }
    assert_eq!(scan_count(&backend).await, 5);

    // Equality filter.
    let by_name = backend
        .query(
            "people",
            &QueryPlan::new(Some(r#"{ "name": "Person 1" }"#.to_string()), None),
            Page::default(),
        )
        .await
        .unwrap();
    assert_eq!(by_name.count, 1);
    assert_eq!(
        by_name.items[0].get("age"),
        Some(&Value::Num(Number::from(21)))
    );

    // Operator filter ($gt).
    let older = backend
        .query(
            "people",
            &QueryPlan::new(Some(r#"{ "age": { "$gt": 22 } }"#.to_string()), None),
            Page::default(),
        )
        .await
        .unwrap();
    assert_eq!(older.count, 3); // ages 23, 24, 25

    // A no-filter query scans.
    let scan = backend
        .query("people", &QueryPlan::default(), Page::default())
        .await
        .unwrap();
    assert_eq!(scan.plan_kind, PlanKind::Scan);

    // _id key lookup (the index-picker path) reports an indexed query.
    let one = backend
        .query(
            "people",
            &QueryPlan::key_lookup(
                "_id".to_string(),
                Value::Str("u1".to_string()),
                IndexHint::Primary,
            ),
            Page::default(),
        )
        .await
        .unwrap();
    assert_eq!(one.count, 1);
    assert_eq!(one.plan_kind, PlanKind::IndexedQuery { index: None });

    // Pagination: limit 2, follow the cursor, see every doc exactly once.
    let mut seen = std::collections::HashSet::new();
    let mut cursor = None;
    loop {
        let page = backend
            .query(
                "people",
                &QueryPlan::default(),
                Page {
                    cursor,
                    limit: Some(2),
                },
            )
            .await
            .unwrap();
        for doc in &page.items {
            if let Some(Value::Str(id)) = doc.get("_id") {
                assert!(seen.insert(id.clone()), "duplicate id {id}");
            }
        }
        cursor = page.next;
        if cursor.is_none() {
            break;
        }
    }
    assert_eq!(seen.len(), 5);

    // upsert/replace by _id.
    backend
        .put_item("people", person("u1", "Renamed", 99))
        .await
        .unwrap();
    let renamed = backend
        .query(
            "people",
            &QueryPlan::new(Some(r#"{ "_id": "u1" }"#.to_string()), None),
            Page::default(),
        )
        .await
        .unwrap();
    assert_eq!(
        renamed.items[0].get("name"),
        Some(&Value::Str("Renamed".to_string()))
    );
    assert_eq!(scan_count(&backend).await, 5); // replaced, not added

    // delete_item.
    backend
        .delete_item(
            "people",
            Key(item(vec![("_id", Value::Str("u1".to_string()))])),
        )
        .await
        .unwrap();
    assert_eq!(scan_count(&backend).await, 4);

    // batch_delete.
    let outcome = backend
        .batch_delete(
            "people",
            vec![
                Key(item(vec![("_id", Value::Str("u2".to_string()))])),
                Key(item(vec![("_id", Value::Str("u3".to_string()))])),
            ],
        )
        .await
        .unwrap();
    assert_eq!(outcome.deleted, 2);
    assert_eq!(scan_count(&backend).await, 2);

    // drop_collection.
    backend.drop_collection("people").await.unwrap();
    assert!(
        !backend
            .list_collections()
            .await
            .unwrap()
            .contains(&"people".to_string())
    );
}

#[tokio::test]
async fn read_only_backend_rejects_writes() {
    let env = new_mongo_env().await.unwrap();
    let writer = new_backend(&env.uri, false).await;
    writer.create_collection(&demo_spec()).await.unwrap();
    writer
        .put_item("people", person("u1", "Ada", 30))
        .await
        .unwrap();

    let reader = new_backend(&env.uri, true).await;
    assert!(reader.is_read_only());
    let err = reader
        .put_item("people", person("u2", "Bob", 31))
        .await
        .unwrap_err();
    assert!(matches!(err, dynamate::core::error::DbError::ReadOnly));

    // Reads still work.
    assert_eq!(scan_count(&reader).await, 1);
}
