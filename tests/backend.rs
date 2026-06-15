//! Integration tests for the DynamoDB [`Datastore`] implementation, exercising
//! the neutral `Value`/`Item`/`Cursor` boundary end-to-end against
//! `amazon/dynamodb-local`.

use std::time::Duration;

use aws_config::BehaviorVersion;
use aws_sdk_dynamodb::config::{Credentials, Region};
use color_eyre::Result;
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt,
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
};

use dynamate::core::datastore::Datastore;
use dynamate::core::query::{CreateCollectionSpec, IndexHint, Key, Page, PlanKind, QueryPlan};
use dynamate::core::schema::{
    IndexKind, IndexSchema, KeyField, KeyRole, KeySchema, Projection, ScalarType,
};
use dynamate::core::value::{Item, Number, Value};
use dynamate::dynamodb::DynamoBackend;

#[allow(dead_code)]
struct DynamoDBEnv {
    container: ContainerAsync<GenericImage>,
    endpoint_url: String,
}

async fn new_dynamodb_env() -> Result<DynamoDBEnv> {
    let container = GenericImage::new("amazon/dynamodb-local", "2.5.2")
        .with_exposed_port(8000.tcp())
        .with_wait_for(WaitFor::message_on_stdout("CorsParams"))
        .with_user("root")
        .with_cmd(vec!["-jar", "DynamoDBLocal.jar", "-inMemory", "-sharedDb"])
        .start()
        .await
        .expect("Failed to start DynamoDB");
    let port = container.get_host_port_ipv4(8000).await?;
    Ok(DynamoDBEnv {
        container,
        endpoint_url: format!("http://127.0.0.1:{port}"),
    })
}

async fn new_backend(endpoint_url: &str, read_only: bool) -> DynamoBackend {
    let config = aws_config::defaults(BehaviorVersion::latest())
        .region(Region::new("us-east-1"))
        .credentials_provider(Credentials::new("local", "local", None, None, "test"))
        .endpoint_url(endpoint_url)
        .load()
        .await;
    DynamoBackend::new(aws_sdk_dynamodb::Client::new(&config), read_only)
}

fn is_transient(err: &dynamate::core::error::DbError) -> bool {
    let rendered = err.to_string();
    rendered.contains("DispatchFailure")
        || rendered.contains("dispatch failure")
        || rendered.contains("Connection reset")
        || rendered.contains("IncompleteMessage")
}

/// Create a collection, retrying the transient connection resets that
/// `dynamodb-local` occasionally emits right after start-up.
async fn create_with_retry(backend: &DynamoBackend, spec: &CreateCollectionSpec) {
    for attempt in 1..=6 {
        match backend.create_collection(spec).await {
            Ok(()) => return,
            Err(err) if attempt < 6 && is_transient(&err) => {
                tokio::time::sleep(Duration::from_millis(150 * attempt)).await;
            }
            Err(err) => panic!("create_collection failed: {err}"),
        }
    }
}

fn item(entries: Vec<(&str, Value)>) -> Item {
    entries
        .into_iter()
        .map(|(key, value)| (key.to_string(), value))
        .collect()
}

fn demo_spec() -> CreateCollectionSpec {
    CreateCollectionSpec {
        name: "demo".to_string(),
        key: KeySchema {
            fields: vec![
                KeyField {
                    name: "PK".to_string(),
                    role: KeyRole::Partition,
                    ty: ScalarType::String,
                },
                KeyField {
                    name: "SK".to_string(),
                    role: KeyRole::Sort,
                    ty: ScalarType::String,
                },
            ],
        },
        indexes: vec![IndexSchema {
            name: "GSI1".to_string(),
            kind: IndexKind::GlobalSecondary,
            key: KeySchema {
                fields: vec![KeyField {
                    name: "GSI1PK".to_string(),
                    role: KeyRole::Partition,
                    ty: ScalarType::String,
                }],
            },
            projection: Projection::All,
        }],
    }
}

async fn wait_until_listed(backend: &DynamoBackend, name: &str) {
    for _ in 0..40 {
        if backend
            .list_collections()
            .await
            .is_ok_and(|names| names.iter().any(|n| n == name))
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("table {name} never became visible");
}

#[tokio::test]
async fn datastore_round_trips_through_neutral_values() {
    let env = new_dynamodb_env().await.unwrap();
    let backend = new_backend(&env.endpoint_url, false).await;

    create_with_retry(&backend, &demo_spec()).await;
    wait_until_listed(&backend, "demo").await;

    // describe_collection reports the neutral schema.
    let schema = backend.describe_collection("demo").await.unwrap();
    assert_eq!(schema.key.partition_key(), Some("PK"));
    assert_eq!(schema.key.sort_key(), Some("SK"));
    assert_eq!(schema.indexes.len(), 1);
    assert_eq!(schema.indexes[0].name, "GSI1");

    // put_item with a rich neutral value (sets + numbers).
    let stored = item(vec![
        ("PK", Value::Str("user#1".to_string())),
        ("SK", Value::Str("profile".to_string())),
        ("GSI1PK", Value::Str("active".to_string())),
        ("age", Value::Num(Number::new("42"))),
        (
            "tags",
            Value::StringSet(vec!["a".to_string(), "b".to_string()]),
        ),
    ]);
    backend.put_item("demo", stored.clone()).await.unwrap();
    backend
        .put_item(
            "demo",
            item(vec![
                ("PK", Value::Str("user#2".to_string())),
                ("SK", Value::Str("profile".to_string())),
                ("GSI1PK", Value::Str("active".to_string())),
            ]),
        )
        .await
        .unwrap();

    // Query the primary key.
    let result = backend
        .query(
            "demo",
            &QueryPlan::new(Some("PK = \"user#1\"".to_string()), None),
            Page::default(),
        )
        .await
        .unwrap();
    assert_eq!(result.count, 1);
    assert!(matches!(
        result.plan_kind,
        PlanKind::IndexedQuery { index: None }
    ));
    assert_eq!(
        result.items[0].get("tags"),
        Some(&Value::StringSet(vec!["a".to_string(), "b".to_string()]))
    );

    // Query the GSI via an explicit index hint.
    let gsi_result = backend
        .query(
            "demo",
            &QueryPlan::new(
                Some("GSI1PK = \"active\"".to_string()),
                Some(IndexHint::Named("GSI1".to_string())),
            ),
            Page::default(),
        )
        .await
        .unwrap();
    assert_eq!(gsi_result.count, 2);
    assert_eq!(
        gsi_result.plan_kind,
        PlanKind::IndexedQuery {
            index: Some("GSI1".to_string())
        }
    );

    // A no-filter query scans.
    let scan = backend
        .query("demo", &QueryPlan::default(), Page::default())
        .await
        .unwrap();
    assert_eq!(scan.count, 2);
    assert_eq!(scan.plan_kind, PlanKind::Scan);

    // delete_item removes by key.
    backend
        .delete_item(
            "demo",
            Key(item(vec![
                ("PK", Value::Str("user#1".to_string())),
                ("SK", Value::Str("profile".to_string())),
            ])),
        )
        .await
        .unwrap();

    // batch_delete clears the rest.
    let outcome = backend
        .batch_delete(
            "demo",
            vec![Key(item(vec![
                ("PK", Value::Str("user#2".to_string())),
                ("SK", Value::Str("profile".to_string())),
            ]))],
        )
        .await
        .unwrap();
    assert_eq!(outcome.deleted, 1);

    let empty = backend
        .query("demo", &QueryPlan::default(), Page::default())
        .await
        .unwrap();
    assert_eq!(empty.count, 0);
}

#[tokio::test]
async fn read_only_backend_rejects_writes() {
    let env = new_dynamodb_env().await.unwrap();
    let writer = new_backend(&env.endpoint_url, false).await;
    create_with_retry(&writer, &demo_spec()).await;
    wait_until_listed(&writer, "demo").await;

    let reader = new_backend(&env.endpoint_url, true).await;
    assert!(reader.is_read_only());
    let err = reader
        .put_item(
            "demo",
            item(vec![
                ("PK", Value::Str("x".to_string())),
                ("SK", Value::Str("y".to_string())),
            ]),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, dynamate::core::error::DbError::ReadOnly));

    // Reads still work.
    let scan = reader
        .query("demo", &QueryPlan::default(), Page::default())
        .await
        .unwrap();
    assert_eq!(scan.count, 0);
}

/// Query with a text filter, returning every page.
async fn query_all(backend: &DynamoBackend, name: &str, plan: &QueryPlan) -> Vec<Item> {
    let mut out = Vec::new();
    let mut cursor = None;
    loop {
        let page = backend
            .query(
                name,
                plan,
                Page {
                    cursor,
                    limit: Some(2),
                },
            )
            .await
            .unwrap();
        out.extend(page.items);
        cursor = page.next;
        if cursor.is_none() {
            break;
        }
    }
    out
}

#[tokio::test]
async fn pagination_sort_key_conditions_and_filters() {
    let env = new_dynamodb_env().await.unwrap();
    let backend = new_backend(&env.endpoint_url, false).await;
    create_with_retry(&backend, &demo_spec()).await;
    wait_until_listed(&backend, "demo").await;

    // One partition, four sort keys, with an `amount` attribute for filtering.
    let rows = [
        ("ORDER#1", 50),
        ("ORDER#2", 150),
        ("ORDER#3", 250),
        ("INVOICE#1", 999),
    ];
    for (sk, amount) in rows {
        backend
            .put_item(
                "demo",
                item(vec![
                    ("PK", Value::Str("u".to_string())),
                    ("SK", Value::Str(sk.to_string())),
                    ("amount", Value::Num(Number::from(amount))),
                ]),
            )
            .await
            .unwrap();
    }

    // Pagination: limit 2, follow the cursor, see all four rows once.
    let all = query_all(
        &backend,
        "demo",
        &QueryPlan::new(Some("PK = \"u\"".to_string()), None),
    )
    .await;
    assert_eq!(all.len(), 4);

    // begins_with on the sort key → the three ORDER# rows, via a Query.
    let orders = backend
        .query(
            "demo",
            &QueryPlan::new(
                Some("PK = \"u\" AND begins_with(SK, \"ORDER#\")".to_string()),
                None,
            ),
            Page::default(),
        )
        .await
        .unwrap();
    assert_eq!(orders.count, 3);
    assert!(matches!(orders.plan_kind, PlanKind::IndexedQuery { .. }));

    // BETWEEN on the sort key.
    let between = backend
        .query(
            "demo",
            &QueryPlan::new(
                Some("PK = \"u\" AND SK BETWEEN \"ORDER#1\" AND \"ORDER#2\"".to_string()),
                None,
            ),
            Page::default(),
        )
        .await
        .unwrap();
    assert_eq!(between.count, 2);

    // Key condition + non-key filter expression.
    let filtered = backend
        .query(
            "demo",
            &QueryPlan::new(Some("PK = \"u\" AND amount > 100".to_string()), None),
            Page::default(),
        )
        .await
        .unwrap();
    assert_eq!(filtered.count, 3); // 150, 250, 999

    // A standalone scan filter (no key condition).
    let scanned = backend
        .query(
            "demo",
            &QueryPlan::new(Some("amount > 200".to_string()), None),
            Page::default(),
        )
        .await
        .unwrap();
    assert_eq!(scanned.count, 2); // 250, 999
    assert_eq!(scanned.plan_kind, PlanKind::Scan);
}

fn lsi_spec() -> CreateCollectionSpec {
    CreateCollectionSpec {
        name: "lsi_demo".to_string(),
        key: KeySchema {
            fields: vec![
                KeyField {
                    name: "PK".to_string(),
                    role: KeyRole::Partition,
                    ty: ScalarType::String,
                },
                KeyField {
                    name: "SK".to_string(),
                    role: KeyRole::Sort,
                    ty: ScalarType::String,
                },
            ],
        },
        indexes: vec![
            IndexSchema {
                name: "LSI1".to_string(),
                kind: IndexKind::LocalSecondary,
                key: KeySchema {
                    fields: vec![KeyField {
                        name: "LSK".to_string(),
                        role: KeyRole::Sort,
                        ty: ScalarType::String,
                    }],
                },
                projection: Projection::All,
            },
            IndexSchema {
                name: "NIDX".to_string(),
                kind: IndexKind::GlobalSecondary,
                key: KeySchema {
                    fields: vec![KeyField {
                        name: "score".to_string(),
                        role: KeyRole::Partition,
                        ty: ScalarType::Number,
                    }],
                },
                projection: Projection::All,
            },
        ],
    }
}

#[tokio::test]
async fn lsi_query_and_numeric_key_lookup() {
    let env = new_dynamodb_env().await.unwrap();
    let backend = new_backend(&env.endpoint_url, false).await;
    create_with_retry(&backend, &lsi_spec()).await;
    wait_until_listed(&backend, "lsi_demo").await;

    // A 54-bit-plus score that an f64 would round; it must survive exactly.
    let big_score = "9007199254740993";
    backend
        .put_item(
            "lsi_demo",
            item(vec![
                ("PK", Value::Str("u".to_string())),
                ("SK", Value::Str("a".to_string())),
                ("LSK", Value::Str("L1".to_string())),
                ("score", Value::Num(Number::new(big_score))),
            ]),
        )
        .await
        .unwrap();

    // Query the LSI by hint.
    let lsi = backend
        .query(
            "lsi_demo",
            &QueryPlan::new(
                Some("PK = \"u\"".to_string()),
                Some(IndexHint::Named("LSI1".to_string())),
            ),
            Page::default(),
        )
        .await
        .unwrap();
    assert_eq!(lsi.count, 1);
    assert_eq!(
        lsi.plan_kind,
        PlanKind::IndexedQuery {
            index: Some("LSI1".to_string())
        }
    );

    // Numeric key_equals lookup against the GSI (the index-picker path),
    // preserving the exact integer.
    let by_score = backend
        .query(
            "lsi_demo",
            &QueryPlan::key_lookup(
                "score".to_string(),
                Value::Num(Number::new(big_score)),
                IndexHint::Named("NIDX".to_string()),
            ),
            Page::default(),
        )
        .await
        .unwrap();
    assert_eq!(by_score.count, 1);
    assert_eq!(
        by_score.items[0].get("score"),
        Some(&Value::Num(Number::new(big_score)))
    );
}

#[tokio::test]
async fn update_binary_number_roundtrip_and_drop() {
    let env = new_dynamodb_env().await.unwrap();
    let backend = new_backend(&env.endpoint_url, false).await;
    create_with_retry(&backend, &demo_spec()).await;
    wait_until_listed(&backend, "demo").await;

    let key = || {
        vec![
            ("PK", Value::Str("u".to_string())),
            ("SK", Value::Str("a".to_string())),
        ]
    };

    // Binary + a 38-digit number must round-trip exactly through put -> query.
    let big_number = "12345678901234567890123456789012345678";
    let mut first = key();
    first.push(("blob", Value::Bytes(vec![0, 1, 2, 255])));
    first.push(("big", Value::Num(Number::new(big_number))));
    first.push(("v", Value::Str("one".to_string())));
    backend.put_item("demo", item(first)).await.unwrap();

    let fetched = backend
        .query(
            "demo",
            &QueryPlan::new(Some("PK = \"u\"".to_string()), None),
            Page::default(),
        )
        .await
        .unwrap();
    assert_eq!(
        fetched.items[0].get("blob"),
        Some(&Value::Bytes(vec![0, 1, 2, 255]))
    );
    assert_eq!(
        fetched.items[0].get("big"),
        Some(&Value::Num(Number::new(big_number)))
    );

    // put_item overwrites the existing key.
    let mut updated = key();
    updated.push(("v", Value::Str("two".to_string())));
    backend.put_item("demo", item(updated)).await.unwrap();
    let after = backend
        .query(
            "demo",
            &QueryPlan::new(Some("PK = \"u\"".to_string()), None),
            Page::default(),
        )
        .await
        .unwrap();
    assert_eq!(after.count, 1);
    assert_eq!(
        after.items[0].get("v"),
        Some(&Value::Str("two".to_string()))
    );

    // drop_collection removes the table.
    backend.drop_collection("demo").await.unwrap();
    assert!(
        !backend
            .list_collections()
            .await
            .unwrap()
            .contains(&"demo".to_string())
    );
}
