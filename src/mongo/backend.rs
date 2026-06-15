//! The MongoDB implementation of the neutral [`Datastore`] trait.

use async_trait::async_trait;
use futures::TryStreamExt;
use mongodb::{
    Collection, Database, IndexModel,
    bson::{Document, doc},
    options::{Hint, IndexOptions},
};

use crate::core::capabilities::{Capabilities, SecondaryIndexSupport};
use crate::core::datastore::Datastore;
use crate::core::error::{DbError, Result};
use crate::core::language::QueryLanguage;
use crate::core::query::{
    BatchDeleteOutcome, CreateCollectionSpec, Cursor, IndexHint, Key, Page, PlanKind, QueryPlan,
    QueryResult,
};
use crate::core::schema::{
    CollectionSchema, IndexKind, IndexSchema, KeyField, KeyRole, KeySchema, ScalarType,
};
use crate::core::value::{Item, Number, Value};

use super::convert::{document_to_item, item_to_document, value_to_bson};
use super::language::{MongoLanguage, parse_filter};

const CAPABILITIES: Capabilities = Capabilities {
    backend_label: "MongoDB",
    set_types: false,
    binary_type: true,
    secondary_indexes: SecondaryIndexSupport::Arbitrary,
    create_collection: true,
    drop_collection: true,
    batch_delete: true,
    ttl: false,
    scanned_count: false,
    consumed_capacity: false,
};

/// Documents per `$or` chunk in a batch delete (keeps the command well under
/// the 16 MB BSON limit).
const BATCH_DELETE_CHUNK: usize = 1000;

/// The pagination-cursor key carrying the running skip offset.
const SKIP_KEY: &str = "__skip";

pub struct MongoBackend {
    db: Database,
    read_only: bool,
}

impl MongoBackend {
    pub fn new(db: Database, read_only: bool) -> Self {
        Self { db, read_only }
    }

    fn collection(&self, name: &str) -> Collection<Document> {
        self.db.collection::<Document>(name)
    }

    fn ensure_writable(&self) -> Result<()> {
        if self.read_only {
            Err(DbError::ReadOnly)
        } else {
            Ok(())
        }
    }
}

#[async_trait]
impl Datastore for MongoBackend {
    fn capabilities(&self) -> &Capabilities {
        &CAPABILITIES
    }

    fn query_language(&self) -> &dyn QueryLanguage {
        static LANGUAGE: MongoLanguage = MongoLanguage;
        &LANGUAGE
    }

    fn is_read_only(&self) -> bool {
        self.read_only
    }

    async fn validate(&self) -> Result<()> {
        self.db
            .run_command(doc! { "ping": 1 })
            .await
            .map(|_| ())
            .map_err(|err| DbError::Backend(format!("Failed to connect to MongoDB: {err}")))
    }

    async fn list_collections(&self) -> Result<Vec<String>> {
        self.db
            .list_collection_names()
            .await
            .map_err(|err| DbError::Backend(err.to_string()))
    }

    async fn describe_collection(&self, name: &str) -> Result<CollectionSchema> {
        let collection = self.collection(name);

        let mut indexes = Vec::new();
        if let Ok(cursor) = collection.list_indexes().await
            && let Ok(models) = cursor.try_collect::<Vec<IndexModel>>().await
        {
            for model in models {
                let index_name = model
                    .options
                    .and_then(|opts| opts.name)
                    .unwrap_or_else(|| model.keys.keys().cloned().collect::<Vec<_>>().join("_"));
                let fields = model
                    .keys
                    .keys()
                    .enumerate()
                    .map(|(idx, field)| KeyField {
                        name: field.clone(),
                        role: if idx == 0 {
                            KeyRole::Partition
                        } else {
                            KeyRole::Sort
                        },
                        ty: ScalarType::String,
                    })
                    .collect();
                indexes.push(IndexSchema {
                    name: index_name,
                    kind: IndexKind::Secondary,
                    key: KeySchema { fields },
                    projection: crate::core::schema::Projection::All,
                });
            }
        }

        let item_count = collection
            .estimated_document_count()
            .await
            .ok()
            .map(|count| count as i64);

        Ok(CollectionSchema {
            name: name.to_string(),
            key: KeySchema {
                fields: vec![KeyField {
                    name: "_id".to_string(),
                    role: KeyRole::Partition,
                    ty: ScalarType::String,
                }],
            },
            indexes,
            ttl_attribute: None,
            status: None,
            item_count,
            size_bytes: None,
        })
    }

    async fn query(&self, name: &str, plan: &QueryPlan, page: Page) -> Result<QueryResult> {
        let collection = self.collection(name);

        let filter = if let Some(key_equals) = plan.key_equals.as_ref() {
            doc! { key_equals.attribute.clone(): value_to_bson(&key_equals.value) }
        } else if let Some(text) = plan.filter.as_ref() {
            parse_filter(text).map_err(DbError::Backend)?
        } else {
            Document::new()
        };

        let pins_id = filter.contains_key("_id");
        let plan_kind = match &plan.index_hint {
            Some(IndexHint::Named(index)) => PlanKind::IndexedQuery {
                index: Some(index.clone()),
            },
            _ if plan.key_equals.is_some() || pins_id => PlanKind::IndexedQuery { index: None },
            _ => PlanKind::Scan,
        };

        let skip = skip_from_cursor(page.cursor.as_ref());
        let limit = page.limit.map(|value| value as i64);

        // Sort by _id for stable skip/limit pagination.
        let mut find = collection.find(filter).sort(doc! { "_id": 1 }).skip(skip);
        if let Some(limit) = limit {
            find = find.limit(limit);
        }
        if let Some(IndexHint::Named(index)) = &plan.index_hint {
            find = find.hint(Hint::Name(index.clone()));
        }

        let cursor = find
            .await
            .map_err(|err| DbError::Backend(err.to_string()))?;
        let docs: Vec<Document> = cursor
            .try_collect()
            .await
            .map_err(|err| DbError::Backend(err.to_string()))?;

        let count = docs.len() as u64;
        let items: Vec<Item> = docs.iter().map(document_to_item).collect();
        let next = match page.limit {
            Some(limit) if count == u64::from(limit) => {
                Some(Cursor(skip_cursor(skip + u64::from(limit))))
            }
            _ => None,
        };

        Ok(QueryResult {
            items,
            count,
            scanned_count: None,
            next,
            plan_kind,
            cost: None,
        })
    }

    async fn put_item(&self, name: &str, item: Item) -> Result<()> {
        self.ensure_writable()?;
        let collection = self.collection(name);
        let document = item_to_document(&item);
        if let Some(id) = document.get("_id").cloned() {
            collection
                .replace_one(doc! { "_id": id }, document)
                .upsert(true)
                .await
                .map(|_| ())
                .map_err(|err| DbError::Backend(err.to_string()))
        } else {
            collection
                .insert_one(document)
                .await
                .map(|_| ())
                .map_err(|err| DbError::Backend(err.to_string()))
        }
    }

    async fn delete_item(&self, name: &str, key: Key) -> Result<()> {
        self.ensure_writable()?;
        let key_doc = item_to_document(&key.0);
        self.collection(name)
            .delete_one(key_doc)
            .await
            .map(|_| ())
            .map_err(|err| DbError::Backend(err.to_string()))
    }

    async fn batch_delete(&self, name: &str, keys: Vec<Key>) -> Result<BatchDeleteOutcome> {
        self.ensure_writable()?;
        let collection = self.collection(name);
        let mut deleted = 0_u64;
        for chunk in keys.chunks(BATCH_DELETE_CHUNK) {
            let clauses: Vec<Document> = chunk.iter().map(|key| item_to_document(&key.0)).collect();
            if clauses.is_empty() {
                continue;
            }
            let outcome = collection
                .delete_many(doc! { "$or": clauses })
                .await
                .map_err(|err| DbError::Backend(err.to_string()))?;
            deleted += outcome.deleted_count;
        }
        Ok(BatchDeleteOutcome { deleted })
    }

    async fn create_collection(&self, spec: &CreateCollectionSpec) -> Result<()> {
        self.ensure_writable()?;
        self.db
            .create_collection(&spec.name)
            .await
            .map_err(|err| DbError::Backend(err.to_string()))?;
        let collection = self.collection(&spec.name);
        for index in &spec.indexes {
            let mut keys = Document::new();
            for field in &index.key.fields {
                keys.insert(field.name.clone(), 1_i32);
            }
            if keys.is_empty() {
                continue;
            }
            let model = IndexModel::builder()
                .keys(keys)
                .options(IndexOptions::builder().name(index.name.clone()).build())
                .build();
            collection
                .create_index(model)
                .await
                .map_err(|err| DbError::Backend(err.to_string()))?;
        }
        Ok(())
    }

    async fn drop_collection(&self, name: &str) -> Result<()> {
        self.ensure_writable()?;
        self.collection(name)
            .drop()
            .await
            .map_err(|err| DbError::Backend(err.to_string()))
    }
}

fn skip_from_cursor(cursor: Option<&Cursor>) -> u64 {
    cursor
        .and_then(|c| c.0.get(SKIP_KEY))
        .and_then(Value::as_number)
        .and_then(Number::as_i64)
        .unwrap_or(0)
        .max(0) as u64
}

fn skip_cursor(skip: u64) -> Item {
    let mut item = Item::new();
    item.insert(
        SKIP_KEY.to_string(),
        Value::Num(Number::new(skip.to_string())),
    );
    item
}
