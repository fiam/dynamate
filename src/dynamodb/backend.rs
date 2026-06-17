//! The DynamoDB implementation of the neutral [`Datastore`] trait.
//!
//! This is the DynamoDB backend's boundary: it converts `AttributeValue`s to and
//! from neutral [`Value`]s, compiles a [`QueryPlan`] to a `DynamoDbRequest`, and
//! enforces read-only mode. Everything DynamoDB-specific stays on this side of
//! the trait.
//!
//! [`Value`]: crate::core::value::Value

use std::collections::HashMap;
use std::sync::Mutex;

use async_trait::async_trait;
use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::types::{
    DeleteRequest, KeyType, ScalarAttributeType, TableDescription, TimeToLiveStatus, WriteRequest,
};

use crate::core::capabilities::{Capabilities, SecondaryIndexSupport};
use crate::core::datastore::Datastore;
use crate::core::error::{DbError, Result};
use crate::core::query::{
    BatchDeleteOutcome, CreateCollectionSpec, IndexHint, Key, Page, PlanExplanation, PlanKind,
    QueryCost, QueryPlan, QueryResult,
};
use crate::core::schema::{
    CollectionSchema, IndexKind, IndexSchema, KeyField, KeyRole, KeySchema, Projection, ScalarType,
};
use crate::core::value::Item;

use super::convert::{attribute_map_from_item, item_from_attribute_map, value_to_attribute_value};
use super::create_table::{
    AttributeType, CreateTableSpec, GsiSpec, IndexProjection, KeySpec, LsiSpec, create_table,
};
use super::executor::{self, Kind, Output};
use super::language::parse_query_text;
use super::request_builder::DynamoDbRequest;
use super::table_analyzer::{KeyCondition, KeyConditionType, QueryType, TableInfo};
use super::{QueryBuilder, ScanBuilder, format_sdk_error, send_dynamo_request};

use crate::core::query::KeyEquals;

const CAPABILITIES: Capabilities = Capabilities {
    backend_label: "DynamoDB",
    set_types: true,
    binary_type: true,
    secondary_indexes: SecondaryIndexSupport::DynamoStyleGsiLsi,
    create_collection: true,
    drop_collection: true,
    batch_delete: true,
    purge: true,
    index_query: true,
    ttl: true,
    scanned_count: true,
    consumed_capacity: true,
    raw_query: false,
};

/// Maximum number of delete requests per `BatchWriteItem` call.
const BATCH_WRITE_CHUNK: usize = 25;

pub struct DynamoBackend {
    client: Client,
    read_only: bool,
    /// Cache of table descriptions, used to route queries without an extra
    /// `DescribeTable` per page. Invalidated on create/drop.
    schema_cache: Mutex<HashMap<String, TableDescription>>,
}

impl DynamoBackend {
    pub fn new(client: Client, read_only: bool) -> Self {
        Self {
            client,
            read_only,
            schema_cache: Mutex::new(HashMap::new()),
        }
    }

    fn cached_description(&self, name: &str) -> Option<TableDescription> {
        self.schema_cache.lock().unwrap().get(name).cloned()
    }

    fn cache_description(&self, name: &str, desc: TableDescription) {
        self.schema_cache
            .lock()
            .unwrap()
            .insert(name.to_string(), desc);
    }

    fn invalidate(&self, name: &str) {
        self.schema_cache.lock().unwrap().remove(name);
    }

    async fn table_description(&self, name: &str) -> Result<TableDescription> {
        if let Some(desc) = self.cached_description(name) {
            return Ok(desc);
        }
        let desc = self.fetch_table_description(name).await?;
        self.cache_description(name, desc.clone());
        Ok(desc)
    }

    async fn fetch_table_description(&self, name: &str) -> Result<TableDescription> {
        let span = tracing::trace_span!("DescribeTable", table = %name);
        let result = send_dynamo_request(
            span,
            || self.client.describe_table().table_name(name).send(),
            format_sdk_error,
        )
        .await;
        let output = result.map_err(|err| DbError::Backend(format_sdk_error(&err)))?;
        output
            .table()
            .cloned()
            .ok_or_else(|| DbError::NotFound(name.to_string()))
    }

    async fn fetch_ttl_attribute(&self, name: &str) -> Option<String> {
        let span = tracing::trace_span!("DescribeTimeToLive", table = %name);
        let output = send_dynamo_request(
            span,
            || self.client.describe_time_to_live().table_name(name).send(),
            std::string::ToString::to_string,
        )
        .await
        .ok()?;
        let desc = output.time_to_live_description()?;
        let enabled = matches!(
            desc.time_to_live_status(),
            Some(TimeToLiveStatus::Enabled | TimeToLiveStatus::Enabling)
        );
        if enabled {
            desc.attribute_name().map(std::string::ToString::to_string)
        } else {
            None
        }
    }

    /// Parse a plan's text filter (with the partition-key shortcut), then build
    /// the SDK request.
    fn build_request(
        &self,
        plan: &QueryPlan,
        table_desc: &TableDescription,
    ) -> Result<DynamoDbRequest> {
        let table_info = TableInfo::from_table_description(table_desc);
        let hash_key = Some(table_info.primary_key.hash_key.as_str()).filter(|key| !key.is_empty());
        let filter = match plan
            .filter
            .as_deref()
            .map(str::trim)
            .filter(|t| !t.is_empty())
        {
            Some(text) => Some(parse_query_text(text, hash_key).map_err(DbError::Backend)?),
            None => None,
        };
        Ok(self.build_request_for(
            filter.as_ref(),
            plan.index_hint.as_ref(),
            plan.key_equals.as_ref(),
            table_desc,
        ))
    }

    fn build_request_for(
        &self,
        filter: Option<&crate::expr::DynamoExpression>,
        index_hint: Option<&IndexHint>,
        key_equals: Option<&KeyEquals>,
        table_desc: &TableDescription,
    ) -> DynamoDbRequest {
        let table_info = TableInfo::from_table_description(table_desc);

        // An exact key lookup (index picker / primary) preserves the precise
        // value and routes straight to a Query.
        if let Some(key_equals) = key_equals {
            let query_type = query_type_for_key_lookup(&table_info, index_hint, key_equals);
            return DynamoDbRequest::Query(Box::new(QueryBuilder::from_query_type(query_type)));
        }

        let Some(filter) = filter else {
            return DynamoDbRequest::Scan(ScanBuilder::new());
        };
        match index_hint {
            None => DynamoDbRequest::from_expression_and_table(filter, table_desc),
            Some(IndexHint::Primary) => {
                request_from_query_type(table_info.primary_query_type(filter), filter)
            }
            Some(IndexHint::Named(index)) => {
                request_from_query_type(table_info.index_query_type(index, filter), filter)
            }
        }
    }
}

fn query_type_for_key_lookup(
    table_info: &TableInfo,
    index_hint: Option<&IndexHint>,
    key_equals: &KeyEquals,
) -> QueryType {
    let condition = KeyCondition {
        attribute_name: key_equals.attribute.clone(),
        condition: KeyConditionType::Equal(value_to_attribute_value(&key_equals.value)),
    };
    match index_hint {
        None | Some(IndexHint::Primary) => QueryType::TableQuery {
            hash_key_condition: condition,
            range_key_condition: None,
        },
        Some(IndexHint::Named(name)) => {
            let is_gsi = table_info
                .global_secondary_indexes
                .iter()
                .any(|gsi| gsi.name == *name);
            if is_gsi {
                QueryType::GlobalSecondaryIndexQuery {
                    index_name: name.clone(),
                    hash_key_condition: condition,
                    range_key_condition: None,
                }
            } else {
                QueryType::LocalSecondaryIndexQuery {
                    index_name: name.clone(),
                    hash_key_condition: condition,
                    range_key_condition: None,
                }
            }
        }
    }
}

fn request_from_query_type(
    query_type: QueryType,
    filter: &crate::expr::DynamoExpression,
) -> DynamoDbRequest {
    match query_type {
        QueryType::TableScan => DynamoDbRequest::Scan(ScanBuilder::from_expression(filter)),
        other => DynamoDbRequest::Query(Box::new(QueryBuilder::from_query_type(other))),
    }
}

#[async_trait]
impl Datastore for DynamoBackend {
    fn capabilities(&self) -> &Capabilities {
        &CAPABILITIES
    }

    fn query_language(&self) -> &dyn crate::core::language::QueryLanguage {
        static LANGUAGE: super::language::DynamoLanguage = super::language::DynamoLanguage;
        &LANGUAGE
    }

    fn is_read_only(&self) -> bool {
        self.read_only
    }

    async fn validate(&self) -> Result<()> {
        let span = tracing::trace_span!("ListTables", validation = true, limit = 1);
        send_dynamo_request(
            span,
            || self.client.list_tables().limit(1).send(),
            std::string::ToString::to_string,
        )
        .await
        .map(|_| ())
        .map_err(|err| DbError::Backend(format!("Failed to connect to DynamoDB: {err}")))
    }

    async fn list_collections(&self) -> Result<Vec<String>> {
        let mut names = Vec::new();
        let mut start = None;
        loop {
            let span = tracing::trace_span!("ListTables");
            let request = self
                .client
                .list_tables()
                .set_exclusive_start_table_name(start.clone());
            let output = send_dynamo_request(span, || request.send(), format_sdk_error)
                .await
                .map_err(|err| DbError::Backend(format_sdk_error(&err)))?;
            names.extend(output.table_names().iter().cloned());
            start = output
                .last_evaluated_table_name()
                .map(std::string::ToString::to_string);
            if start.is_none() {
                break;
            }
        }
        Ok(names)
    }

    async fn describe_collection(&self, name: &str) -> Result<CollectionSchema> {
        // A single DescribeTable; TTL is fetched separately via `describe_ttl`
        // only by callers that need it (the picker lists many tables and doesn't).
        let desc = self.fetch_table_description(name).await?;
        self.cache_description(name, desc.clone());
        Ok(collection_schema_from(&desc, None))
    }

    async fn query(&self, name: &str, plan: &QueryPlan, page: Page) -> Result<QueryResult> {
        let table_desc = self.table_description(name).await?;
        let request = self.build_request(plan, &table_desc)?;
        let start_key = page.cursor.map(|cursor| attribute_map_from_item(&cursor.0));
        let limit = page.limit.map(|value| value as i32);
        let output = executor::execute_page(&self.client, name, &request, start_key, limit)
            .await
            .map_err(|err| DbError::Backend(err.to_string()))?;
        Ok(query_result_from(output))
    }

    async fn put_item(&self, name: &str, item: Item) -> Result<()> {
        if self.read_only {
            return Err(DbError::ReadOnly);
        }
        let attributes = attribute_map_from_item(&item);
        let span = tracing::trace_span!("PutItem", table = %name);
        send_dynamo_request(
            span,
            || {
                self.client
                    .put_item()
                    .table_name(name)
                    .set_item(Some(attributes.clone()))
                    .send()
            },
            format_sdk_error,
        )
        .await
        .map(|_| ())
        .map_err(|err| DbError::Backend(format_sdk_error(&err)))
    }

    async fn delete_item(&self, name: &str, key: Key) -> Result<()> {
        if self.read_only {
            return Err(DbError::ReadOnly);
        }
        let key_map = attribute_map_from_item(&key.0);
        let span = tracing::trace_span!("DeleteItem", table = %name);
        send_dynamo_request(
            span,
            || {
                self.client
                    .delete_item()
                    .table_name(name)
                    .set_key(Some(key_map.clone()))
                    .send()
            },
            format_sdk_error,
        )
        .await
        .map(|_| ())
        .map_err(|err| DbError::Backend(format_sdk_error(&err)))
    }

    async fn batch_delete(&self, name: &str, keys: Vec<Key>) -> Result<BatchDeleteOutcome> {
        if self.read_only {
            return Err(DbError::ReadOnly);
        }
        let mut deleted = 0_u64;
        for chunk in keys.chunks(BATCH_WRITE_CHUNK) {
            let mut requests: Vec<WriteRequest> = chunk
                .iter()
                .map(|key| {
                    let delete = DeleteRequest::builder()
                        .set_key(Some(attribute_map_from_item(&key.0)))
                        .build()
                        .map_err(|err| DbError::Backend(err.to_string()))?;
                    Ok(WriteRequest::builder().delete_request(delete).build())
                })
                .collect::<Result<_>>()?;

            // Retry unprocessed items until the batch drains.
            while !requests.is_empty() {
                let batch = HashMap::from([(name.to_string(), requests.clone())]);
                let span = tracing::trace_span!("BatchWriteItem", table = %name);
                let output = send_dynamo_request(
                    span,
                    || {
                        self.client
                            .batch_write_item()
                            .set_request_items(Some(batch.clone()))
                            .send()
                    },
                    format_sdk_error,
                )
                .await
                .map_err(|err| DbError::Backend(format_sdk_error(&err)))?;

                let unprocessed = output
                    .unprocessed_items()
                    .and_then(|items| items.get(name))
                    .cloned()
                    .unwrap_or_default();
                deleted += (requests.len() - unprocessed.len()) as u64;
                requests = unprocessed;
            }
        }
        Ok(BatchDeleteOutcome { deleted })
    }

    async fn create_collection(&self, spec: &CreateCollectionSpec) -> Result<()> {
        if self.read_only {
            return Err(DbError::ReadOnly);
        }
        let table_spec = create_table_spec_from(spec)?;
        let result = create_table(self.client.clone(), table_spec)
            .await
            .map_err(DbError::Backend);
        self.invalidate(&spec.name);
        result
    }

    async fn drop_collection(&self, name: &str) -> Result<()> {
        if self.read_only {
            return Err(DbError::ReadOnly);
        }
        let span = tracing::trace_span!("DeleteTable", table = %name);
        let result = send_dynamo_request(
            span,
            || self.client.delete_table().table_name(name).send(),
            format_sdk_error,
        )
        .await
        .map(|_| ())
        .map_err(|err| DbError::Backend(format_sdk_error(&err)));
        self.invalidate(name);
        result
    }

    async fn describe_ttl(&self, name: &str) -> Result<Option<String>> {
        Ok(self.fetch_ttl_attribute(name).await)
    }

    async fn explain(&self, name: &str, plan: &QueryPlan) -> PlanExplanation {
        let Ok(table_desc) = self.table_description(name).await else {
            return PlanExplanation::Unknown;
        };
        let Ok(request) = self.build_request(plan, &table_desc) else {
            return PlanExplanation::Unknown;
        };
        let kind = match request {
            DynamoDbRequest::Scan(_) => PlanKind::Scan,
            DynamoDbRequest::Query(builder) => PlanKind::IndexedQuery {
                index: builder.index_name().cloned(),
            },
        };
        PlanExplanation::Predicted(kind)
    }
}

fn query_result_from(output: Output) -> QueryResult {
    let plan_kind = match output.kind() {
        Kind::Scan => PlanKind::Scan,
        Kind::Query => PlanKind::IndexedQuery { index: None },
        Kind::QueryGSI(name) | Kind::QueryLSI(name) => PlanKind::IndexedQuery {
            index: Some(name.clone()),
        },
    };
    let cost = output.consumed_capacity().map(|capacity| QueryCost {
        capacity_units: capacity.capacity_units(),
    });
    let next = output
        .last_evaluated_key()
        .map(|key| crate::core::query::Cursor(item_from_attribute_map(key)));
    let items = output.items().iter().map(item_from_attribute_map).collect();
    QueryResult {
        items,
        count: output.count().max(0) as u64,
        scanned_count: Some(output.scanned_count().max(0) as u64),
        next,
        plan_kind,
        cost,
    }
}

fn collection_schema_from(
    desc: &TableDescription,
    ttl_attribute: Option<String>,
) -> CollectionSchema {
    let types = attribute_types(desc);
    let key = key_schema_from(desc.key_schema(), &types);
    let mut indexes = Vec::new();
    for gsi in desc.global_secondary_indexes() {
        indexes.push(IndexSchema {
            name: gsi.index_name().unwrap_or_default().to_string(),
            kind: IndexKind::GlobalSecondary,
            key: key_schema_from(gsi.key_schema(), &types),
            projection: projection_from(gsi.projection()),
        });
    }
    for lsi in desc.local_secondary_indexes() {
        indexes.push(IndexSchema {
            name: lsi.index_name().unwrap_or_default().to_string(),
            kind: IndexKind::LocalSecondary,
            key: key_schema_from(lsi.key_schema(), &types),
            projection: projection_from(lsi.projection()),
        });
    }
    CollectionSchema {
        name: desc.table_name().unwrap_or_default().to_string(),
        key,
        indexes,
        columns: Vec::new(),
        ttl_attribute,
        status: desc
            .table_status()
            .map(|status| status.as_str().to_string()),
        item_count: desc.item_count(),
        size_bytes: desc.table_size_bytes(),
    }
}

fn attribute_types(desc: &TableDescription) -> HashMap<String, ScalarType> {
    desc.attribute_definitions()
        .iter()
        .map(|def| {
            let ty = match def.attribute_type() {
                ScalarAttributeType::N => ScalarType::Number,
                ScalarAttributeType::B => ScalarType::Binary,
                _ => ScalarType::String,
            };
            (def.attribute_name().to_string(), ty)
        })
        .collect()
}

fn key_schema_from(
    schema: &[aws_sdk_dynamodb::types::KeySchemaElement],
    types: &HashMap<String, ScalarType>,
) -> KeySchema {
    let fields = schema
        .iter()
        .filter_map(|element| {
            let role = match element.key_type() {
                KeyType::Hash => KeyRole::Partition,
                KeyType::Range => KeyRole::Sort,
                _ => return None,
            };
            Some(KeyField {
                name: element.attribute_name().to_string(),
                role,
                ty: types
                    .get(element.attribute_name())
                    .copied()
                    .unwrap_or(ScalarType::String),
            })
        })
        .collect();
    KeySchema { fields }
}

fn projection_from(projection: Option<&aws_sdk_dynamodb::types::Projection>) -> Projection {
    use aws_sdk_dynamodb::types::ProjectionType;
    match projection.and_then(aws_sdk_dynamodb::types::Projection::projection_type) {
        Some(ProjectionType::KeysOnly) => Projection::KeysOnly,
        Some(ProjectionType::Include) => Projection::Include(
            projection
                .and_then(|p| p.non_key_attributes.clone())
                .unwrap_or_default(),
        ),
        _ => Projection::All,
    }
}

fn create_table_spec_from(spec: &CreateCollectionSpec) -> Result<CreateTableSpec> {
    let hash_key = key_spec_for_role(&spec.key, KeyRole::Partition)
        .ok_or_else(|| DbError::Backend("Partition key is required".to_string()))?;
    let sort_key = key_spec_for_role(&spec.key, KeyRole::Sort);

    let mut gsis = Vec::new();
    let mut lsis = Vec::new();
    for index in &spec.indexes {
        match index.kind {
            IndexKind::GlobalSecondary | IndexKind::Secondary | IndexKind::Composite => {
                let gsi_hash =
                    key_spec_for_role(&index.key, KeyRole::Partition).ok_or_else(|| {
                        DbError::Backend(format!("Index {} needs a partition key", index.name))
                    })?;
                gsis.push(GsiSpec {
                    name: index.name.clone(),
                    hash_key: gsi_hash,
                    sort_key: key_spec_for_role(&index.key, KeyRole::Sort),
                    projection: index_projection_from(&index.projection),
                });
            }
            IndexKind::LocalSecondary => {
                let sort = key_spec_for_role(&index.key, KeyRole::Sort).ok_or_else(|| {
                    DbError::Backend(format!("LSI {} needs a sort key", index.name))
                })?;
                lsis.push(LsiSpec {
                    name: index.name.clone(),
                    sort_key: sort,
                    projection: index_projection_from(&index.projection),
                });
            }
        }
    }

    Ok(CreateTableSpec {
        table_name: spec.name.clone(),
        hash_key,
        sort_key,
        gsis,
        lsis,
    })
}

fn key_spec_for_role(schema: &KeySchema, role: KeyRole) -> Option<KeySpec> {
    schema
        .fields
        .iter()
        .find(|field| field.role == role)
        .map(|field| KeySpec {
            name: field.name.clone(),
            attr_type: attribute_type_from(field.ty),
        })
}

fn attribute_type_from(ty: ScalarType) -> AttributeType {
    match ty {
        ScalarType::String => AttributeType::String,
        ScalarType::Number => AttributeType::Number,
        ScalarType::Binary => AttributeType::Binary,
    }
}

fn index_projection_from(projection: &Projection) -> IndexProjection {
    match projection {
        Projection::All => IndexProjection::All,
        Projection::KeysOnly => IndexProjection::KeysOnly,
        Projection::Include(attrs) => IndexProjection::Include(attrs.clone()),
    }
}
