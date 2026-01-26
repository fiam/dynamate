use std::{collections::HashMap, sync::Arc};

use aws_sdk_dynamodb::{
    Client, Error,
    operation::{query::QueryOutput, scan::ScanOutput},
    types::{AttributeValue, ConsumedCapacity},
};

use super::{DynamoDbRequest, QueryBuilder, QueryType, ScanBuilder};

pub enum Kind {
    Scan,
    Query,
    QueryGSI(String), // index_name
    QueryLSI(String), // index_name
}

pub struct Output {
    items: Option<Vec<HashMap<String, AttributeValue>>>,
    count: i32,
    scanned_count: i32,
    last_evaluated_key: Option<HashMap<String, AttributeValue>>,
    consumed_capacity: Option<ConsumedCapacity>,
    kind: Kind,
}

impl Output {
    pub fn items(&self) -> &[HashMap<String, AttributeValue>] {
        self.items.as_deref().unwrap_or(&[])
    }

    pub fn count(&self) -> i32 {
        self.count
    }

    pub fn scanned_count(&self) -> i32 {
        self.scanned_count
    }

    pub fn last_evaluated_key(&self) -> Option<&HashMap<String, AttributeValue>> {
        self.last_evaluated_key.as_ref()
    }

    pub fn consumed_capacity(&self) -> Option<&ConsumedCapacity> {
        self.consumed_capacity.as_ref()
    }

    pub fn kind(&self) -> &Kind {
        &self.kind
    }
}

pub async fn execute(
    client: Arc<Client>,
    table_name: &str,
    db_request: &DynamoDbRequest,
) -> Result<Output, Error> {
    execute_page(client, table_name, db_request, None, None).await
}

pub async fn execute_page(
    client: Arc<Client>,
    table_name: &str,
    db_request: &DynamoDbRequest,
    start_key: Option<HashMap<String, AttributeValue>>,
    limit: Option<i32>,
) -> Result<Output, Error> {
    tracing::debug!(
        "Dynamo request: table={}, kind={:?}, start_key_present={}, limit={:?}",
        table_name,
        match db_request {
            DynamoDbRequest::Query(_) => "Query",
            DynamoDbRequest::Scan(_) => "Scan",
        },
        start_key.is_some(),
        limit
    );
    match db_request {
        DynamoDbRequest::Query(builder) => {
            let output = execute_query(client, table_name, builder, start_key, limit).await?;
            let kind = match builder.query_type() {
                QueryType::TableQuery { .. } => Kind::Query,
                QueryType::GlobalSecondaryIndexQuery { index_name, .. } => {
                    Kind::QueryGSI(index_name.clone())
                }
                QueryType::LocalSecondaryIndexQuery { index_name, .. } => {
                    Kind::QueryLSI(index_name.clone())
                }
                QueryType::TableScan => panic!("Unexpected TableScan for Query"),
            };
            Ok(Output {
                items: output.items,
                count: output.count,
                scanned_count: output.scanned_count,
                last_evaluated_key: output.last_evaluated_key,
                consumed_capacity: output.consumed_capacity,
                kind,
            })
        }
        DynamoDbRequest::Scan(builder) => {
            let result = execute_scan(client, table_name, builder, start_key, limit).await?;
            Ok(Output {
                items: result.items,
                count: result.count,
                scanned_count: result.scanned_count,
                last_evaluated_key: result.last_evaluated_key,
                consumed_capacity: result.consumed_capacity,
                kind: Kind::Scan,
            })
        }
    }
}

async fn execute_scan(
    client: Arc<Client>,
    table_name: &str,
    builder: &ScanBuilder,
    start_key: Option<HashMap<String, AttributeValue>>,
    limit: Option<i32>,
) -> Result<ScanOutput, aws_sdk_dynamodb::Error> {
    let mut request = client.scan().table_name(table_name);

    tracing::debug!(
        "Scan: table={}, filter expression: {:?}, attribute names: {:?}, attribute values {:?}, start_key_present={}, limit={:?}",
        table_name,
        builder.filter_expression(),
        builder.expression_attribute_names(),
        builder.expression_attribute_values(),
        start_key.is_some(),
        limit
    );

    if let Some(filter_expr) = builder.filter_expression() {
        request = request.filter_expression(filter_expr);

        for (key, value) in builder.expression_attribute_names() {
            request = request.expression_attribute_names(key.clone(), value.clone());
        }

        for (key, value) in builder.expression_attribute_values() {
            request = request.expression_attribute_values(key.clone(), value.clone());
        }
    }

    if let Some(start_key) = start_key {
        request = request.set_exclusive_start_key(Some(start_key));
    }

    if let Some(limit) = limit {
        request = request.limit(limit);
    }

    let output = request.send().await?;
    Ok(output)
}

async fn execute_query(
    client: Arc<Client>,
    table_name: &str,
    builder: &QueryBuilder,
    start_key: Option<HashMap<String, AttributeValue>>,
    limit: Option<i32>,
) -> Result<QueryOutput, Error> {
    let mut request = client.query().table_name(table_name);

    // Set index name if this is an index query
    if let Some(index_name) = builder.index_name() {
        request = request.index_name(index_name.clone());
    }

    // Set key condition expression
    if let Some(key_condition) = builder.key_condition_expression() {
        request = request.key_condition_expression(key_condition);
    }

    // Set filter expression if present
    if let Some(filter_expr) = builder.filter_expression() {
        request = request.filter_expression(filter_expr);
    }

    // Set expression attribute names and values
    for (key, value) in builder.expression_attribute_names() {
        request = request.expression_attribute_names(key.clone(), value.clone());
    }

    for (key, value) in builder.expression_attribute_values() {
        request = request.expression_attribute_values(key.clone(), value.clone());
    }

    let start_key_present = start_key.is_some();
    if let Some(start_key) = start_key {
        request = request.set_exclusive_start_key(Some(start_key));
    }

    if let Some(limit) = limit {
        request = request.limit(limit);
    }

    tracing::debug!(
        "Query: table={}, index={:?}, key condition expression: {:?}, filter expression: {:?}, attribute names: {:?}, attribute values: {:?}, start_key_present={}, limit={:?}",
        table_name,
        builder.index_name(),
        builder.key_condition_expression(),
        builder.filter_expression(),
        builder.expression_attribute_names(),
        builder.expression_attribute_values(),
        start_key_present,
        limit
    );

    let output = request.send().await?;

    Ok(output)
}
