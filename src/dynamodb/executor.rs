use aws_sdk_dynamodb::{
    Client, Error,
    operation::{query::QueryOutput, scan::ScanOutput},
    types::{AttributeValue, ConsumedCapacity},
};
use std::collections::HashMap;

use super::{DynamoDbRequest, QueryBuilder, QueryType, ScanBuilder, send_dynamo_request};

#[derive(Debug, Clone)]
pub enum Kind {
    Scan,
    Query,
    QueryGSI(String), // index_name
    QueryLSI(String), // index_name
}

#[derive(Debug, Clone)]
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
    client: &Client,
    table_name: &str,
    db_request: &DynamoDbRequest,
) -> Result<Output, Error> {
    execute_page(client, table_name, db_request, None, None).await
}

pub async fn execute_page(
    client: &Client,
    table_name: &str,
    db_request: &DynamoDbRequest,
    start_key: Option<HashMap<String, AttributeValue>>,
    limit: Option<i32>,
) -> Result<Output, Error> {
    if let Some(start_key) = start_key.as_ref() {
        tracing::trace!(
            table=%table_name,
            start_key=?start_key,
            "Pagination start key"
        );
    }
    tracing::trace!(
        table=%table_name,
        kind=match db_request {
            DynamoDbRequest::Query(_) => "Query",
            DynamoDbRequest::Scan(_) => "Scan",
        },
        start_key=?start_key,
        start_key_present=start_key.is_some(),
        limit=?limit,
        "Dynamo request"
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
    client: &Client,
    table_name: &str,
    builder: &ScanBuilder,
    start_key: Option<HashMap<String, AttributeValue>>,
    limit: Option<i32>,
) -> Result<ScanOutput, aws_sdk_dynamodb::Error> {
    let mut request = client.scan().table_name(table_name);

    tracing::trace!(
        table=%table_name,
        filter_expression=?builder.filter_expression(),
        attribute_names=?builder.expression_attribute_names(),
        attribute_values=?builder.expression_attribute_values(),
        start_key=?start_key,
        start_key_present=start_key.is_some(),
        limit=?limit,
        "Scan"
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

    let start_key_present = start_key.is_some();
    if let Some(start_key) = start_key {
        request = request.set_exclusive_start_key(Some(start_key));
    }

    if let Some(limit) = limit {
        request = request.limit(limit);
    }

    let span = tracing::trace_span!(
        "Scan",
        table = %table_name,
        start_key_present = start_key_present,
        limit = ?limit
    );
    let result = send_dynamo_request(span, || request.send(), |err| format!("{err:?}")).await;
    Ok(result?)
}

async fn execute_query(
    client: &Client,
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
    tracing::trace!(
        table=%table_name,
        index=?builder.index_name(),
        key_condition_expression=?builder.key_condition_expression(),
        filter_expression=?builder.filter_expression(),
        attribute_names=?builder.expression_attribute_names(),
        attribute_values=?builder.expression_attribute_values(),
        start_key=?start_key,
        start_key_present,
        limit=?limit,
        "Query"
    );

    if let Some(start_key) = start_key {
        request = request.set_exclusive_start_key(Some(start_key));
    }

    if let Some(limit) = limit {
        request = request.limit(limit);
    }

    let span = tracing::trace_span!(
        "Query",
        table = %table_name,
        start_key_present = start_key_present,
        limit = ?limit
    );
    let result = send_dynamo_request(span, || request.send(), |err| format!("{err:?}")).await;
    Ok(result?)
}
