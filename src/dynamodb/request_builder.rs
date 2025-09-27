use aws_sdk_dynamodb::types::TableDescription;

use crate::expr::DynamoExpression;
use super::{QueryBuilder, ScanBuilder, TableInfo, QueryType};

pub enum DynamoDbRequest {
    Query(QueryBuilder),
    Scan(ScanBuilder),
}

impl DynamoDbRequest {
    pub fn from_expression_and_table(expr: &DynamoExpression, table_desc: &TableDescription) -> Self {
        let table_info = TableInfo::from_table_description(table_desc);
        let query_builder = QueryBuilder::new(&table_info, expr);

        if query_builder.is_query() {
            Self::Query(query_builder)
        } else {
            Self::Scan(ScanBuilder::from_expression(expr))
        }
    }

    pub fn is_query(&self) -> bool {
        matches!(self, Self::Query(_))
    }

    pub fn is_scan(&self) -> bool {
        matches!(self, Self::Scan(_))
    }

    pub fn query_builder(&self) -> Option<&QueryBuilder> {
        match self {
            Self::Query(builder) => Some(builder),
            Self::Scan(_) => None,
        }
    }

    pub fn scan_builder(&self) -> Option<&ScanBuilder> {
        match self {
            Self::Query(_) => None,
            Self::Scan(builder) => Some(builder),
        }
    }

    pub fn operation_type(&self) -> String {
        match self {
            Self::Query(builder) => match builder.query_type() {
                QueryType::TableQuery { .. } => "Query (Table)".to_string(),
                QueryType::GlobalSecondaryIndexQuery { index_name, .. } =>
                    format!("Query (GSI: {})", index_name),
                QueryType::LocalSecondaryIndexQuery { index_name, .. } =>
                    format!("Query (LSI: {})", index_name),
                QueryType::TableScan => "Scan".to_string(), // This shouldn't happen
            },
            Self::Scan(_) => "Scan".to_string(),
        }
    }
}