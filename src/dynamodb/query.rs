use std::collections::HashMap;

use aws_sdk_dynamodb::types::AttributeValue;

use super::table_analyzer::{KeyCondition, KeyConditionType, QueryType, TableInfo};
use crate::expr::DynamoExpression;

pub struct QueryBuilder {
    query_type: QueryType,
    key_condition_expression: Option<String>,
    filter_expression: Option<String>,
    expression_attribute_names: HashMap<String, String>,
    expression_attribute_values: HashMap<String, AttributeValue>,
}

impl QueryBuilder {
    pub fn new(table_info: &TableInfo, expr: &DynamoExpression) -> Self {
        let query_type = table_info.analyze_query_type(expr);
        let mut builder = Self {
            query_type,
            key_condition_expression: None,
            filter_expression: None,
            expression_attribute_names: HashMap::new(),
            expression_attribute_values: HashMap::new(),
        };

        builder.build_conditions_from_expression(expr);
        builder
    }

    pub fn query_type(&self) -> &QueryType {
        &self.query_type
    }

    pub fn key_condition_expression(&self) -> Option<&String> {
        self.key_condition_expression.as_ref()
    }

    pub fn filter_expression(&self) -> Option<&String> {
        self.filter_expression.as_ref()
    }

    pub fn expression_attribute_names(&self) -> &HashMap<String, String> {
        &self.expression_attribute_names
    }

    pub fn expression_attribute_values(&self) -> &HashMap<String, AttributeValue> {
        &self.expression_attribute_values
    }

    pub fn index_name(&self) -> Option<&String> {
        match &self.query_type {
            QueryType::GlobalSecondaryIndexQuery { index_name, .. }
            | QueryType::LocalSecondaryIndexQuery { index_name, .. } => Some(index_name),
            _ => None,
        }
    }

    pub fn is_query(&self) -> bool {
        !matches!(self.query_type, QueryType::TableScan)
    }

    fn build_conditions_from_expression(&mut self, expr: &DynamoExpression) {
        let mut name_counter = 0;
        let mut value_counter = 0;

        match &self.query_type {
            QueryType::TableQuery {
                hash_key_condition,
                range_key_condition,
            }
            | QueryType::GlobalSecondaryIndexQuery {
                hash_key_condition,
                range_key_condition,
                ..
            }
            | QueryType::LocalSecondaryIndexQuery {
                hash_key_condition,
                range_key_condition,
                ..
            } => {
                // Build key condition expression
                let mut key_conditions = Vec::new();

                // Add hash key condition
                let hash_condition_str = Self::build_key_condition_string_static(
                    hash_key_condition,
                    &mut self.expression_attribute_names,
                    &mut self.expression_attribute_values,
                    &mut name_counter,
                    &mut value_counter,
                );
                key_conditions.push(hash_condition_str);

                // Add range key condition if present
                if let Some(range_condition) = range_key_condition {
                    let range_condition_str = Self::build_key_condition_string_static(
                        range_condition,
                        &mut self.expression_attribute_names,
                        &mut self.expression_attribute_values,
                        &mut name_counter,
                        &mut value_counter,
                    );
                    key_conditions.push(range_condition_str);
                }

                self.key_condition_expression = Some(key_conditions.join(" AND "));

                // Build filter expression for remaining conditions
                if let Some(remaining_expr) = self.extract_non_key_conditions(expr) {
                    self.filter_expression =
                        Some(super::scan::ScanBuilder::build_filter_expression_static(
                            &remaining_expr,
                            &mut self.expression_attribute_names,
                            &mut self.expression_attribute_values,
                            &mut name_counter,
                            &mut value_counter,
                        ));
                }
            }
            QueryType::TableScan => {
                // For scan, everything goes into filter expression
                self.filter_expression =
                    Some(super::scan::ScanBuilder::build_filter_expression_static(
                        expr,
                        &mut self.expression_attribute_names,
                        &mut self.expression_attribute_values,
                        &mut name_counter,
                        &mut value_counter,
                    ));
            }
        }
    }

    fn build_key_condition_string_static(
        key_condition: &KeyCondition,
        expression_attribute_names: &mut HashMap<String, String>,
        expression_attribute_values: &mut HashMap<String, AttributeValue>,
        name_counter: &mut u32,
        value_counter: &mut u32,
    ) -> String {
        let name_placeholder = format!("#name{}", name_counter);
        *name_counter += 1;
        expression_attribute_names.insert(
            name_placeholder.clone(),
            key_condition.attribute_name.clone(),
        );

        match &key_condition.condition {
            KeyConditionType::Equal(value) => {
                let value_placeholder = format!(":val{}", value_counter);
                *value_counter += 1;
                expression_attribute_values.insert(value_placeholder.clone(), value.clone());
                format!("{} = {}", name_placeholder, value_placeholder)
            }
            KeyConditionType::Between(lower, upper) => {
                let lower_placeholder = format!(":val{}", value_counter);
                *value_counter += 1;
                let upper_placeholder = format!(":val{}", value_counter);
                *value_counter += 1;
                expression_attribute_values.insert(lower_placeholder.clone(), lower.clone());
                expression_attribute_values.insert(upper_placeholder.clone(), upper.clone());
                format!(
                    "{} BETWEEN {} AND {}",
                    name_placeholder, lower_placeholder, upper_placeholder
                )
            }
            KeyConditionType::LessThan(value) => {
                let value_placeholder = format!(":val{}", value_counter);
                *value_counter += 1;
                expression_attribute_values.insert(value_placeholder.clone(), value.clone());
                format!("{} < {}", name_placeholder, value_placeholder)
            }
            KeyConditionType::LessThanOrEqual(value) => {
                let value_placeholder = format!(":val{}", value_counter);
                *value_counter += 1;
                expression_attribute_values.insert(value_placeholder.clone(), value.clone());
                format!("{} <= {}", name_placeholder, value_placeholder)
            }
            KeyConditionType::GreaterThan(value) => {
                let value_placeholder = format!(":val{}", value_counter);
                *value_counter += 1;
                expression_attribute_values.insert(value_placeholder.clone(), value.clone());
                format!("{} > {}", name_placeholder, value_placeholder)
            }
            KeyConditionType::GreaterThanOrEqual(value) => {
                let value_placeholder = format!(":val{}", value_counter);
                *value_counter += 1;
                expression_attribute_values.insert(value_placeholder.clone(), value.clone());
                format!("{} >= {}", name_placeholder, value_placeholder)
            }
            KeyConditionType::BeginsWith(value) => {
                let value_placeholder = format!(":val{}", value_counter);
                *value_counter += 1;
                expression_attribute_values.insert(value_placeholder.clone(), value.clone());
                format!("begins_with({}, {})", name_placeholder, value_placeholder)
            }
        }
    }

    fn extract_non_key_conditions(&self, expr: &DynamoExpression) -> Option<DynamoExpression> {
        // This is a simplified implementation
        // In a complete implementation, you would recursively walk the expression tree
        // and extract only the parts that are NOT used in the key condition
        // For now, we'll return None to indicate no additional filter is needed
        // when we have exact key matches

        match &self.query_type {
            QueryType::TableQuery {
                hash_key_condition,
                range_key_condition,
            }
            | QueryType::GlobalSecondaryIndexQuery {
                hash_key_condition,
                range_key_condition,
                ..
            }
            | QueryType::LocalSecondaryIndexQuery {
                hash_key_condition,
                range_key_condition,
                ..
            } => {
                // If we have both hash and range key conditions that are exact matches,
                // and the expression is a simple AND of those conditions, no filter needed
                if range_key_condition.is_some() {
                    if let Some(remaining) = self.extract_remaining_conditions(
                        expr,
                        hash_key_condition,
                        range_key_condition.as_ref(),
                    ) {
                        return Some(remaining);
                    }
                } else {
                    if let Some(remaining) =
                        self.extract_remaining_conditions(expr, hash_key_condition, None)
                    {
                        return Some(remaining);
                    }
                }
            }
            QueryType::TableScan => {
                return Some(expr.clone());
            }
        }

        None
    }

    fn extract_remaining_conditions(
        &self,
        _expr: &DynamoExpression,
        _hash_condition: &KeyCondition,
        _range_condition: Option<&KeyCondition>,
    ) -> Option<DynamoExpression> {
        // Simplified: assume all conditions are used for the key condition
        // A full implementation would parse the expression tree and extract
        // only the conditions that are NOT part of the key conditions
        None
    }
}
