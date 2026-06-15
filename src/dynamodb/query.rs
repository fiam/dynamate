use std::collections::HashMap;

use aws_sdk_dynamodb::types::AttributeValue;

use super::table_analyzer::{KeyCondition, KeyConditionType, QueryType, TableInfo};
use crate::expr::{DynamoExpression, FunctionName, Operand};

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

    pub fn from_query_type(query_type: QueryType) -> Self {
        let mut builder = Self {
            query_type,
            key_condition_expression: None,
            filter_expression: None,
            expression_attribute_names: HashMap::new(),
            expression_attribute_values: HashMap::new(),
        };

        let mut name_counter = 0;
        let mut value_counter = 0;

        match &builder.query_type {
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
                let mut key_conditions = Vec::new();
                let hash_condition_str = Self::build_key_condition_string_static(
                    hash_key_condition,
                    &mut builder.expression_attribute_names,
                    &mut builder.expression_attribute_values,
                    &mut name_counter,
                    &mut value_counter,
                );
                key_conditions.push(hash_condition_str);

                if let Some(range_condition) = range_key_condition {
                    let range_condition_str = Self::build_key_condition_string_static(
                        range_condition,
                        &mut builder.expression_attribute_names,
                        &mut builder.expression_attribute_values,
                        &mut name_counter,
                        &mut value_counter,
                    );
                    key_conditions.push(range_condition_str);
                }

                builder.key_condition_expression = Some(key_conditions.join(" AND "));
            }
            QueryType::TableScan => {}
        }

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
        let name_placeholder = format!("#name{name_counter}");
        *name_counter += 1;
        expression_attribute_names.insert(
            name_placeholder.clone(),
            key_condition.attribute_name.clone(),
        );

        match &key_condition.condition {
            KeyConditionType::Equal(value) => {
                let value_placeholder = format!(":val{value_counter}");
                *value_counter += 1;
                expression_attribute_values.insert(value_placeholder.clone(), value.clone());
                format!("{name_placeholder} = {value_placeholder}")
            }
            KeyConditionType::Between(lower, upper) => {
                let lower_placeholder = format!(":val{value_counter}");
                *value_counter += 1;
                let upper_placeholder = format!(":val{value_counter}");
                *value_counter += 1;
                expression_attribute_values.insert(lower_placeholder.clone(), lower.clone());
                expression_attribute_values.insert(upper_placeholder.clone(), upper.clone());
                format!("{name_placeholder} BETWEEN {lower_placeholder} AND {upper_placeholder}")
            }
            KeyConditionType::LessThan(value) => {
                let value_placeholder = format!(":val{value_counter}");
                *value_counter += 1;
                expression_attribute_values.insert(value_placeholder.clone(), value.clone());
                format!("{name_placeholder} < {value_placeholder}")
            }
            KeyConditionType::LessThanOrEqual(value) => {
                let value_placeholder = format!(":val{value_counter}");
                *value_counter += 1;
                expression_attribute_values.insert(value_placeholder.clone(), value.clone());
                format!("{name_placeholder} <= {value_placeholder}")
            }
            KeyConditionType::GreaterThan(value) => {
                let value_placeholder = format!(":val{value_counter}");
                *value_counter += 1;
                expression_attribute_values.insert(value_placeholder.clone(), value.clone());
                format!("{name_placeholder} > {value_placeholder}")
            }
            KeyConditionType::GreaterThanOrEqual(value) => {
                let value_placeholder = format!(":val{value_counter}");
                *value_counter += 1;
                expression_attribute_values.insert(value_placeholder.clone(), value.clone());
                format!("{name_placeholder} >= {value_placeholder}")
            }
            KeyConditionType::BeginsWith(value) => {
                let value_placeholder = format!(":val{value_counter}");
                *value_counter += 1;
                expression_attribute_values.insert(value_placeholder.clone(), value.clone());
                format!("begins_with({name_placeholder}, {value_placeholder})")
            }
        }
    }

    /// The sub-expression that is *not* part of the key condition, to be applied
    /// as a `FilterExpression`. The query routing only succeeds for an AND-chain
    /// of leaf conditions (see `table_analyzer::extract_conditions_recursive`),
    /// so we drop the leaves that target a key attribute and AND the rest.
    fn extract_non_key_conditions(&self, expr: &DynamoExpression) -> Option<DynamoExpression> {
        let key_attrs = self.key_attribute_names();
        let mut leaves = Vec::new();
        collect_and_leaves(expr, &mut leaves);
        let filters: Vec<DynamoExpression> = leaves
            .into_iter()
            .filter(|leaf| !leaf_targets_key(leaf, &key_attrs))
            .cloned()
            .collect();
        combine_and(filters)
    }

    fn key_attribute_names(&self) -> Vec<String> {
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
                let mut names = vec![hash_key_condition.attribute_name.clone()];
                if let Some(range) = range_key_condition {
                    names.push(range.attribute_name.clone());
                }
                names
            }
            QueryType::TableScan => Vec::new(),
        }
    }
}

/// Flatten an AND/parenthesised tree into its leaf conditions.
fn collect_and_leaves<'a>(expr: &'a DynamoExpression, out: &mut Vec<&'a DynamoExpression>) {
    match expr {
        DynamoExpression::And(left, right) => {
            collect_and_leaves(left, out);
            collect_and_leaves(right, out);
        }
        DynamoExpression::Parentheses(inner) => collect_and_leaves(inner, out),
        other => out.push(other),
    }
}

/// Whether a leaf condition targets one of the key attributes (and so was
/// consumed by the key condition rather than the filter).
fn leaf_targets_key(expr: &DynamoExpression, key_attrs: &[String]) -> bool {
    let attr = match expr {
        DynamoExpression::Comparison {
            left: Operand::Path(attr),
            ..
        }
        | DynamoExpression::Between {
            operand: Operand::Path(attr),
            ..
        } => Some(attr),
        DynamoExpression::Function {
            name: FunctionName::BeginsWith,
            args,
        } => match args.first() {
            Some(Operand::Path(attr)) => Some(attr),
            _ => None,
        },
        _ => None,
    };
    attr.is_some_and(|attr| key_attrs.iter().any(|k| k == attr))
}

/// AND a list of conditions into a single expression (None when empty).
fn combine_and(exprs: Vec<DynamoExpression>) -> Option<DynamoExpression> {
    let mut iter = exprs.into_iter();
    let first = iter.next()?;
    Some(iter.fold(first, |acc, expr| {
        DynamoExpression::And(Box::new(acc), Box::new(expr))
    }))
}
