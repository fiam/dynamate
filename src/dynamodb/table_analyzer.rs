use std::collections::HashMap;

use aws_sdk_dynamodb::types::{
    AttributeValue, KeySchemaElement, KeyType, TableDescription,
};

use crate::expr::{DynamoExpression, Operand, Comparator};

#[derive(Debug, Clone)]
pub struct TableInfo {
    pub table_name: String,
    pub primary_key: PrimaryKey,
    pub global_secondary_indexes: Vec<SecondaryIndex>,
    pub local_secondary_indexes: Vec<SecondaryIndex>,
}

#[derive(Debug, Clone)]
pub struct PrimaryKey {
    pub hash_key: String,
    pub range_key: Option<String>,
}

#[derive(Debug, Clone)]
pub struct SecondaryIndex {
    pub name: String,
    pub hash_key: String,
    pub range_key: Option<String>,
}

#[derive(Debug, Clone)]
pub enum QueryType {
    TableQuery {
        hash_key_condition: KeyCondition,
        range_key_condition: Option<KeyCondition>,
    },
    GlobalSecondaryIndexQuery {
        index_name: String,
        hash_key_condition: KeyCondition,
        range_key_condition: Option<KeyCondition>,
    },
    LocalSecondaryIndexQuery {
        index_name: String,
        hash_key_condition: KeyCondition,
        range_key_condition: Option<KeyCondition>,
    },
    TableScan,
}

#[derive(Debug, Clone)]
pub struct KeyCondition {
    pub attribute_name: String,
    pub condition: KeyConditionType,
}

#[derive(Debug, Clone)]
pub enum KeyConditionType {
    Equal(AttributeValue),
    Between(AttributeValue, AttributeValue),
    LessThan(AttributeValue),
    LessThanOrEqual(AttributeValue),
    GreaterThan(AttributeValue),
    GreaterThanOrEqual(AttributeValue),
    BeginsWith(AttributeValue),
}

impl TableInfo {
    pub fn from_table_description(table_desc: &TableDescription) -> Self {
        let table_name = table_desc.table_name().unwrap_or("").to_string();

        let primary_key = extract_primary_key(table_desc);

        let global_secondary_indexes = table_desc
            .global_secondary_indexes()
            .iter()
            .map(|gsi| SecondaryIndex {
                name: gsi.index_name().unwrap_or("").to_string(),
                hash_key: extract_hash_key_from_schema(gsi.key_schema()),
                range_key: extract_range_key_from_schema(gsi.key_schema()),
            })
            .collect();

        let local_secondary_indexes = table_desc
            .local_secondary_indexes()
            .iter()
            .map(|lsi| SecondaryIndex {
                name: lsi.index_name().unwrap_or("").to_string(),
                hash_key: primary_key.hash_key.clone(), // LSI shares hash key with table
                range_key: extract_range_key_from_schema(lsi.key_schema()),
            })
            .collect();

        Self {
            table_name,
            primary_key,
            global_secondary_indexes,
            local_secondary_indexes,
        }
    }

    pub fn analyze_query_type(&self, expression: &DynamoExpression) -> QueryType {
        if let Some(conditions) = extract_equality_conditions(expression) {
            // Try primary table first
            if let Some(query_type) = self.try_table_query(&conditions) {
                return query_type;
            }

            // Try global secondary indexes
            for gsi in &self.global_secondary_indexes {
                if let Some(query_type) = self.try_gsi_query(gsi, &conditions) {
                    return query_type;
                }
            }

            // Try local secondary indexes
            for lsi in &self.local_secondary_indexes {
                if let Some(query_type) = self.try_lsi_query(lsi, &conditions) {
                    return query_type;
                }
            }
        }

        QueryType::TableScan
    }

    fn try_table_query(&self, conditions: &HashMap<String, ConditionInfo>) -> Option<QueryType> {
        let hash_condition = conditions.get(&self.primary_key.hash_key)?;

        if let Some(hash_key_condition) = hash_condition.to_key_condition() {
            let range_key_condition = if let Some(ref range_key) = self.primary_key.range_key {
                conditions.get(range_key).and_then(|c| c.to_key_condition())
            } else {
                None
            };

            return Some(QueryType::TableQuery {
                hash_key_condition,
                range_key_condition,
            });
        }

        None
    }

    fn try_gsi_query(&self, gsi: &SecondaryIndex, conditions: &HashMap<String, ConditionInfo>) -> Option<QueryType> {
        let hash_condition = conditions.get(&gsi.hash_key)?;

        if let Some(hash_key_condition) = hash_condition.to_key_condition() {
            let range_key_condition = if let Some(ref range_key) = gsi.range_key {
                conditions.get(range_key).and_then(|c| c.to_key_condition())
            } else {
                None
            };

            return Some(QueryType::GlobalSecondaryIndexQuery {
                index_name: gsi.name.clone(),
                hash_key_condition,
                range_key_condition,
            });
        }

        None
    }

    fn try_lsi_query(&self, lsi: &SecondaryIndex, conditions: &HashMap<String, ConditionInfo>) -> Option<QueryType> {
        let hash_condition = conditions.get(&lsi.hash_key)?;

        if let Some(hash_key_condition) = hash_condition.to_key_condition() {
            let range_key_condition = if let Some(ref range_key) = lsi.range_key {
                conditions.get(range_key).and_then(|c| c.to_key_condition())
            } else {
                None
            };

            return Some(QueryType::LocalSecondaryIndexQuery {
                index_name: lsi.name.clone(),
                hash_key_condition,
                range_key_condition,
            });
        }

        None
    }
}

#[derive(Debug, Clone)]
struct ConditionInfo {
    attribute_name: String,
    comparator: Comparator,
    operand: Operand,
    upper_bound: Option<Operand>, // For BETWEEN operations
}

impl ConditionInfo {
    fn to_key_condition(&self) -> Option<KeyCondition> {
        let condition = match &self.comparator {
            Comparator::Equal => KeyConditionType::Equal(operand_to_attribute_value(&self.operand)?),
            Comparator::Less => KeyConditionType::LessThan(operand_to_attribute_value(&self.operand)?),
            Comparator::LessOrEqual => KeyConditionType::LessThanOrEqual(operand_to_attribute_value(&self.operand)?),
            Comparator::Greater => KeyConditionType::GreaterThan(operand_to_attribute_value(&self.operand)?),
            Comparator::GreaterOrEqual => KeyConditionType::GreaterThanOrEqual(operand_to_attribute_value(&self.operand)?),
            Comparator::NotEqual => return None, // Not supported for key conditions
        };

        Some(KeyCondition {
            attribute_name: self.attribute_name.clone(),
            condition,
        })
    }

    fn to_between_condition(&self, upper: &Operand) -> Option<KeyCondition> {
        let lower_val = operand_to_attribute_value(&self.operand)?;
        let upper_val = operand_to_attribute_value(upper)?;

        Some(KeyCondition {
            attribute_name: self.attribute_name.clone(),
            condition: KeyConditionType::Between(lower_val, upper_val),
        })
    }
}

fn extract_primary_key(table_desc: &TableDescription) -> PrimaryKey {
    let (hash_key, range_key) = extract_hash_range_from_schema(table_desc.key_schema());

    PrimaryKey {
        hash_key: hash_key.unwrap_or_default(),
        range_key,
    }
}

fn extract_hash_range_from_schema(schema: &[KeySchemaElement]) -> (Option<String>, Option<String>) {
    let mut hash = None;
    let mut range = None;

    for KeySchemaElement {
        attribute_name,
        key_type,
        ..
    } in schema
    {
        match key_type {
            KeyType::Hash => hash = Some(attribute_name.clone()),
            KeyType::Range => range = Some(attribute_name.clone()),
            _ => {}
        }
    }

    (hash, range)
}

fn extract_hash_key_from_schema(schema: &[KeySchemaElement]) -> String {
    for KeySchemaElement {
        attribute_name,
        key_type,
        ..
    } in schema
    {
        if matches!(key_type, KeyType::Hash) {
            return attribute_name.clone();
        }
    }
    String::new()
}

fn extract_range_key_from_schema(schema: &[KeySchemaElement]) -> Option<String> {
    for KeySchemaElement {
        attribute_name,
        key_type,
        ..
    } in schema
    {
        if matches!(key_type, KeyType::Range) {
            return Some(attribute_name.clone());
        }
    }
    None
}

fn extract_equality_conditions(expr: &DynamoExpression) -> Option<HashMap<String, ConditionInfo>> {
    let mut conditions = HashMap::new();
    extract_conditions_recursive(expr, &mut conditions)?;

    if conditions.is_empty() {
        None
    } else {
        Some(conditions)
    }
}

fn extract_conditions_recursive(
    expr: &DynamoExpression,
    conditions: &mut HashMap<String, ConditionInfo>,
) -> Option<()> {
    match expr {
        DynamoExpression::Comparison { left, operator, right } => {
            if let Operand::Path(attr_name) = left {
                let condition = ConditionInfo {
                    attribute_name: attr_name.clone(),
                    comparator: operator.clone(),
                    operand: right.clone(),
                    upper_bound: None,
                };
                conditions.insert(attr_name.clone(), condition);
            }
        }
        DynamoExpression::Between { operand, lower, upper } => {
            if let Operand::Path(attr_name) = operand {
                let condition = ConditionInfo {
                    attribute_name: attr_name.clone(),
                    comparator: Comparator::GreaterOrEqual, // BETWEEN is >= lower AND <= upper
                    operand: lower.clone(),
                    upper_bound: Some(upper.clone()),
                };
                conditions.insert(attr_name.clone(), condition);
            }
        }
        DynamoExpression::And(left, right) => {
            extract_conditions_recursive(left, conditions)?;
            extract_conditions_recursive(right, conditions)?;
        }
        DynamoExpression::Function { name, args } => {
            // Handle begins_with function for range key conditions
            if matches!(name, crate::expr::FunctionName::BeginsWith) && args.len() == 2 {
                if let (Operand::Path(attr_name), prefix_operand) = (&args[0], &args[1]) {
                    conditions.insert(
                        attr_name.clone(),
                        ConditionInfo {
                            attribute_name: attr_name.clone(),
                            comparator: Comparator::GreaterOrEqual, // We'll handle begins_with specially
                            operand: prefix_operand.clone(),
                            upper_bound: None,
                        },
                    );
                }
            }
        }
        _ => {
            // For other types of expressions (OR, NOT, IN, etc.), we can't use Query
            return None;
        }
    }
    Some(())
}

fn operand_to_attribute_value(operand: &Operand) -> Option<AttributeValue> {
    match operand {
        Operand::Value(s) => Some(AttributeValue::S(s.clone())),
        Operand::Number(n) => Some(AttributeValue::N(n.to_string())),
        Operand::Boolean(b) => Some(AttributeValue::Bool(*b)),
        Operand::Null => Some(AttributeValue::Null(true)),
        Operand::Path(_) => None, // Path references can't be converted to values
    }
}