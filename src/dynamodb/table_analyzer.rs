use std::collections::HashMap;

use aws_sdk_dynamodb::types::{AttributeValue, KeySchemaElement, KeyType, TableDescription};

use crate::expr::{Comparator, DynamoExpression, Operand};

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

    /// Build a `TableInfo` from a neutral
    /// [`CollectionSchema`](crate::core::schema::CollectionSchema), for routing
    /// predictions in the query language (which has the schema, not the SDK
    /// `TableDescription`).
    pub fn from_collection_schema(schema: &crate::core::schema::CollectionSchema) -> Self {
        use crate::core::schema::IndexKind;
        let primary_key = PrimaryKey {
            hash_key: schema.key.partition_key().unwrap_or_default().to_string(),
            range_key: schema.key.sort_key().map(str::to_string),
        };
        let index = |kind_match: fn(IndexKind) -> bool| {
            schema
                .indexes
                .iter()
                .filter(|idx| kind_match(idx.kind))
                .map(|idx| SecondaryIndex {
                    name: idx.name.clone(),
                    hash_key: idx.key.partition_key().unwrap_or_default().to_string(),
                    range_key: idx.key.sort_key().map(str::to_string),
                })
                .collect()
        };
        Self {
            table_name: schema.name.clone(),
            primary_key,
            global_secondary_indexes: index(|k| !matches!(k, IndexKind::LocalSecondary)),
            local_secondary_indexes: index(|k| matches!(k, IndexKind::LocalSecondary)),
        }
    }

    /// Build a query type forced onto the primary table key, falling back to a
    /// scan if the expression has no usable key condition.
    pub fn primary_query_type(&self, expression: &DynamoExpression) -> QueryType {
        extract_equality_conditions(expression)
            .and_then(|conditions| self.try_table_query(&conditions))
            .unwrap_or(QueryType::TableScan)
    }

    /// Build a query type forced onto the named secondary index, falling back to
    /// a scan if the index is unknown or the expression lacks its key condition.
    pub fn index_query_type(&self, index_name: &str, expression: &DynamoExpression) -> QueryType {
        let Some(conditions) = extract_equality_conditions(expression) else {
            return QueryType::TableScan;
        };
        if let Some(gsi) = self
            .global_secondary_indexes
            .iter()
            .find(|gsi| gsi.name == index_name)
            && let Some(query_type) = self.try_gsi_query(gsi, &conditions)
        {
            return query_type;
        }
        if let Some(lsi) = self
            .local_secondary_indexes
            .iter()
            .find(|lsi| lsi.name == index_name)
            && let Some(query_type) = self.try_lsi_query(lsi, &conditions)
        {
            return query_type;
        }
        QueryType::TableScan
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
                conditions
                    .get(range_key)
                    .and_then(ConditionInfo::to_key_condition)
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

    fn try_gsi_query(
        &self,
        gsi: &SecondaryIndex,
        conditions: &HashMap<String, ConditionInfo>,
    ) -> Option<QueryType> {
        let hash_condition = conditions.get(&gsi.hash_key)?;

        if let Some(hash_key_condition) = hash_condition.to_key_condition() {
            let range_key_condition = if let Some(ref range_key) = gsi.range_key {
                conditions
                    .get(range_key)
                    .and_then(ConditionInfo::to_key_condition)
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

    fn try_lsi_query(
        &self,
        lsi: &SecondaryIndex,
        conditions: &HashMap<String, ConditionInfo>,
    ) -> Option<QueryType> {
        let hash_condition = conditions.get(&lsi.hash_key)?;

        if let Some(hash_key_condition) = hash_condition.to_key_condition() {
            let range_key_condition = if let Some(ref range_key) = lsi.range_key {
                conditions
                    .get(range_key)
                    .and_then(ConditionInfo::to_key_condition)
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
    /// The resolved key condition (already includes both BETWEEN bounds and
    /// begins_with), so downstream just wraps it.
    condition: KeyConditionType,
}

impl ConditionInfo {
    fn to_key_condition(&self) -> Option<KeyCondition> {
        Some(KeyCondition {
            attribute_name: self.attribute_name.clone(),
            condition: self.condition.clone(),
        })
    }
}

/// Map a comparison operator + operand to a key condition type (None for the
/// not-equal operator or a non-value operand, which can't key a query).
fn comparison_key_condition(operator: &Comparator, operand: &Operand) -> Option<KeyConditionType> {
    let value = operand_to_attribute_value(operand)?;
    Some(match operator {
        Comparator::Equal => KeyConditionType::Equal(value),
        Comparator::Less => KeyConditionType::LessThan(value),
        Comparator::LessOrEqual => KeyConditionType::LessThanOrEqual(value),
        Comparator::Greater => KeyConditionType::GreaterThan(value),
        Comparator::GreaterOrEqual => KeyConditionType::GreaterThanOrEqual(value),
        Comparator::NotEqual => return None,
    })
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
        DynamoExpression::Comparison {
            left,
            operator,
            right,
        } => {
            if let Operand::Path(attr_name) = left
                && let Some(condition) = comparison_key_condition(operator, right)
            {
                conditions.insert(
                    attr_name.clone(),
                    ConditionInfo {
                        attribute_name: attr_name.clone(),
                        condition,
                    },
                );
            }
        }
        DynamoExpression::Between {
            operand,
            lower,
            upper,
        } => {
            if let Operand::Path(attr_name) = operand
                && let (Some(lower), Some(upper)) = (
                    operand_to_attribute_value(lower),
                    operand_to_attribute_value(upper),
                )
            {
                conditions.insert(
                    attr_name.clone(),
                    ConditionInfo {
                        attribute_name: attr_name.clone(),
                        condition: KeyConditionType::Between(lower, upper),
                    },
                );
            }
        }
        DynamoExpression::And(left, right) => {
            extract_conditions_recursive(left, conditions)?;
            extract_conditions_recursive(right, conditions)?;
        }
        DynamoExpression::Function { name, args } => {
            // begins_with(path, prefix) is a valid sort-key key condition.
            if matches!(name, crate::expr::FunctionName::BeginsWith)
                && args.len() == 2
                && let (Operand::Path(attr_name), prefix_operand) = (&args[0], &args[1])
                && let Some(prefix) = operand_to_attribute_value(prefix_operand)
            {
                conditions.insert(
                    attr_name.clone(),
                    ConditionInfo {
                        attribute_name: attr_name.clone(),
                        condition: KeyConditionType::BeginsWith(prefix),
                    },
                );
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
