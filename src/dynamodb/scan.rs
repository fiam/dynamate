use std::collections::HashMap;

use aws_sdk_dynamodb::types::AttributeValue;

use crate::expr::{Comparator, DynamoExpression, FunctionName, Operand};

#[derive(Default)]
pub struct ScanBuilder {
    filter_expression: Option<String>,
    expression_attribute_names: HashMap<String, String>,
    expression_attribute_values: HashMap<String, AttributeValue>,
}

impl ScanBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_expression(expr: &DynamoExpression) -> Self {
        let mut builder = Self::new();
        builder.set_filter_from_expression(expr);
        builder
    }

    pub fn set_filter_from_expression(&mut self, expr: &DynamoExpression) {
        let mut name_counter = 0;
        let mut value_counter = 0;

        let filter_expr = Self::build_filter_expression_static(
            expr,
            &mut self.expression_attribute_names,
            &mut self.expression_attribute_values,
            &mut name_counter,
            &mut value_counter,
        );

        self.filter_expression = Some(filter_expr);
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

    pub fn build_filter_expression_static(
        expr: &DynamoExpression,
        attr_names: &mut HashMap<String, String>,
        attr_values: &mut HashMap<String, AttributeValue>,
        name_counter: &mut u32,
        value_counter: &mut u32,
    ) -> String {
        match expr {
            DynamoExpression::Comparison {
                left,
                operator,
                right,
            } => {
                let left_str = Self::operand_to_string_static(
                    left,
                    attr_names,
                    attr_values,
                    name_counter,
                    value_counter,
                );
                let op_str = match operator {
                    Comparator::Equal => "=",
                    Comparator::NotEqual => "<>",
                    Comparator::Less => "<",
                    Comparator::LessOrEqual => "<=",
                    Comparator::Greater => ">",
                    Comparator::GreaterOrEqual => ">=",
                };
                let right_str = Self::operand_to_string_static(
                    right,
                    attr_names,
                    attr_values,
                    name_counter,
                    value_counter,
                );
                format!("{} {} {}", left_str, op_str, right_str)
            }
            DynamoExpression::Between {
                operand,
                lower,
                upper,
            } => {
                let operand_str = Self::operand_to_string_static(
                    operand,
                    attr_names,
                    attr_values,
                    name_counter,
                    value_counter,
                );
                let lower_str = Self::operand_to_string_static(
                    lower,
                    attr_names,
                    attr_values,
                    name_counter,
                    value_counter,
                );
                let upper_str = Self::operand_to_string_static(
                    upper,
                    attr_names,
                    attr_values,
                    name_counter,
                    value_counter,
                );
                format!("{} BETWEEN {} AND {}", operand_str, lower_str, upper_str)
            }
            DynamoExpression::In { operand, values } => {
                let operand_str = Self::operand_to_string_static(
                    operand,
                    attr_names,
                    attr_values,
                    name_counter,
                    value_counter,
                );
                let value_strs: Vec<String> = values
                    .iter()
                    .map(|v| {
                        Self::operand_to_string_static(
                            v,
                            attr_names,
                            attr_values,
                            name_counter,
                            value_counter,
                        )
                    })
                    .collect();
                format!("{} IN ({})", operand_str, value_strs.join(", "))
            }
            DynamoExpression::Function { name, args } => {
                let func_name = match name {
                    FunctionName::AttributeExists => "attribute_exists",
                    FunctionName::AttributeNotExists => "attribute_not_exists",
                    FunctionName::AttributeType => "attribute_type",
                    FunctionName::BeginsWith => "begins_with",
                    FunctionName::Contains => "contains",
                    FunctionName::Size => "size",
                };
                let arg_strs: Vec<String> = args
                    .iter()
                    .map(|arg| {
                        Self::operand_to_string_static(
                            arg,
                            attr_names,
                            attr_values,
                            name_counter,
                            value_counter,
                        )
                    })
                    .collect();
                format!("{}({})", func_name, arg_strs.join(", "))
            }
            DynamoExpression::And(left, right) => {
                let left_str = Self::build_filter_expression_static(
                    left,
                    attr_names,
                    attr_values,
                    name_counter,
                    value_counter,
                );
                let right_str = Self::build_filter_expression_static(
                    right,
                    attr_names,
                    attr_values,
                    name_counter,
                    value_counter,
                );
                format!("({}) AND ({})", left_str, right_str)
            }
            DynamoExpression::Or(left, right) => {
                let left_str = Self::build_filter_expression_static(
                    left,
                    attr_names,
                    attr_values,
                    name_counter,
                    value_counter,
                );
                let right_str = Self::build_filter_expression_static(
                    right,
                    attr_names,
                    attr_values,
                    name_counter,
                    value_counter,
                );
                format!("({}) OR ({})", left_str, right_str)
            }
            DynamoExpression::Not(inner) => {
                let inner_str = Self::build_filter_expression_static(
                    inner,
                    attr_names,
                    attr_values,
                    name_counter,
                    value_counter,
                );
                format!("NOT ({})", inner_str)
            }
            DynamoExpression::Parentheses(inner) => {
                let inner_str = Self::build_filter_expression_static(
                    inner,
                    attr_names,
                    attr_values,
                    name_counter,
                    value_counter,
                );
                format!("({})", inner_str)
            }
        }
    }

    pub fn operand_to_string_static(
        operand: &Operand,
        attr_names: &mut HashMap<String, String>,
        attr_values: &mut HashMap<String, AttributeValue>,
        name_counter: &mut u32,
        value_counter: &mut u32,
    ) -> String {
        match operand {
            Operand::Path(path) => {
                let name_placeholder = format!("#name{}", name_counter);
                *name_counter += 1;
                attr_names.insert(name_placeholder.clone(), path.clone());
                name_placeholder
            }
            Operand::Value(val) => {
                let value_placeholder = format!(":val{}", value_counter);
                *value_counter += 1;
                attr_values.insert(value_placeholder.clone(), AttributeValue::S(val.clone()));
                value_placeholder
            }
            Operand::Number(num) => {
                let value_placeholder = format!(":val{}", value_counter);
                *value_counter += 1;
                attr_values.insert(
                    value_placeholder.clone(),
                    AttributeValue::N(num.to_string()),
                );
                value_placeholder
            }
            Operand::Boolean(b) => {
                let value_placeholder = format!(":val{}", value_counter);
                *value_counter += 1;
                attr_values.insert(value_placeholder.clone(), AttributeValue::Bool(*b));
                value_placeholder
            }
            Operand::Null => {
                let value_placeholder = format!(":val{}", value_counter);
                *value_counter += 1;
                attr_values.insert(value_placeholder.clone(), AttributeValue::Null(true));
                value_placeholder
            }
        }
    }
}
