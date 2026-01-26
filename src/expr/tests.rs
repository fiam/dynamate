#[cfg(test)]
mod expr_tests {
    use super::super::*;

    // DynamoDB Expression Tests
    #[test]
    fn test_simple_comparison() {
        let result = parse_dynamo_expression("age = 25").unwrap();
        assert_eq!(
            result,
            DynamoExpression::Comparison {
                left: Operand::Path("age".to_string()),
                operator: Comparator::Equal,
                right: Operand::Number(25.0),
            }
        );
    }

    #[test]
    fn test_string_comparison() {
        let result = parse_dynamo_expression(r#"name = "John""#).unwrap();
        assert_eq!(
            result,
            DynamoExpression::Comparison {
                left: Operand::Path("name".to_string()),
                operator: Comparator::Equal,
                right: Operand::Value("John".to_string()),
            }
        );
    }

    #[test]
    fn test_unquoted_string_defaults_to_value() {
        let result = parse_dynamo_expression("city = Sidney").unwrap();
        assert_eq!(
            result,
            DynamoExpression::Comparison {
                left: Operand::Path("city".to_string()),
                operator: Comparator::Equal,
                right: Operand::Value("Sidney".to_string()),
            }
        );
    }

    #[test]
    fn test_backtick_path_on_rhs() {
        let result = parse_dynamo_expression("city = `other field`").unwrap();
        assert_eq!(
            result,
            DynamoExpression::Comparison {
                left: Operand::Path("city".to_string()),
                operator: Comparator::Equal,
                right: Operand::Path("other field".to_string()),
            }
        );
    }

    #[test]
    fn test_all_comparators() {
        let tests = vec![
            ("age = 25", Comparator::Equal),
            ("age <> 25", Comparator::NotEqual),
            ("age != 25", Comparator::NotEqual),
            ("age < 25", Comparator::Less),
            ("age <= 25", Comparator::LessOrEqual),
            ("age > 25", Comparator::Greater),
            ("age >= 25", Comparator::GreaterOrEqual),
        ];

        for (input, expected_op) in tests {
            let result = parse_dynamo_expression(input).unwrap();
            match result {
                DynamoExpression::Comparison { operator, .. } => {
                    assert_eq!(operator, expected_op);
                }
                _ => panic!("Expected comparison expression"),
            }
        }
    }

    #[test]
    fn test_not_equal_operators() {
        // Test both != and <> work identically
        let expr1 = parse_dynamo_expression("status != \"inactive\"").unwrap();
        let expr2 = parse_dynamo_expression("status <> \"inactive\"").unwrap();

        // Both should parse to the same structure
        assert_eq!(
            expr1,
            DynamoExpression::Comparison {
                left: Operand::Path("status".to_string()),
                operator: Comparator::NotEqual,
                right: Operand::Value("inactive".to_string()),
            }
        );

        assert_eq!(expr1, expr2);
    }

    #[test]
    fn test_not_equal_with_numbers() {
        let result = parse_dynamo_expression("count != 0").unwrap();
        assert_eq!(
            result,
            DynamoExpression::Comparison {
                left: Operand::Path("count".to_string()),
                operator: Comparator::NotEqual,
                right: Operand::Number(0.0),
            }
        );
    }

    #[test]
    fn test_not_equal_with_boolean() {
        let result = parse_dynamo_expression("active != false").unwrap();
        assert_eq!(
            result,
            DynamoExpression::Comparison {
                left: Operand::Path("active".to_string()),
                operator: Comparator::NotEqual,
                right: Operand::Boolean(false),
            }
        );
    }

    #[test]
    fn test_not_equal_with_null() {
        let result = parse_dynamo_expression("deleted_at != null").unwrap();
        assert_eq!(
            result,
            DynamoExpression::Comparison {
                left: Operand::Path("deleted_at".to_string()),
                operator: Comparator::NotEqual,
                right: Operand::Null,
            }
        );
    }

    #[test]
    fn test_complex_expression_with_not_equal() {
        let result = parse_dynamo_expression("status != \"inactive\" AND age >= 18").unwrap();
        match result {
            DynamoExpression::And(left, right) => match (*left, *right) {
                (
                    DynamoExpression::Comparison {
                        left: left_op,
                        operator: Comparator::NotEqual,
                        right: right_op,
                    },
                    DynamoExpression::Comparison {
                        left: left_op2,
                        operator: Comparator::GreaterOrEqual,
                        right: right_op2,
                    },
                ) => {
                    assert_eq!(left_op, Operand::Path("status".to_string()));
                    assert_eq!(right_op, Operand::Value("inactive".to_string()));
                    assert_eq!(left_op2, Operand::Path("age".to_string()));
                    assert_eq!(right_op2, Operand::Number(18.0));
                }
                _ => panic!("Unexpected expression structure"),
            },
            _ => panic!("Expected AND expression"),
        }
    }

    #[test]
    fn test_mixed_not_equal_operators() {
        // Test mixing != and <> in the same expression
        let result =
            parse_dynamo_expression("status != \"inactive\" OR role <> \"guest\"").unwrap();
        match result {
            DynamoExpression::Or(left, right) => {
                match (*left, *right) {
                    (
                        DynamoExpression::Comparison {
                            operator: Comparator::NotEqual,
                            ..
                        },
                        DynamoExpression::Comparison {
                            operator: Comparator::NotEqual,
                            ..
                        },
                    ) => {
                        // Both should be NotEqual operators
                    }
                    _ => panic!("Expected both comparisons to be NotEqual"),
                }
            }
            _ => panic!("Expected OR expression"),
        }
    }

    #[test]
    fn test_invalid_exclamation_mark() {
        // Test that standalone ! without = is rejected
        let result = parse_dynamo_expression("age ! 25");
        assert!(result.is_err());

        if let Err(ParseError::UnexpectedToken { token, .. }) = result {
            assert_eq!(token, "!");
        } else {
            panic!("Expected UnexpectedToken error for standalone !");
        }
    }

    #[test]
    fn test_between_expression() {
        let result = parse_dynamo_expression("age BETWEEN 18 AND 65").unwrap();
        assert_eq!(
            result,
            DynamoExpression::Between {
                operand: Operand::Path("age".to_string()),
                lower: Operand::Number(18.0),
                upper: Operand::Number(65.0),
            }
        );
    }

    #[test]
    fn test_in_expression() {
        let result = parse_dynamo_expression(r#"status IN ("active", "pending")"#).unwrap();
        assert_eq!(
            result,
            DynamoExpression::In {
                operand: Operand::Path("status".to_string()),
                values: vec![
                    Operand::Value("active".to_string()),
                    Operand::Value("pending".to_string()),
                ],
            }
        );
    }

    #[test]
    fn test_function_calls() {
        let result = parse_dynamo_expression("attribute_exists(name)").unwrap();
        assert_eq!(
            result,
            DynamoExpression::Function {
                name: FunctionName::AttributeExists,
                args: vec![Operand::Path("name".to_string())],
            }
        );

        let result = parse_dynamo_expression(r#"begins_with(name, "John")"#).unwrap();
        assert_eq!(
            result,
            DynamoExpression::Function {
                name: FunctionName::BeginsWith,
                args: vec![
                    Operand::Path("name".to_string()),
                    Operand::Value("John".to_string()),
                ],
            }
        );
    }

    #[test]
    fn test_and_expression() {
        let result = parse_dynamo_expression("age >= 18 AND status = \"active\"").unwrap();
        match result {
            DynamoExpression::And(left, right) => match (*left, *right) {
                (
                    DynamoExpression::Comparison {
                        left: left_op,
                        operator: Comparator::GreaterOrEqual,
                        right: right_op,
                    },
                    DynamoExpression::Comparison {
                        left: left_op2,
                        operator: Comparator::Equal,
                        right: right_op2,
                    },
                ) => {
                    assert_eq!(left_op, Operand::Path("age".to_string()));
                    assert_eq!(right_op, Operand::Number(18.0));
                    assert_eq!(left_op2, Operand::Path("status".to_string()));
                    assert_eq!(right_op2, Operand::Value("active".to_string()));
                }
                _ => panic!("Unexpected expression structure"),
            },
            _ => panic!("Expected AND expression"),
        }
    }

    #[test]
    fn test_or_expression() {
        let result = parse_dynamo_expression("age < 18 OR age > 65").unwrap();
        match result {
            DynamoExpression::Or(_, _) => {
                // Structure is correct
            }
            _ => panic!("Expected OR expression"),
        }
    }

    #[test]
    fn test_not_expression() {
        let result = parse_dynamo_expression("NOT attribute_exists(deleted_at)").unwrap();
        match result {
            DynamoExpression::Not(inner) => match *inner {
                DynamoExpression::Function {
                    name: FunctionName::AttributeExists,
                    args,
                } => {
                    assert_eq!(args, vec![Operand::Path("deleted_at".to_string())]);
                }
                _ => panic!("Expected function in NOT expression"),
            },
            _ => panic!("Expected NOT expression"),
        }
    }

    #[test]
    fn test_parentheses() {
        let result =
            parse_dynamo_expression("(age >= 18 AND age <= 65) OR status = \"vip\"").unwrap();
        match result {
            DynamoExpression::Or(left, _right) => {
                match *left {
                    DynamoExpression::Parentheses(_) => {
                        // Structure is correct
                    }
                    _ => panic!("Expected parentheses expression"),
                }
            }
            _ => panic!("Expected OR with parentheses"),
        }
    }

    #[test]
    fn test_complex_expression() {
        let input = r#"(attribute_exists(name) AND age BETWEEN 18 AND 65) OR (status IN ("active", "premium") AND NOT attribute_exists(deleted_at))"#;
        let result = parse_dynamo_expression(input);
        assert!(
            result.is_ok(),
            "Failed to parse complex expression: {:?}",
            result
        );
    }

    #[test]
    fn test_boolean_operands() {
        let result = parse_dynamo_expression("active = true").unwrap();
        assert_eq!(
            result,
            DynamoExpression::Comparison {
                left: Operand::Path("active".to_string()),
                operator: Comparator::Equal,
                right: Operand::Boolean(true),
            }
        );
    }

    #[test]
    fn test_null_operands() {
        let result = parse_dynamo_expression("deleted_at = null").unwrap();
        assert_eq!(
            result,
            DynamoExpression::Comparison {
                left: Operand::Path("deleted_at".to_string()),
                operator: Comparator::Equal,
                right: Operand::Null,
            }
        );
    }

    #[test]
    fn test_all_functions() {
        let functions = vec![
            ("attribute_exists(path)", FunctionName::AttributeExists),
            (
                "attribute_not_exists(path)",
                FunctionName::AttributeNotExists,
            ),
            ("attribute_type(path, \"S\")", FunctionName::AttributeType),
            ("begins_with(path, \"prefix\")", FunctionName::BeginsWith),
            ("contains(path, \"substring\")", FunctionName::Contains),
            ("size(path)", FunctionName::Size),
        ];

        for (input, expected_func) in functions {
            let result = parse_dynamo_expression(input).unwrap();
            match result {
                DynamoExpression::Function { name, .. } => {
                    assert_eq!(name, expected_func);
                }
                _ => panic!("Expected function expression for: {}", input),
            }
        }
    }

    #[test]
    fn test_parse_errors() {
        let invalid_inputs = vec![
            "age =",          // Missing operand
            "= 25",           // Missing left operand
            "age BETWEEN 18", // Missing AND clause
            "age IN (",       // Unclosed parenthesis
            "invalid_func()", // Invalid function name
            "age >< 25",      // Invalid operator
            "age ! 25",       // Invalid use of ! without =
        ];

        for input in invalid_inputs {
            let result = parse_dynamo_expression(input);
            assert!(result.is_err(), "Expected error for input: {}", input);
        }
    }

    // Key-Value Parser Tests
    #[test]
    fn test_simple_key_value() {
        let result = parse_expressions("key=value").unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].key, "key");
        assert_eq!(result[0].value, Value::String("value".to_string()));
    }

    #[test]
    fn test_number_inference() {
        let result = parse_expressions("age=25 height=5.9 weight=70").unwrap();
        assert_eq!(result.len(), 3);

        assert_eq!(result[0].key, "age");
        assert_eq!(result[0].value, Value::Number(25.0));

        assert_eq!(result[1].key, "height");
        assert_eq!(result[1].value, Value::Number(5.9));

        assert_eq!(result[2].key, "weight");
        assert_eq!(result[2].value, Value::Number(70.0));
    }

    #[test]
    fn test_quoted_strings_force_string_type() {
        let result = parse_expressions(r#"number_as_string="123" regular_number=123"#).unwrap();
        assert_eq!(result.len(), 2);

        assert_eq!(result[0].key, "number_as_string");
        assert_eq!(result[0].value, Value::String("123".to_string()));

        assert_eq!(result[1].key, "regular_number");
        assert_eq!(result[1].value, Value::Number(123.0));
    }

    #[test]
    fn test_boolean_inference() {
        let result = parse_expressions("active=true debug=false").unwrap();
        assert_eq!(result.len(), 2);

        assert_eq!(result[0].key, "active");
        assert_eq!(result[0].value, Value::Boolean(true));

        assert_eq!(result[1].key, "debug");
        assert_eq!(result[1].value, Value::Boolean(false));
    }

    #[test]
    fn test_quoted_booleans_force_string_type() {
        let result = parse_expressions(r#"bool_as_string="true" regular_bool=true"#).unwrap();
        assert_eq!(result.len(), 2);

        assert_eq!(result[0].key, "bool_as_string");
        assert_eq!(result[0].value, Value::String("true".to_string()));

        assert_eq!(result[1].key, "regular_bool");
        assert_eq!(result[1].value, Value::Boolean(true));
    }

    #[test]
    fn test_null_inference() {
        let result = parse_expressions("empty=null").unwrap();
        assert_eq!(result.len(), 1);

        assert_eq!(result[0].key, "empty");
        assert_eq!(result[0].value, Value::Null);
    }

    #[test]
    fn test_quoted_null_force_string_type() {
        let result = parse_expressions(r#"null_as_string="null" regular_null=null"#).unwrap();
        assert_eq!(result.len(), 2);

        assert_eq!(result[0].key, "null_as_string");
        assert_eq!(result[0].value, Value::String("null".to_string()));

        assert_eq!(result[1].key, "regular_null");
        assert_eq!(result[1].value, Value::Null);
    }

    #[test]
    fn test_mixed_types() {
        let result =
            parse_expressions(r#"name="John" age=30 active=true score=95.5 metadata=null"#)
                .unwrap();
        assert_eq!(result.len(), 5);

        assert_eq!(
            result[0],
            KeyValue {
                key: "name".to_string(),
                value: Value::String("John".to_string())
            }
        );
        assert_eq!(
            result[1],
            KeyValue {
                key: "age".to_string(),
                value: Value::Number(30.0)
            }
        );
        assert_eq!(
            result[2],
            KeyValue {
                key: "active".to_string(),
                value: Value::Boolean(true)
            }
        );
        assert_eq!(
            result[3],
            KeyValue {
                key: "score".to_string(),
                value: Value::Number(95.5)
            }
        );
        assert_eq!(
            result[4],
            KeyValue {
                key: "metadata".to_string(),
                value: Value::Null
            }
        );
    }

    #[test]
    fn test_escaped_quotes() {
        let result = parse_expressions(r#"message="He said \"Hello\" to me""#).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].key, "message");
        assert_eq!(
            result[0].value,
            Value::String(r#"He said "Hello" to me"#.to_string())
        );
    }

    #[test]
    fn test_yaml_style_examples() {
        // Examples that demonstrate YAML-style type inference vs explicit strings
        let yaml_style = r#"
            port=8080
            host="localhost"
            ssl=true
            timeout=30.5
            database=null
            version="1.2.3"
            count="007"
        "#;

        let result = parse_expressions(yaml_style).unwrap();
        assert_eq!(result.len(), 7);

        // Find each key-value pair
        let port = result.iter().find(|kv| kv.key == "port").unwrap();
        assert_eq!(port.value, Value::Number(8080.0));

        let host = result.iter().find(|kv| kv.key == "host").unwrap();
        assert_eq!(host.value, Value::String("localhost".to_string()));

        let ssl = result.iter().find(|kv| kv.key == "ssl").unwrap();
        assert_eq!(ssl.value, Value::Boolean(true));

        let timeout = result.iter().find(|kv| kv.key == "timeout").unwrap();
        assert_eq!(timeout.value, Value::Number(30.5));

        let database = result.iter().find(|kv| kv.key == "database").unwrap();
        assert_eq!(database.value, Value::Null);

        let version = result.iter().find(|kv| kv.key == "version").unwrap();
        assert_eq!(version.value, Value::String("1.2.3".to_string()));

        let count = result.iter().find(|kv| kv.key == "count").unwrap();
        assert_eq!(count.value, Value::String("007".to_string())); // Leading zero preserved
    }

    #[test]
    fn test_legacy_compatibility() {
        // Test that the legacy functions still work
        let result = parse_expressions_legacy("key=value age=25 active=true").unwrap();
        assert_eq!(result.len(), 3);

        assert_eq!(
            result[0],
            LegacyKeyValue {
                key: "key".to_string(),
                value: "value".to_string()
            }
        );
        assert_eq!(
            result[1],
            LegacyKeyValue {
                key: "age".to_string(),
                value: "25".to_string()
            }
        );
        assert_eq!(
            result[2],
            LegacyKeyValue {
                key: "active".to_string(),
                value: "true".to_string()
            }
        );

        let map = parse_to_map_legacy("name=John age=30").unwrap();
        assert_eq!(map.get("name"), Some(&"John".to_string()));
        assert_eq!(map.get("age"), Some(&"30".to_string()));
    }

    #[test]
    fn test_whitespace_handling() {
        let result = parse_expressions("  key1=value1     key2=value2  ").unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].key, "key1");
        assert_eq!(result[0].value, Value::String("value1".to_string()));
        assert_eq!(result[1].key, "key2");
        assert_eq!(result[1].value, Value::String("value2".to_string()));
    }
}
