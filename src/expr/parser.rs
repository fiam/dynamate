use super::ast::{DynamoExpression, FunctionName, Operand, Comparator};
use super::error::ParseError;
use super::lexer::{Lexer, Token};

pub fn parse_dynamo_expression(input: &str) -> Result<DynamoExpression, ParseError> {
    let mut lexer = Lexer::new(input);
    parse_or_expression(&mut lexer)
}

pub fn parse_or_expression(lexer: &mut Lexer) -> Result<DynamoExpression, ParseError> {
    let mut expr = parse_and_expression(lexer)?;

    while let Ok(Token::Or) = lexer.peek_token() {
        lexer.next_token()?; // consume OR
        let right = parse_and_expression(lexer)?;
        expr = DynamoExpression::Or(Box::new(expr), Box::new(right));
    }

    Ok(expr)
}

pub fn parse_and_expression(lexer: &mut Lexer) -> Result<DynamoExpression, ParseError> {
    let mut expr = parse_not_expression(lexer)?;

    while let Ok(Token::And) = lexer.peek_token() {
        lexer.next_token()?; // consume AND
        let right = parse_not_expression(lexer)?;
        expr = DynamoExpression::And(Box::new(expr), Box::new(right));
    }

    Ok(expr)
}

pub fn parse_not_expression(lexer: &mut Lexer) -> Result<DynamoExpression, ParseError> {
    if let Ok(Token::Not) = lexer.peek_token() {
        lexer.next_token()?; // consume NOT
        let expr = parse_primary_expression(lexer)?;
        Ok(DynamoExpression::Not(Box::new(expr)))
    } else {
        parse_primary_expression(lexer)
    }
}

pub fn parse_primary_expression(lexer: &mut Lexer) -> Result<DynamoExpression, ParseError> {
    match lexer.peek_token()? {
        Token::LeftParen => {
            lexer.next_token()?; // consume (
            let expr = parse_or_expression(lexer)?;
            match lexer.next_token()? {
                Token::RightParen => Ok(DynamoExpression::Parentheses(Box::new(expr))),
                token => Err(ParseError::UnexpectedToken {
                    token: format!("{:?}", token),
                    position: lexer.position,
                }),
            }
        }
        Token::Identifier(_) => {
            if is_function_start(lexer)? {
                parse_function(lexer)
            } else {
                parse_operand_expression(lexer)
            }
        }
        _ => parse_operand_expression(lexer),
    }
}

pub fn is_function_start(lexer: &mut Lexer) -> Result<bool, ParseError> {
    let saved_position = lexer.position;
    if let Ok(Token::Identifier(name)) = lexer.next_token() {
        let is_func = matches!(
            name.to_lowercase().as_str(),
            "attribute_exists" | "attribute_not_exists" | "attribute_type" | "begins_with" | "contains" | "size"
        );
        lexer.position = saved_position;
        Ok(is_func)
    } else {
        lexer.position = saved_position;
        Ok(false)
    }
}

pub fn parse_function(lexer: &mut Lexer) -> Result<DynamoExpression, ParseError> {
    let name_token = lexer.next_token()?;
    let name = if let Token::Identifier(name) = name_token {
        match name.to_lowercase().as_str() {
            "attribute_exists" => FunctionName::AttributeExists,
            "attribute_not_exists" => FunctionName::AttributeNotExists,
            "attribute_type" => FunctionName::AttributeType,
            "begins_with" => FunctionName::BeginsWith,
            "contains" => FunctionName::Contains,
            "size" => FunctionName::Size,
            _ => return Err(ParseError::InvalidFunction {
                name,
                position: lexer.position,
            }),
        }
    } else {
        return Err(ParseError::InvalidSyntax {
            message: "Expected function name".to_string(),
            position: lexer.position,
        });
    };

    match lexer.next_token()? {
        Token::LeftParen => {}
        token => return Err(ParseError::UnexpectedToken {
            token: format!("{:?}", token),
            position: lexer.position,
        }),
    }

    let mut args = Vec::new();
    loop {
        if let Ok(Token::RightParen) = lexer.peek_token() {
            lexer.next_token()?; // consume )
            break;
        }

        args.push(parse_operand(lexer)?);

        match lexer.peek_token()? {
            Token::Comma => {
                lexer.next_token()?; // consume comma
            }
            Token::RightParen => {
                lexer.next_token()?; // consume )
                break;
            }
            token => return Err(ParseError::UnexpectedToken {
                token: format!("{:?}", token),
                position: lexer.position,
            }),
        }
    }

    Ok(DynamoExpression::Function { name, args })
}

pub fn parse_operand_expression(lexer: &mut Lexer) -> Result<DynamoExpression, ParseError> {
    let left = parse_operand(lexer)?;

    match lexer.peek_token()? {
        Token::Between => {
            lexer.next_token()?; // consume BETWEEN
            let lower = parse_operand(lexer)?;
            match lexer.next_token()? {
                Token::And => {}
                token => return Err(ParseError::UnexpectedToken {
                    token: format!("{:?}", token),
                    position: lexer.position,
                }),
            }
            let upper = parse_operand(lexer)?;
            Ok(DynamoExpression::Between {
                operand: left,
                lower,
                upper,
            })
        }
        Token::In => {
            lexer.next_token()?; // consume IN
            match lexer.next_token()? {
                Token::LeftParen => {}
                token => return Err(ParseError::UnexpectedToken {
                    token: format!("{:?}", token),
                    position: lexer.position,
                }),
            }

            let mut values = Vec::new();
            loop {
                if let Ok(Token::RightParen) = lexer.peek_token() {
                    lexer.next_token()?; // consume )
                    break;
                }

                values.push(parse_operand(lexer)?);

                match lexer.peek_token()? {
                    Token::Comma => {
                        lexer.next_token()?; // consume comma
                    }
                    Token::RightParen => {
                        lexer.next_token()?; // consume )
                        break;
                    }
                    token => return Err(ParseError::UnexpectedToken {
                        token: format!("{:?}", token),
                        position: lexer.position,
                    }),
                }
            }

            Ok(DynamoExpression::In { operand: left, values })
        }
        Token::Equal => {
            lexer.next_token()?;
            let right = parse_operand(lexer)?;
            Ok(DynamoExpression::Comparison {
                left,
                operator: Comparator::Equal,
                right,
            })
        }
        Token::NotEqual => {
            lexer.next_token()?;
            let right = parse_operand(lexer)?;
            Ok(DynamoExpression::Comparison {
                left,
                operator: Comparator::NotEqual,
                right,
            })
        }
        Token::Less => {
            lexer.next_token()?;
            let right = parse_operand(lexer)?;
            Ok(DynamoExpression::Comparison {
                left,
                operator: Comparator::Less,
                right,
            })
        }
        Token::LessOrEqual => {
            lexer.next_token()?;
            let right = parse_operand(lexer)?;
            Ok(DynamoExpression::Comparison {
                left,
                operator: Comparator::LessOrEqual,
                right,
            })
        }
        Token::Greater => {
            lexer.next_token()?;
            let right = parse_operand(lexer)?;
            Ok(DynamoExpression::Comparison {
                left,
                operator: Comparator::Greater,
                right,
            })
        }
        Token::GreaterOrEqual => {
            lexer.next_token()?;
            let right = parse_operand(lexer)?;
            Ok(DynamoExpression::Comparison {
                left,
                operator: Comparator::GreaterOrEqual,
                right,
            })
        }
        _ => Err(ParseError::InvalidSyntax {
            message: "Expected comparison operator, BETWEEN, or IN".to_string(),
            position: lexer.position,
        }),
    }
}

pub fn parse_operand(lexer: &mut Lexer) -> Result<Operand, ParseError> {
    match lexer.next_token()? {
        Token::Identifier(name) => Ok(Operand::Path(name)),
        Token::String(s) => Ok(Operand::Value(s)),
        Token::Number(n) => Ok(Operand::Number(n)),
        Token::Boolean(b) => Ok(Operand::Boolean(b)),
        Token::Null => Ok(Operand::Null),
        token => Err(ParseError::UnexpectedToken {
            token: format!("{:?}", token),
            position: lexer.position,
        }),
    }
}