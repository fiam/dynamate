use super::ast::{Comparator, DynamoExpression, Operand};
use super::builtins::{self, Dialect};
use super::error::ParseError;
use super::lexer::{Lexer, Token};

/// Parse a query expression using the default (DynamoDB) dialect.
pub fn parse_dynamo_expression(input: &str) -> Result<DynamoExpression, ParseError> {
    parse_dynamo_expression_with(input, builtins::default_dialect())
}

/// Parse a query expression, recognizing the functions of `dialect`.
pub fn parse_dynamo_expression_with(
    input: &str,
    dialect: &Dialect,
) -> Result<DynamoExpression, ParseError> {
    let mut parser = Parser {
        lexer: Lexer::new(input),
        dialect,
    };
    let expr = parser.parse_or_expression()?;
    match parser.lexer.next_token()? {
        Token::EOF => Ok(expr),
        token => Err(ParseError::UnexpectedToken {
            token: format!("{token:?}"),
            position: parser.lexer.position,
        }),
    }
}

/// Recursive-descent parser bound to a [`Dialect`] for function recognition.
struct Parser<'a> {
    lexer: Lexer,
    dialect: &'a Dialect,
}

pub fn parse_single_value_token(input: &str) -> Result<Operand, ParseError> {
    let mut lexer = Lexer::new(input);
    let operand = match lexer.next_token()? {
        Token::Identifier(name) => infer_identifier_operand(&name),
        Token::String(value) => Operand::Value(value),
        Token::Number(number) => Operand::Number(number),
        Token::Boolean(value) => Operand::Boolean(value),
        Token::Null => Operand::Null,
        Token::EOF => {
            return Err(ParseError::InvalidSyntax {
                message: "Expected a value token".to_string(),
                position: lexer.position,
            });
        }
        token => {
            return Err(ParseError::UnexpectedToken {
                token: format!("{token:?}"),
                position: lexer.position,
            });
        }
    };

    match lexer.next_token()? {
        Token::EOF => Ok(operand),
        token => Err(ParseError::UnexpectedToken {
            token: format!("{token:?}"),
            position: lexer.position,
        }),
    }
}

impl Parser<'_> {
    fn parse_or_expression(&mut self) -> Result<DynamoExpression, ParseError> {
        let mut expr = self.parse_and_expression()?;

        while let Ok(Token::Or) = self.lexer.peek_token() {
            self.lexer.next_token()?; // consume OR
            let right = self.parse_and_expression()?;
            expr = DynamoExpression::Or(Box::new(expr), Box::new(right));
        }

        Ok(expr)
    }

    fn parse_and_expression(&mut self) -> Result<DynamoExpression, ParseError> {
        let mut expr = self.parse_not_expression()?;

        while let Ok(Token::And) = self.lexer.peek_token() {
            self.lexer.next_token()?; // consume AND
            let right = self.parse_not_expression()?;
            expr = DynamoExpression::And(Box::new(expr), Box::new(right));
        }

        Ok(expr)
    }

    fn parse_not_expression(&mut self) -> Result<DynamoExpression, ParseError> {
        if let Ok(Token::Not) = self.lexer.peek_token() {
            self.lexer.next_token()?; // consume NOT
            let expr = self.parse_primary_expression()?;
            Ok(DynamoExpression::Not(Box::new(expr)))
        } else {
            self.parse_primary_expression()
        }
    }

    fn parse_primary_expression(&mut self) -> Result<DynamoExpression, ParseError> {
        match self.lexer.peek_token()? {
            Token::LeftParen => {
                self.lexer.next_token()?; // consume (
                let expr = self.parse_or_expression()?;
                match self.lexer.next_token()? {
                    Token::RightParen => Ok(DynamoExpression::Parentheses(Box::new(expr))),
                    token => Err(ParseError::UnexpectedToken {
                        token: format!("{token:?}"),
                        position: self.lexer.position,
                    }),
                }
            }
            Token::Identifier(_) => {
                if self.is_function_start()? {
                    self.parse_function()
                } else {
                    self.parse_operand_expression()
                }
            }
            _ => self.parse_operand_expression(),
        }
    }

    fn is_function_start(&mut self) -> Result<bool, ParseError> {
        let saved_position = self.lexer.position;
        if let Ok(Token::Identifier(name)) = self.lexer.next_token() {
            let is_func = self.dialect.is_function_name(&name);
            self.lexer.position = saved_position;
            Ok(is_func)
        } else {
            self.lexer.position = saved_position;
            Ok(false)
        }
    }

    fn parse_function(&mut self) -> Result<DynamoExpression, ParseError> {
        let name_token = self.lexer.next_token()?;
        let name = if let Token::Identifier(name) = name_token {
            match self.dialect.function_by_name(&name) {
                Some(doc) => doc.func.clone(),
                None => {
                    return Err(ParseError::InvalidFunction {
                        name,
                        position: self.lexer.position,
                    });
                }
            }
        } else {
            return Err(ParseError::InvalidSyntax {
                message: "Expected function name".to_string(),
                position: self.lexer.position,
            });
        };

        match self.lexer.next_token()? {
            Token::LeftParen => {}
            token => {
                return Err(ParseError::UnexpectedToken {
                    token: format!("{token:?}"),
                    position: self.lexer.position,
                });
            }
        }

        let mut args = Vec::new();
        let mut arg_index = 0usize;
        loop {
            if let Ok(Token::RightParen) = self.lexer.peek_token() {
                self.lexer.next_token()?; // consume )
                break;
            }

            if arg_index == 0 {
                args.push(self.parse_path_operand()?);
            } else {
                args.push(self.parse_value_operand()?);
            }
            arg_index += 1;

            match self.lexer.peek_token()? {
                Token::Comma => {
                    self.lexer.next_token()?; // consume comma
                }
                Token::RightParen => {
                    self.lexer.next_token()?; // consume )
                    break;
                }
                token => {
                    return Err(ParseError::UnexpectedToken {
                        token: format!("{token:?}"),
                        position: self.lexer.position,
                    });
                }
            }
        }

        Ok(DynamoExpression::Function { name, args })
    }

    fn parse_operand_expression(&mut self) -> Result<DynamoExpression, ParseError> {
        let left = self.parse_path_operand()?;

        match self.lexer.peek_token()? {
            Token::Between => {
                self.lexer.next_token()?; // consume BETWEEN
                let lower = self.parse_value_operand()?;
                match self.lexer.next_token()? {
                    Token::And => {}
                    token => {
                        return Err(ParseError::UnexpectedToken {
                            token: format!("{token:?}"),
                            position: self.lexer.position,
                        });
                    }
                }
                let upper = self.parse_value_operand()?;
                Ok(DynamoExpression::Between {
                    operand: left,
                    lower,
                    upper,
                })
            }
            Token::In => {
                self.lexer.next_token()?; // consume IN
                match self.lexer.next_token()? {
                    Token::LeftParen => {}
                    token => {
                        return Err(ParseError::UnexpectedToken {
                            token: format!("{token:?}"),
                            position: self.lexer.position,
                        });
                    }
                }

                let mut values = Vec::new();
                loop {
                    if let Ok(Token::RightParen) = self.lexer.peek_token() {
                        self.lexer.next_token()?; // consume )
                        break;
                    }

                    values.push(self.parse_value_operand()?);

                    match self.lexer.peek_token()? {
                        Token::Comma => {
                            self.lexer.next_token()?; // consume comma
                        }
                        Token::RightParen => {
                            self.lexer.next_token()?; // consume )
                            break;
                        }
                        token => {
                            return Err(ParseError::UnexpectedToken {
                                token: format!("{token:?}"),
                                position: self.lexer.position,
                            });
                        }
                    }
                }

                Ok(DynamoExpression::In {
                    operand: left,
                    values,
                })
            }
            Token::Equal => {
                self.lexer.next_token()?;
                let right = self.parse_value_operand()?;
                Ok(DynamoExpression::Comparison {
                    left,
                    operator: Comparator::Equal,
                    right,
                })
            }
            Token::NotEqual => {
                self.lexer.next_token()?;
                let right = self.parse_value_operand()?;
                Ok(DynamoExpression::Comparison {
                    left,
                    operator: Comparator::NotEqual,
                    right,
                })
            }
            Token::Less => {
                self.lexer.next_token()?;
                let right = self.parse_value_operand()?;
                Ok(DynamoExpression::Comparison {
                    left,
                    operator: Comparator::Less,
                    right,
                })
            }
            Token::LessOrEqual => {
                self.lexer.next_token()?;
                let right = self.parse_value_operand()?;
                Ok(DynamoExpression::Comparison {
                    left,
                    operator: Comparator::LessOrEqual,
                    right,
                })
            }
            Token::Greater => {
                self.lexer.next_token()?;
                let right = self.parse_value_operand()?;
                Ok(DynamoExpression::Comparison {
                    left,
                    operator: Comparator::Greater,
                    right,
                })
            }
            Token::GreaterOrEqual => {
                self.lexer.next_token()?;
                let right = self.parse_value_operand()?;
                Ok(DynamoExpression::Comparison {
                    left,
                    operator: Comparator::GreaterOrEqual,
                    right,
                })
            }
            _ => Err(ParseError::InvalidSyntax {
                message: "Expected comparison operator, BETWEEN, or IN".to_string(),
                position: self.lexer.position,
            }),
        }
    }

    fn parse_path_operand(&mut self) -> Result<Operand, ParseError> {
        match self.lexer.next_token()? {
            Token::Identifier(name) => Ok(Operand::Path(name)),
            Token::Path(name) => Ok(Operand::Path(name)),
            token => Err(ParseError::UnexpectedToken {
                token: format!("{token:?}"),
                position: self.lexer.position,
            }),
        }
    }

    fn parse_value_operand(&mut self) -> Result<Operand, ParseError> {
        match self.lexer.next_token()? {
            Token::Identifier(name) => Ok(infer_identifier_operand(&name)),
            Token::Path(name) => Ok(Operand::Path(name)),
            Token::String(s) => Ok(Operand::Value(s)),
            Token::Number(n) => Ok(Operand::Number(n)),
            Token::Boolean(b) => Ok(Operand::Boolean(b)),
            Token::Null => Ok(Operand::Null),
            token => Err(ParseError::UnexpectedToken {
                token: format!("{token:?}"),
                position: self.lexer.position,
            }),
        }
    }
}

fn infer_identifier_operand(token: &str) -> Operand {
    let lower = token.to_ascii_lowercase();
    if lower == "true" {
        return Operand::Boolean(true);
    }
    if lower == "false" {
        return Operand::Boolean(false);
    }
    if lower == "null" {
        return Operand::Null;
    }
    if let Some(num) = parse_numeric_identifier(token) {
        return Operand::Number(num);
    }
    Operand::Value(token.to_string())
}

fn parse_numeric_identifier(token: &str) -> Option<f64> {
    if !token.chars().any(|c| c.is_ascii_digit()) {
        return None;
    }
    if !token
        .chars()
        .all(|c| c.is_ascii_digit() || matches!(c, '.' | '-' | '+' | 'e' | 'E'))
    {
        return None;
    }
    token.parse::<f64>().ok()
}
