use super::error::ParseError;

pub struct Lexer {
    pub input: Vec<char>,
    pub position: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    Identifier(String),
    String(String),
    Number(f64),
    Boolean(bool),
    Null,
    Equal,
    NotEqual,
    Less,
    LessOrEqual,
    Greater,
    GreaterOrEqual,
    And,
    Or,
    Not,
    Between,
    In,
    LeftParen,
    RightParen,
    Comma,
    EOF,
}

impl Lexer {
    pub fn new(input: &str) -> Self {
        Self {
            input: input.chars().collect(),
            position: 0,
        }
    }

    pub fn current_char(&self) -> Option<char> {
        self.input.get(self.position).copied()
    }

    pub fn peek_char(&self) -> Option<char> {
        self.input.get(self.position + 1).copied()
    }

    pub fn advance(&mut self) {
        self.position += 1;
    }

    pub fn skip_whitespace(&mut self) {
        while let Some(ch) = self.current_char() {
            if ch.is_whitespace() {
                self.advance();
            } else {
                break;
            }
        }
    }

    pub fn read_string(&mut self, quote_char: char) -> Result<String, ParseError> {
        let start = self.position;
        self.advance(); // Skip opening quote
        let mut result = String::new();

        while let Some(ch) = self.current_char() {
            if ch == quote_char {
                self.advance(); // Skip closing quote
                return Ok(result);
            } else if ch == '\\' {
                self.advance();
                if let Some(escaped) = self.current_char() {
                    match escaped {
                        '\\' => result.push('\\'),
                        '"' => result.push('"'),
                        '\'' => result.push('\''),
                        'n' => result.push('\n'),
                        'r' => result.push('\r'),
                        't' => result.push('\t'),
                        c => result.push(c),
                    }
                    self.advance();
                } else {
                    return Err(ParseError::InvalidEscapeSequence { position: self.position });
                }
            } else {
                result.push(ch);
                self.advance();
            }
        }

        Err(ParseError::UnterminatedQuote {
            position: start,
            quote_char,
        })
    }

    pub fn read_identifier(&mut self) -> String {
        let mut result = String::new();
        while let Some(ch) = self.current_char() {
            if ch.is_alphanumeric() || ch == '_' {
                result.push(ch);
                self.advance();
            } else {
                break;
            }
        }
        result
    }

    pub fn read_number(&mut self) -> Result<f64, ParseError> {
        let mut result = String::new();
        while let Some(ch) = self.current_char() {
            if ch.is_ascii_digit() || ch == '.' {
                result.push(ch);
                self.advance();
            } else {
                break;
            }
        }
        result.parse().map_err(|_| ParseError::InvalidSyntax {
            message: format!("Invalid number: {}", result),
            position: self.position,
        })
    }

    pub fn next_token(&mut self) -> Result<Token, ParseError> {
        self.skip_whitespace();

        match self.current_char() {
            None => Ok(Token::EOF),
            Some('(') => {
                self.advance();
                Ok(Token::LeftParen)
            }
            Some(')') => {
                self.advance();
                Ok(Token::RightParen)
            }
            Some(',') => {
                self.advance();
                Ok(Token::Comma)
            }
            Some('=') => {
                self.advance();
                Ok(Token::Equal)
            }
            Some('!') => {
                self.advance();
                if self.current_char() == Some('=') {
                    self.advance();
                    Ok(Token::NotEqual)
                } else {
                    Err(ParseError::UnexpectedToken {
                        token: "!".to_string(),
                        position: self.position - 1,
                    })
                }
            }
            Some('<') => {
                self.advance();
                if self.current_char() == Some('=') {
                    self.advance();
                    Ok(Token::LessOrEqual)
                } else if self.current_char() == Some('>') {
                    self.advance();
                    Ok(Token::NotEqual)
                } else {
                    Ok(Token::Less)
                }
            }
            Some('>') => {
                self.advance();
                if self.current_char() == Some('=') {
                    self.advance();
                    Ok(Token::GreaterOrEqual)
                } else {
                    Ok(Token::Greater)
                }
            }
            Some('"') => {
                let s = self.read_string('"')?;
                Ok(Token::String(s))
            }
            Some('\'') => {
                let s = self.read_string('\'')?;
                Ok(Token::String(s))
            }
            Some(ch) if ch.is_ascii_digit() => {
                let num = self.read_number()?;
                Ok(Token::Number(num))
            }
            Some(ch) if ch.is_alphabetic() || ch == '_' => {
                let ident = self.read_identifier();
                match ident.to_uppercase().as_str() {
                    "AND" => Ok(Token::And),
                    "OR" => Ok(Token::Or),
                    "NOT" => Ok(Token::Not),
                    "BETWEEN" => Ok(Token::Between),
                    "IN" => Ok(Token::In),
                    "TRUE" => Ok(Token::Boolean(true)),
                    "FALSE" => Ok(Token::Boolean(false)),
                    "NULL" => Ok(Token::Null),
                    _ => Ok(Token::Identifier(ident)),
                }
            }
            Some(ch) => Err(ParseError::UnexpectedToken {
                token: ch.to_string(),
                position: self.position,
            }),
        }
    }

    pub fn peek_token(&mut self) -> Result<Token, ParseError> {
        let saved_position = self.position;
        let token = self.next_token()?;
        self.position = saved_position;
        Ok(token)
    }
}