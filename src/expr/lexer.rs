use super::error::ParseError;

pub struct Lexer {
    pub input: Vec<char>,
    pub position: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    Identifier(String),
    Path(String),
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
                    return Err(ParseError::InvalidEscapeSequence {
                        position: self.position,
                    });
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
            if is_bare_token_char(ch) {
                result.push(ch);
                self.advance();
            } else {
                break;
            }
        }
        result
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
            Some('`') => {
                let s = self.read_string('`')?;
                Ok(Token::Path(s))
            }
            Some('\'') => {
                let s = self.read_string('\'')?;
                Ok(Token::String(s))
            }
            Some(ch) if is_bare_token_start(ch) => {
                let token = self.read_identifier();
                classify_bare_token(&token)
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

fn is_bare_token_start(ch: char) -> bool {
    is_bare_token_char(ch)
}

fn is_bare_token_char(ch: char) -> bool {
    !ch.is_whitespace()
        && !matches!(
            ch,
            '(' | ')' | ',' | '=' | '!' | '<' | '>' | '"' | '\'' | '`'
        )
}

fn classify_bare_token(token: &str) -> Result<Token, ParseError> {
    let upper = token.to_ascii_uppercase();
    let classified = match upper.as_str() {
        "AND" => Token::And,
        "OR" => Token::Or,
        "NOT" => Token::Not,
        "BETWEEN" => Token::Between,
        "IN" => Token::In,
        "TRUE" => Token::Boolean(true),
        "FALSE" => Token::Boolean(false),
        "NULL" => Token::Null,
        _ => match parse_numeric_bare_token(token) {
            Some(number) => Token::Number(number),
            None => Token::Identifier(token.to_string()),
        },
    };
    Ok(classified)
}

fn parse_numeric_bare_token(token: &str) -> Option<f64> {
    if !token.chars().any(|ch| ch.is_ascii_digit()) {
        return None;
    }
    if !token
        .chars()
        .all(|ch| ch.is_ascii_digit() || matches!(ch, '.' | '-' | '+' | 'e' | 'E'))
    {
        return None;
    }
    token.parse::<f64>().ok()
}
