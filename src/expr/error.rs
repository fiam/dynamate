#[derive(Debug)]
pub enum ParseError {
    UnterminatedQuote { position: usize, quote_char: char },
    InvalidEscapeSequence { position: usize },
    MissingValue { key: String, position: usize },
    InvalidSyntax { message: String, position: usize },
    UnexpectedToken { token: String, position: usize },
    UnexpectedEndOfInput { position: usize },
    InvalidFunction { name: String, position: usize },
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ParseError::UnterminatedQuote {
                position,
                quote_char,
            } => {
                write!(
                    f,
                    "Unterminated quote '{}' at position {}",
                    quote_char, position
                )
            }
            ParseError::InvalidEscapeSequence { position } => {
                write!(f, "Invalid escape sequence at position {}", position)
            }
            ParseError::MissingValue { key, position } => {
                write!(
                    f,
                    "Missing value for key '{}' at position {}",
                    key, position
                )
            }
            ParseError::InvalidSyntax { message, position } => {
                write!(f, "Invalid syntax: {} at position {}", message, position)
            }
            ParseError::UnexpectedToken { token, position } => {
                write!(f, "Unexpected token '{}' at position {}", token, position)
            }
            ParseError::UnexpectedEndOfInput { position } => {
                write!(f, "Unexpected end of input at position {}", position)
            }
            ParseError::InvalidFunction { name, position } => {
                write!(f, "Invalid function '{}' at position {}", name, position)
            }
        }
    }
}

impl std::error::Error for ParseError {}
