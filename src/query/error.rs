#[derive(Debug)]
pub enum QueryError {
    SyntaxError(String),
    TableNotFound(String),
    ColumnNotFound(String),
    TypeMismatch(String),
    DuplicateKey(String),
    InvalidValue(String),
    InternalError(String),
}

impl std::fmt::Display for QueryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            QueryError::SyntaxError(msg) => write!(f, "Syntax error: {}", msg),
            QueryError::TableNotFound(msg) => write!(f, "Table not found: {}", msg),
            QueryError::ColumnNotFound(msg) => write!(f, "Column not found: {}", msg),
            QueryError::TypeMismatch(msg) => write!(f, "Type mismatch: {}", msg),
            QueryError::DuplicateKey(msg) => write!(f, "Duplicate key: {}", msg),
            QueryError::InvalidValue(msg) => write!(f, "Invalid value: {}", msg),
            QueryError::InternalError(msg) => write!(f, "Internal error: {}", msg),
        }
    }
} 