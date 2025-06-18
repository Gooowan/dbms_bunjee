use crate::query::error::QueryError;

pub struct ColumnParser;

impl ColumnParser {
    pub fn new() -> Self {
        ColumnParser
    }

    pub fn parse_column_list(&self, tokens: &[&str]) -> Result<Vec<String>, QueryError> {
        let mut columns = Vec::new();
        let mut i = 0;
        
        while i < tokens.len() && tokens[i].to_uppercase() != "FROM" {
            let col = tokens[i].trim_matches(',');
            if col.is_empty() {
                return Err(QueryError::SyntaxError("Empty column name".to_string()));
            }
            columns.push(col.to_string());
            i += 1;
        }

        if columns.is_empty() {
            return Err(QueryError::SyntaxError("No columns specified".to_string()));
        }

        Ok(columns)
    }
} 