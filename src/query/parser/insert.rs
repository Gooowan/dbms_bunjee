use crate::metadata::{Table, Column, ColumnType};
use crate::storage::{Block, Record};
use crate::query::error::QueryError;
use crate::query::result::QueryResult;

pub struct InsertParser;

impl InsertParser {
    pub fn new() -> Self {
        InsertParser
    }

    pub fn parse_and_execute(
        &self,
        tokens: &[&str],
        table: &Table,
        storage_blocks: &mut Vec<Block>,
    ) -> Result<QueryResult, QueryError> {
        if tokens.len() < 4 {
            return Err(QueryError::SyntaxError("Invalid INSERT syntax".to_string()));
        }

        // Parse INTO clause
        if tokens[1].to_uppercase() != "INTO" {
            return Err(QueryError::SyntaxError("Expected INTO clause".to_string()));
        }

        // Parse VALUES clause
        let values_start = tokens.iter()
            .position(|&t| t.to_uppercase() == "VALUES")
            .ok_or_else(|| QueryError::SyntaxError("Expected VALUES clause".to_string()))?;

        // Parse column names if specified
        let columns = if tokens[2].starts_with('(') {
            let col_end = tokens.iter()
                .position(|&t| t.ends_with(')'))
                .ok_or_else(|| QueryError::SyntaxError("Expected closing parenthesis for columns".to_string()))?;
            
            let col_tokens = &tokens[2..=col_end];
            self.parse_column_list(col_tokens)?
        } else {
            // Use all columns in table order
            table.columns.iter().map(|c| c.name.clone()).collect()
        };

        // Parse values
        let values_vec = self.parse_values(&tokens[values_start + 1..])?;
        let mut total_inserted = 0;
        for values in values_vec {
            if columns.len() != values.len() {
                return Err(QueryError::SyntaxError(format!(
                    "Column count ({}) does not match value count ({})",
                    columns.len(),
                    values.len()
                )));
            }

            // Validate and convert values
            let mut record_data = Vec::new();
            for (col_name, value) in columns.iter().zip(values.iter()) {
                let column = table.columns.iter()
                    .find(|c| c.name == *col_name)
                    .ok_or_else(|| QueryError::ColumnNotFound(col_name.clone()))?;

                if !column.validate_value(value) {
                    return Err(QueryError::TypeMismatch(format!(
                        "Invalid value '{}' for column '{}'",
                        value, col_name
                    )));
                }

                // Convert value to bytes based on column type
                let value_bytes = self.convert_value_to_bytes(value, column)?;
                record_data.extend(value_bytes);
            }

            // Create and insert record
            let record = Record::new(
                self.generate_record_id(storage_blocks),
                record_data
            );

            // Find or create a block to insert into
            let block = if let Some(block) = storage_blocks.last_mut() {
                if block.count() < 1000 {
                    block
                } else {
                    let new_block = Block::new();
                    storage_blocks.push(new_block);
                    storage_blocks.last_mut().unwrap()
                }
            } else {
                let new_block = Block::new();
                storage_blocks.push(new_block);
                storage_blocks.last_mut().unwrap()
            };

            if block.insert(record) {
                total_inserted += 1;
            }
        }
        if total_inserted > 0 {
            Ok(QueryResult::Insert(total_inserted))
        } else {
            Err(QueryError::InternalError("Failed to insert record(s)".to_string()))
        }
    }

    fn parse_column_list(&self, tokens: &[&str]) -> Result<Vec<String>, QueryError> {
        let mut columns = Vec::new();

        for token in tokens {
            let token = token.trim_matches(|c| c == '(' || c == ')' || c == ',');
            if !token.is_empty() {
                columns.push(token.to_string());
            }
        }

        if columns.is_empty() {
            return Err(QueryError::SyntaxError("No columns specified".to_string()));
        }

        Ok(columns)
    }

    fn parse_values(&self, tokens: &[&str]) -> Result<Vec<Vec<String>>, QueryError> {
        let joined = tokens.join(" ");
        let mut values = Vec::new();
        
        let mut start = None;
        for (i, c) in joined.char_indices() {
            if c == '(' {
                start = Some(i + 1);
            } else if c == ')' {
                if let Some(s) = start {
                    let group = &joined[s..i];
                    let row: Vec<String> = group
                        .split(',')
                        .map(|v| v.trim().to_string())
                        .filter(|v| !v.is_empty())
                        .collect();
                    if row.is_empty() {
                        return Err(QueryError::SyntaxError("No values specified in group".to_string()));
                    }
                    values.push(row);
                    start = None;
                }
            }
        }
    
        if values.is_empty() {
            return Err(QueryError::SyntaxError("No values specified".to_string()));
        }
    
        Ok(values)
    }

    fn convert_value_to_bytes(&self, value: &str, column: &Column) -> Result<Vec<u8>, QueryError> {
        match column.data_type {
            ColumnType::Integer => {
                let value = value.parse::<i64>()
                    .map_err(|_| QueryError::TypeMismatch(format!("Invalid integer value: {}", value)))?;
                Ok(value.to_be_bytes().to_vec())
            }
            ColumnType::Float => {
                let value = value.parse::<f64>()
                    .map_err(|_| QueryError::TypeMismatch(format!("Invalid float value: {}", value)))?;
                Ok(value.to_be_bytes().to_vec())
            }
            ColumnType::Varchar(max_len) => {
                // Remove quotes if present
                let cleaned_value = value.trim_matches(|c| c == '\'' || c == '"');
                if cleaned_value.len() > max_len {
                    return Err(QueryError::TypeMismatch(format!(
                        "String value exceeds maximum length of {}",
                        max_len
                    )));
                }
                let mut bytes = (cleaned_value.len() as u32).to_be_bytes().to_vec();
                bytes.extend(cleaned_value.as_bytes());
                Ok(bytes)
            }
            ColumnType::Boolean => {
                // Remove quotes if present and convert to lowercase
                let cleaned_value = value.trim_matches(|c| c == '\'' || c == '"').to_lowercase();
                if cleaned_value != "true" && cleaned_value != "false" {
                    return Err(QueryError::TypeMismatch(format!("Invalid boolean value: {}", value)));
                }
                Ok(vec![if cleaned_value == "true" { 1 } else { 0 }])
            }
            ColumnType::Timestamp => {
                let value = value.parse::<i64>()
                    .map_err(|_| QueryError::TypeMismatch(format!("Invalid timestamp value: {}", value)))?;
                Ok(value.to_be_bytes().to_vec())
            }
        }
    }

    fn generate_record_id(&self, blocks: &[Block]) -> u64 {
        let mut max_id = 0;
        for block in blocks {
            for record in block.get_all() {
                max_id = max_id.max(record.id);
            }
        }
        max_id + 1
    }
} 