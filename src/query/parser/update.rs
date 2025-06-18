use crate::metadata::{Table, Column, ColumnType};
use crate::storage::Block;
use crate::query::error::QueryError;
use crate::query::result::QueryResult;
use super::r#where::WhereParser;

pub struct UpdateParser {
    where_parser: WhereParser,
}

impl UpdateParser {
    pub fn new() -> Self {
        UpdateParser {
            where_parser: WhereParser::new(),
        }
    }

    pub fn parse_and_execute(
        &mut self,
        tokens: &[&str],
        table: &Table,
        storage_blocks: &mut Vec<Block>,
    ) -> Result<QueryResult, QueryError> {
        // Parse SET clause
        let set_index = tokens.iter()
            .position(|&t| t.to_uppercase() == "SET")
            .ok_or_else(|| QueryError::SyntaxError("Expected SET clause".to_string()))?;

        // Parse column updates
        let mut updates = Vec::new();
        let mut current_index = set_index + 1;
        
        while current_index < tokens.len() {
            if tokens[current_index].to_uppercase() == "WHERE" {
                break;
            }

            let column_name = tokens[current_index];
            if tokens[current_index + 1] != "=" {
                return Err(QueryError::SyntaxError("Expected = after column name".to_string()));
            }

            let value = tokens[current_index + 2];
            let column = table.columns.iter().find(|c| c.name == column_name)
                .ok_or_else(|| QueryError::ColumnNotFound(column_name.to_string()))?;

            let value_bytes = Self::parse_value(value, column)?;
            updates.push((column_name.to_string(), value_bytes));

            current_index += 3;
            if current_index < tokens.len() && tokens[current_index] == "," {
                current_index += 1;
            }
        }

        // Parse WHERE clause if present
        if current_index < tokens.len() && tokens[current_index].to_uppercase() == "WHERE" {
            self.where_parser.parse_where_clause(&tokens[current_index + 1..])?;
        }

        // Execute update
        let mut updated_count = 0;
        for block in storage_blocks.iter_mut() {
            // Update record values
            let record_ids: Vec<u64> = block.get_all().iter().map(|r| r.id).collect();
            for record_id in record_ids {
                let mut updated = false;
                for (col_name, new_value) in &updates {
                    if let Some(idx) = table.get_column_index(&col_name) {
                        let offset = table.get_column_offset(idx);
                        block.update_record(record_id, offset, new_value);
                        updated = true;
                    }
                }
                if updated {
                    updated_count += 1;
                }
            }
        }

        Ok(QueryResult::Update(updated_count))
    }

    fn parse_value(value: &str, column: &Column) -> Result<Vec<u8>, QueryError> {
        match column.data_type {
            ColumnType::Integer => {
                let num = value.parse::<i64>()
                    .map_err(|_| QueryError::TypeMismatch(format!("Invalid integer value: {}", value)))?;
                Ok(num.to_be_bytes().to_vec())
            },
            ColumnType::Float => {
                let num = value.parse::<f64>()
                    .map_err(|_| QueryError::TypeMismatch(format!("Invalid float value: {}", value)))?;
                Ok(num.to_be_bytes().to_vec())
            },
            ColumnType::String(max_len) => {
                if value.len() > max_len {
                    return Err(QueryError::TypeMismatch(format!(
                        "Value '{}' exceeds column length of {}", value, max_len
                    )));
                }
                let mut bytes = (value.len() as u32).to_be_bytes().to_vec();
                bytes.extend(value.as_bytes());
                Ok(bytes)
            },
            ColumnType::Boolean => {
                let value = value.to_lowercase();
                if value != "true" && value != "false" {
                    return Err(QueryError::TypeMismatch(format!("Invalid boolean value: {}", value)));
                }
                Ok(vec![if value == "true" { 1 } else { 0 }])
            },
            ColumnType::Timestamp => {
                let num = value.parse::<i64>()
                    .map_err(|_| QueryError::TypeMismatch(format!("Invalid timestamp value: {}", value)))?;
                Ok(num.to_be_bytes().to_vec())
            },
        }
    }
} 