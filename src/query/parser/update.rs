use crate::metadata::{Table, Column, ColumnType};
use crate::storage::{Block, LSMEngine};
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

    // Original method for backward compatibility
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

    // New LSM engine method
    pub fn parse_and_execute_lsm(
        &mut self,
        tokens: &[&str],
        table: &Table,
        storage_engine: &mut LSMEngine,
    ) -> Result<QueryResult, QueryError> {
        // Parse SET clause
        let set_index = tokens.iter()
            .position(|&t| t.to_uppercase() == "SET")
            .ok_or_else(|| QueryError::SyntaxError("Expected SET clause".to_string()))?;

        // Parse column updates - for simplicity, we'll only handle single column updates
        if set_index + 3 >= tokens.len() {
            return Err(QueryError::SyntaxError("Invalid SET clause".to_string()));
        }

        let column_name = tokens[set_index + 1];
        if tokens[set_index + 2] != "=" {
            return Err(QueryError::SyntaxError("Expected = after column name".to_string()));
        }

        let value = tokens[set_index + 3];
        let column = table.columns.iter().find(|c| c.name == column_name)
            .ok_or_else(|| QueryError::ColumnNotFound(column_name.to_string()))?;

        // Parse WHERE clause if present
        let where_clause = if set_index + 4 < tokens.len() && tokens[set_index + 4].to_uppercase() == "WHERE" {
            Some(self.where_parser.parse_where_clause(&tokens[set_index + 5..])?)
        } else {
            None
        };

        // For simplicity in this demo, we'll scan and update matching records
        // In a production system, you'd want more efficient indexing
        let mut updated_count = 0;
        
        // Get all records from the LSM engine
        let all_records = storage_engine.get_all_records()
            .map_err(|e| QueryError::InternalError(format!("Failed to get all records: {}", e)))?;
        
        for record in all_records {
            // Parse record to check WHERE clause
            if let Some(ref where_clause) = where_clause {
                let row_data = self.parse_record_data(&record, table)?;
                if !self.where_parser.evaluate_where_clause(&row_data, table, where_clause)? {
                    continue;
                }
            }

            // Build new record data with updated value
            let new_data = self.build_updated_record_data(&record, table, column_name, value)?;
            
            // Update in LSM engine (this actually inserts a new version)
            if storage_engine.update(record.id, new_data)
                .map_err(|e| QueryError::InternalError(format!("Failed to update record: {}", e)))? {
                updated_count += 1;
            }
        }

        Ok(QueryResult::Update(updated_count))
    }

    fn parse_record_data(&self, record: &crate::storage::Record, table: &Table) -> Result<Vec<String>, QueryError> {
        let mut offset = 0;
        let row_data: Vec<String> = table.columns.iter().map(|col| {
            let result = match col.data_type {
                ColumnType::Integer => {
                    if offset + 8 <= record.data.len() {
                        let bytes = &record.data[offset..offset+8];
                        let num = i64::from_be_bytes(bytes.try_into().unwrap());
                        offset += 8;
                        num.to_string()
                    } else {
                        offset += 8;
                        "0".to_string()
                    }
                },
                ColumnType::Float => {
                    if offset + 8 <= record.data.len() {
                        let bytes = &record.data[offset..offset+8];
                        let num = f64::from_be_bytes(bytes.try_into().unwrap());
                        offset += 8;
                        num.to_string()
                    } else {
                        offset += 8;
                        "0.0".to_string()
                    }
                },
                ColumnType::Varchar(_max_len) => {
                    if offset + 4 <= record.data.len() {
                        let length_bytes = &record.data[offset..offset+4];
                        let length = u32::from_be_bytes(length_bytes.try_into().unwrap()) as usize;
                        offset += 4;
                        
                        if offset + length <= record.data.len() {
                            let string_bytes = &record.data[offset..offset+length];
                            offset += length;
                            String::from_utf8_lossy(string_bytes).to_string()
                        } else {
                            offset += length;
                            String::new()
                        }
                    } else {
                        offset += 4;
                        String::new()
                    }
                },
                ColumnType::Boolean => {
                    let result = if offset < record.data.len() && record.data[offset] == 1 { 
                        "true".to_string() 
                    } else { 
                        "false".to_string() 
                    };
                    offset += 1;
                    result
                },
                ColumnType::Timestamp => {
                    if offset + 8 <= record.data.len() {
                        let bytes = &record.data[offset..offset+8];
                        let num = i64::from_be_bytes(bytes.try_into().unwrap());
                        offset += 8;
                        num.to_string()
                    } else {
                        offset += 8;
                        "0".to_string()
                    }
                },
            };
            result
        }).collect();

        Ok(row_data)
    }

    fn build_updated_record_data(&self, original_record: &crate::storage::Record, table: &Table, update_column: &str, new_value: &str) -> Result<Vec<u8>, QueryError> {
        // For simplicity, rebuild the entire record with the updated value
        // In production, you might want to optimize this
        
        let row_data = self.parse_record_data(original_record, table)?;
        let mut new_data = Vec::new();
        
        for (i, column) in table.columns.iter().enumerate() {
            let value = if column.name == update_column {
                new_value
            } else {
                &row_data[i]
            };
            
            let value_bytes = Self::parse_value(value, column)?;
            new_data.extend(value_bytes);
        }
        
        Ok(new_data)
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
            ColumnType::Varchar(max_len) => {
                let cleaned_value = value.trim_matches(|c| c == '\'' || c == '"');
                if cleaned_value.len() > max_len {
                    return Err(QueryError::TypeMismatch(format!(
                        "Value '{}' exceeds column length of {}", cleaned_value, max_len
                    )));
                }
                let mut bytes = (cleaned_value.len() as u32).to_be_bytes().to_vec();
                bytes.extend(cleaned_value.as_bytes());
                Ok(bytes)
            },
            ColumnType::Boolean => {
                let cleaned_value = value.trim_matches(|c| c == '\'' || c == '"').to_lowercase();
                if cleaned_value != "true" && cleaned_value != "false" {
                    return Err(QueryError::TypeMismatch(format!("Invalid boolean value: {}", value)));
                }
                Ok(vec![if cleaned_value == "true" { 1 } else { 0 }])
            },
            ColumnType::Timestamp => {
                let num = value.parse::<i64>()
                    .map_err(|_| QueryError::TypeMismatch(format!("Invalid timestamp value: {}", value)))?;
                Ok(num.to_be_bytes().to_vec())
            },
        }
    }
} 