use std::collections::HashMap;
use crate::metadata::{Table, ColumnType};
use crate::storage::{LSMEngine, Record};
use crate::query::error::QueryError;
use crate::query::result::{QueryResult, JoinResult};

#[derive(Debug)]
pub struct JoinClause {
    pub join_type: JoinType,
    pub left_table: String,
    pub right_table: String,
    pub left_column: String,
    pub right_column: String,
}

#[derive(Debug)]
pub enum JoinType {
    Inner,
    // Can extend with Left, Right, Full later
}

pub struct JoinParser;

impl JoinParser {
    pub fn new() -> Self {
        JoinParser
    }

    /// Parse JOIN clause from tokens
    /// Expected format: table1 INNER JOIN table2 ON table1.col = table2.col
    pub fn parse_join_clause(&self, tokens: &[&str]) -> Result<JoinClause, QueryError> {
        if tokens.len() < 6 {
            return Err(QueryError::SyntaxError("Invalid JOIN syntax".to_string()));
        }

        // Find INNER, JOIN, and ON keywords
        let join_type_index = tokens.iter()
            .position(|&t| t.to_uppercase() == "INNER")
            .ok_or_else(|| QueryError::SyntaxError("Expected INNER keyword".to_string()))?;

        let join_index = tokens.iter()
            .position(|&t| t.to_uppercase() == "JOIN")
            .ok_or_else(|| QueryError::SyntaxError("Expected JOIN keyword".to_string()))?;

        let on_index = tokens.iter()
            .position(|&t| t.to_uppercase() == "ON")
            .ok_or_else(|| QueryError::SyntaxError("Expected ON keyword".to_string()))?;

        if join_type_index + 1 != join_index || join_index + 2 != on_index {
            return Err(QueryError::SyntaxError("Invalid JOIN syntax order".to_string()));
        }

        let left_table = tokens[join_type_index - 1].to_string();
        let right_table = tokens[join_index + 1].to_string();

        // Parse ON condition: table1.col = table2.col
        if on_index + 3 >= tokens.len() {
            return Err(QueryError::SyntaxError("Invalid ON clause".to_string()));
        }

        let left_condition = tokens[on_index + 1];
        let operator = tokens[on_index + 2];
        let right_condition = tokens[on_index + 3];

        if operator != "=" {
            return Err(QueryError::SyntaxError("Only equality joins are supported".to_string()));
        }

        // Parse table.column format
        let left_parts: Vec<&str> = left_condition.split('.').collect();
        let right_parts: Vec<&str> = right_condition.split('.').collect();

        if left_parts.len() != 2 || right_parts.len() != 2 {
            return Err(QueryError::SyntaxError("Expected table.column format in ON clause".to_string()));
        }

        Ok(JoinClause {
            join_type: JoinType::Inner,
            left_table,
            right_table,
            left_column: left_parts[1].to_string(),
            right_column: right_parts[1].to_string(),
        })
    }

    /// Execute hash join algorithm
    pub fn execute_hash_join(
        &self,
        join_clause: &JoinClause,
        left_table: &Table,
        right_table: &Table,
        left_engine: &mut LSMEngine,
        right_engine: &mut LSMEngine,
        selected_columns: &[String],
    ) -> Result<QueryResult, QueryError> {
        // Get all records from both tables
        let left_records = left_engine.get_all_records()
            .map_err(|e| QueryError::InternalError(format!("Failed to get left table records: {}", e)))?;
        
        let right_records = right_engine.get_all_records()
            .map_err(|e| QueryError::InternalError(format!("Failed to get right table records: {}", e)))?;

        // Find column indices for join condition
        let left_join_col_index = left_table.columns.iter()
            .position(|c| c.name == join_clause.left_column)
            .ok_or_else(|| QueryError::ColumnNotFound(join_clause.left_column.clone()))?;

        let right_join_col_index = right_table.columns.iter()
            .position(|c| c.name == join_clause.right_column)
            .ok_or_else(|| QueryError::ColumnNotFound(join_clause.right_column.clone()))?;

        // Build hash table from smaller table (right table for simplicity)
        let mut hash_table: HashMap<String, Vec<Vec<String>>> = HashMap::new();
        
        for record in &right_records {
            let row_data = self.parse_record_data(record, right_table)?;
            let join_key = row_data[right_join_col_index].clone();
            
            hash_table.entry(join_key)
                .or_insert_with(Vec::new)
                .push(row_data);
        }

        // Probe left table and build results
        let mut result_rows = Vec::new();
        
        for record in &left_records {
            let left_row_data = self.parse_record_data(record, left_table)?;
            let join_key = &left_row_data[left_join_col_index];
            
            if let Some(matching_right_rows) = hash_table.get(join_key) {
                for right_row_data in matching_right_rows {
                    // Combine left and right row data
                    let mut combined_row = left_row_data.clone();
                    combined_row.extend(right_row_data.iter().cloned());
                    result_rows.push(combined_row);
                }
            }
        }

        // Build headers for result
        let mut headers = Vec::new();
        for col in &left_table.columns {
            headers.push(format!("{}.{}", join_clause.left_table, col.name));
        }
        for col in &right_table.columns {
            headers.push(format!("{}.{}", join_clause.right_table, col.name));
        }

        // Filter columns if specific columns were selected
        let (filtered_headers, filtered_rows) = if selected_columns.is_empty() || selected_columns[0] == "*" {
            (headers, result_rows)
        } else {
            self.filter_selected_columns(&headers, &result_rows, selected_columns)?
        };

        Ok(QueryResult::Join(JoinResult {
            headers: filtered_headers,
            rows: filtered_rows,
        }))
    }

    fn parse_record_data(&self, record: &Record, table: &Table) -> Result<Vec<String>, QueryError> {
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

    fn filter_selected_columns(
        &self,
        headers: &[String],
        rows: &[Vec<String>],
        selected_columns: &[String],
    ) -> Result<(Vec<String>, Vec<Vec<String>>), QueryError> {
        let mut selected_indices = Vec::new();
        let mut filtered_headers = Vec::new();

        for col_name in selected_columns {
            // Handle table.column format or just column name
            let column_index = if col_name.contains('.') {
                headers.iter().position(|h| h == col_name)
            } else {
                headers.iter().position(|h| h.ends_with(&format!(".{}", col_name)))
            };

            match column_index {
                Some(index) => {
                    selected_indices.push(index);
                    filtered_headers.push(headers[index].clone());
                }
                None => return Err(QueryError::ColumnNotFound(col_name.clone())),
            }
        }

        let filtered_rows: Vec<Vec<String>> = rows.iter()
            .map(|row| {
                selected_indices.iter()
                    .map(|&index| row[index].clone())
                    .collect()
            })
            .collect();

        Ok((filtered_headers, filtered_rows))
    }
} 