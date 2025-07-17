use crate::metadata::{Table, ColumnType};
use crate::storage::{Block, LSMEngine, Record};
use crate::query::error::QueryError;
use crate::query::result::QueryResult;
use super::r#where::WhereParser;
use super::column::ColumnParser;
use super::join::{JoinParser, JoinClause};
use super::aggregation::{AggregationParser, AggregationClause, AggregateFunction};
use std::collections::HashMap;

pub struct SelectParser {
    where_parser: WhereParser,
    pub column_parser: ColumnParser,
    join_parser: JoinParser,
    aggregation_parser: AggregationParser,
}

impl SelectParser {
    pub fn new() -> Self {
        SelectParser {
            where_parser: WhereParser::new(),
            column_parser: ColumnParser::new(),
            join_parser: JoinParser::new(),
            aggregation_parser: AggregationParser::new(),
        }
    }

    // Original method for backward compatibility
    pub fn parse_and_execute(
        &mut self,
        tokens: &[&str],
        table: &Table,
        storage_blocks: &[Block],
    ) -> Result<QueryResult, QueryError> {
        // Parse columns
        let columns = if tokens[1] == "*" {
            table.columns.clone()
        } else {
            let col_names = self.column_parser.parse_column_list(&tokens[1..])?;
            table.columns.iter().filter(|c| col_names.contains(&c.name)).cloned().collect::<Vec<_>>()
        };

        // Parse WHERE clause if present
        let where_index = tokens.iter()
            .position(|&t| t.to_uppercase() == "WHERE")
            .unwrap_or(tokens.len());

        let where_clause = if where_index < tokens.len() {
            Some(self.where_parser.parse_where_clause(&tokens[where_index + 1..])?)
        } else {
            None
        };

        // Execute select
        let mut results = Vec::new();
        for block in storage_blocks {
            for record in block.get_all() {
                // Build row data as Vec<String> for WHERE evaluation
                let mut offset = 0;
                let row_data: Vec<String> = table.columns.iter().map(|col| {
                    let result = match col.data_type {
                        ColumnType::Integer => {
                            let bytes = &record.data[offset..offset+8];
                            let num = i64::from_be_bytes(bytes.try_into().unwrap());
                            offset += 8;
                            num.to_string()
                        },
                        ColumnType::Float => {
                            let bytes = &record.data[offset..offset+8];
                            let num = f64::from_be_bytes(bytes.try_into().unwrap());
                            offset += 8;
                            num.to_string()
                        },
                        ColumnType::Varchar(_max_len) => {
                            // Read length prefix (4 bytes)
                            let length_bytes = &record.data[offset..offset+4];
                            let length = u32::from_be_bytes(length_bytes.try_into().unwrap()) as usize;
                            offset += 4;
                            
                            // Read string data
                            let string_bytes = &record.data[offset..offset+length];
                            offset += length;
                            String::from_utf8_lossy(string_bytes).to_string()
                        },
                        ColumnType::Boolean => {
                            let result = if !record.data.is_empty() && record.data[offset] == 1 { 
                                "true".to_string() 
                            } else { 
                                "false".to_string() 
                            };
                            offset += 1;
                            result
                        },
                        ColumnType::Timestamp => {
                            let bytes = &record.data[offset..offset+8];
                            let num = i64::from_be_bytes(bytes.try_into().unwrap());
                            offset += 8;
                            num.to_string()
                        },
                    };
                    result
                }).collect();

                if let Some(ref where_clause) = where_clause {
                    if !self.where_parser.evaluate_where_clause(&row_data, table, where_clause)? {
                        continue;
                    }
                }

                // Build result row for selected columns as Vec<String>
                let mut row = Vec::new();
                for column in &columns {
                    let idx = table.columns.iter().position(|c| c.name == column.name).unwrap();
                    row.push(row_data[idx].clone());
                }
                results.push(row);
            }
        }

        Ok(QueryResult::Select(results))
    }

    // Enhanced LSM engine method that supports JOIN and aggregation
    pub fn parse_and_execute_lsm(
        &mut self,
        tokens: &[&str],
        table: &Table,
        storage_engine: &mut LSMEngine,
    ) -> Result<QueryResult, QueryError> {
        // This method is for single table queries - should not be used for joins
        let mut tables_and_engines = [(table, storage_engine)];
        self.parse_and_execute_lsm_with_tables(tokens, &mut tables_and_engines)
    }

    /// Enhanced method that can handle both single table and join queries
    pub fn parse_and_execute_lsm_with_tables(
        &mut self,
        tokens: &[&str],
        tables_and_engines: &mut [(&Table, &mut LSMEngine)],
    ) -> Result<QueryResult, QueryError> {
        // Check if this is a JOIN query
        let has_join = tokens.iter().any(|&t| t.to_uppercase() == "JOIN");
        
        if has_join {
            // For JOIN queries, we need to handle this differently due to borrowing restrictions
            return Err(QueryError::SyntaxError("JOIN queries should use the QueryEngine directly".to_string()));
        } else {
            // Check if this is an aggregation query
            let select_end = tokens.iter()
                .position(|&t| t.to_uppercase() == "FROM")
                .unwrap_or(tokens.len());
            
            let select_tokens = &tokens[1..select_end];
            let aggregate_functions = self.aggregation_parser.parse_aggregation_functions(select_tokens)?;
            
            let has_group_by = tokens.iter().any(|&t| t.to_uppercase() == "GROUP");
            
            if !aggregate_functions.is_empty() || has_group_by {
                self.execute_aggregation_query(tokens, tables_and_engines)
            } else {
                // Regular single table query
                if tables_and_engines.len() != 1 {
                    return Err(QueryError::SyntaxError("Expected single table for non-join query".to_string()));
                }
                
                let (table, storage_engine) = &mut tables_and_engines[0];
                self.execute_regular_select(tokens, table, storage_engine)
            }
        }
    }

    fn execute_aggregation_query(
        &mut self,
        tokens: &[&str],
        tables_and_engines: &mut [(&Table, &mut LSMEngine)],
    ) -> Result<QueryResult, QueryError> {
        if tables_and_engines.len() != 1 {
            return Err(QueryError::SyntaxError("Aggregation queries support only single table".to_string()));
        }

        let (table, storage_engine) = &mut tables_and_engines[0];

        // Parse aggregation functions
        let select_end = tokens.iter()
            .position(|&t| t.to_uppercase() == "FROM")
            .unwrap_or(tokens.len());
        
        let select_tokens = &tokens[1..select_end];
        let aggregate_functions = self.aggregation_parser.parse_aggregation_functions(select_tokens)?;

        // Parse GROUP BY clause if present
        let group_by_columns = if tokens.iter().any(|&t| t.to_uppercase() == "GROUP") {
            self.aggregation_parser.parse_group_by(tokens)?
        } else {
            Vec::new()
        };

        let aggregation_clause = AggregationClause {
            functions: aggregate_functions,
            group_by_columns,
        };

        // We need to clone the table to avoid borrowing issues
        let table_clone = (*table).clone();
        self.aggregation_parser.execute_aggregation(&aggregation_clause, &table_clone, storage_engine)
    }

    fn execute_regular_select(
        &mut self,
        tokens: &[&str],
        table: &Table,
        storage_engine: &mut LSMEngine,
    ) -> Result<QueryResult, QueryError> {
        // Parse columns
        let columns = if tokens[1] == "*" {
            table.columns.clone()
        } else {
            let col_names = self.column_parser.parse_column_list(&tokens[1..])?;
            table.columns.iter().filter(|c| col_names.contains(&c.name)).cloned().collect::<Vec<_>>()
        };

        // Parse WHERE clause if present
        let where_index = tokens.iter()
            .position(|&t| t.to_uppercase() == "WHERE")
            .unwrap_or(tokens.len());

        let where_clause = if where_index < tokens.len() {
            Some(self.where_parser.parse_where_clause(&tokens[where_index + 1..])?)
        } else {
            None
        };

        // Get all records from the LSM engine
        let all_records = storage_engine.get_all_records()
            .map_err(|e| QueryError::InternalError(format!("Failed to get all records: {}", e)))?;
        
        let mut results = Vec::new();
        
        for record in all_records {
            let row_data = self.parse_record_data(&record, table)?;

            if let Some(ref where_clause) = where_clause {
                if !self.where_parser.evaluate_where_clause(&row_data, table, where_clause)? {
                    continue;
                }
            }

            // Build result row for selected columns
            let mut row = Vec::new();
            for column in &columns {
                let idx = table.columns.iter().position(|c| c.name == column.name).unwrap();
                row.push(row_data[idx].clone());
            }
            results.push(row);
        }

        Ok(QueryResult::Select(results))
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
                        // Read length prefix (4 bytes)
                        let length_bytes = &record.data[offset..offset+4];
                        let length = u32::from_be_bytes(length_bytes.try_into().unwrap()) as usize;
                        offset += 4;
                        
                        if offset + length <= record.data.len() {
                            // Read string data
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
} 