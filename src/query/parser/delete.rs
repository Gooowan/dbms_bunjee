use crate::metadata::{Table, ColumnType};
use crate::storage::Block;
use crate::query::error::QueryError;
use crate::query::result::QueryResult;
use super::r#where::WhereParser;

pub struct DeleteParser {
    where_parser: WhereParser,
}

impl DeleteParser {
    pub fn new() -> Self {
        DeleteParser {
            where_parser: WhereParser::new(),
        }
    }

    pub fn parse_and_execute(
        &mut self,
        tokens: &[&str],
        table: &Table,
        storage_blocks: &mut Vec<Block>,
    ) -> Result<QueryResult, QueryError> {
        // Parse FROM clause
        if tokens[1].to_uppercase() != "FROM" {
            return Err(QueryError::SyntaxError("Expected FROM clause".to_string()));
        }

        // Parse WHERE clause if present
        let where_clause = if tokens.len() > 3 && tokens[3].to_uppercase() == "WHERE" {
            Some(self.where_parser.parse_where_clause(&tokens[4..])?)
        } else {
            None
        };

        // Execute delete (simulate by counting matching records)
        let mut deleted_count = 0;
        for block in storage_blocks.iter_mut() {
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

                // Simulate marking as deleted (just count)
                deleted_count += 1;
            }
        }

        Ok(QueryResult::Delete(deleted_count))
    }
} 