use crate::metadata::{Table, ColumnType};
use crate::storage::Block;
use crate::query::error::QueryError;
use crate::query::result::QueryResult;
use super::r#where::WhereParser;
use super::column::ColumnParser;

pub struct SelectParser {
    where_parser: WhereParser,
    column_parser: ColumnParser,
}

impl SelectParser {
    pub fn new() -> Self {
        SelectParser {
            where_parser: WhereParser::new(),
            column_parser: ColumnParser::new(),
        }
    }

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
                let row_data: Vec<String> = table.columns.iter().map(|col| {
                    match col.data_type {
                        ColumnType::Integer => {
                            let bytes = &record.data[0..8];
                            let num = i64::from_be_bytes(bytes.try_into().unwrap());
                            num.to_string()
                        },
                        ColumnType::Float => {
                            let bytes = &record.data[0..8];
                            let num = f64::from_be_bytes(bytes.try_into().unwrap());
                            num.to_string()
                        },
                        ColumnType::String(_max_len) => {
                            String::from_utf8_lossy(&record.data).to_string()
                        },
                        ColumnType::Boolean => {
                            if !record.data.is_empty() && record.data[0] == 1 { "true".to_string() } else { "false".to_string() }
                        },
                        ColumnType::Timestamp => {
                            let bytes = &record.data[0..8];
                            let num = i64::from_be_bytes(bytes.try_into().unwrap());
                            num.to_string()
                        },
                    }
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
} 