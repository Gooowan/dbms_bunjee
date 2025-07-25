use crate::metadata::{Table, Column, ColumnType};
use crate::query::error::QueryError;
use super::column::ColumnParser;
use crate::query::result::QueryResult;

pub struct CreateParser;

impl CreateParser {
    pub fn new() -> Self {
        CreateParser
    }

    pub fn parse_and_execute(
        &self,
        tokens: &[&str],
    ) -> Result<(String, Table), QueryError> {
        // Parse TABLE keyword
        if tokens[1].to_uppercase() != "TABLE" {
            return Err(QueryError::SyntaxError("Expected TABLE keyword".to_string()));
        }

        let table_name = tokens[2].to_string();

        // Check if there are column definitions
        if tokens.len() > 3 {
            // Parse column definitions
            if !tokens[3].starts_with('(') || !tokens.last().unwrap().ends_with(')') {
                return Err(QueryError::SyntaxError("Expected column definitions in parentheses".to_string()));
            }

            // Join all tokens between parentheses and split by commas
            let col_defs = tokens[3..].join(" ");
            let col_defs = col_defs.trim_start_matches('(').trim_end_matches(')');
            let col_defs: Vec<&str> = col_defs.split(',').map(|s| s.trim()).collect();

            let mut table = Table::new(table_name.clone());
            
            for col_def in col_defs {
                let parts: Vec<&str> = col_def.split_whitespace().collect();
                if parts.len() < 2 {
                    return Err(QueryError::SyntaxError(format!(
                        "Invalid column definition: {}", col_def
                    )));
                }

                let col_name = parts[0].to_string();
                let col_type = self.parse_column_type(&parts[1..])?;
                
                table.add_column(Column::new(col_name, col_type));
            }

            Ok((table_name.clone(), table))
        } else {
            // Create table without columns
            Ok((table_name.clone(), Table::new(table_name)))
        }
    }

    fn parse_column_type(&self, parts: &[&str]) -> Result<ColumnType, QueryError> {
        let type_str = parts[0].to_uppercase();
        
        if type_str.starts_with("VARCHAR") {
            // Handle VARCHAR(n) format
            if type_str.contains('(') {
                let start = type_str.find('(').unwrap() + 1;
                let end = type_str.find(')').ok_or_else(|| QueryError::SyntaxError("Missing closing parenthesis for VARCHAR".to_string()))?;
                let length_str = &type_str[start..end];
                let len = length_str.parse::<usize>().map_err(|_| QueryError::SyntaxError(format!(
                    "Invalid length for VARCHAR: {}", length_str
                )))?;
                Ok(ColumnType::Varchar(len))
            } else if parts.len() >= 2 {
                // Handle VARCHAR n format
                let length_str = parts[1].trim_matches(|c| c == '(' || c == ')');
                let len = length_str.parse::<usize>().map_err(|_| QueryError::SyntaxError(format!(
                    "Invalid length for VARCHAR: {}", length_str
                )))?;
                Ok(ColumnType::Varchar(len))
            } else {
                Err(QueryError::SyntaxError("VARCHAR requires length specification".to_string()))
            }
        } else {
            match type_str.as_str() {
                "INT" | "INTEGER" => Ok(ColumnType::Integer),
                "FLOAT" => Ok(ColumnType::Float),
                "BOOLEAN" => Ok(ColumnType::Boolean),
                "TIMESTAMP" => Ok(ColumnType::Timestamp),
                _ => Err(QueryError::SyntaxError(format!(
                    "Unsupported column type: {}", parts[0]
                ))),
            }
        }
    }
} 