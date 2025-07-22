use std::collections::HashMap;
use crate::metadata::{Table, ColumnType};
use crate::storage::{LSMEngine, Record};
use crate::query::error::QueryError;
use crate::query::result::{QueryResult, AggregationResult};

#[derive(Debug, Clone)]
pub enum AggregateFunction {
    Sum(String),      // column name
    Count(String),    // column name or "*"
    Avg(String),      // column name
    Min(String),      // column name
    Max(String),      // column name
}

#[derive(Debug)]
pub struct AggregationClause {
    pub functions: Vec<AggregateFunction>,
    pub group_by_columns: Vec<String>,
}

pub struct AggregationParser;

impl AggregationParser {
    pub fn new() -> Self {
        AggregationParser
    }

    /// Parse aggregation functions from SELECT clause tokens
    /// Example: SUM(amount), COUNT(*), AVG(score)
    pub fn parse_aggregation_functions(&self, select_tokens: &[&str]) -> Result<Vec<AggregateFunction>, QueryError> {
        let mut functions = Vec::new();
        
        for token in select_tokens {
            if token.contains('(') && token.contains(')') {
                let func = self.parse_single_function(token)?;
                functions.push(func);
            }
        }
        
        Ok(functions)
    }

    /// Parse GROUP BY clause
    /// Example: GROUP BY customer_id, region
    pub fn parse_group_by(&self, tokens: &[&str]) -> Result<Vec<String>, QueryError> {
        let group_by_index = tokens.iter()
            .position(|&t| t.to_uppercase() == "GROUP")
            .ok_or_else(|| QueryError::SyntaxError("Expected GROUP keyword".to_string()))?;

        if group_by_index + 1 >= tokens.len() || tokens[group_by_index + 1].to_uppercase() != "BY" {
            return Err(QueryError::SyntaxError("Expected BY after GROUP".to_string()));
        }

        let mut group_columns = Vec::new();
        let mut i = group_by_index + 2;
        
        while i < tokens.len() {
            let token = tokens[i].trim_end_matches(',');
            if token.to_uppercase() == "ORDER" || token.to_uppercase() == "HAVING" {
                break;
            }
            group_columns.push(token.to_string());
            i += 1;
        }

        if group_columns.is_empty() {
            return Err(QueryError::SyntaxError("GROUP BY must specify at least one column".to_string()));
        }

        Ok(group_columns)
    }

    fn parse_single_function(&self, token: &str) -> Result<AggregateFunction, QueryError> {
        let open_paren = token.find('(')
            .ok_or_else(|| QueryError::SyntaxError("Invalid function syntax".to_string()))?;
        let close_paren = token.find(')')
            .ok_or_else(|| QueryError::SyntaxError("Invalid function syntax".to_string()))?;

        let func_name = &token[..open_paren];
        let column_name = &token[open_paren + 1..close_paren];

        match func_name.to_uppercase().as_str() {
            "SUM" => Ok(AggregateFunction::Sum(column_name.to_string())),
            "COUNT" => Ok(AggregateFunction::Count(column_name.to_string())),
            "AVG" => Ok(AggregateFunction::Avg(column_name.to_string())),
            "MIN" => Ok(AggregateFunction::Min(column_name.to_string())),
            "MAX" => Ok(AggregateFunction::Max(column_name.to_string())),
            _ => Err(QueryError::SyntaxError(format!("Unsupported function: {}", func_name))),
        }
    }

    /// Execute aggregation query
    pub fn execute_aggregation(
        &self,
        aggregation_clause: &AggregationClause,
        table: &Table,
        storage_engine: &mut LSMEngine,
    ) -> Result<QueryResult, QueryError> {
        // Get all records
        let all_records = storage_engine.get_all_records()
            .map_err(|e| QueryError::InternalError(format!("Failed to get all records: {}", e)))?;

        // Parse all records to row data
        let mut all_rows = Vec::new();
        for record in all_records {
            let row_data = self.parse_record_data(&record, table)?;
            all_rows.push(row_data);
        }

        if aggregation_clause.group_by_columns.is_empty() {
            // No GROUP BY - single aggregation result
            self.execute_single_aggregation(&aggregation_clause.functions, table, &all_rows)
        } else {
            // GROUP BY aggregation
            self.execute_grouped_aggregation(aggregation_clause, table, &all_rows)
        }
    }

    /// Execute aggregation query with WHERE clause support
    pub fn execute_aggregation_with_where(
        &self,
        aggregation_clause: &AggregationClause,
        table: &Table,
        storage_engine: &mut LSMEngine,
        where_clause: Option<&super::r#where::WhereClause>,
    ) -> Result<QueryResult, QueryError> {
        use super::r#where::WhereParser;
        let where_parser = WhereParser::new();

        // Get all records
        let all_records = storage_engine.get_all_records()
            .map_err(|e| QueryError::InternalError(format!("Failed to get all records: {}", e)))?;

        // Parse and filter records based on WHERE clause
        let mut filtered_rows = Vec::new();
        for record in all_records {
            let row_data = self.parse_record_data(&record, table)?;
            
            // Apply WHERE clause filter if present
            if let Some(where_clause) = where_clause {
                if !where_parser.evaluate_where_clause(&row_data, table, where_clause)? {
                    continue; // Skip this record
                }
            }
            
            filtered_rows.push(row_data);
        }

        if aggregation_clause.group_by_columns.is_empty() {
            // No GROUP BY - single aggregation result
            self.execute_single_aggregation(&aggregation_clause.functions, table, &filtered_rows)
        } else {
            // GROUP BY aggregation
            self.execute_grouped_aggregation(aggregation_clause, table, &filtered_rows)
        }
    }

    fn execute_single_aggregation(
        &self,
        functions: &[AggregateFunction],
        table: &Table,
        rows: &[Vec<String>],
    ) -> Result<QueryResult, QueryError> {
        let mut result_row = Vec::new();
        let mut headers = Vec::new();

        for func in functions {
            let (header, value) = self.compute_aggregate_value(func, table, rows)?;
            headers.push(header);
            result_row.push(value);
        }

        Ok(QueryResult::Aggregation(AggregationResult {
            headers,
            rows: vec![result_row],
            group_by_columns: Vec::new(),
        }))
    }

    fn execute_grouped_aggregation(
        &self,
        aggregation_clause: &AggregationClause,
        table: &Table,
        rows: &[Vec<String>],
    ) -> Result<QueryResult, QueryError> {
        // Find group by column indices
        let mut group_col_indices = Vec::new();
        for col_name in &aggregation_clause.group_by_columns {
            let index = table.columns.iter()
                .position(|c| c.name == *col_name)
                .ok_or_else(|| QueryError::ColumnNotFound(col_name.clone()))?;
            group_col_indices.push(index);
        }

        // Group rows by group key
        let mut groups: HashMap<Vec<String>, Vec<Vec<String>>> = HashMap::new();
        
        for row in rows {
            let group_key: Vec<String> = group_col_indices.iter()
                .map(|&index| row[index].clone())
                .collect();
            
            groups.entry(group_key)
                .or_insert_with(Vec::new)
                .push(row.clone());
        }

        // Compute aggregations for each group
        let mut result_rows = Vec::new();
        let mut headers = aggregation_clause.group_by_columns.clone();
        
        // Add aggregate function headers
        for func in &aggregation_clause.functions {
            headers.push(self.get_function_header(func));
        }

        for (group_key, group_rows) in groups {
            let mut result_row = group_key;
            
            for func in &aggregation_clause.functions {
                let (_, value) = self.compute_aggregate_value(func, table, &group_rows)?;
                result_row.push(value);
            }
            
            result_rows.push(result_row);
        }

        // Sort results for consistent output
        result_rows.sort();

        Ok(QueryResult::Aggregation(AggregationResult {
            headers,
            rows: result_rows,
            group_by_columns: aggregation_clause.group_by_columns.clone(),
        }))
    }

    fn compute_aggregate_value(
        &self,
        function: &AggregateFunction,
        table: &Table,
        rows: &[Vec<String>],
    ) -> Result<(String, String), QueryError> {
        match function {
            AggregateFunction::Count(col_name) => {
                let header = if col_name == "*" {
                    "COUNT(*)".to_string()
                } else {
                    format!("COUNT({})", col_name)
                };
                let count = if col_name == "*" {
                    rows.len()
                } else {
                    // Count non-null values
                    let col_index = table.columns.iter()
                        .position(|c| c.name == *col_name)
                        .ok_or_else(|| QueryError::ColumnNotFound(col_name.clone()))?;
                    
                    rows.iter()
                        .filter(|row| !row[col_index].is_empty() && row[col_index] != "null")
                        .count()
                };
                Ok((header, count.to_string()))
            },
            
            AggregateFunction::Sum(col_name) => {
                let header = format!("SUM({})", col_name);
                let col_index = table.columns.iter()
                    .position(|c| c.name == *col_name)
                    .ok_or_else(|| QueryError::ColumnNotFound(col_name.clone()))?;
                
                let sum: f64 = rows.iter()
                    .filter_map(|row| row[col_index].parse::<f64>().ok())
                    .sum();
                
                Ok((header, sum.to_string()))
            },
            
            AggregateFunction::Avg(col_name) => {
                let header = format!("AVG({})", col_name);
                let col_index = table.columns.iter()
                    .position(|c| c.name == *col_name)
                    .ok_or_else(|| QueryError::ColumnNotFound(col_name.clone()))?;
                
                let values: Vec<f64> = rows.iter()
                    .filter_map(|row| row[col_index].parse::<f64>().ok())
                    .collect();
                
                let avg = if values.is_empty() {
                    0.0
                } else {
                    values.iter().sum::<f64>() / values.len() as f64
                };
                
                Ok((header, avg.to_string()))
            },
            
            AggregateFunction::Min(col_name) => {
                let header = format!("MIN({})", col_name);
                let col_index = table.columns.iter()
                    .position(|c| c.name == *col_name)
                    .ok_or_else(|| QueryError::ColumnNotFound(col_name.clone()))?;
                
                let min_value = rows.iter()
                    .map(|row| &row[col_index])
                    .filter(|val| !val.is_empty() && *val != "null")
                    .min()
                    .unwrap_or(&"".to_string())
                    .clone();
                
                Ok((header, min_value))
            },
            
            AggregateFunction::Max(col_name) => {
                let header = format!("MAX({})", col_name);
                let col_index = table.columns.iter()
                    .position(|c| c.name == *col_name)
                    .ok_or_else(|| QueryError::ColumnNotFound(col_name.clone()))?;
                
                let max_value = rows.iter()
                    .map(|row| &row[col_index])
                    .filter(|val| !val.is_empty() && *val != "null")
                    .max()
                    .unwrap_or(&"".to_string())
                    .clone();
                
                Ok((header, max_value))
            },
        }
    }

    fn get_function_header(&self, function: &AggregateFunction) -> String {
        match function {
            AggregateFunction::Count(col) => {
                if col == "*" { "COUNT(*)".to_string() } else { format!("COUNT({})", col) }
            },
            AggregateFunction::Sum(col) => format!("SUM({})", col),
            AggregateFunction::Avg(col) => format!("AVG({})", col),
            AggregateFunction::Min(col) => format!("MIN({})", col),
            AggregateFunction::Max(col) => format!("MAX({})", col),
        }
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
} 