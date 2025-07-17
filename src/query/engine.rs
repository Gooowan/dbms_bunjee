use std::collections::HashMap;
use std::fs;
use crate::metadata::Table;
use crate::storage::{LSMEngine, Record};
use super::error::QueryError;
use super::result::QueryResult;
use super::parser::{SelectParser, InsertParser, UpdateParser, DeleteParser, CreateParser};

// TODO: AST mode for tree and plan execution + DEBUG MODE

pub struct QueryEngine {
    tables: HashMap<String, Table>,
    storage_engines: HashMap<String, LSMEngine>,
    select_parser: SelectParser,
    insert_parser: InsertParser,
    update_parser: UpdateParser,
    delete_parser: DeleteParser,
    create_parser: CreateParser,
    data_dir: String,
}

impl QueryEngine {
    pub fn new() -> Self {
        QueryEngine::new_with_data_dir("./db_data")
    }

    pub fn new_with_data_dir(data_dir: &str) -> Self {
        let mut engine = QueryEngine {
            tables: HashMap::new(),
            storage_engines: HashMap::new(),
            select_parser: SelectParser::new(),
            insert_parser: InsertParser::new(),
            update_parser: UpdateParser::new(),
            delete_parser: DeleteParser::new(),
            create_parser: CreateParser::new(),
            data_dir: data_dir.to_string(),
        };
        
        // Load existing tables and their storage engines
        if let Err(e) = engine.load_existing_tables() {
            eprintln!("Warning: Failed to load existing tables: {}", e);
        }
        
        engine
    }

    /// Load existing tables from the data directory
    fn load_existing_tables(&mut self) -> Result<(), QueryError> {
        // Create data directory if it doesn't exist
        if let Err(e) = fs::create_dir_all(&self.data_dir) {
            return Err(QueryError::InternalError(format!("Failed to create data directory: {}", e)));
        }

        // Check for table metadata file
        let metadata_path = format!("{}/tables.json", self.data_dir);
        if !std::path::Path::new(&metadata_path).exists() {
            return Ok(()); // No existing tables
        }

        // Load table metadata
        match fs::read_to_string(&metadata_path) {
            Ok(content) => {
                match serde_json::from_str::<HashMap<String, Table>>(&content) {
                    Ok(loaded_tables) => {
                        for (table_name, table) in loaded_tables {
                            // Create LSM storage engine for this table
                            let table_data_dir = format!("{}/{}", self.data_dir, table_name);
                            match LSMEngine::new(&table_data_dir, 100) {
                                Ok(storage_engine) => {
                                    self.tables.insert(table_name.clone(), table);
                                    self.storage_engines.insert(table_name.clone(), storage_engine);
                                    println!("Restored table: {}", table_name);
                                }
                                Err(e) => {
                                    eprintln!("Warning: Failed to restore table '{}': {}", table_name, e);
                                }
                            }
                        }
                    }
                    Err(e) => {
                        return Err(QueryError::InternalError(format!("Failed to parse table metadata: {}", e)));
                    }
                }
            }
            Err(e) => {
                return Err(QueryError::InternalError(format!("Failed to read table metadata: {}", e)));
            }
        }

        Ok(())
    }

    /// Save table metadata to disk
    fn save_table_metadata(&self) -> Result<(), QueryError> {
        let metadata_path = format!("{}/tables.json", self.data_dir);
        
        match serde_json::to_string_pretty(&self.tables) {
            Ok(content) => {
                if let Err(e) = fs::write(&metadata_path, content) {
                    return Err(QueryError::InternalError(format!("Failed to save table metadata: {}", e)));
                }
            }
            Err(e) => {
                return Err(QueryError::InternalError(format!("Failed to serialize table metadata: {}", e)));
            }
        }

        Ok(())
    }

    pub fn execute(&mut self, query: &str) -> Result<QueryResult, QueryError> {
        let tokens: Vec<&str> = query.split_whitespace().collect();
        if tokens.is_empty() {
            return Err(QueryError::SyntaxError("Empty query".to_string()));
        }

        match tokens[0].to_uppercase().as_str() {
            "SELECT" => self.execute_select(&tokens),
            "INSERT" => self.execute_insert(&tokens),
            "UPDATE" => self.execute_update(&tokens),
            "DELETE" => self.execute_delete(&tokens),
            "CREATE" => self.execute_create(&tokens),
            "DROP" => self.execute_drop(&tokens),
            _ => Err(QueryError::SyntaxError(format!("Unknown command: {}", tokens[0]))),
        }
    }

    fn execute_select(&mut self, tokens: &[&str]) -> Result<QueryResult, QueryError> {
        if tokens.len() < 4 {
            return Err(QueryError::SyntaxError("Invalid SELECT syntax".to_string()));
        }

        // Check if this is a JOIN query
        let has_join = tokens.iter().any(|&t| t.to_uppercase() == "JOIN");

        if has_join {
            // Handle JOIN query with multiple tables
            self.execute_join_select(tokens)
        } else {
            // Handle single table SELECT
            let from_index = if tokens[1] == "*" { 2 } else {
                tokens.iter()
                    .position(|&t| t.to_uppercase() == "FROM")
                    .ok_or_else(|| QueryError::SyntaxError("Expected FROM clause".to_string()))?
            };

            let table_name = tokens[from_index + 1];
            let table = self.tables.get(table_name)
                .ok_or_else(|| QueryError::TableNotFound(table_name.to_string()))?;

            let storage_engine = self.storage_engines.get_mut(table_name)
                .ok_or_else(|| QueryError::TableNotFound(format!("Storage engine not found: {}", table_name)))?;

            self.select_parser.parse_and_execute_lsm(tokens, table, storage_engine)
        }
    }

    fn execute_join_select(&mut self, tokens: &[&str]) -> Result<QueryResult, QueryError> {
        // Parse table names from JOIN query
        // Expected format: SELECT ... FROM table1 INNER JOIN table2 ON ...
        
        let from_index = tokens.iter()
            .position(|&t| t.to_uppercase() == "FROM")
            .ok_or_else(|| QueryError::SyntaxError("Expected FROM clause".to_string()))?;

        let join_index = tokens.iter()
            .position(|&t| t.to_uppercase() == "JOIN")
            .ok_or_else(|| QueryError::SyntaxError("Expected JOIN clause".to_string()))?;

        if from_index + 1 >= tokens.len() || join_index + 1 >= tokens.len() {
            return Err(QueryError::SyntaxError("Invalid JOIN syntax".to_string()));
        }

        let left_table_name = tokens[from_index + 1];
        let right_table_name = tokens[join_index + 1];

        // Clone table metadata to avoid borrowing conflicts
        let left_table = self.tables.get(left_table_name)
            .ok_or_else(|| QueryError::TableNotFound(left_table_name.to_string()))?
            .clone();
        let right_table = self.tables.get(right_table_name)
            .ok_or_else(|| QueryError::TableNotFound(right_table_name.to_string()))?
            .clone();

        // Since we need mutable references to storage engines, we need to handle them carefully
        // We'll process them one at a time to avoid borrowing conflicts
        
        // First, collect the results from both engines separately
        let left_records = {
            let left_engine = self.storage_engines.get_mut(left_table_name)
                .ok_or_else(|| QueryError::TableNotFound(format!("Storage engine not found: {}", left_table_name)))?;
            left_engine.get_all_records()
                .map_err(|e| QueryError::InternalError(format!("Failed to get left table records: {}", e)))?
        };

        let right_records = {
            let right_engine = self.storage_engines.get_mut(right_table_name)
                .ok_or_else(|| QueryError::TableNotFound(format!("Storage engine not found: {}", right_table_name)))?;
            right_engine.get_all_records()
                .map_err(|e| QueryError::InternalError(format!("Failed to get right table records: {}", e)))?
        };

        // Execute the join using the collected records
        self.execute_join_with_records(tokens, &left_table, &right_table, &left_records, &right_records)
    }

    fn execute_join_with_records(
        &mut self,
        tokens: &[&str],
        left_table: &Table,
        right_table: &Table,
        left_records: &[crate::storage::Record],
        right_records: &[crate::storage::Record],
    ) -> Result<QueryResult, QueryError> {
        use super::parser::{JoinParser, JoinClause};
        use super::result::{QueryResult, JoinResult};
        use crate::metadata::ColumnType;
        use std::collections::HashMap;

        let join_parser = JoinParser::new();
        
        // Parse JOIN clause
        let join_clause = join_parser.parse_join_clause(tokens)?;
        
        // Find column indices for join condition
        let left_join_col_index = left_table.columns.iter()
            .position(|c| c.name == join_clause.left_column)
            .ok_or_else(|| QueryError::ColumnNotFound(join_clause.left_column.clone()))?;

        let right_join_col_index = right_table.columns.iter()
            .position(|c| c.name == join_clause.right_column)
            .ok_or_else(|| QueryError::ColumnNotFound(join_clause.right_column.clone()))?;

        // Parse record data helper function
        let parse_record_data = |record: &crate::storage::Record, table: &Table| -> Result<Vec<String>, QueryError> {
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
        };

        // Build hash table from right table (smaller table assumed)
        let mut hash_table: HashMap<String, Vec<Vec<String>>> = HashMap::new();
        
        for record in right_records {
            let row_data = parse_record_data(record, right_table)?;
            let join_key = row_data[right_join_col_index].clone();
            
            hash_table.entry(join_key)
                .or_insert_with(Vec::new)
                .push(row_data);
        }

        // Probe left table and build results
        let mut result_rows = Vec::new();
        
        for record in left_records {
            let left_row_data = parse_record_data(record, left_table)?;
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

        // Handle column selection
        let select_end = tokens.iter()
            .position(|&t| t.to_uppercase() == "FROM")
            .unwrap_or(tokens.len());
        
        let (filtered_headers, filtered_rows) = if tokens[1] == "*" {
            (headers, result_rows)
        } else {
            // Parse selected columns and filter
            let selected_columns = self.select_parser.column_parser.parse_column_list(&tokens[1..select_end])?;
            self.filter_join_columns(&headers, &result_rows, &selected_columns)?
        };

        Ok(QueryResult::Join(JoinResult {
            headers: filtered_headers,
            rows: filtered_rows,
        }))
    }

    fn filter_join_columns(
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

    fn execute_insert(&mut self, tokens: &[&str]) -> Result<QueryResult, QueryError> {
        if tokens.len() < 4 {
            return Err(QueryError::SyntaxError("Invalid INSERT syntax".to_string()));
        }

        let table_name = tokens[2];
        let table = self.tables.get(table_name)
            .ok_or_else(|| QueryError::TableNotFound(table_name.to_string()))?;

        let storage_engine = self.storage_engines.get_mut(table_name)
            .ok_or_else(|| QueryError::TableNotFound(format!("Storage engine not found: {}", table_name)))?;

        self.insert_parser.parse_and_execute_lsm(tokens, table, storage_engine)
    }

    fn execute_update(&mut self, tokens: &[&str]) -> Result<QueryResult, QueryError> {
        if tokens.len() < 4 {
            return Err(QueryError::SyntaxError("Invalid UPDATE syntax".to_string()));
        }

        let table_name = tokens[1];
        let table = self.tables.get(table_name)
            .ok_or_else(|| QueryError::TableNotFound(table_name.to_string()))?;

        let storage_engine = self.storage_engines.get_mut(table_name)
            .ok_or_else(|| QueryError::TableNotFound(format!("Storage engine not found: {}", table_name)))?;

        self.update_parser.parse_and_execute_lsm(tokens, table, storage_engine)
    }

    fn execute_delete(&mut self, tokens: &[&str]) -> Result<QueryResult, QueryError> {
        if tokens.len() < 3 {
            return Err(QueryError::SyntaxError("Invalid DELETE syntax".to_string()));
        }

        let table_name = tokens[2];
        let table = self.tables.get(table_name)
            .ok_or_else(|| QueryError::TableNotFound(table_name.to_string()))?;

        let storage_engine = self.storage_engines.get_mut(table_name)
            .ok_or_else(|| QueryError::TableNotFound(format!("Storage engine not found: {}", table_name)))?;

        self.delete_parser.parse_and_execute_lsm(tokens, table, storage_engine)
    }

    fn execute_create(&mut self, tokens: &[&str]) -> Result<QueryResult, QueryError> {
        if tokens.len() < 3 {
            return Err(QueryError::SyntaxError("Invalid CREATE TABLE syntax".to_string()));
        }

        let (table_name, table) = self.create_parser.parse_and_execute(tokens)?;

        if self.tables.contains_key(&table_name) {
            return Err(QueryError::DuplicateKey(format!("Table {} already exists", table_name)));
        }

        // Create LSM storage engine for this table
        let table_data_dir = format!("{}/{}", self.data_dir, table_name);
        let storage_engine = LSMEngine::new(&table_data_dir, 100) // 100 records per memtable
            .map_err(|e| QueryError::InternalError(format!("Failed to create storage engine: {}", e)))?;

        self.tables.insert(table_name.clone(), table);
        self.storage_engines.insert(table_name.clone(), storage_engine);

        // Save table metadata
        self.save_table_metadata()?;

        Ok(QueryResult::CreateTable)
    }

    fn execute_drop(&mut self, tokens: &[&str]) -> Result<QueryResult, QueryError> {
        if tokens.len() < 3 {
            return Err(QueryError::SyntaxError("Invalid DROP TABLE syntax".to_string()));
        }

        // Parse TABLE keyword
        if tokens[1].to_uppercase() != "TABLE" {
            return Err(QueryError::SyntaxError("Expected TABLE keyword".to_string()));
        }

        let table_name = tokens[2];
        if !self.tables.contains_key(table_name) {
            return Err(QueryError::TableNotFound(table_name.to_string()));
        }

        self.tables.remove(table_name);
        self.storage_engines.remove(table_name);

        // Remove table data directory
        let table_data_dir = format!("{}/{}", self.data_dir, table_name);
        if let Err(e) = fs::remove_dir_all(&table_data_dir) {
            eprintln!("Warning: Failed to remove table data directory: {}", e);
        }

        // Save updated table metadata
        self.save_table_metadata()?;

        Ok(QueryResult::DropTable)
    }

    /// Get storage engine statistics for a table
    pub fn get_table_stats(&mut self, table_name: &str) -> Result<crate::storage::EngineStats, QueryError> {
        let storage_engine = self.storage_engines.get_mut(table_name)
            .ok_or_else(|| QueryError::TableNotFound(table_name.to_string()))?;
        
        storage_engine.stats()
            .map_err(|e| QueryError::InternalError(format!("Failed to get stats: {}", e)))
    }

    /// Flush all tables to disk
    pub fn flush_all(&mut self) -> Result<(), QueryError> {
        for (_, engine) in self.storage_engines.iter_mut() {
            engine.flush()
                .map_err(|e| QueryError::InternalError(format!("Failed to flush: {}", e)))?;
        }
        Ok(())
    }

    /// List all tables
    pub fn list_tables(&self) -> Vec<String> {
        self.tables.keys().cloned().collect()
    }
}

struct WhereClause {
    column: String,
    operator: String,
    value: String,
} 