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