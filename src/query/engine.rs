use std::collections::HashMap;
use crate::metadata::Table;
use crate::storage::Block;
use super::error::QueryError;
use super::result::QueryResult;
use super::parser::{SelectParser, InsertParser, UpdateParser, DeleteParser, CreateParser};

pub struct QueryEngine {
    tables: HashMap<String, Table>,
    storage_tables: HashMap<String, Vec<Block>>,
    select_parser: SelectParser,
    insert_parser: InsertParser,
    update_parser: UpdateParser,
    delete_parser: DeleteParser,
    create_parser: CreateParser,
}

impl QueryEngine {
    pub fn new() -> Self {
        QueryEngine {
            tables: HashMap::new(),
            storage_tables: HashMap::new(),
            select_parser: SelectParser::new(),
            insert_parser: InsertParser::new(),
            update_parser: UpdateParser::new(),
            delete_parser: DeleteParser::new(),
            create_parser: CreateParser::new(),
        }
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

        let storage_blocks = self.storage_tables.get(table_name)
            .ok_or_else(|| QueryError::TableNotFound(format!("Storage table not found: {}", table_name)))?;

        self.select_parser.parse_and_execute(tokens, table, storage_blocks)
    }

    fn execute_insert(&mut self, tokens: &[&str]) -> Result<QueryResult, QueryError> {
        if tokens.len() < 4 {
            return Err(QueryError::SyntaxError("Invalid INSERT syntax".to_string()));
        }

        let table_name = tokens[2];
        let table = self.tables.get(table_name)
            .ok_or_else(|| QueryError::TableNotFound(table_name.to_string()))?;

        let storage_blocks = self.storage_tables.get_mut(table_name)
            .ok_or_else(|| QueryError::TableNotFound(format!("Storage table not found: {}", table_name)))?;

        self.insert_parser.parse_and_execute(tokens, table, storage_blocks)
    }

    fn execute_update(&mut self, tokens: &[&str]) -> Result<QueryResult, QueryError> {
        if tokens.len() < 4 {
            return Err(QueryError::SyntaxError("Invalid UPDATE syntax".to_string()));
        }

        let table_name = tokens[1];
        let table = self.tables.get(table_name)
            .ok_or_else(|| QueryError::TableNotFound(table_name.to_string()))?;

        let storage_blocks = self.storage_tables.get_mut(table_name)
            .ok_or_else(|| QueryError::TableNotFound(format!("Storage table not found: {}", table_name)))?;

        self.update_parser.parse_and_execute(tokens, table, storage_blocks)
    }

    fn execute_delete(&mut self, tokens: &[&str]) -> Result<QueryResult, QueryError> {
        if tokens.len() < 3 {
            return Err(QueryError::SyntaxError("Invalid DELETE syntax".to_string()));
        }

        let table_name = tokens[2];
        let table = self.tables.get(table_name)
            .ok_or_else(|| QueryError::TableNotFound(table_name.to_string()))?;

        let storage_blocks = self.storage_tables.get_mut(table_name)
            .ok_or_else(|| QueryError::TableNotFound(format!("Storage table not found: {}", table_name)))?;

        self.delete_parser.parse_and_execute(tokens, table, storage_blocks)
    }

    fn execute_create(&mut self, tokens: &[&str]) -> Result<QueryResult, QueryError> {
        if tokens.len() < 3 {
            return Err(QueryError::SyntaxError("Invalid CREATE TABLE syntax".to_string()));
        }

        let (table_name, table) = self.create_parser.parse_and_execute(tokens)?;

        if self.tables.contains_key(&table_name) {
            return Err(QueryError::DuplicateKey(format!("Table {} already exists", table_name)));
        }

        self.tables.insert(table_name.clone(), table);
        self.storage_tables.insert(table_name, Vec::new());

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
        self.storage_tables.remove(table_name);
        Ok(QueryResult::DropTable)
    }
}

struct WhereClause {
    column: String,
    operator: String,
    value: String,
} 