use crate::metadata::Column;
use std::collections::HashMap;
use crate::metadata::ColumnType;

pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
    pub primary_key: Option<String>,
    pub indexes: HashMap<String, Vec<usize>>,
}

impl Table {
    pub fn new(name: String) -> Self {
        Table {
            name,
            columns: Vec::new(),
            primary_key: None,
            indexes: HashMap::new(),
        }
    }

    pub fn add_column(&mut self, column: Column) {
        self.columns.push(column);
    }

    pub fn set_primary_key(&mut self, column_name: String) {
        self.primary_key = Some(column_name);
    }

    pub fn create_index(&mut self, column_name: String) {
        self.indexes.insert(column_name, Vec::new());
    }

    pub fn get_column_index(&self, column_name: &str) -> Option<usize> {
        self.columns.iter().position(|col| col.name == column_name)
    }

    pub fn get_column_offset(&self, column_index: usize) -> usize {
        self.columns[..column_index].iter()
            .map(|col| match &col.data_type {
                ColumnType::String(max_len) => *max_len,
                _ => 8, // Default size for other types
            })
            .sum()
    }

    pub fn get_column_length(&self, column_index: usize) -> usize {
        match &self.columns[column_index].data_type {
            ColumnType::String(max_len) => *max_len,
            _ => 8, // Default size for other types
        }
    }
} 