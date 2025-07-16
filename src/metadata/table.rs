use super::column::Column;
use std::collections::HashMap;
use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
    pub primary_key: Option<String>,
    pub indexes: HashMap<String, Vec<usize>>,
}

impl Table {
    pub fn new(name: String) -> Self {
        Self {
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
        self.columns.iter().position(|c| c.name == column_name)
    }

    pub fn get_column_offset(&self, column_index: usize) -> usize {
        let mut offset = 0;
        for (i, column) in self.columns.iter().enumerate() {
            if i == column_index {
                break;
            }
            offset += self.get_column_length(i);
        }
        offset
    }

    pub fn get_column_length(&self, column_index: usize) -> usize {
        if let Some(column) = self.columns.get(column_index) {
            match column.data_type {
                super::ColumnType::Integer => 8,
                super::ColumnType::Float => 8,
                super::ColumnType::Varchar(len) => 4 + len, // 4 bytes for length + data
                super::ColumnType::Boolean => 1,
                super::ColumnType::Timestamp => 8,
            }
        } else {
            0
        }
    }
} 