use crate::metadata::Column;
use std::collections::HashMap;

pub struct Schema {
    pub name: String,
    pub tables: HashMap<String, Vec<Column>>,
}

impl Schema {
    pub fn new(name: String) -> Self {
        Schema {
            name,
            tables: HashMap::new(),
        }
    }

    pub fn add_table(&mut self, table_name: String, columns: Vec<Column>) {
        self.tables.insert(table_name, columns);
    }

    pub fn get_table_columns(&self, table_name: &str) -> Option<&Vec<Column>> {
        self.tables.get(table_name)
    }
} 