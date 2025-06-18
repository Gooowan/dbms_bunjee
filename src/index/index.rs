use std::collections::HashMap;

pub struct Index {
    name: String,
    column: String,
    values: HashMap<String, Vec<usize>>,
}

impl Index {
    pub fn new(name: String, column: String) -> Self {
        Index {
            name,
            column,
            values: HashMap::new(),
        }
    }

    pub fn insert(&mut self, value: String, row_id: usize) {
        self.values
            .entry(value)
            .or_insert_with(Vec::new)
            .push(row_id);
    }

    pub fn remove(&mut self, value: &str, row_id: usize) {
        if let Some(rows) = self.values.get_mut(value) {
            if let Some(pos) = rows.iter().position(|&id| id == row_id) {
                rows.remove(pos);
            }
        }
    }

    pub fn find(&self, value: &str) -> Option<&Vec<usize>> {
        self.values.get(value)
    }
}