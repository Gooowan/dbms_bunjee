use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ColumnType {
    Integer,
    Float,
    Varchar(usize),
    Boolean,
    Timestamp,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub data_type: ColumnType,
    pub nullable: bool,
    pub is_primary_key: bool,
    pub is_unique: bool,
    pub default_value: Option<String>,
}

impl Column {
    pub fn new(name: String, data_type: ColumnType) -> Self {
        Column {
            name,
            data_type,
            nullable: false,
            is_primary_key: false,
            is_unique: false,
            default_value: None,
        }
    }

    pub fn validate_value(&self, value: &str) -> bool {
        match self.data_type {
            ColumnType::Integer => value.parse::<i64>().is_ok(),
            ColumnType::Float => value.parse::<f64>().is_ok(),
            ColumnType::Varchar(max_len) => {
                // Remove quotes if present and check length
                let cleaned_value = value.trim_matches(|c| c == '\'' || c == '"');
                cleaned_value.len() <= max_len
            },
            ColumnType::Boolean => {
                let cleaned_value = value.trim_matches(|c| c == '\'' || c == '"').to_lowercase();
                cleaned_value == "true" || cleaned_value == "false"
            },
            ColumnType::Timestamp => chrono::DateTime::parse_from_rfc3339(value).is_ok(),
        }
    }
} 