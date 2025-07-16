use chrono::{DateTime, Utc};
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
    pub constraints: Vec<ColumnConstraint>,
    pub default_value: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ColumnConstraint {
    NotNull,
    Unique,
    PrimaryKey,
    ForeignKey(String, String), // (table_name, column_name)
    Check(String), // SQL condition
    Default(String), // Default value
}

impl Column {
    pub fn new(name: String, data_type: ColumnType) -> Self {
        let now = Utc::now();
        Self {
            name,
            data_type,
            constraints: Vec::new(),
            default_value: None,
            created_at: now,
            updated_at: now,
        }
    }

    pub fn with_constraint(mut self, constraint: ColumnConstraint) -> Self {
        self.constraints.push(constraint);
        self
    }

    pub fn with_default(mut self, default: String) -> Self {
        self.default_value = Some(default);
        self
    }

    pub fn validate_value(&self, value: &str) -> bool {
        match self.data_type {
            ColumnType::Integer => value.parse::<i64>().is_ok(),
            ColumnType::Float => value.parse::<f64>().is_ok(),
            ColumnType::Varchar(max_len) => {
                let cleaned_value = value.trim_matches(|c| c == '\'' || c == '"');
                cleaned_value.len() <= max_len
            }
            ColumnType::Boolean => {
                let cleaned_value = value.trim_matches(|c| c == '\'' || c == '"').to_lowercase();
                matches!(cleaned_value.as_str(), "true" | "false")
            }
            ColumnType::Timestamp => value.parse::<i64>().is_ok(),
        }
    }

    pub fn is_nullable(&self) -> bool {
        !self.constraints.iter().any(|c| matches!(c, ColumnConstraint::NotNull))
    }

    pub fn is_unique(&self) -> bool {
        self.constraints.iter().any(|c| matches!(c, ColumnConstraint::Unique | ColumnConstraint::PrimaryKey))
    }

    pub fn is_primary_key(&self) -> bool {
        self.constraints.iter().any(|c| matches!(c, ColumnConstraint::PrimaryKey))
    }
} 