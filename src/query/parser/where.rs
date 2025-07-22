use crate::metadata::Table;
use crate::query::error::QueryError;

#[derive(Debug)]
pub struct WhereClause {
    pub column: String,
    pub operator: String,
    pub value: String,
}

pub struct WhereParser;

impl WhereParser {
    pub fn new() -> Self {
        WhereParser
    }

    pub fn parse_where_clause(&self, tokens: &[&str]) -> Result<WhereClause, QueryError> {
        if tokens.len() < 3 {
            return Err(QueryError::SyntaxError("Invalid WHERE clause".to_string()));
        }

        let column = tokens[0].to_string();
        let operator = tokens[1].to_string();
        let value = tokens[2].to_string();

        Ok(WhereClause {
            column,
            operator,
            value,
        })
    }

    pub fn evaluate_where_clause(
        &self,
        row_data: &[String],
        table: &Table,
        where_clause: &WhereClause,
    ) -> Result<bool, QueryError> {
        let col_index = table.columns.iter()
            .position(|c| c.name == where_clause.column)
            .ok_or_else(|| QueryError::ColumnNotFound(where_clause.column.clone()))?;

        let value = &row_data[col_index];
        // Strip quotes from comparison value and convert to String for consistent types
        let compare_value = where_clause.value.trim_matches(|c| c == '\'' || c == '"').to_string();

        match where_clause.operator.as_str() {
            "=" => Ok(value == &compare_value),
            "!=" => Ok(value != &compare_value),
            ">" => {
                // Try numeric comparison first, fall back to string comparison
                if let (Ok(val1), Ok(val2)) = (value.parse::<f64>(), compare_value.parse::<f64>()) {
                    Ok(val1 > val2)
                } else {
                    Ok(value.as_str() > compare_value.as_str())
                }
            },
            "<" => {
                // Try numeric comparison first, fall back to string comparison
                if let (Ok(val1), Ok(val2)) = (value.parse::<f64>(), compare_value.parse::<f64>()) {
                    Ok(val1 < val2)
                } else {
                    Ok(value.as_str() < compare_value.as_str())
                }
            },
            ">=" => {
                // Try numeric comparison first, fall back to string comparison
                if let (Ok(val1), Ok(val2)) = (value.parse::<f64>(), compare_value.parse::<f64>()) {
                    Ok(val1 >= val2)
                } else {
                    Ok(value.as_str() >= compare_value.as_str())
                }
            },
            "<=" => {
                // Try numeric comparison first, fall back to string comparison
                if let (Ok(val1), Ok(val2)) = (value.parse::<f64>(), compare_value.parse::<f64>()) {
                    Ok(val1 <= val2)
                } else {
                    Ok(value.as_str() <= compare_value.as_str())
                }
            },
            _ => Err(QueryError::SyntaxError(format!("Invalid operator: {}", where_clause.operator))),
        }
    }
} 