#[derive(Debug)]
pub enum QueryResult {
    Select(Vec<Vec<String>>),
    Insert(usize),
    Update(usize),
    Delete(usize),
    CreateTable,
    DropTable,
    Error(String),
    // New variants for joins and aggregations
    Join(JoinResult),
    Aggregation(AggregationResult),
}

#[derive(Debug)]
pub struct JoinResult {
    pub headers: Vec<String>,
    pub rows: Vec<Vec<String>>,
}

#[derive(Debug)]
pub struct AggregationResult {
    pub headers: Vec<String>,
    pub rows: Vec<Vec<String>>,
    pub group_by_columns: Vec<String>,
} 