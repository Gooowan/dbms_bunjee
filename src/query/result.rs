#[derive(Debug)]
pub enum QueryResult {
    Select(Vec<Vec<String>>),
    Insert(usize),
    Update(usize),
    Delete(usize),
    CreateTable,
    DropTable,
    Error(String),
} 