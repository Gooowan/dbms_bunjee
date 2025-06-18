#[derive(Debug)]
pub enum IndexError {
    ColumnNotFound(String),
    IndexAlreadyExists(String),
    IndexNotFound(String),
}