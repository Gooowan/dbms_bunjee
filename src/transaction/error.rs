#[derive(Debug)]
pub enum TransactionError {
    AlreadyInTransaction,
    NotInTransaction,
    TableNotFound(String),
    ExecutionError(String),
}