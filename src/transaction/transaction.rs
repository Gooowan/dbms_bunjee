use std::collections::HashMap;
use crate::metadata::Table;
use super::error::TransactionError;

pub struct Transaction {
    tables: HashMap<String, Table>,
    is_active: bool,
}

impl Transaction {
    pub fn new() -> Self {
        Transaction {
            tables: HashMap::new(),
            is_active: false,
        }
    }

    pub fn begin(&mut self) -> Result<(), TransactionError> {
        if self.is_active {
            return Err(TransactionError::AlreadyInTransaction);
        }
        self.is_active = true;
        Ok(())
    }

    pub fn commit(&mut self) -> Result<(), TransactionError> {
        if !self.is_active {
            return Err(TransactionError::NotInTransaction);
        }
        self.is_active = false;
        self.tables.clear();
        Ok(())
    }

    pub fn rollback(&mut self) -> Result<(), TransactionError> {
        if !self.is_active {
            return Err(TransactionError::NotInTransaction);
        }
        self.is_active = false;
        self.tables.clear();
        Ok(())
    }
}