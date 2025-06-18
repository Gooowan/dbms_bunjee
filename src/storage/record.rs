use serde::{Serialize, Deserialize};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct Record {
    pub id: u64,
    pub data: Vec<u8>
}

impl Record {
    pub fn new(id:u64, data: Vec<u8>) -> Self{
        Self {id, data }
    }
}