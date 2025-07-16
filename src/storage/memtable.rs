use super::{Block, Record};
use std::collections::BTreeMap;

/// In-memory table for recent writes, backed by existing Block structure
pub struct MemTable {
    data: Block,
    index: BTreeMap<u64, bool>, // Simple index to track what's in memory
    max_size: usize,
}

impl MemTable {
    pub fn new(max_size: usize) -> Self {
        Self {
            data: Block::new(),
            index: BTreeMap::new(),
            max_size,
        }
    }

    pub fn insert(&mut self, record: Record) -> bool {
        if self.is_full() {
            return false;
        }
        
        let id = record.id;
        let success = self.data.insert(record);
        if success {
            self.index.insert(id, true);
        }
        success
    }

    pub fn get(&self, id: u64) -> Option<&Record> {
        self.data.get(id)
    }

    pub fn update(&mut self, id: u64, new_data: Vec<u8>) -> bool {
        self.data.update(id, new_data)
    }

    pub fn delete(&mut self, id: u64) -> bool {
        let success = self.data.delete(id);
        if success {
            self.index.remove(&id);
        }
        success
    }

    pub fn is_full(&self) -> bool {
        self.data.count() >= self.max_size
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn flush_to_block(&mut self) -> Block {
        let mut block = Block::new();
        for record in self.data.get_all() {
            block.insert(record.clone());
        }
        self.clear();
        block
    }

    pub fn clear(&mut self) {
        self.data.clear();
        self.index.clear();
    }

    pub fn size(&self) -> usize {
        self.data.count()
    }

    /// Get all records in sorted order by ID
    pub fn get_sorted_records(&self) -> Vec<&Record> {
        let mut records = self.data.get_all();
        records.sort_by_key(|r| r.id);
        records
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memtable_basic_ops() {
        let mut memtable = MemTable::new(3);
        
        // Test insert
        assert!(memtable.insert(Record::new(1, vec![1, 2, 3])));
        assert!(memtable.insert(Record::new(2, vec![4, 5, 6])));
        
        // Test get
        assert_eq!(memtable.get(1).unwrap().data, vec![1, 2, 3]);
        assert_eq!(memtable.get(2).unwrap().data, vec![4, 5, 6]);
        
        // Test update
        assert!(memtable.update(1, vec![7, 8, 9]));
        assert_eq!(memtable.get(1).unwrap().data, vec![7, 8, 9]);
        
        // Test delete
        assert!(memtable.delete(1));
        assert!(memtable.get(1).is_none());
    }

    #[test]
    fn test_memtable_full() {
        let mut memtable = MemTable::new(2);
        
        assert!(memtable.insert(Record::new(1, vec![1])));
        assert!(memtable.insert(Record::new(2, vec![2])));
        assert!(!memtable.insert(Record::new(3, vec![3]))); // Should fail - full
        
        assert!(memtable.is_full());
    }

    #[test]
    fn test_memtable_flush() {
        let mut memtable = MemTable::new(5);
        memtable.insert(Record::new(1, vec![1]));
        memtable.insert(Record::new(2, vec![2]));
        
        let block = memtable.flush_to_block();
        assert_eq!(block.count(), 2);
        assert_eq!(block.get(1).unwrap().data, vec![1]);
        assert_eq!(block.get(2).unwrap().data, vec![2]);
        
        // After flush, memtable should be empty
        assert!(memtable.is_empty());
    }
} 