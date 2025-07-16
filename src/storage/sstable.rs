use super::{Block, Record};
use std::io;

/// Sorted String Table - immutable sorted storage on disk
pub struct SSTable {
    block: Block,
    file_path: String,
    is_loaded: bool,
}

impl SSTable {
    /// Create a new SSTable from a sorted block of records
    pub fn create_from_block(block: Block, file_path: &str) -> io::Result<Self> {
        // Ensure records are sorted by ID
        let mut sorted_records: Vec<_> = block.get_all().into_iter().cloned().collect();
        sorted_records.sort_by_key(|r| r.id);
        
        let mut sorted_block = Block::new();
        for record in sorted_records {
            sorted_block.insert(record);
        }
        
        // Save to disk
        sorted_block.save_to_disk(file_path)?;
        
        Ok(Self {
            block: sorted_block,
            file_path: file_path.to_string(),
            is_loaded: true,
        })
    }

    /// Load an existing SSTable from disk
    pub fn load_from_disk(file_path: &str) -> io::Result<Self> {
        let block = Block::load_from_disk(file_path)?;
        
        Ok(Self {
            block,
            file_path: file_path.to_string(),
            is_loaded: true,
        })
    }

    /// Create an SSTable reference without loading data (lazy loading)
    pub fn new_lazy(file_path: &str) -> Self {
        Self {
            block: Block::new(),
            file_path: file_path.to_string(),
            is_loaded: false,
        }
    }

    /// Ensure the SSTable is loaded in memory
    fn ensure_loaded(&mut self) -> io::Result<()> {
        if !self.is_loaded {
            self.block = Block::load_from_disk(&self.file_path)?;
            self.is_loaded = true;
        }
        Ok(())
    }

    /// Get a record by ID (binary search since records are sorted)
    pub fn get(&mut self, id: u64) -> io::Result<Option<&Record>> {
        self.ensure_loaded()?;
        
        // Use binary search on sorted records
        let records = self.block.get_all();
        match records.binary_search_by_key(&id, |r| r.id) {
            Ok(index) => Ok(Some(records[index])),
            Err(_) => Ok(None),
        }
    }

    /// Get all records in the SSTable
    pub fn get_all(&mut self) -> io::Result<Vec<&Record>> {
        self.ensure_loaded()?;
        Ok(self.block.get_all())
    }

    /// Get records in a range [start_id, end_id]
    pub fn get_range(&mut self, start_id: u64, end_id: u64) -> io::Result<Vec<&Record>> {
        self.ensure_loaded()?;
        
        let all_records = self.block.get_all();
        let mut result = Vec::new();
        
        for record in all_records {
            if record.id >= start_id && record.id <= end_id {
                result.push(record);
            } else if record.id > end_id {
                break; // Since records are sorted, we can stop here
            }
        }
        
        Ok(result)
    }

    /// Check if SSTable is empty
    pub fn is_empty(&mut self) -> io::Result<bool> {
        self.ensure_loaded()?;
        Ok(self.block.is_empty())
    }

    /// Get number of records
    pub fn size(&mut self) -> io::Result<usize> {
        self.ensure_loaded()?;
        Ok(self.block.count())
    }

    /// Get the file path
    pub fn file_path(&self) -> &str {
        &self.file_path
    }

    /// Check if this SSTable contains a record with the given ID
    pub fn contains(&mut self, id: u64) -> io::Result<bool> {
        self.ensure_loaded()?;
        
        let records = self.block.get_all();
        Ok(records.binary_search_by_key(&id, |r| r.id).is_ok())
    }

    /// Merge this SSTable with another to create a new SSTable
    pub fn merge_with(&mut self, other: &mut SSTable, output_path: &str) -> io::Result<SSTable> {
        self.ensure_loaded()?;
        other.ensure_loaded()?;
        
        let mut all_records = Vec::new();
        all_records.extend(self.block.get_all().iter().cloned());
        all_records.extend(other.block.get_all().iter().cloned());
        
        // Sort and deduplicate (keeping the latest version)
        all_records.sort_by_key(|r| r.id);
        all_records.dedup_by_key(|r| r.id);
        
        let mut merged_block = Block::new();
        for record in all_records {
            merged_block.insert(record.clone());
        }
        
        SSTable::create_from_block(merged_block, output_path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test_sstable_create_and_load() {
        let temp_file = NamedTempFile::new().unwrap();
        let file_path = temp_file.path().to_str().unwrap();
        
        // Create SSTable
        let mut block = Block::new();
        block.insert(Record::new(3, vec![3]));
        block.insert(Record::new(1, vec![1]));
        block.insert(Record::new(2, vec![2]));
        
        let mut sstable = SSTable::create_from_block(block, file_path).unwrap();
        
        // Test get
        assert_eq!(sstable.get(1).unwrap().unwrap().data, vec![1]);
        assert_eq!(sstable.get(2).unwrap().unwrap().data, vec![2]);
        assert_eq!(sstable.get(3).unwrap().unwrap().data, vec![3]);
        
        // Test that records are sorted
        let all_records = sstable.get_all().unwrap();
        assert_eq!(all_records[0].id, 1);
        assert_eq!(all_records[1].id, 2);
        assert_eq!(all_records[2].id, 3);
    }

    #[test]
    fn test_sstable_range_query() {
        let temp_file = NamedTempFile::new().unwrap();
        let file_path = temp_file.path().to_str().unwrap();
        
        let mut block = Block::new();
        block.insert(Record::new(1, vec![1]));
        block.insert(Record::new(2, vec![2]));
        block.insert(Record::new(3, vec![3]));
        block.insert(Record::new(4, vec![4]));
        block.insert(Record::new(5, vec![5]));
        
        let mut sstable = SSTable::create_from_block(block, file_path).unwrap();
        
        let range_records = sstable.get_range(2, 4).unwrap();
        assert_eq!(range_records.len(), 3);
        assert_eq!(range_records[0].id, 2);
        assert_eq!(range_records[1].id, 3);
        assert_eq!(range_records[2].id, 4);
    }

    #[test]
    fn test_sstable_lazy_loading() {
        let temp_file = NamedTempFile::new().unwrap();
        let file_path = temp_file.path().to_str().unwrap();
        
        // Create and save SSTable
        let mut block = Block::new();
        block.insert(Record::new(1, vec![1]));
        block.save_to_disk(file_path).unwrap();
        
        // Create lazy SSTable
        let mut lazy_sstable = SSTable::new_lazy(file_path);
        assert!(!lazy_sstable.is_loaded);
        
        // Access should trigger loading
        let record = lazy_sstable.get(1).unwrap().unwrap();
        assert_eq!(record.data, vec![1]);
        assert!(lazy_sstable.is_loaded);
    }
} 