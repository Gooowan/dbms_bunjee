use super::{Record, MemTable, WriteLog, SSTable, LogEntry};
use std::fs;
use std::io;

/// Simple LSM Tree Storage Engine
pub struct LSMEngine {
    memtable: MemTable,
    writelog: WriteLog,
    sstables: Vec<SSTable>,
    data_dir: String,
    next_sstable_id: u64,
}

impl LSMEngine {
    /// Create a new LSM engine
    pub fn new(data_dir: &str, memtable_size: usize) -> io::Result<Self> {
        // Create data directory if it doesn't exist
        fs::create_dir_all(data_dir)?;
        
        let log_path = format!("{}/write.log", data_dir);
        let writelog = WriteLog::new(&log_path)?;
        
        let mut engine = Self {
            memtable: MemTable::new(memtable_size),
            writelog,
            sstables: Vec::new(),
            data_dir: data_dir.to_string(),
            next_sstable_id: 1,
        };
        
        // Load existing SSTables
        engine.load_existing_sstables()?;
        
        // Replay write log
        engine.replay_write_log()?;
        
        Ok(engine)
    }

    /// Insert a record
    pub fn insert(&mut self, record: Record) -> io::Result<()> {
        // Log the operation first (WAL)
        self.writelog.log_insert(&record)?;
        
        // Try to insert into memtable
        if !self.memtable.insert(record.clone()) {
            // Memtable is full, flush it to disk
            self.flush_memtable()?;
            
            // Now insert into the new empty memtable
            if !self.memtable.insert(record) {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    "Failed to insert after flush"
                ));
            }
        }
        
        Ok(())
    }

    /// Get a record by ID
    pub fn get(&mut self, id: u64) -> io::Result<Option<Record>> {
        // First check memtable (most recent data)
        if let Some(record) = self.memtable.get(id) {
            return Ok(Some(record.clone()));
        }
        
        // Then check SSTables (newest to oldest)
        for sstable in &mut self.sstables {
            if let Some(record) = sstable.get(id)? {
                return Ok(Some(record.clone()));
            }
        }
        
        Ok(None)
    }

    /// Update a record
    pub fn update(&mut self, id: u64, new_data: Vec<u8>) -> io::Result<bool> {
        // Log the operation first
        self.writelog.log_update(id, &new_data)?;
        
        // Try to update in memtable first
        if self.memtable.update(id, new_data.clone()) {
            return Ok(true);
        }
        
        // If not in memtable, insert as new record (LSM semantics)
        let record = Record::new(id, new_data);
        if !self.memtable.insert(record.clone()) {
            // Memtable full, flush and try again
            self.flush_memtable()?;
            if !self.memtable.insert(record) {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    "Failed to insert update after flush"
                ));
            }
        }
        
        Ok(true)
    }

    /// Delete a record
    pub fn delete(&mut self, id: u64) -> io::Result<bool> {
        // Log the operation first
        self.writelog.log_delete(id)?;
        
        // Try to delete from memtable
        let deleted_from_mem = self.memtable.delete(id);
        
        // In LSM trees, we typically use tombstones for deletions
        // For simplicity, we'll just remove from memtable if present
        // In a real implementation, you'd insert a tombstone record
        
        Ok(deleted_from_mem)
    }

    /// Flush memtable to disk as SSTable
    fn flush_memtable(&mut self) -> io::Result<()> {
        if self.memtable.is_empty() {
            return Ok(());
        }
        
        // Create SSTable from memtable
        let block = self.memtable.flush_to_block();
        let sstable_path = format!("{}/sstable_{}.dat", self.data_dir, self.next_sstable_id);
        
        let sstable = SSTable::create_from_block(block, &sstable_path)?;
        self.sstables.insert(0, sstable); // Insert at beginning (newest first)
        
        self.next_sstable_id += 1;
        
        // Clear memtable
        self.memtable.clear();
        
        // Clear write log since data is now persisted
        self.writelog.clear()?;
        
        // Trigger compaction if we have too many SSTables
        if self.sstables.len() > 4 {
            self.compact_sstables()?;
        }
        
        Ok(())
    }

    /// Simple compaction: merge oldest SSTables
    fn compact_sstables(&mut self) -> io::Result<()> {
        if self.sstables.len() < 2 {
            return Ok(());
        }
        
        // Take the two oldest SSTables
        let mut sstable1 = self.sstables.pop().unwrap();
        let mut sstable2 = self.sstables.pop().unwrap();
        
        // Merge them
        let merged_path = format!("{}/sstable_{}.dat", self.data_dir, self.next_sstable_id);
        let merged_sstable = sstable1.merge_with(&mut sstable2, &merged_path)?;
        
        // Add merged SSTable back
        self.sstables.push(merged_sstable);
        self.next_sstable_id += 1;
        
        // Clean up old files (in production, you'd want better error handling)
        let _ = fs::remove_file(sstable1.file_path());
        let _ = fs::remove_file(sstable2.file_path());
        
        Ok(())
    }

    /// Load existing SSTables from disk
    fn load_existing_sstables(&mut self) -> io::Result<()> {
        let entries = fs::read_dir(&self.data_dir)?;
        let mut sstable_files = Vec::new();
        
        for entry in entries {
            let entry = entry?;
            let path = entry.path();
            if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                if name.starts_with("sstable_") && name.ends_with(".dat") {
                    sstable_files.push(path);
                }
            }
        }
        
        // Sort by creation time (newer first)
        sstable_files.sort_by(|a, b| {
            let a_metadata = fs::metadata(a).unwrap();
            let b_metadata = fs::metadata(b).unwrap();
            b_metadata.modified().unwrap().cmp(&a_metadata.modified().unwrap())
        });
        
        // Load SSTables
        for path in sstable_files {
            if let Some(path_str) = path.to_str() {
                let sstable = SSTable::new_lazy(path_str);
                self.sstables.push(sstable);
                
                // Update next_sstable_id
                if let Some(name) = path.file_stem().and_then(|n| n.to_str()) {
                    if let Some(id_str) = name.strip_prefix("sstable_") {
                        if let Ok(id) = id_str.parse::<u64>() {
                            self.next_sstable_id = self.next_sstable_id.max(id + 1);
                        }
                    }
                }
            }
        }
        
        Ok(())
    }

    /// Replay write log to restore memtable state
    fn replay_write_log(&mut self) -> io::Result<()> {
        let entries = self.writelog.replay()?;
        
        for entry in entries {
            match entry {
                LogEntry::Insert(record) => {
                    if !self.memtable.insert(record.clone()) {
                        // If memtable is full during replay, flush and continue
                        self.flush_memtable()?;
                        self.memtable.insert(record);
                    }
                }
                LogEntry::Update { id, data } => {
                    if !self.memtable.update(id, data.clone()) {
                        // Insert as new record if not found
                        let record = Record::new(id, data);
                        if !self.memtable.insert(record.clone()) {
                            self.flush_memtable()?;
                            self.memtable.insert(record);
                        }
                    }
                }
                LogEntry::Delete { id } => {
                    self.memtable.delete(id);
                }
            }
        }
        
        Ok(())
    }

    /// Force flush memtable (useful for shutdown)
    pub fn flush(&mut self) -> io::Result<()> {
        self.flush_memtable()
    }

    /// Get statistics about the storage engine
    pub fn stats(&mut self) -> io::Result<EngineStats> {
        let mut total_records = self.memtable.size();
        
        for sstable in &mut self.sstables {
            total_records += sstable.size()?;
        }
        
        Ok(EngineStats {
            memtable_size: self.memtable.size(),
            sstable_count: self.sstables.len(),
            total_records,
        })
    }

    /// Get all records from the LSM engine (memtable + SSTables)
    /// Returns the latest version of each record (by ID)
    pub fn get_all_records(&mut self) -> io::Result<Vec<Record>> {
        use std::collections::HashMap;
        
        let mut all_records: HashMap<u64, Record> = HashMap::new();
        
        // First, add all records from SSTables (oldest to newest)
        for sstable in self.sstables.iter_mut().rev() {
            let records = sstable.get_all()?;
            for record in records {
                all_records.insert(record.id, record.clone());
            }
        }
        
        // Then, add records from memtable (most recent)
        // This will overwrite any older versions from SSTables
        for record in self.memtable.get_sorted_records() {
            all_records.insert(record.id, record.clone());
        }
        
        // Convert to vector and sort by ID for consistent ordering
        let mut result: Vec<Record> = all_records.into_values().collect();
        result.sort_by_key(|r| r.id);
        
        Ok(result)
    }
}

#[derive(Debug)]
pub struct EngineStats {
    pub memtable_size: usize,
    pub sstable_count: usize,
    pub total_records: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_lsm_engine_basic_ops() {
        let temp_dir = TempDir::new().unwrap();
        let mut engine = LSMEngine::new(temp_dir.path().to_str().unwrap(), 3).unwrap();
        
        // Test insert and get
        let record1 = Record::new(1, vec![1, 2, 3]);
        let record2 = Record::new(2, vec![4, 5, 6]);
        
        engine.insert(record1.clone()).unwrap();
        engine.insert(record2.clone()).unwrap();
        
        assert_eq!(engine.get(1).unwrap().unwrap().data, vec![1, 2, 3]);
        assert_eq!(engine.get(2).unwrap().unwrap().data, vec![4, 5, 6]);
        
        // Test update
        engine.update(1, vec![7, 8, 9]).unwrap();
        assert_eq!(engine.get(1).unwrap().unwrap().data, vec![7, 8, 9]);
        
        // Test delete
        engine.delete(1).unwrap();
        assert!(engine.get(1).unwrap().is_none());
    }

    #[test]
    fn test_lsm_engine_memtable_flush() {
        let temp_dir = TempDir::new().unwrap();
        let mut engine = LSMEngine::new(temp_dir.path().to_str().unwrap(), 2).unwrap();
        
        // Fill memtable to trigger flush
        engine.insert(Record::new(1, vec![1])).unwrap();
        engine.insert(Record::new(2, vec![2])).unwrap();
        engine.insert(Record::new(3, vec![3])).unwrap(); // This should trigger flush
        
        // All records should still be accessible
        assert_eq!(engine.get(1).unwrap().unwrap().data, vec![1]);
        assert_eq!(engine.get(2).unwrap().unwrap().data, vec![2]);
        assert_eq!(engine.get(3).unwrap().unwrap().data, vec![3]);
        
        let stats = engine.stats().unwrap();
        assert_eq!(stats.sstable_count, 1);
        assert_eq!(stats.memtable_size, 1);
    }

    #[test]
    fn test_lsm_engine_recovery() {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().to_str().unwrap();
        
        // Create engine and insert some data
        {
            let mut engine = LSMEngine::new(data_dir, 5).unwrap();
            engine.insert(Record::new(1, vec![1])).unwrap();
            engine.insert(Record::new(2, vec![2])).unwrap();
            // Don't flush - data should be in write log
        }
        
        // Create new engine (simulating restart)
        {
            let mut engine = LSMEngine::new(data_dir, 5).unwrap();
            // Data should be recovered from write log
            assert_eq!(engine.get(1).unwrap().unwrap().data, vec![1]);
            assert_eq!(engine.get(2).unwrap().unwrap().data, vec![2]);
        }
    }
} 