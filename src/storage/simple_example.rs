use super::{LSMEngine, Record};
use std::io;

/// Simple example showing how to use the LSM storage engine
pub fn run_example() -> io::Result<()> {
    println!("=== LSM Storage Engine Example ===");
    
    // Create LSM engine with memtable size of 100 records
    let mut engine = LSMEngine::new("./data", 100)?;
    
    println!("Created LSM engine with data directory: ./data");
    
    // Insert some records
    println!("\nInserting records...");
    for i in 1..=10 {
        let data = format!("Record data for ID {}", i).into_bytes();
        let record = Record::new(i, data);
        engine.insert(record)?;
        println!("Inserted record with ID: {}", i);
    }
    
    // Read some records
    println!("\nReading records...");
    for i in [1, 5, 10] {
        if let Some(record) = engine.get(i)? {
            let data_str = String::from_utf8_lossy(&record.data);
            println!("Record {}: {}", i, data_str);
        } else {
            println!("Record {} not found", i);
        }
    }
    
    // Update a record
    println!("\nUpdating record 5...");
    let new_data = "Updated data for record 5".as_bytes().to_vec();
    engine.update(5, new_data)?;
    
    if let Some(record) = engine.get(5)? {
        let data_str = String::from_utf8_lossy(&record.data);
        println!("Updated record 5: {}", data_str);
    }
    
    // Delete a record
    println!("\nDeleting record 3...");
    engine.delete(3)?;
    
    if engine.get(3)?.is_none() {
        println!("Record 3 successfully deleted");
    }
    
    // Show engine statistics
    println!("\nEngine statistics:");
    let stats = engine.stats()?;
    println!("  Memtable size: {} records", stats.memtable_size);
    println!("  SSTable count: {}", stats.sstable_count);
    println!("  Total records: {}", stats.total_records);
    
    // Force flush memtable to disk
    println!("\nFlushing memtable to disk...");
    engine.flush()?;
    
    let stats = engine.stats()?;
    println!("After flush - Memtable size: {} records", stats.memtable_size);
    println!("After flush - SSTable count: {}", stats.sstable_count);
    
    println!("\n=== Example completed successfully! ===");
    
    Ok(())
}

/// Example showing how to create and use individual components
pub fn component_example() -> io::Result<()> {
    use super::{MemTable, WriteLog, SSTable};
    
    println!("\n=== Individual Components Example ===");
    
    // 1. MemTable example
    println!("\n1. MemTable example:");
    let mut memtable = MemTable::new(3);
    
    memtable.insert(Record::new(1, b"data1".to_vec()));
    memtable.insert(Record::new(2, b"data2".to_vec()));
    
    if let Some(record) = memtable.get(1) {
        println!("   Retrieved from memtable: {:?}", String::from_utf8_lossy(&record.data));
    }
    
    println!("   Memtable size: {}", memtable.size());
    
    // 2. WriteLog example
    println!("\n2. WriteLog example:");
    let mut writelog = WriteLog::new("./example.log")?;
    let record = Record::new(100, b"logged data".to_vec());
    
    writelog.log_insert(&record)?;
    writelog.log_update(100, b"updated logged data")?;
    writelog.log_delete(100)?;
    
    let entries = writelog.replay()?;
    println!("   Replayed {} log entries", entries.len());
    
    // 3. SSTable example
    println!("\n3. SSTable example:");
    let block = memtable.flush_to_block();
    let mut sstable = SSTable::create_from_block(block, "./example.sst")?;
    
    if let Some(record) = sstable.get(1)? {
        println!("   Retrieved from SSTable: {:?}", String::from_utf8_lossy(&record.data));
    }
    
    println!("   SSTable size: {}", sstable.size()?);
    
    println!("\n=== Components example completed! ===");
    
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_examples_run_without_error() {
        // These tests just ensure the examples can run without panicking
        // In a real scenario, you might want to use a temporary directory
        
        // Note: These will create files in the current directory
        // In production code, you'd want to use proper temporary directories
        
        assert!(run_example().is_ok());
        assert!(component_example().is_ok());
    }
} 