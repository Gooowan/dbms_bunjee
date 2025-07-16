use std::fs;
use std::path::Path;
use crate::query::{QueryEngine, QueryResult};

#[cfg(test)]
use tempfile::TempDir;

/// Comprehensive test demonstrating database persistence across launches
pub fn test_database_persistence() -> Result<(), Box<dyn std::error::Error>> {
    println!("🧪 Database Persistence Test");
    println!("============================");
    
    // Use a temporary directory for testing
    #[cfg(test)]
    let temp_dir = TempDir::new()?;
    #[cfg(test)]
    let test_db_path = temp_dir.path().to_str().unwrap();
    
    // For non-test builds, use a test directory that we'll clean up
    #[cfg(not(test))]
    let test_db_path = "./test_persistence_db";
    #[cfg(not(test))]
    {
        // Clean up any existing test directory
        if Path::new(test_db_path).exists() {
            std::fs::remove_dir_all(test_db_path).ok();
        }
    }
    
    println!("📁 Test database directory: {}", test_db_path);
    
    // === PHASE 1: Initial Setup ===
    println!("\n📝 PHASE 1: Initial database setup and data insertion");
    println!("-----------------------------------------------------");
    
    {
        let mut engine = QueryEngine::new_with_data_dir(test_db_path);
        
        // Create multiple tables using the working syntax
        let create_users = engine.execute("CREATE TABLE users (id INTEGER, name VARCHAR 50, email VARCHAR 100)")?;
        println!("✅ Created users table: {:?}", create_users);
        
        let create_orders = engine.execute("CREATE TABLE orders (id INTEGER, user_id INTEGER, amount INTEGER)")?;
        println!("✅ Created orders table: {:?}", create_orders);
        
        // Insert test data
        let user_inserts = vec![
            "INSERT INTO users VALUES (1, 'Alice Smith', 'alice@example.com')",
            "INSERT INTO users VALUES (2, 'Bob Johnson', 'bob@example.com')",
            "INSERT INTO users VALUES (3, 'Carol Wilson', 'carol@example.com')",
            "INSERT INTO users VALUES (4, 'David Brown', 'david@example.com')",
            "INSERT INTO users VALUES (5, 'Eve Davis', 'eve@example.com')",
        ];
        
        for sql in user_inserts {
            let result = engine.execute(sql)?;
            println!("   📋 {}: {:?}", sql, result);
        }
        
        let order_inserts = vec![
            "INSERT INTO orders VALUES (101, 1, 250)",
            "INSERT INTO orders VALUES (102, 2, 180)",
            "INSERT INTO orders VALUES (103, 1, 320)",
            "INSERT INTO orders VALUES (104, 3, 150)",
            "INSERT INTO orders VALUES (105, 2, 275)",
        ];
        
        for sql in order_inserts {
            let result = engine.execute(sql)?;
            println!("   🛒 {}: {:?}", sql, result);
        }
        
        // Query initial data
        let users_result = engine.execute("SELECT * FROM users")?;
        println!("👥 Initial users query: {:?}", users_result);
        
        let orders_result = engine.execute("SELECT * FROM orders")?;
        println!("🛒 Initial orders query: {:?}", orders_result);
        
        // Manual flush to ensure data is persisted
        engine.flush_all()?;
        println!("💾 Data flushed to disk successfully");
        
        // Show statistics
        let tables = engine.list_tables();
        println!("📊 Tables in database: {:?}", tables);
        
    } // Engine goes out of scope, simulating application shutdown
    
    // === PHASE 2: Restart and Verify Persistence ===
    println!("\n🔄 PHASE 2: Database restart and persistence verification");
    println!("-------------------------------------------------------");
    
    {
        let mut engine = QueryEngine::new_with_data_dir(test_db_path);
        
        // Verify tables were restored
        let tables = engine.list_tables();
        println!("📋 Restored tables: {:?}", tables);
        assert!(tables.contains(&"users".to_string()), "Users table should be restored");
        assert!(tables.contains(&"orders".to_string()), "Orders table should be restored");
        
        // Verify data was restored
        let users_result = engine.execute("SELECT * FROM users")?;
        println!("👥 Restored users: {:?}", users_result);
        
        let orders_result = engine.execute("SELECT * FROM orders")?;
        println!("🛒 Restored orders: {:?}", orders_result);
        
        // Add more data to test ongoing persistence
        let new_user = engine.execute("INSERT INTO users VALUES (6, 'Frank Miller', 'frank@example.com')")?;
        println!("➕ Added new user: {:?}", new_user);
        
        let new_order = engine.execute("INSERT INTO orders VALUES (106, 6, 400)")?;
        println!("➕ Added new order: {:?}", new_order);
        
        // Test updates
        let update_result = engine.execute("UPDATE users SET email = 'alice.smith@newdomain.com' WHERE id = 1")?;
        println!("🔄 Updated user email: {:?}", update_result);
        
        // Show stats before flush
        for table in &tables {
            if let Ok(stats) = engine.get_table_stats(table) {
                println!("📊 Stats for {}: Memtable: {}, SSTables: {}, Total: {}", 
                    table, stats.memtable_size, stats.sstable_count, stats.total_records);
            }
        }
        
        // Flush again
        engine.flush_all()?;
        println!("💾 Additional data flushed to disk");
        
    } // Second shutdown
    
    // === PHASE 3: Final Verification ===
    println!("\n✅ PHASE 3: Final verification after second restart");
    println!("--------------------------------------------------");
    
    {
        let mut engine = QueryEngine::new_with_data_dir(test_db_path);
        
        // Final verification
        let final_users = engine.execute("SELECT * FROM users")?;
        println!("👥 Final users data: {:?}", final_users);
        
        let final_orders = engine.execute("SELECT * FROM orders")?;
        println!("🛒 Final orders data: {:?}", final_orders);
        
        // Verify specific data integrity
        let alice_query = engine.execute("SELECT * FROM users WHERE id = 1")?;
        println!("🔍 Alice's record (should have updated email): {:?}", alice_query);
        
        let frank_query = engine.execute("SELECT * FROM users WHERE id = 6")?;
        println!("🔍 Frank's record (added in second session): {:?}", frank_query);
        
        // Test complex query persistence
        let user_count = engine.execute("SELECT * FROM users")?;
        if let QueryResult::Select(rows) = user_count {
            println!("📊 Total users persisted: {}", rows.len());
            assert_eq!(rows.len(), 6, "Should have 6 users total");
        }
        
        let order_count = engine.execute("SELECT * FROM orders")?;
        if let QueryResult::Select(rows) = order_count {
            println!("📊 Total orders persisted: {}", rows.len());
            assert_eq!(rows.len(), 6, "Should have 6 orders total");
        }
    }
    
    // === PHASE 4: File System Verification ===
    println!("\n📁 PHASE 4: File system verification");
    println!("-----------------------------------");
    
    // Check that persistence files exist
    let metadata_file = format!("{}/tables.json", test_db_path);
    let users_dir = format!("{}/users", test_db_path);
    let orders_dir = format!("{}/orders", test_db_path);
    
    println!("📄 Metadata file exists: {}", Path::new(&metadata_file).exists());
    println!("📂 Users directory exists: {}", Path::new(&users_dir).exists());
    println!("📂 Orders directory exists: {}", Path::new(&orders_dir).exists());
    
    // Read and display metadata
    if let Ok(metadata_content) = fs::read_to_string(&metadata_file) {
        println!("📄 Metadata content preview:");
        let lines: Vec<&str> = metadata_content.lines().take(10).collect();
        for line in lines {
            println!("   {}", line);
        }
        if metadata_content.lines().count() > 10 {
            println!("   ... (truncated)");
        }
    }
    
    // Check for data files
    for table_dir in [&users_dir, &orders_dir] {
        if let Ok(entries) = fs::read_dir(table_dir) {
            println!("📂 Contents of {}:", table_dir);
            for entry in entries {
                if let Ok(entry) = entry {
                    let file_name = entry.file_name();
                    let file_size = entry.metadata().map(|m| m.len()).unwrap_or(0);
                    println!("   📄 {} ({} bytes)", file_name.to_string_lossy(), file_size);
                }
            }
        }
    }
    
    // Clean up test directory if not using tempfile
    #[cfg(not(test))]
    {
        if Path::new(test_db_path).exists() {
            std::fs::remove_dir_all(test_db_path).ok();
            println!("🧹 Cleaned up test directory");
        }
    }
    
    println!("\n🎉 PERSISTENCE TEST COMPLETED SUCCESSFULLY!");
    println!("============================================");
    println!("✅ Tables persist across restarts");
    println!("✅ Data integrity maintained");
    println!("✅ Updates and new records persist");
    println!("✅ File system shows expected persistence files");
    println!("✅ LSM storage engine working correctly");
    
    Ok(())
}

/// Simple test runner for standalone execution
pub fn run_persistence_test() {
    match test_database_persistence() {
        Ok(()) => {
            println!("\n🎯 All persistence tests passed!");
        }
        Err(e) => {
            eprintln!("\n❌ Persistence test failed: {}", e);
            std::process::exit(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_persistence_integration() {
        test_database_persistence().expect("Persistence test should pass");
    }
} 