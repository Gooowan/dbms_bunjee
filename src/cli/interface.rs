use std::io::{self, Write};
use crate::query::QueryEngine;
use super::display::display_result;

pub struct CLI {
    pub query_engine: QueryEngine,
    pub prompt: String,
}

impl CLI {
    pub fn new() -> Self {
        CLI {
            query_engine: QueryEngine::new(),
            prompt: "dbms> ".to_string(),
        }
    }

    pub fn run(&mut self) {
        println!("Welcome to BUNJEE DBMS CLI!");
        println!("Type 'exit' or 'quit' to exit");
        println!("Type 'help' for available commands");
        println!("Type 'flush' to manually flush all data to disk");

        loop {
            print!("{}", self.prompt);
            io::stdout().flush().unwrap();

            let mut input = String::new();
            if io::stdin().read_line(&mut input).is_err() {
                println!("Error reading input");
                continue;
            }

            let input = input.trim();
            if input.is_empty() {
                continue;
            }

            match input.to_lowercase().as_str() {
                "exit" | "quit" => {
                    println!("Shutting down database...");
                    if let Err(e) = self.shutdown() {
                        eprintln!("Warning: Error during shutdown: {:?}", e);
                    } else {
                        println!("Database shutdown complete. All data has been persisted.");
                    }
                    break;
                },
                "help" => self.show_help(),
                "flush" => self.manual_flush(),
                "tables" => self.list_tables(),
                "stats" => self.show_stats(),
                _ => self.execute_query(input),
            }
        }
    }

    fn execute_query(&mut self, query: &str) {
        match self.query_engine.execute(query) {
            Ok(result) => display_result(&result),
            Err(error) => println!("Error: {:?}", error),
        }
    }

    fn shutdown(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        // Flush all data to ensure persistence
        self.query_engine.flush_all()?;
        Ok(())
    }

    fn manual_flush(&mut self) {
        match self.query_engine.flush_all() {
            Ok(()) => println!("✅ All data flushed to disk successfully"),
            Err(e) => println!("❌ Error flushing data: {:?}", e),
        }
    }

    fn list_tables(&self) {
        let tables = self.query_engine.list_tables();
        if tables.is_empty() {
            println!("No tables found");
        } else {
            println!("Available tables:");
            for table in tables {
                println!("  - {}", table);
            }
        }
    }

    fn show_stats(&mut self) {
        let tables = self.query_engine.list_tables();
        if tables.is_empty() {
            println!("No tables found");
            return;
        }

        println!("Database Statistics:");
        println!("===================");
        for table_name in tables {
            match self.query_engine.get_table_stats(&table_name) {
                Ok(stats) => {
                    println!("Table: {}", table_name);
                    println!("  Memtable records: {}", stats.memtable_size);
                    println!("  SSTable count: {}", stats.sstable_count);
                    println!("  Total records: {}", stats.total_records);
                    println!();
                }
                Err(e) => {
                    println!("  Error getting stats for {}: {:?}", table_name, e);
                }
            }
        }
    }

    fn show_help(&self) {
        println!("\nAvailable commands:");
        println!("  SELECT * FROM table_name [WHERE condition]");
        println!("  INSERT INTO table_name (col1, col2) VALUES (val1, val2)");
        println!("  UPDATE table_name SET col1 = val1 [WHERE condition]");
        println!("  DELETE FROM table_name [WHERE condition]");
        println!("  CREATE TABLE table_name (col1 type1, col2 type2, ...)");
        println!("  DROP TABLE table_name");
        println!();
        println!("Utility commands:");
        println!("  help    - Show this help message");
        println!("  tables  - List all tables");
        println!("  stats   - Show database statistics");
        println!("  flush   - Manually flush all data to disk");
        println!("  exit    - Exit the database (automatically flushes data)");
        println!("  quit    - Same as exit");
    }
} 