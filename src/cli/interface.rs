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
                "exit" | "quit" => break,
                "help" => self.show_help(),
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

    fn show_help(&self) {
        println!("\nAvailable commands:");
        println!("  SELECT * FROM table_name [WHERE condition]");
        println!("  INSERT INTO table_name (col1, col2) VALUES (val1, val2)");
        println!("  UPDATE table_name SET col1 = val1 [WHERE condition]");
        println!("  DELETE FROM table_name [WHERE condition]");
        println!("  CREATE TABLE table_name (col1 type1, col2 type2, ...)");
        println!("  DROP TABLE table_name");
        println!("\nOther commands:");
        println!("  help    - Show this help message");
        println!("  exit    - Exit the program");
        println!("  quit    - Exit the program");
    }
} 