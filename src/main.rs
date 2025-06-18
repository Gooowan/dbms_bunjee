mod storage;
mod query;
mod cli;
mod metadata;
mod transaction;
mod index;

use cli::CLI;
use dms_try1::query::engine::QueryEngine;

fn main() {
    let mut cli = CLI::new();
    cli.run();

    let mut engine = QueryEngine::new();

    // Example 1: Create a simple table with basic columns
    let create_table_query = "CREATE TABLE users (
        id INTEGER PRIMARY KEY,
        name STRING(50) NOT NULL,
        age INTEGER,
        email STRING(100) UNIQUE
    )";
    
    match engine.execute(create_table_query) {
        Ok(result) => println!("Table created successfully: {:?}", result),
        Err(e) => println!("Error creating table: {}", e),
    }

    // Example 2: Create a table with more complex constraints
    let create_orders_query = "CREATE TABLE orders (
        order_id INTEGER PRIMARY KEY,
        customer_id INTEGER NOT NULL,
        order_date TIMESTAMP NOT NULL,
        total_amount FLOAT NOT NULL,
        status STRING(20) DEFAULT 'pending'
    )";
    
    match engine.execute(create_orders_query) {
        Ok(result) => println!("Orders table created successfully: {:?}", result),
        Err(e) => println!("Error creating orders table: {}", e),
    }
}