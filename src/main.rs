mod storage;
mod query;
mod cli;
mod metadata;
mod transaction;
mod index;
mod persistence_test;

use cli::CLI;
use std::env;

fn main() {
    let args: Vec<String> = env::args().collect();
    
    if args.len() > 1 && args[1] == "test-persistence" {
        println!("🧪 Running standalone persistence test...");
        persistence_test::run_persistence_test();
        return;
    }
    
    let mut cli = CLI::new();
    cli.run();
}