use crate::query::QueryResult;

pub fn display_result(result: &QueryResult) {
    match result {
        QueryResult::Select(rows) => {
            if rows.is_empty() {
                println!("No results found");
                return;
            }

            if let Some(first_row) = rows.first() {
                for (_i, value) in first_row.iter().enumerate() {
                    print!("{} | ", value);
                }
                println!();
                println!("{}", "-".repeat(rows[0].len() * 12));

                for row in rows {
                    for value in row {
                        print!("{} | ", value);
                    }
                    println!();
                }
            }
        }
        QueryResult::Insert(count) => println!("Inserted {} rows", count),
        QueryResult::Update(count) => println!("Updated {} rows", count),
        QueryResult::Delete(count) => println!("Deleted {} rows", count),
        QueryResult::CreateTable => println!("Table created successfully"),
        QueryResult::DropTable => println!("Table dropped successfully"),
        QueryResult::Error(msg) => println!("Error: {}", msg),
    }
} 