# 🗄️ BUNJEE DMS (Database Management System)

<div align="center">

![Rust](https://img.shields.io/badge/language-Rust-orange.svg)
![License](https://img.shields.io/badge/license-MIT-blue.svg)
![Version](https://img.shields.io/badge/version-0.1.0-green.svg)
![Status](https://img.shields.io/badge/status-Active-brightgreen.svg)

*A modern, high-performance database management system built with Rust*

[Features](#-features) • [Installation](#-installation) • [Quick Start](#-quick-start) • [Documentation](#-documentation) • [Examples](#-examples)

</div>

---

## 🚀 Overview

**BUNJEE DMS** is a lightweight yet powerful database management system implemented in Rust, featuring an LSM-tree storage engine with full SQL support. Designed for performance, reliability, and ease of use.

### ✨ Key Highlights

- 🔥 **High Performance**: LSM-tree based storage engine optimized for write-heavy workloads
- 🛡️ **Memory Safe**: Built with Rust for guaranteed memory safety and zero-cost abstractions
- 💾 **Persistent Storage**: Data persists between sessions with automatic recovery
- 🔄 **Transaction Support**: ACID-compliant transactions for data integrity
- 📊 **Rich SQL**: Comprehensive SQL support including JOINs, aggregations, and complex queries
- 🖥️ **Interactive CLI**: User-friendly command-line interface with helpful prompts

---

## 🎯 Features

### 📝 SQL Operations

| Feature | Status | Description |
|---------|--------|-------------|
| **CREATE TABLE** | ✅ | Create tables with typed columns |
| **INSERT** | ✅ | Insert single and multiple records |
| **SELECT** | ✅ | Query data with WHERE conditions |
| **UPDATE** | ✅ | Modify existing records |
| **DELETE** | ✅ | Remove records with conditions |
| **JOIN** | ✅ | INNER JOIN support for multi-table queries |
| **GROUP BY** | ✅ | Group results for aggregation |
| **Aggregation** | ✅ | COUNT, SUM, AVG, MIN, MAX functions |

### 🗂️ Data Types

- **INTEGER** - 64-bit signed integers
- **FLOAT** - 64-bit floating point numbers
- **VARCHAR(n)** - Variable-length strings with max length
- **BOOLEAN** - True/false values
- **TIMESTAMP** - Unix timestamp values

### 🏗️ Storage Engine

- **LSM Tree Architecture**: Optimized for write performance
- **SSTables**: Sorted string tables for efficient storage
- **Write-Ahead Logging**: Ensures data durability
- **Automatic Compaction**: Background maintenance for optimal performance
- **Block-based Storage**: Efficient memory utilization

---

## 🛠️ Installation

### Prerequisites

- **Rust 1.70+** (install from [rustup.rs](https://rustup.rs/))
- **Git** for cloning the repository

### Build from Source

```bash
# Clone the repository
git clone https://github.com/your-username/dms_try1.git
cd dms_try1

# Build the project
cargo build --release

# Run the database
cargo run
```

### Development Setup

```bash
# Install development dependencies
cargo install --path .

# Run tests
cargo test

# Check code formatting
cargo fmt --check

# Run clippy for linting
cargo clippy -- -D warnings
```

---

## 🚀 Quick Start

### 1. Start the Database

```bash
cargo run
```

You'll see the interactive CLI:

```
Welcome to BUNJEE DBMS CLI!
Type 'exit' or 'quit' to exit
Type 'help' for available commands
Type 'flush' to manually flush all data to disk
dbms> 
```

### 2. Create Your First Table

```sql
CREATE TABLE users (id INTEGER, name VARCHAR(50), age INTEGER, email VARCHAR(100))
```

### 3. Insert Data

```sql
INSERT INTO users VALUES (1, 'Alice Johnson', 28, 'alice@email.com')
INSERT INTO users VALUES (2, 'Bob Smith', 35, 'bob@email.com')
```

### 4. Query Data

```sql
SELECT * FROM users
SELECT name, age FROM users WHERE age > 30
```

### 5. Advanced Queries

```sql
-- Aggregation
SELECT COUNT(*) FROM users
SELECT AVG(age) FROM users

-- Group By
SELECT department, AVG(salary) FROM employees GROUP BY department

-- Joins
SELECT users.name, orders.total_price 
FROM users INNER JOIN orders ON users.id = orders.user_id
```

---

## 📖 Documentation

### CLI Commands

| Command | Description |
|---------|-------------|
| `help` | Show available commands |
| `tables` | List all tables |
| `stats` | Show database statistics |
| `flush` | Manually flush data to disk |
| `exit` / `quit` | Exit the database |

### SQL Syntax

#### CREATE TABLE
```sql
CREATE TABLE table_name (
    column1 TYPE,
    column2 TYPE,
    ...
)
```

#### INSERT
```sql
INSERT INTO table_name VALUES (value1, value2, ...)
INSERT INTO table_name (col1, col2) VALUES (val1, val2)
```

#### SELECT
```sql
SELECT * FROM table_name
SELECT col1, col2 FROM table_name WHERE condition
SELECT col1, COUNT(*) FROM table_name GROUP BY col1
```

#### UPDATE
```sql
UPDATE table_name SET column = value WHERE condition
```

#### DELETE
```sql
DELETE FROM table_name WHERE condition
```

#### JOIN
```sql
SELECT t1.col1, t2.col2 
FROM table1 t1 
INNER JOIN table2 t2 ON t1.id = t2.foreign_id
```

### Performance Tuning

- **Memory Management**: The LSM engine automatically manages memory with configurable thresholds
- **Compaction**: Background compaction keeps storage optimal
- **Batch Operations**: Use transactions for bulk operations
- **Indexing**: Primary key indexing is automatic, secondary indexes planned

---

## 💡 Examples

### E-commerce Database Example

```sql
-- Create tables
CREATE TABLE users (id INTEGER, name VARCHAR(50), age INTEGER, email VARCHAR(100))
CREATE TABLE products (id INTEGER, name VARCHAR(30), price INTEGER, category VARCHAR(20))
CREATE TABLE orders (id INTEGER, user_id INTEGER, product_id INTEGER, quantity INTEGER, total_price INTEGER)

-- Insert sample data
INSERT INTO users VALUES (1, 'Alice Johnson', 28, 'alice@email.com')
INSERT INTO users VALUES (2, 'Bob Smith', 35, 'bob@email.com')

INSERT INTO products VALUES (1, 'Laptop', 1200, 'Electronics')
INSERT INTO products VALUES (2, 'Phone', 800, 'Electronics')

INSERT INTO orders VALUES (1, 1, 1, 1, 1200)
INSERT INTO orders VALUES (2, 2, 2, 2, 1600)

-- Complex queries
-- Find all orders with user and product details
SELECT users.name, products.name, orders.quantity, orders.total_price
FROM users 
INNER JOIN orders ON users.id = orders.user_id 
INNER JOIN products ON orders.product_id = products.id

-- Calculate total sales by category
SELECT products.category, SUM(orders.total_price) as total_sales
FROM products 
INNER JOIN orders ON products.id = orders.product_id 
GROUP BY products.category

-- Find users who spent more than $1000
SELECT users.name, SUM(orders.total_price) as total_spent
FROM users 
INNER JOIN orders ON users.id = orders.user_id 
GROUP BY users.name, users.id
HAVING SUM(orders.total_price) > 1000
```

### Company Employee Analysis

```sql
-- Create company structure
CREATE TABLE companies (id INTEGER, name VARCHAR(40), industry VARCHAR(30), revenue INTEGER)
CREATE TABLE employees (id INTEGER, name VARCHAR(50), company_id INTEGER, salary INTEGER, department VARCHAR(30))

-- Insert data
INSERT INTO companies VALUES (1, 'TechCorp', 'Technology', 5000000)
INSERT INTO companies VALUES (2, 'DataSoft', 'Software', 3200000)

INSERT INTO employees VALUES (1, 'Sarah Connor', 1, 85000, 'Engineering')
INSERT INTO employees VALUES (2, 'John Doe', 1, 75000, 'Engineering')
INSERT INTO employees VALUES (3, 'Jane Smith', 2, 95000, 'Engineering')

-- Analytics queries
-- Average salary by company
SELECT companies.name, AVG(employees.salary) as avg_salary
FROM companies 
INNER JOIN employees ON companies.id = employees.company_id 
GROUP BY companies.name

-- Top earners
SELECT name, salary FROM employees WHERE salary > 80000 ORDER BY salary DESC

-- Department statistics
SELECT department, COUNT(*) as employee_count, AVG(salary) as avg_salary
FROM employees 
GROUP BY department
```

### Data Analysis & Reporting

```sql
-- Complex aggregation with filtering
SELECT 
    products.category,
    COUNT(*) as total_orders,
    SUM(orders.quantity) as total_quantity,
    AVG(orders.total_price) as avg_order_value,
    MAX(orders.total_price) as highest_order
FROM products 
INNER JOIN orders ON products.id = orders.product_id 
WHERE orders.total_price > 100
GROUP BY products.category

-- Time-based analysis (if using timestamps)
SELECT 
    DATE(order_date) as date,
    COUNT(*) as daily_orders,
    SUM(total_price) as daily_revenue
FROM orders 
WHERE order_date >= '2024-01-01'
GROUP BY DATE(order_date)
ORDER BY date
```

---

## 🔧 Architecture

### System Components

```
┌─────────────────┐
│   CLI Interface │
├─────────────────┤
│  Query Engine   │
├─────────────────┤
│   SQL Parser    │
├─────────────────┤
│ Storage Engine  │
│   (LSM Tree)    │
├─────────────────┤
│  Persistence    │
│   (SSTables)    │
└─────────────────┘
```

### Data Flow

1. **Input**: SQL commands via CLI
2. **Parsing**: Query parser validates and transforms SQL
3. **Execution**: Query engine executes against storage
4. **Storage**: LSM engine manages data in memory and disk
5. **Output**: Results formatted and displayed

---

## 🤝 Contributing

We welcome contributions! Here's how you can help:

1. **Fork** the repository
2. **Create** a feature branch (`git checkout -b feature/amazing-feature`)
3. **Commit** your changes (`git commit -m 'Add amazing feature'`)
4. **Push** to the branch (`git push origin feature/amazing-feature`)
5. **Open** a Pull Request

### Development Guidelines

- Follow Rust formatting standards (`cargo fmt`)
- Add tests for new features
- Update documentation as needed
- Ensure all tests pass (`cargo test`)

---

## 📊 Performance

GREAT 

- **🚀 Write Optimized**: LSM-tree architecture provides excellent write performance
- **⚡ Fast Reads**: Efficient block-based storage with minimal I/O overhead  
- **📈 Scalable**: Performance scales linearly with additional CPU cores
- **💾 Memory Efficient**: Configurable memory usage with automatic flushing

---

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## 🙏 Acknowledgments

- **RocksDB** - Inspiration for LSM tree implementation
- **SQLite** - SQL syntax and semantics reference
- **Rust Community** - Amazing ecosystem and tools

---

## 🧪 Quick Test Commands

### Complete Test Suite

Copy and paste these commands to quickly test all database features:

<details>
<summary><b>📋 Click to expand all test commands</b></summary>

```sql
-- 🏗️ Create Tables
CREATE TABLE users (id INTEGER, name VARCHAR(50), age INTEGER, email VARCHAR(100))
CREATE TABLE products (id INTEGER, name VARCHAR(30), price INTEGER, category VARCHAR(20))
CREATE TABLE orders (id INTEGER, user_id INTEGER, product_id INTEGER, quantity INTEGER, total_price INTEGER)
CREATE TABLE companies (id INTEGER, name VARCHAR(40), industry VARCHAR(30), revenue INTEGER)
CREATE TABLE employees (id INTEGER, name VARCHAR(50), company_id INTEGER, salary INTEGER, department VARCHAR(30))

-- 👥 Insert Users
INSERT INTO users VALUES (1, 'Alice Johnson', 28, 'alice@email.com')
INSERT INTO users VALUES (2, 'Bob Smith', 35, 'bob@email.com')
INSERT INTO users VALUES (3, 'Charlie Brown', 22, 'charlie@email.com')
INSERT INTO users VALUES (4, 'Diana Wilson', 31, 'diana@email.com')
INSERT INTO users VALUES (5, 'Eve Davis', 27, 'eve@email.com')

-- 🛍️ Insert Products
INSERT INTO products VALUES (1, 'Laptop', 1200, 'Electronics')
INSERT INTO products VALUES (2, 'Phone', 800, 'Electronics')
INSERT INTO products VALUES (3, 'Book', 25, 'Education')
INSERT INTO products VALUES (4, 'Headphones', 150, 'Electronics')
INSERT INTO products VALUES (5, 'Desk Chair', 300, 'Furniture')

-- 🛒 Insert Orders
INSERT INTO orders VALUES (1, 1, 1, 1, 1200)
INSERT INTO orders VALUES (2, 2, 2, 2, 1600)
INSERT INTO orders VALUES (3, 3, 3, 5, 125)
INSERT INTO orders VALUES (4, 1, 4, 1, 150)
INSERT INTO orders VALUES (5, 4, 5, 1, 300)

-- 🏢 Insert Companies
INSERT INTO companies VALUES (1, 'TechCorp', 'Technology', 5000000)
INSERT INTO companies VALUES (2, 'DataSoft', 'Software', 3200000)
INSERT INTO companies VALUES (3, 'CloudSystems', 'Cloud', 7500000)

-- 👨‍💼 Insert Employees
INSERT INTO employees VALUES (1, 'Sarah Connor', 1, 85000, 'Engineering')
INSERT INTO employees VALUES (2, 'John Doe', 1, 75000, 'Engineering')
INSERT INTO employees VALUES (3, 'Jane Smith', 2, 95000, 'Engineering')
INSERT INTO employees VALUES (4, 'Mike Johnson', 2, 70000, 'Marketing')
INSERT INTO employees VALUES (5, 'Lisa Brown', 3, 120000, 'Engineering')

-- 🔍 Basic SELECT Queries
SELECT * FROM users
SELECT name, age FROM users
SELECT * FROM products WHERE category = 'Electronics'
SELECT * FROM users WHERE age > 30
SELECT name, price FROM products WHERE price < 200

-- 📊 Aggregation Queries
SELECT COUNT(*) FROM users
SELECT SUM(price) FROM products
SELECT AVG(age) FROM users
SELECT MIN(price) FROM products
SELECT MAX(salary) FROM employees

-- 📈 GROUP BY Queries
SELECT category, COUNT(*) FROM products GROUP BY category
SELECT category, AVG(price) FROM products GROUP BY category
SELECT department, AVG(salary) FROM employees GROUP BY department
SELECT company_id, COUNT(*) FROM employees GROUP BY company_id

-- 🔗 JOIN Queries
SELECT users.name, orders.total_price FROM users INNER JOIN orders ON users.id = orders.user_id
SELECT employees.name, companies.name FROM employees INNER JOIN companies ON employees.company_id = companies.id
SELECT companies.name, AVG(employees.salary) FROM companies INNER JOIN employees ON companies.id = employees.company_id GROUP BY companies.name

-- ✏️ UPDATE Operations
UPDATE users SET age = 29 WHERE id = 1
UPDATE products SET price = 1300 WHERE id = 1
UPDATE employees SET salary = 90000 WHERE id = 2

-- 🗑️ DELETE Operations
DELETE FROM orders WHERE quantity = 1
DELETE FROM products WHERE price < 30

-- 🔧 Management Commands
tables
stats
flush
```

</details>

### Step-by-Step Testing

#### 1. Create and Populate Basic Tables
```sql
CREATE TABLE users (id INTEGER, name VARCHAR(50), age INTEGER, email VARCHAR(100))
INSERT INTO users VALUES (1, 'Alice Johnson', 28, 'alice@email.com')
INSERT INTO users VALUES (2, 'Bob Smith', 35, 'bob@email.com')
SELECT * FROM users
```

#### 2. Test Aggregation
```sql
CREATE TABLE products (id INTEGER, name VARCHAR(30), price INTEGER, category VARCHAR(20))
INSERT INTO products VALUES (1, 'Laptop', 1200, 'Electronics')
INSERT INTO products VALUES (2, 'Phone', 800, 'Electronics')
INSERT INTO products VALUES (3, 'Book', 25, 'Education')

SELECT category, COUNT(*) FROM products GROUP BY category
SELECT AVG(price) FROM products WHERE category = 'Electronics'
```

#### 3. Test JOINs
```sql
CREATE TABLE orders (id INTEGER, user_id INTEGER, product_id INTEGER, quantity INTEGER, total_price INTEGER)
INSERT INTO orders VALUES (1, 1, 1, 1, 1200)
INSERT INTO orders VALUES (2, 2, 2, 2, 1600)

SELECT users.name, products.name, orders.quantity
FROM users 
INNER JOIN orders ON users.id = orders.user_id 
INNER JOIN products ON orders.product_id = products.id
```

#### 4. Test Complex Analytics
```sql
-- Average salary by company
CREATE TABLE companies (id INTEGER, name VARCHAR(40), industry VARCHAR(30), revenue INTEGER)
CREATE TABLE employees (id INTEGER, name VARCHAR(50), company_id INTEGER, salary INTEGER, department VARCHAR(30))

INSERT INTO companies VALUES (1, 'TechCorp', 'Technology', 5000000)
INSERT INTO employees VALUES (1, 'Sarah Connor', 1, 85000, 'Engineering')
INSERT INTO employees VALUES (2, 'John Doe', 1, 75000, 'Engineering')

SELECT companies.name, AVG(employees.salary) 
FROM companies INNER JOIN employees ON companies.id = employees.company_id 
GROUP BY companies.name
```

---

<div align="center">

**[⬆ Back to Top](#-bunjee-dms-database-management-system)**

Made with ❤️ by **BUNJEE**

</div> 