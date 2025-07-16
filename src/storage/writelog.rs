use super::Record;
use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Write, BufReader, BufRead};

/// Write-ahead log for durability
pub struct WriteLog {
    log_file: BufWriter<File>,
    log_path: String,
}

impl WriteLog {
    pub fn new(log_path: &str) -> io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(log_path)?;
        
        Ok(Self {
            log_file: BufWriter::new(file),
            log_path: log_path.to_string(),
        })
    }

    pub fn log_insert(&mut self, record: &Record) -> io::Result<()> {
        use base64::{Engine as _, engine::general_purpose};
        let log_entry = format!("INSERT,{},{}\n", 
            record.id, 
            general_purpose::STANDARD.encode(&record.data)
        );
        self.log_file.write_all(log_entry.as_bytes())?;
        self.log_file.flush()?;
        Ok(())
    }

    pub fn log_update(&mut self, id: u64, new_data: &[u8]) -> io::Result<()> {
        use base64::{Engine as _, engine::general_purpose};
        let log_entry = format!("UPDATE,{},{}\n", 
            id, 
            general_purpose::STANDARD.encode(new_data)
        );
        self.log_file.write_all(log_entry.as_bytes())?;
        self.log_file.flush()?;
        Ok(())
    }

    pub fn log_delete(&mut self, id: u64) -> io::Result<()> {
        let log_entry = format!("DELETE,{}\n", id);
        self.log_file.write_all(log_entry.as_bytes())?;
        self.log_file.flush()?;
        Ok(())
    }

    pub fn replay(&self) -> io::Result<Vec<LogEntry>> {
        let file = File::open(&self.log_path)?;
        let reader = BufReader::new(file);
        let mut entries = Vec::new();

        for line in reader.lines() {
            let line = line?;
            if let Some(entry) = LogEntry::parse(&line) {
                entries.push(entry);
            }
        }

        Ok(entries)
    }

    pub fn clear(&mut self) -> io::Result<()> {
        // Truncate the log file
        let file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&self.log_path)?;
        self.log_file = BufWriter::new(file);
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub enum LogEntry {
    Insert(Record),
    Update { id: u64, data: Vec<u8> },
    Delete { id: u64 },
}

impl LogEntry {
    fn parse(line: &str) -> Option<Self> {
        use base64::{Engine as _, engine::general_purpose};
        let parts: Vec<&str> = line.split(',').collect();
        
        match parts.get(0).map(|s| *s)? {
            "INSERT" => {
                if parts.len() == 3 {
                    let id = parts[1].parse().ok()?;
                    let data = general_purpose::STANDARD.decode(parts[2]).ok()?;
                    Some(LogEntry::Insert(Record::new(id, data)))
                } else {
                    None
                }
            }
            "UPDATE" => {
                if parts.len() == 3 {
                    let id = parts[1].parse().ok()?;
                    let data = general_purpose::STANDARD.decode(parts[2]).ok()?;
                    Some(LogEntry::Update { id, data })
                } else {
                    None
                }
            }
            "DELETE" => {
                if parts.len() == 2 {
                    let id = parts[1].parse().ok()?;
                    Some(LogEntry::Delete { id })
                } else {
                    None
                }
            }
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test_writelog_basic_ops() {
        let temp_file = NamedTempFile::new().unwrap();
        let log_path = temp_file.path().to_str().unwrap();
        
        {
            let mut log = WriteLog::new(log_path).unwrap();
            let record = Record::new(1, vec![1, 2, 3]);
            
            log.log_insert(&record).unwrap();
            log.log_update(1, &[4, 5, 6]).unwrap();
            log.log_delete(1).unwrap();
        }
        
        // Test replay
        let log = WriteLog::new(log_path).unwrap();
        let entries = log.replay().unwrap();
        
        assert_eq!(entries.len(), 3);
        match &entries[0] {
            LogEntry::Insert(r) => {
                assert_eq!(r.id, 1);
                assert_eq!(r.data, vec![1, 2, 3]);
            }
            _ => panic!("Expected insert"),
        }
    }
} 