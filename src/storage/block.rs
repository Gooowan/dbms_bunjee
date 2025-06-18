use serde::{Serialize, Deserialize};
use super::record::Record;
use bincode;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};

#[derive(Serialize, Deserialize, Clone, PartialEq)]
pub struct Block {
    records: Vec<Record>
}

impl Block{
    pub fn new() -> Self {
        Self {
            records: Vec::new()
        }
    }
    
    pub fn insert(&mut self, record: Record) -> bool {
        if self.get(record.id).is_some() {
            false;
        }
        self.records.push(record);
        true
    }
    
    pub fn get(&self, id: u64) -> Option<&Record>{
        self.records.iter().find(|&record| record.id == id)
    }

    pub fn get_all(&self) -> Vec<&Record>{
        self.records.iter().collect()
    }

    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    pub fn update(&mut self, id: u64, new_data: Vec<u8>) -> bool {
        if let Some(record) = self.records.iter_mut().find(|rec| rec.id == id) {
            record.data = new_data;
            true
        } else{
            false
        }
    }

    pub fn delete(&mut self, id: u64) -> bool {
        if let Some(record) = self.records.iter_mut().position(|rec|rec.id == id){
            self.records.remove(record);
            true
        } else {
            false
        }
    }

    pub fn count(&self) -> usize {
        self.records.len()
    }

    pub fn clear(&mut self){
        self.records.clear();
    }
    
    pub fn save_to_disk(&self, filename: &str) -> io::Result<()> {
        let encode = bincode::serialize(&self.records)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(filename)?;
        file.write_all(&encode)?;
        Ok(())
    }
    
    pub fn load_from_disk(filename: &str) -> io::Result<Self> {
        let mut file = File::open(filename)?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;
        let records: Vec<Record> = bincode::deserialize(&buffer)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        Ok(Self{records})
    }

    pub fn update_record(&mut self, record_id: u64, offset: usize, new_value: &[u8]) {
        if let Some(record) = self.records.iter_mut().find(|r| r.id == record_id) {
            record.data[offset..offset + new_value.len()].copy_from_slice(new_value);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_insert_get() {
        let mut block = Block::new();
        block.insert(Record::new(1, vec![1,2,3]));
        let result = block.get(1).unwrap();
        assert_eq!(result.data, vec![1,2,3]);
    }
    
    #[test]
    fn test_load_unload() {
        let mut block = Block::new();
        block.insert(Record::new(1, vec![1,2,3]));
        block.insert(Record::new(2, vec![1,2,3,4]));
        
        block.save_to_disk("block_test").unwrap();
        
        let loaded_block = Block::load_from_disk("block_test").unwrap();
        
        assert_eq!(block.records.len(), loaded_block.records.len());
        assert_eq!(block.get(1).unwrap().data, loaded_block.get(1).unwrap().data);
        assert_eq!(block.get(2).unwrap().data, loaded_block.get(2).unwrap().data);
    }

    #[test]
    fn test_update(){
        let mut block = Block::new();
        block.insert(Record::new(1, vec![1, 2, 3]));

        let updated = block.update(1, vec![4, 5, 6]);
        assert!(updated);

        let result = block.get(1).unwrap();
        assert_eq!(result.data, vec![4, 5, 6]);

        let non_existing_update = block.update(2, vec![4, 5, 6]);
        assert!(!non_existing_update);
    }

    #[test]
    fn test_remove(){
        let mut block = Block::new();
        block.insert(Record::new(1, vec![1, 2, 3]));
        block.insert(Record::new(2, vec![1, 2]));

        let deleted = block.delete(1);
        assert!(deleted);
        assert!(block.get(1).is_none());

        let non_existing_delete = block.delete(3);
        assert!(!non_existing_delete);

        assert!(block.get(2).is_some());
    }

    #[test]
    fn test_count(){
        let mut block = Block::new();
        block.insert(Record::new(1, vec![1, 2, 3]));
        block.insert(Record::new(2, vec![1, 2]));

        assert_eq!(block.count(), 2);

        block.delete(1);
        assert_eq!(block.count(), 1);

        block.delete(2);
        assert_eq!(block.count(), 0);
    }

    #[test]
    fn test_clear_empty(){
        let mut block = Block::new();
        block.insert(Record::new(1, vec![1, 2, 3]));
        block.insert(Record::new(2, vec![1, 2]));
        assert_eq!(block.count(), 2);

        block.clear();

        assert_eq!(block.count(), 0);
        assert!(block.get(1).is_none());
        assert!(block.get(2).is_none());
        assert!(block.is_empty())
    }

    #[test]
    fn test_get_all(){
        let mut block = Block::new();
        block.insert(Record::new(1, vec![1, 2, 3]));
        block.insert(Record::new(2, vec![1, 2]));

        let mut result = block.get_all();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].data, vec![1, 2, 3]);
        assert_eq!(result[1].data, vec![1, 2]);

        block.delete(1);
        result = block.get_all();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].data, vec![1, 2]);
    }

    #[test]
    fn test_clone(){
        let mut block = Block::new();
        block.insert(Record::new(1, vec![1, 2, 3]));
        block.insert(Record::new(2, vec![1, 2]));

        let clone_block = block.clone();
        
        assert_eq!(clone_block.count(), block.count());
        assert!(block == clone_block);

        for record in block.get_all(){
            let clone_record = clone_block.get(record.id).unwrap();
            assert_eq!(clone_record.data, record.data)
        }

        block.insert(Record::new(3, vec![4, 5, 6]));
        assert_ne!(clone_block.count(), block.count())
    }
}