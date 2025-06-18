use bincode;
use crate::storage::block::Block;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::collections::HashMap;

pub struct Table {
    name: String,
    blocks: Vec<Block>,
    index: HashMap<u64, usize>
}

impl Table {
    pub fn new(name: String) -> Self {
        Self {
            name,
            blocks: Vec::new(),
            index: HashMap::new()
        }
    }

    pub fn add_block(&mut self, block: Block) {
        let block_index = self.blocks.len();
        for record in block.get_all() {
            self.index.insert(record.id, block_index);
        }
        self.blocks.push(block);
    }

    pub fn get_block(&self, index: usize) -> Option<&Block> {
        self.blocks.get(index)
    }

    pub fn save_to_disk(&self, filename: &str) -> io::Result<()> {
        let encoded = bincode::serialize(&self.blocks)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        let mut file = OpenOptions::new().write(true).create(true).truncate(true).open(filename)?;
        file.write_all(&encoded)?;
        Ok(())
    }

    pub fn load_from_disk(name: String, filename: &str) -> io::Result<Self> {
        let mut file = File::open(filename)?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;
        
        let blocks: Vec<Block> = bincode::deserialize(&buffer)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        
        let mut index = HashMap::new();
        for (i, block) in blocks.iter().enumerate() {
            for record in block.get_all(){
                index.insert(record.id, i);
            }
        }
        
        Ok(Self { name, blocks, index })
    }
}