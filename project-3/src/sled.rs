use sled;
use super::error::*;
use super::engine::KvsEngine;

pub struct SledKvsEngine {
    store: sled::Db
}

impl KvsEngine for SledKvsEngine {
    fn set(&mut self, key: String, value: String) -> Result<()> {
        self.store.insert(key.as_bytes(), value.as_bytes())?;
        Ok(())
    }
    fn get(&mut self, key: String) -> Result<Option<String>> {
        Ok(match self.store.get(key)? {
            Some(bytes) => {
                Some(std::str::from_utf8(&bytes)?.to_owned())
            },
            None => None,
        })
    }
    fn remove(&mut self, key: String) -> Result<()> {
        self.store.remove(key)?;
        Ok(())
    }
}