use std::path::PathBuf;
use sled;
use super::error::*;
use super::engine::KvsEngine;

/// todo
pub struct SledKvsEngine {
    store: sled::Db
}

impl KvsEngine for SledKvsEngine {
    fn set(&mut self, key: String, value: String) -> Result<Option<String>> {
        Ok(match self.store.insert(key, value.as_bytes())? {
            Some(bytes) => {
                Some(std::str::from_utf8(&bytes)?.to_owned())
            },
            None => None
        })
    }
    fn get(&mut self, key: String) -> Result<Option<String>> {
        Ok(match self.store.get(key)? {
            Some(bytes) => {
                Some(std::str::from_utf8(&bytes)?.to_owned())
            },
            None => None,
        })
    }
    fn remove(&mut self, key: String) -> Result<Option<String>> {
        match self.store.remove(key)? {
            Some(bytes) => {
                Ok(Some(std::str::from_utf8(&bytes)?.to_owned()))
            },
            None => Err(err_msg("Key not found")),
        }
    }
}
impl Clone for SledKvsEngine {
    fn clone(&self) -> Self {
        SledKvsEngine {
            store: self.store.clone()
        }
    }
}

impl SledKvsEngine {
    /// todo
    pub fn open(dir_path: impl Into<PathBuf>) -> Result<SledKvsEngine> {
        let dir_path = dir_path.into();
        Ok(SledKvsEngine{
            store: sled::open(dir_path)?
        })
    } 
}