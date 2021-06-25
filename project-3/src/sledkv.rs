use std::path::PathBuf;
use sled;
use super::error::*;
use super::engine::KvsEngine;

/// This package and implementation `sled` as one of the engines in this crate 
pub struct SledKvsEngine {
    store: sled::Db
}

impl KvsEngine for SledKvsEngine {
    fn set(&mut self, key: String, value: String) -> Result<()> {
        self.store.insert(key, value.as_bytes())?;
        self.store.flush()?;
        Ok(())
    }
    fn get(&mut self, key: String) -> Result<Option<String>> {
        Ok(self.store.get(key)?.and_then(|bytes| 
            std::str::from_utf8(&bytes).ok()
                .and_then(|s| Some(s.to_owned()))
        ))
    }
    fn remove(&mut self, key: String) -> Result<()> {
        self.store.remove(key)?.ok_or(err_msg("Key not found"))?;
        self.store.flush()?;
        Ok(())
    }
}

impl SledKvsEngine {
    /// Create a `SledKvsEngine` with given path
    pub fn open(dir_path: impl Into<PathBuf>) -> Result<SledKvsEngine> {
        let dir_path = dir_path.into();
        Ok(SledKvsEngine{
            store: sled::open(dir_path)?
        })
    } 
}