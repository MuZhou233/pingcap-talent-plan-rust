use std::{env::current_dir, path::{Path, PathBuf}, sync::{Arc, Mutex}};
use sled;
use crate::error::*;
use crate::engine::KvsEngine;

/// todo
pub struct SledKvsEngine {
    store: Arc<Mutex<sled::Db>>
}

impl KvsEngine for SledKvsEngine {
    fn set(&self, key: String, value: String) -> Result<()> {
        let store = self.store.lock().expect(
            "Can't lock store"
        );
        store.insert(key, value.as_bytes())?;
        store.flush()?;
        Ok(())
    }
    fn get(&self, key: String) -> Result<Option<String>> {
        let store = self.store.lock().expect(
            "Can't lock store"
        );
        Ok(match store.get(key)? {
            Some(bytes) => {
                Some(std::str::from_utf8(&bytes)?.to_owned())
            },
            None => None,
        })
    }
    fn remove(&self, key: String) -> Result<()> {
        let store = self.store.lock().expect(
            "Can't lock store"
        );
        store.remove(key)?;
        store.flush()?;
        Ok(())
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
    /// return a new SledKvsEngine variable
    pub fn new() -> Result<Self> {
        Ok(SledKvsEngine::open(current_dir()?)?)
    }
    /// todo
    pub fn open(dir_path: impl Into<PathBuf>) -> Result<SledKvsEngine> {
        let dir_path = dir_path.into();
        Ok(SledKvsEngine{
            store: Arc::new(Mutex::new(sled::open(dir_path)?))
        })
    } 
}