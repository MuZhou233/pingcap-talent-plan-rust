#![feature(try_trait)]
#![deny(missing_docs)]
//! KvStore example
//! ```rust
//! use kvs::KvStore;
//!
//! let mut store = KvStore::new();
//! store.set("key".to_owned(), "value".to_owned());
//! assert_eq!(store.get("key".to_owned()), Some("value".to_owned()));
//! ```
use std::{collections::HashMap};
use std::path::Path;
mod error;
pub use error::{Result, ErrorKind};
/// todo
pub mod log;

/// KvStore struct contains a std HashMap
pub struct KvStore {
    store: HashMap<String, String>,
}

impl KvStore {
    /// return a new KvStore variable
    pub fn new() -> Self {
        KvStore {
            store: HashMap::new(),
        }
    }

    /// this function package the HashMap::insert
    pub fn set(&mut self, key: String, value: String) -> Result<Option<String>> {
        Ok(self.store.insert(key, value))
    }

    /// this function package the HashMap::get
    pub fn get(&self, key: String) -> Result<Option<String>> {
        match self.store.get(&key) {
            Some(value) => Ok(Some(value.clone().to_owned())),
            None => Ok(None),
        }
    }

    /// this function package the HashMap::remove
    pub fn remove(&mut self, key: String) -> Result<Option<String>> {
        Ok(self.store.remove(&key))
    }

    /// todo
    pub fn open(_path: &Path) -> Result<KvStore> {
        Err(ErrorKind::Unimplement)?
    }
}
