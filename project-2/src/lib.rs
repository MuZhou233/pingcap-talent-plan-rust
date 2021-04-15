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
// mod error;
// pub use error::{Result, ErrorKind};
pub use failure::{Error, err_msg};
/// todo
pub type Result<T> = std::result::Result<T, Error>;
mod log;
use log::{Cmd, CmdName};

/// KvStore struct contains a std HashMap
pub struct KvStore {
    store: HashMap<String, String>,
    path: String
}

impl KvStore {
    /// return a new KvStore variable
    pub fn new() -> Self {
        KvStore {
            store: HashMap::new(),
            path: "kvs.log".to_owned()
        }
    }

    /// this function package the HashMap::insert
    pub fn set(&mut self, key: String, value: String) -> Result<Option<String>> {
        let data = Cmd::new(CmdName::Set, key.clone(), value.clone());
        log::append(data, &self.path)?;
        Ok(self.store.insert(key, value))
    }

    /// this function package the HashMap::get
    pub fn get(&self, key: String) -> Result<Option<String>> {
        Err(err_msg("Unimplemented"))
    }

    /// this function package the HashMap::remove
    pub fn remove(&mut self, key: String) -> Result<Option<String>> {
        let data = Cmd::new(CmdName::Rm, key.clone(), String::new());
        log::append(data, &self.path)?;
        Ok(self.store.remove(&key))
    }

    /// todo
    pub fn open(_path: &Path) -> Result<KvStore> {
        Err(err_msg("Unimplemented"))
    }
}
