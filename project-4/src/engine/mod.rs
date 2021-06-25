use crate::error::Result;

/// Trait for a key value store engine
pub trait KvsEngine: Clone + Send + 'static {
    /// Set the pair of key and value
    fn set(&self, key: String, vale: String) -> Result<()>;
    /// Get the value by key
    ///
    /// Return `None` if key does not exist
    fn get(&self, key: String) -> Result<Option<String>>;
    /// Remove the value by key
    ///
    /// # Errors
    ///
    /// Error with message `Key not found` will be retured if key does not exist 
    fn remove(&self, key: String) -> Result<()>;
}

mod kvs;
mod sled;

pub use self::kvs::KvStore;
pub use self::sled::SledKvsEngine;