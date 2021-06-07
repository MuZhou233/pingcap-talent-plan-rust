use super::error::Result;

mod kvs;
mod sled;

pub use self::kvs::KvStore;
pub use self::sled::SledKvsEngine;

/// todo
pub trait KvsEngine: Clone + Send + 'static {
    /// todo
    fn set(&self, key: String, vale: String) -> Result<()>;
    /// todo
    fn get(&self, key: String) -> Result<Option<String>>;
    /// todo
    fn remove(&self, key: String) -> Result<()>;
}