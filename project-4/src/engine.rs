use super::error::Result;

/// todo
pub trait KvsEngine {
    /// todo
    fn set(&self, key: String, vale: String) -> Result<Option<String>>;
    /// todo
    fn get(&self, key: String) -> Result<Option<String>>;
    /// todo
    fn remove(&self, key: String) -> Result<Option<String>>;
}