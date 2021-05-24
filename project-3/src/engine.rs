use super::error::Result;

/// todo
pub trait KvsEngine {
    /// todo
    fn set(&mut self, key: String, vale: String) -> Result<Option<String>>;
    /// todo
    fn get(&mut self, key: String) -> Result<Option<String>>;
    /// todo
    fn remove(&mut self, key: String) -> Result<Option<String>>;
}