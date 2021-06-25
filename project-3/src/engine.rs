use super::error::Result;

/// Trait for a key value store engine
pub trait KvsEngine {
    /// Set the pair of key and value
    fn set(&mut self, key: String, vale: String) -> Result<()>;
    /// Get the value by key
    ///
    /// Return `None` if key does not exist
    fn get(&mut self, key: String) -> Result<Option<String>>;
    /// Remove the value by key
    ///
    /// # Errors
    ///
    /// Error with message `Key not found` will be retured if key does not exist 
    fn remove(&mut self, key: String) -> Result<()>;
}