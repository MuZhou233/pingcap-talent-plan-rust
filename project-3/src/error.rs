pub use failure::{Error, err_msg};
/// Result type for kvs
pub type Result<T> = std::result::Result<T, Error>;