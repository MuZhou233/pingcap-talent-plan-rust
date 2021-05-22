pub use failure::{Error, err_msg};
/// Custom Result type
pub type Result<T> = std::result::Result<T, Error>;