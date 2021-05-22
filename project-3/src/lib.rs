#![deny(missing_docs)]
//! KvStore example
//! ```rust
//! use kvs::KvStore;
//!
//! let mut store = KvStore::new();
//! store.set("key".to_owned(), "value".to_owned());
//! assert_eq!(store.get("key".to_owned()).unwrap(), Some("value".to_owned()));
//! ```

mod kv;
mod error;
mod engine;
mod sled;

pub use engine::KvsEngine;
pub use kv::KvStore;
pub use error::Result;
