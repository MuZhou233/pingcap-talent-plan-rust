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
mod sledkv;
mod protocol;

pub use engine::KvsEngine;
pub use sledkv::SledKvsEngine;
pub use kv::KvStore;
pub use error::Result;
pub use protocol::{Protocol, Request, Response};