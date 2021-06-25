#![deny(missing_docs)]
//! KvStore 
//! a simple key-value store

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