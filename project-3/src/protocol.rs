use serde::{Serialize, Deserialize};

const VERSION: &str = "0.1";

/// todo
#[derive(Serialize, Deserialize, Debug)]
pub struct Protocol<T: ProtocolPayload> {
    version: String,
    /// todo
    pub payload: T
}

pub trait ProtocolPayload {}

impl<T: ProtocolPayload> Protocol<T> {
    /// todo
    pub fn new(data: T) -> Self {
        Protocol {
            version: VERSION.to_owned(),
            payload: data
        }
    }
}

/// todo
#[derive(Serialize, Deserialize, Debug)]
pub enum Request {
    /// todo
    Ping(i8),
    /// todo
    Shutdown(i8),
    /// todo
    Set {
        /// todo
        key: String,
        /// todo
        value: String
    },
    /// todo
    Get {
        /// todo
        key: String
    },
    /// todo
    Rm {
        /// todo
        key: String
    }
}
impl ProtocolPayload for Request {}

/// todo
#[derive(Serialize, Deserialize, Debug)]
pub enum Response {
    /// todo
    Pong(i8),
    /// todo
    Shutdown(i8),
    /// todo
    Success {
        /// todo
        value: Option<String>
    },
    /// todo
    Error {
        /// todo
        msg: String
    }
}
impl ProtocolPayload for Response {}