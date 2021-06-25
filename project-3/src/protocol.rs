use std::io::{Read, Write};
use serde::{Serialize, Deserialize};
use crate::error::Result;

const VERSION: &str = "0.2";

/// Protocol used by server and client
///
/// This provides the structure of message and function to send and receive message
#[derive(Serialize, Deserialize, Debug)]
pub struct Protocol<T: ProtocolPayload> {
    /// Set private to prevent wrong version of protocol lib
    version: String,
    /// Protocol payload
    pub payload: T
}

/// This identify available payloads
pub trait ProtocolPayload {}

impl<'a, T: ProtocolPayload + Serialize + Deserialize<'a>> Protocol<T> {
    /// Create `Protocol` with given payload
    pub fn new(data: T) -> Self {
        Protocol {
            version: VERSION.to_owned(),
            payload: data
        }
    }

    /// Return protocol version
    pub fn version(&self) -> &str {
        &self.version
    } 

    /// Listen on reader and call handler when every single message has received
    ///
    /// Listen will keep when handler returns `Ok(false)`
    /// You could think that as "Ok? really?", so you return "Ok, but one more" to keep listening
    pub fn listen<R: Read, F>(reader: &mut R, mut handler: F) -> Result<()> 
      where F: FnMut(Protocol<T>) -> Result<bool> {
        let mut stream = serde_json::Deserializer::from_reader(reader).into_iter();
            
        while let Some(cmd) = stream.next() {
            if handler(cmd?)? {
                break;
            }
        }

        Ok(())
    }

    /// Send message to writer with given payload
    pub fn send<W: Write>(writer: &mut W, data: Self) -> Result<()> {
        serde_json::ser::to_writer(writer, &data)?;
        Ok(())
    }
}

/// Payload send from client to server
#[derive(Serialize, Deserialize, Debug)]
pub enum Request {
    /// Response should be `Pong` with same number
    Ping(i8),
    /// Both shutdown connection
    Shutdown,
    /// set key to value
    Set {
        /// 
        key: String,
        /// 
        value: String
    },
    /// get value by key
    Get {
        /// 
        key: String
    },
    /// rm value by key
    Rm {
        /// 
        key: String
    }
}
impl ProtocolPayload for Request {}

/// Payload send from server to client
#[derive(Serialize, Deserialize, Debug)]
pub enum Response {
    /// Send when received a `Ping`
    Pong(i8),
    /// Both shutdown connection
    Shutdown,
    /// Request command success, response with String when needed
    Success {
        /// 
        value: Option<String>
    },
    /// Request command failed, response with error message
    Error {
        /// 
        msg: String
    }
}
impl ProtocolPayload for Response {}