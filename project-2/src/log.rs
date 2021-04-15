use std::fs::File;
use std::io::prelude::*;
use serde::{Serialize, Deserialize};
use super::error::Result;

/// todo
#[derive(Serialize, Deserialize)]
pub struct Cmd {
    name: CmdName,
    key: String,
    value: String
}
/// todo
#[derive(Serialize, Deserialize)]
pub enum CmdName {
    /// todo
    Set,
    /// todo
    Get,
    /// todo
    Rm
}
impl Cmd {
    /// todo
    pub fn new(n: CmdName, k: String, v: String) -> Self {
        return Cmd {
            name: n,
            key: k,
            value: v
        }
    }
}

/// todo
pub fn append(data: Cmd, path: String) -> Result<()> {
    File::create(&path)?;
    let mut file = File::open(path)?;
    let data_string = ron::ser::to_string(&data)?;
    file.write_all(data_string.as_bytes())?;
    file.write(b"\n")?;
    file.sync_all()?;
    Ok(())
}