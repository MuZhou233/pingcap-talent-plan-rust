use std::fs::OpenOptions;
use std::io::prelude::*;
use serde::{Serialize, Deserialize};
use super::{Result, err_msg};

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
pub fn append(data: Cmd, path: &String) -> Result<()> {
    let data_string = ron::ser::to_string(&data)?;
    
    let file_options = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path);

    match file_options {
        Ok(mut file) => {
            file.write(data_string.as_bytes())?;
            file.write(b"\n")?;
            file.sync_all()?;
            Ok(())
        },
        Err(e) => Err(err_msg(e))
    }
}