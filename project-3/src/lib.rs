#![feature(try_trait)]
#![deny(missing_docs)]
//! KvStore example
//! ```rust
//! use kvs::KvStore;
//!
//! let mut store = KvStore::new();
//! store.set("key".to_owned(), "value".to_owned());
//! assert_eq!(store.get("key".to_owned()).unwrap(), Some("value".to_owned()));
//! ```
use std::{collections::HashMap, fs::{OpenOptions, rename}, io::{BufRead, BufReader, Read, Seek, SeekFrom, Write}, path::PathBuf};
use std::path::Path;
use std::fs::File;
use serde::{Serialize, Deserialize};
pub use failure::{Error, err_msg};
/// Custom Result type
pub type Result<T> = std::result::Result<T, Error>;

/// Used to store serialized data to file
#[derive(Serialize, Deserialize)]
pub struct Cmd {
    name: CmdName,
    key: String,
    value: Option<String>
}
/// Used by `Cmd`
#[derive(Serialize, Deserialize)]
pub enum CmdName {
    /// 
    Set,
    /// 
    Rm
}
impl Cmd {
    /// Easy to create
    pub fn new(n: CmdName, k: String, v: Option<String>) -> Self {
        return Cmd {
            name: n,
            key: k,
            value: v
        }
    }
}

/// KvsEngine
pub struct KvsEngine {}

/// KvStore struct contains a std HashMap
pub struct KvStore {
    store: HashMap<String, u64>,
    file: File,
    dir_path: PathBuf,
    allow_compact: bool
}

impl KvStore {
    /// return a new KvStore variable
    pub fn new() -> Self {
        KvStore::open(Path::new("")).unwrap()
    }

    /// this function package the HashMap::insert
    pub fn set(&mut self, key: String, value: String) -> Result<Option<u64>> {
        let data = Cmd::new(CmdName::Set, key.clone(), Some(value.clone()));
        let position = self.append(data)?;
        Ok(self.store.insert(key, position))
    }

    /// this function package the HashMap::get
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        match self.store.get(&key) {
            Some(position) => {
                KvStore::get_from_file(&mut self.file, position)
            },
            None => {
                println!("Key not found");
                Ok(None)
            }
        }
    }

    fn get_from_file(file: &mut File, position: &u64) -> Result<Option<String>> {
        let mut reader = BufReader::new(Read::by_ref(file));
        reader.seek(SeekFrom::Start(position.clone()))?;
        let mut line = String::new();
        reader.read_line(&mut line)?;
        
        let cmd: Cmd = ron::de::from_bytes(line.as_bytes())?;
        match &cmd.name {
            CmdName::Set => {
                let value = cmd.value.unwrap();
                println!("{}", value);
                Ok(Some(value))
            },
            _ => {
                Err(err_msg("position error"))
            }
        }
    }

    /// this function package the HashMap::remove
    pub fn remove(&mut self, key: String) -> Result<Option<u64>> {
        match self.store.get(&key) {
            Some(_) => {
                let data = Cmd::new(CmdName::Rm, key.clone(), None);
                self.append(data)?;
                Ok(self.store.remove(&key))
            }
            None => {
                let msg = "Key not found";
                println!("{}", msg);
                Err(err_msg(msg))
            }
        }
    }

    /// create new KvStore with given path
    pub fn open(dir_path: &Path) -> Result<KvStore> {
        KvStore::open_with_file_name(dir_path.to_owned(), "kvs.log".to_owned())
    }

    /// create new KvStore with path and file name
    pub fn open_with_file_name(dir_path: PathBuf, file_name: String) -> Result<KvStore> {
        let file_path = dir_path.join(file_name);
        let store = KvStore::restore(&file_path)?;

        Ok(KvStore {
            store: store,
            file: KvStore::open_with_file_path(file_path)?,
            dir_path: dir_path.to_owned(),
            allow_compact: true
        })
    }

    fn open_with_file_path(file_path: PathBuf) -> Result<File> {
        let file_options = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(&file_path);

        match file_options {
            Ok(file) => {
                Ok(file)
            },
            Err(e) => Err(err_msg(e))
        }
    }

    fn append(&mut self, data: Cmd) -> Result<u64> {
        let data_string = ron::ser::to_string(&data)?;
        
        if self.allow_compact && self.dir_path.join("kvs.log").metadata()?.len() > 100000 {
            self.compact()?;
        }
        let position = self.file.seek(SeekFrom::End(0))?;
        self.file.write(data_string.as_bytes())?;
        self.file.write(b"\n")?;
        self.file.sync_all()?;
        Ok(position)
    }

    fn restore(path: &Path) -> Result<HashMap<String, u64>> {
        let mut store: HashMap<String, u64> = HashMap::new();
        match path.metadata() {
            Ok(metadata) if metadata.is_file() => (),
            _ => return Ok(store)
        }

        let file = File::open(path)?;
        let mut reader = BufReader::new(file);
        let mut position = 0;
        
        loop{
            let mut line = String::new();
            match reader.read_line(&mut line) {
                Ok(n) if n > 1 => {
                    let cmd: Cmd = ron::de::from_bytes(line.as_bytes())?;
                    
                    match &cmd.name {
                        CmdName::Set => {
                            store.insert(cmd.key, position);
                        },
                        CmdName::Rm => {
                            store.remove(&cmd.key);
                        }
                    }

                    position = reader.seek(SeekFrom::Current(0))?;
                },
                _ => break
            }
        }

        Ok(store)
    }

    fn compact(&mut self) -> Result<()> {
        let new_store = {
            let mut new = KvStore::open_with_file_name(self.dir_path.to_owned(), "kvs.log.log".to_owned())?;
            new.allow_compact = false;
            for (key, position) in &self.store {
                new.set(key.to_owned(), KvStore::get_from_file(&mut self.file, position).unwrap().unwrap())?;
            }
            new.store
        };
        
        rename(self.dir_path.join("kvs.log.log"), self.dir_path.join("kvs.log"))?;
        
        let file_path = self.dir_path.join("kvs.log");
        self.file = KvStore::open_with_file_path(file_path)?;
        self.store = new_store;

        Ok(())
    }
}
