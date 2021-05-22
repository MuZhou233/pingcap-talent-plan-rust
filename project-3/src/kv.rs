use std::{collections::HashMap, fs::{OpenOptions, rename}, io::{BufRead, BufReader, Read, Seek, SeekFrom, Write}, path::PathBuf};
use std::path::Path;
use std::fs::File;
use serde::{Serialize, Deserialize};
use super::error::*;
use super::engine::KvsEngine;

/// KvStore struct contains a std HashMap
pub struct KvStore {
    store: HashMap<String, u64>,
    file: File,
    dir_path: PathBuf,
    allow_compact: bool
}

impl KvsEngine for KvStore {
    fn set(&mut self, key: String, value: String) -> Result<()> {
        let data = Cmd::set(key.clone(), value.clone());
        let position = self.append(data)?;
        Ok(())
    }

    fn get(&mut self, key: String) -> Result<Option<String>> {
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

    fn remove(&mut self, key: String) -> Result<()> {
        match self.store.get(&key) {
            Some(_) => {
                let data = Cmd::rm(key.clone());
                self.append(data)?;
                Ok(())
            }
            None => {
                let msg = "Key not found";
                println!("{}", msg);
                Err(err_msg(msg))
            }
        }
    }
}

impl KvStore {
    /// return a new KvStore variable
    pub fn new() -> Self {
        KvStore::open(Path::new("")).unwrap()
    }

    fn get_from_file(file: &mut File, position: &u64) -> Result<Option<String>> {
        let mut reader = BufReader::new(Read::by_ref(file));
        reader.seek(SeekFrom::Start(position.clone()))?;
        let mut line = String::new();
        reader.read_line(&mut line)?;
        
        let cmd: Cmd = ron::de::from_bytes(line.as_bytes())?;
        match &cmd {
            Cmd::Set { key, value } => {
                println!("{}", value);
                Ok(Some(value.clone()))
            },
            _ => {
                Err(err_msg("position error"))
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
                    
                    match cmd {
                        Cmd::Set { key, value } => {
                            store.insert(key, position);
                        },
                        Cmd::Rm { key } => {
                            store.remove(&key);
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

/// Use to store serialized data to file
#[derive(Serialize, Deserialize)]
enum Cmd {
    Set {
        key: String,
        value: String
    },
    Rm {
        key: String
    }
}

impl Cmd {
    /// Easy to create
    pub fn set(k: String, v: String) -> Self {
        return Cmd::Set {
            key: k,
            value: v
        }
    }

    /// Easy to create
    pub fn rm(k: String) -> Self {
        return Cmd::Rm {
            key: k
        }
    }
}