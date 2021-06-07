use std::{collections::HashMap, env::current_dir, fs::{OpenOptions, rename}, io::{BufRead, BufReader, Read, Seek, SeekFrom, Write}, path::PathBuf, sync::{Arc, Mutex}};
use std::path::Path;
use std::fs::File;
use serde::{Serialize, Deserialize};
use crate::error::*;
use crate::engine::KvsEngine;

/// KvStore struct contains a std HashMap
pub struct KvStore {
    store: Arc<Mutex<HashMap<String, u64>>>,
    file: Arc<Mutex<File>>,
    dir_path: PathBuf,
    allow_compact: bool
}

impl KvsEngine for KvStore {
    fn set(&self, key: String, value: String) -> Result<()> {
        let data = Cmd::set(key.clone(), value.clone());
        let position = self.append(data)?;
        {
            let mut store = self.store.lock().expect(
                "Can't lock store"
            );
            store.insert(key, position);
        }
        Ok(())
    }

    fn get(&self, key: String) -> Result<Option<String>> {
        let position = {
            let store = self.store.lock().expect(
                "Can't lock store"
            );
            if let Some(v) = store.get(&key) {
                v.clone()
            } else {
                return Ok(None)
            }
        };
        let mut file = self.file.lock().expect(
            "Can't lock file"
        );
        KvStore::get_from_file(&mut file, &position)
    }

    fn remove(&self, key: String) -> Result<()> {
        {
            let store = self.store.lock().expect(
                "Can't lock store"
            );
            if let None = store.get(&key) {
                return Err(err_msg("Key not found"))
            }
        }

        let data = Cmd::rm(key.clone());
        self.append(data)?;
        Ok(())
    }
}
impl Clone for KvStore {
    fn clone(&self) -> Self {
        KvStore{
            store: self.store.clone(),
            file: self.file.clone(),
            dir_path: self.dir_path.clone(),
            allow_compact: self.allow_compact.clone()
        }
    }
}

impl KvStore {
    /// return a new KvStore variable
    pub fn new() -> Result<Self> {
        Ok(KvStore::open(current_dir()?)?)
    }

    fn get_from_file(file: &mut File, position: &u64) -> Result<Option<String>> {
        let mut reader = BufReader::new(Read::by_ref(file));
        reader.seek(SeekFrom::Start(position.clone()))?;
        let mut line = String::new();
        reader.read_line(&mut line)?;
        
        let cmd: Cmd = ron::de::from_bytes(line.as_bytes())?;
        match &cmd {
            Cmd::Set { key, value } => {
                // println!("{}", value);
                Ok(Some(value.clone()))
            },
            _ => {
                Err(err_msg("position error"))
            }
        }
    }

    /// create new KvStore with given path
    pub fn open(dir_path: impl Into<PathBuf>) -> Result<KvStore> {
        KvStore::open_with_file_name(dir_path, "kvs.log".to_owned())
    }

    /// create new KvStore with path and file name
    pub fn open_with_file_name(dir_path: impl Into<PathBuf>, file_name: String) -> Result<KvStore> {
        let dir_path = dir_path.into();
        let file_path = dir_path.join(file_name);
        let store = KvStore::restore(&file_path)?;

        Ok(KvStore {
            store: Arc::new(Mutex::new(store)),
            file: KvStore::open_with_file_path(file_path)?,
            dir_path: dir_path.to_owned(),
            allow_compact: true
        })
    }

    fn open_with_file_path(file_path: impl Into<PathBuf>) -> Result<Arc<Mutex<File>>> {
        let file_path = file_path.into();
        let file_options = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(&file_path);

        match file_options {
            Ok(file) => {
                Ok(Arc::new(Mutex::new(file)))
            },
            Err(e) => Err(err_msg(e))
        }
    }

    fn append(&self, data: Cmd) -> Result<u64> {
        let data_string = ron::ser::to_string(&data)?;
        
        if self.allow_compact && self.dir_path.join("kvs.log").metadata()?.len() > 100000 {
            // self.compact()?;
        }
        let position = {
            let mut file = self.file.lock().expect(
                "Can't lock file"
            );

            let position = file.seek(SeekFrom::End(0))?;
            file.write(data_string.as_bytes())?;
            file.write(b"\n")?;
            file.sync_all()?;
            position
        };

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

    // fn compact(&mut self) -> Result<()> {
    //     let new_store = {
    //         let mut new = KvStore::open_with_file_name(self.dir_path.to_owned(), "kvs.log.log".to_owned())?;
    //         new.allow_compact = false;
    //         for (key, position) in &self.store {
    //             new.set(key.to_owned(), KvStore::get_from_file(&mut self.file, position).unwrap().unwrap())?;
    //         }
    //         new.store
    //     };
        
    //     rename(self.dir_path.join("kvs.log.log"), self.dir_path.join("kvs.log"))?;
        
    //     let file_path = self.dir_path.join("kvs.log");
    //     self.file = KvStore::open_with_file_path(file_path)?;
    //     self.store = new_store;

    //     Ok(())
    // }
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