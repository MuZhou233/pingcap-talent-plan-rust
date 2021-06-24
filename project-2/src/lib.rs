#![deny(missing_docs)]
//! KvStore example
//! ```rust
//! use kvs::KvStore;
//!
//! let mut store = KvStore::new();
//! store.set("key".to_owned(), "value".to_owned());
//! assert_eq!(store.get("key".to_owned()).unwrap(), Some("value".to_owned()));
//! ```
use std::{collections::{BTreeMap, HashMap}, fs::{self, OpenOptions, rename}, io::{self, BufRead, BufReader, BufWriter, Read, Seek, SeekFrom, Write}, ops::Range, path::PathBuf};
use std::path::Path;
use std::fs::File;
use serde::{Serialize, Deserialize};
pub use failure::{Error, err_msg};
/// Custom Result type
pub type Result<T> = std::result::Result<T, Error>;

const COMPACTION_THRESHOLD: u64 = 1024 * 1024;

/// KvStore struct contains a std HashMap
pub struct KvStore {
    index: BTreeMap<String, CmdPos>,
    writer: CmdWriter,
    readers: HashMap<u64, CmdReader>,
    dir_path: PathBuf,
    uncompacted: u64,
}

/// Used to store serialized data to file
#[derive(Serialize, Deserialize)]
enum Cmd {
    Set { key: String, value: String },
    Rm { key: String }
}

/// Used to find command position in files
struct CmdPos {
    id:  u64,
    pos: u64,
    len: u64
}

struct CmdWriter {
    writer: BufWriter<File>,
    pos: u64,
    id:  u64
}
struct CmdReader {
    reader: BufReader<File>,
    origin_file: File,
    pos: u64,
    id:  u64
}

impl KvStore {
    /// create new KvStore with given path
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let path: PathBuf = path.into();
        fs::create_dir_all(&path)?;

        let slug_file = path.clone().join(".kvs");
        if slug_file.exists() && fs::metadata(slug_file)?.is_file() {
            restore(path.clone())
        } else {
            let default_id = 1;
            let index: BTreeMap<String, CmdPos> = BTreeMap::new();
            let mut readers: HashMap<u64, CmdReader> = HashMap::new();
            File::create(path.clone().join(".kvs"))?;

            let (read_file, write_file) = new_log_file(default_id)?;
            let writer = CmdWriter::new(write_file, 1)?;
            readers.insert(default_id, CmdReader::new(read_file, default_id)?);

            Ok(KvStore {
                index,
                writer,
                readers,
                dir_path: path,
                uncompacted: 0,
            })
        }
    }

    /// this function package the HashMap::insert
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let data = Cmd::set(key.clone(), value.clone());
        let pos = self.append(data)?;
        self.uncompacted += pos.end - pos.start;
        self.index.insert(key, CmdPos::new(self.writer.id, pos.start, pos.end - pos.start));
        Ok(())
    }

    /// this function package the HashMap::get
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        if let Some(pos) = self.index.get(&key) {
            let reader = self.readers.get_mut(&pos.id)
                .expect("Cannot find log reader");
            reader.seek(SeekFrom::Start(pos.pos))?;
            let cmd = reader.take(pos.len);
            if let Cmd::Set{value, ..} = ron::de::from_reader(cmd)? {
                Ok(Some(value))
            } else {
                Err(err_msg("Unexpected Command"))
            }
        } else {
            println!("Key not found");
            Ok(None)
        }
    }

    // fn get_from_file(file: &mut File, position: &u64) -> Result<Option<String>> {
    //     let mut reader = BufReader::new(Read::by_ref(file));
    //     reader.seek(SeekFrom::Start(position.clone()))?;
    //     let mut line = String::new();
    //     reader.read_line(&mut line)?;
        
    //     let cmd: Cmd = ron::de::from_bytes(line.as_bytes())?;
    //     match &cmd.name {
    //         CmdName::Set => {
    //             let value = cmd.value.unwrap();
    //             println!("{}", value);
    //             Ok(Some(value))
    //         },
    //         _ => {
    //             Err(err_msg("position error"))
    //         }
    //     }
    // }

    /// this function package the HashMap::remove
    pub fn remove(&mut self, key: String) -> Result<()> {
        if self.index.contains_key(&key) {
            let data = Cmd::rm(key.clone());
            self.append(data)?;
            self.index.remove(&key).expect("Key not found");
            Ok(())
        } else {
            let msg = "Key not found";
            println!("{}", msg);
            Err(err_msg(msg))
        }
    }

    // /// create new KvStore with path and file name
    // pub fn open_with_file_name(dir_path: PathBuf, file_name: String) -> Result<KvStore> {
    //     let file_path = dir_path.join(file_name);
    //     let store = KvStore::restore(&file_path)?;

    //     Ok(KvStore {
    //         store: store,
    //         file: KvStore::open_with_file_path(file_path)?,
    //         dir_path: dir_path.to_owned(),
    //         allow_compact: true
    //     })
    // }

    // fn open_with_file_path(file_path: PathBuf) -> Result<File> {
    //     let file_options = OpenOptions::new()
    //     .create(true)
    //     .read(true)
    //     .write(true)
    //     .open(&file_path);

    //     match file_options {
    //         Ok(file) => {
    //             Ok(file)
    //         },
    //         Err(e) => Err(err_msg(e))
    //     }
    // }

    fn append(&mut self, data: Cmd) -> Result<Range<u64>> {
        if self.uncompacted >= COMPACTION_THRESHOLD {
            // self.compact()?;
        }
        let pos = self.writer.pos;
        ron::ser::to_writer(&mut self.writer, &data)?;
        self.writer.flush()?;
        Ok(pos..self.writer.pos)
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

fn restore(path: PathBuf) -> Result<KvStore> {
    let mut file_list: Vec<u64> = fs::read_dir(&path)?.flatten()
        .map(|entry| entry.path())
        .filter(|path| path.is_file() && path.extension() == Some("log".as_ref()))
        .flat_map(|path| {
            path.file_name().and_then(std::ffi::OsStr::to_str)
                .map(|s| s.trim_end_matches(".log"))
                .map(str::parse::<u64>)
        }).flatten().collect();
    file_list.sort();
    
    let compact_lock = path.clone().join(".compact-lock");
    if compact_lock.exists() && fs::metadata(compact_lock)?.is_file() {
        unimplemented!()
    } else {
        if file_list.is_empty() {
            return Err(err_msg(".kvs file exist but no log files"))
        } else if file_list.get(file_list.len() - 1) != Some(&(file_list.len() as u64 + 1)) {
            return Err(err_msg("Unexpected exist log files"))
        }
        let mut index: BTreeMap<String, CmdPos> = BTreeMap::new();
        let mut readers: HashMap<u64, CmdReader> = HashMap::new();
        let mut uncompacted = 0;

        for i in file_list.iter() {
            let file = File::open(Path::new(&(i.to_string()+".log")))?;
            let mut reader = CmdReader::new(file, i.to_owned())?;
            let mut pos = reader.seek(SeekFrom::Start(0))?;
            let mut buffer = ron::de::from_reader(&mut reader).into_iter();

            if i < &(file_list.len() as u64) {
                uncompacted = pos;
                reader.id = reader.id - 1;
            }

            while let Some(cmd) = buffer.next() {
                match cmd {
                    Cmd::Set {key, ..} => {
                        index.insert(key, CmdPos::new(i.to_owned(), pos, reader.pos - pos));
                        pos = reader.pos.clone();
                    },
                    Cmd::Rm {key} => {
                        index.remove(&key);
                        pos = reader.pos.clone();
                    }
                }
            }

            readers.insert(i.to_owned(), reader);
        }

        let writer = {
            let id = file_list.len() as u64;
            let write_file = OpenOptions::new()
                .write(true).open(Path::new(&((id + 1).to_string()+".log")))?;
            CmdWriter::new(write_file, id)?
        };

        Ok(KvStore{
            index,
            writer,
            readers,
            dir_path: path,
            uncompacted,  
        })
    }
}

fn new_log_file(id: u64) -> Result<(File, File)> {
    let file_name = (id + 1).to_string()+".log";
    let write_file = OpenOptions::new()
        .create(true).write(true).open(Path::new(&file_name))?;
    let read_file = File::open(file_name)?;
    Ok((read_file, write_file))
}

impl Cmd {
    /// create a set command
    pub fn set(k: String, v: String) -> Self {
        return Cmd::Set {
            key: k,
            value: v
        }
    }
    /// create a rm command
    pub fn rm(k: String) -> Self {
        return Cmd::Rm {
            key: k
        }
    }
}

impl  CmdPos {
    fn new(id: u64, pos: u64, len: u64) -> Self {
        CmdPos {
            id, pos, len
        }
    }
}

impl CmdWriter {
    fn new(mut inner: File, id: u64) -> Result<Self> {
        let pos = inner.seek(SeekFrom::Current(0))?;
        Ok(CmdWriter {
            writer: BufWriter::new(inner),
            pos,
            id,
        })
    }
}

impl Write for CmdWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let len = self.writer.write(buf)?;
        self.pos += len as u64;
        Ok(len)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

impl CmdReader {
    fn new(mut inner: File, id: u64) -> Result<Self> {
        let pos = inner.seek(SeekFrom::Current(0))?;
        Ok(CmdReader {
            reader: BufReader::new(inner.try_clone()?),
            origin_file: inner,
            pos,
            id
        })
    }

    fn try_clone(&self) -> Result<Self> {
        Ok(CmdReader {
            reader: BufReader::new(self.origin_file.try_clone()?),
            origin_file: self.origin_file.try_clone()?,
            pos: self.pos.clone(),
            id: self.id.clone(),  
        })
    }
}

impl Read for CmdReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let len = self.reader.read(buf)?;
        self.pos += len as u64;
        Ok(len)
    }
}

impl Seek for CmdReader {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.pos = self.reader.seek(pos)?;
        Ok(self.pos)
    }
}