use std::{collections::{BTreeMap, HashMap}, fs::{self, OpenOptions}, io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write}, ops::Range, path::PathBuf};
use std::fs::File;
use serde::{Serialize, Deserialize};
use crate::error::*;
use crate::engine::KvsEngine;

const COMPACTION_THRESHOLD: u64 = 1024 * 1024 * 100;

/// The `KvStore` stores string key-value pairs
///
/// This engine act as a simple and weak Log-Structued Database.
/// Data will be stored on disk named by id number with `.log` extension.
/// It will keep a `BTreeMap` in memory for quick indexing.
pub struct KvStore {
    index: BTreeMap<String, CmdPos>,
    writer: CmdWriter,
    readers: HashMap<u64, CmdReader>,
    dir_path: PathBuf,
    uncompacted: u64,
    compacting: bool,
}

/// Store serialized data to files
#[derive(Serialize, Deserialize)]
enum Cmd {
    Set { key: String, value: String },
    Rm { key: String }
}

/// Store command position in files
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
    /// Used by `try_clone` function.
    /// 
    /// Not a good solution for `try_clone` but it Works!
    origin_file: File,
    pos: u64,
    id:  u64
}

impl KvsEngine for KvStore {
    fn set(&mut self, key: String, value: String) -> Result<()> {
        let data = Cmd::set(key.clone(), value.clone());
        let pos = self.append(data)?;
        self.index.insert(key, CmdPos::new(self.writer.id, pos.start, pos.end - pos.start));
        Ok(())
    }

    fn get(&mut self, key: String) -> Result<Option<String>> {
        if let Some(pos) = self.index.get(&key) {
            let reader = self.readers.get_mut(&pos.id)
                .expect("Cannot find log reader");
            reader.seek(SeekFrom::Start(pos.pos))?;
            let cmd = reader.take(pos.len);
            if let Cmd::Set{value, ..} = serde_json::de::from_reader(cmd)? {
                Ok(Some(value))
            } else {
                Err(err_msg("Unexpected Command"))
            }
        } else {
            Ok(None)
        }
    }

    fn remove(&mut self, key: String) -> Result<()> {
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
}

impl KvStore {
    /// Create new `KvStore` in given path
    /// 
    /// This will create the directory if the given one does not exist.
    /// This will restore exist data if the file exist.
    /// Or new data file will be created.
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let path: PathBuf = path.into();
        fs::create_dir_all(&path)?;
        
        let slug_file = path.join(".kvs");
        if slug_file.exists() && fs::metadata(slug_file)?.is_file() {
            restore(path.clone())
        } else {
            let default_id = 2;
            let index: BTreeMap<String, CmdPos> = BTreeMap::new();
            let mut readers: HashMap<u64, CmdReader> = HashMap::new();
            File::create(path.join(".kvs"))?;

            let (reader, writer) = new_log_file(path.clone(), default_id)?;
            readers.insert(default_id, reader);

            Ok(KvStore {
                index,
                writer,
                readers,
                dir_path: path,
                uncompacted: 0,
                compacting: false,
            })
        }
    }

    fn append(&mut self, data: Cmd) -> Result<Range<u64>> {
        if self.uncompacted >= COMPACTION_THRESHOLD {
            self.compact()?;
        }
        let pos = self.writer.pos;
        serde_json::ser::to_writer(&mut self.writer, &data)?;
        self.writer.flush()?;
        self.uncompacted += self.writer.pos - pos;
        Ok(pos..self.writer.pos)
    }

/// Clears stale entries in the log.
///
/// Log files' name should be a continuous number with `.log` extension.
/// The second biggest number of log file is reserved to be the target
/// file of next compaction. And the biggest number of log file is active
/// to write new data.
///
/// `.compact-lock` file is used to protect log files from a compact failure
    fn compact(&mut self) -> Result<()> {
        let compact_lock = self.dir_path.join(".compact-lock");
        if self.compacting {
            return Ok(());
        } else {
            self.compacting = true;
            if compact_lock.exists() {
                return Err(err_msg("Unexpected compact lock file"));
            } else {
                OpenOptions::new().create(true).write(true).open(compact_lock.clone())?;
            }
        }

        let id = self.writer.id - 1;
        // new active log file
        {
            let (reader, writer) = new_log_file(self.dir_path.clone(), id + 2)?;
            self.readers.insert(id + 2, reader);
            self.writer = writer;
        };
        // log file to compact to
        let mut writer = {
            let (reader, writer) = new_log_file(self.dir_path.clone(), id)?;
            self.readers.insert(id, reader);
            writer
        };
        // log file tobe compacted and then delete
        let mut reader = self.readers.get(&(id + 1))
            .expect("Cannot find log reader").try_clone()?;
        
        let mut stream = serde_json::Deserializer::from_reader(&mut reader).into_iter();
        let mut index: HashMap<String, Option<String>> = HashMap::new();
        
        while let Some(cmd) = stream.next() {
            match cmd? {
                Cmd::Set {key, value} => {
                    index.insert(key, Some(value));
                },
                Cmd::Rm {key} => {
                    index.insert(key, None);
                }
            }
        }

        let mut pos = writer.seek(SeekFrom::Start(0))?;
        for (key, v) in index {
            if let Some(value) = v {
                if self.index.contains_key(&key) {
                    let data = Cmd::set(key.clone(), value);
                    serde_json::ser::to_writer(&mut writer, &data)?;
                    self.index.insert(key, CmdPos::new(writer.id, pos, writer.pos - pos));
                    writer.flush()?;
                    pos = writer.pos;
                }
            } else {
                if !self.index.contains_key(&key) {
                    let data = Cmd::rm(key.clone());
                    serde_json::ser::to_writer(&mut writer, &data)?;
                    writer.flush()?;
                    pos = writer.pos;
                }
            }
        }

        let reader = self.readers.remove(&(id + 1))
            .expect("Cannot remove log reader");
        let reader_id = reader.id;
        drop(reader);
        fs::remove_file(self.dir_path.join(reader_id.to_string()+".log"))?;
        
        fs::remove_file(compact_lock)?;
        self.compacting = false;
        Ok(())
    }
}

/// Restore logs from exist files.
/// 
/// # Errors
///
/// Error will be returned while the exist file list does not arranged as expected.
/// Currently this function will not try to fix the files.
/// 
/// See `compact` function for more information
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
    
    let compact_lock = path.join(".compact-lock");
    if compact_lock.exists() && fs::metadata(compact_lock)?.is_file() {
        // TODO: try to resume compact
        return Err(err_msg("Log files has an uncompleted compact process.(Failure handle unimplemented)"))
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
            let file = File::open(path.join(i.to_string()+".log"))?;
            let mut reader = CmdReader::new(file, i.to_owned())?;
            let mut pos = reader.seek(SeekFrom::Start(0))?;
            let mut stream = serde_json::Deserializer::from_reader(&mut reader).into_iter();
            
            while let Some(cmd) = stream.next() {
                let new_pos = stream.byte_offset() as u64;
                match cmd? {
                    Cmd::Set {key, ..} => {
                        index.insert(key, CmdPos::new(i.to_owned(), pos, new_pos - pos));
                        pos = new_pos;
                    },
                    Cmd::Rm {key} => {
                        index.remove(&key);
                        pos = new_pos;
                    }
                }
            }

            if i > &(file_list.len() as u64) {
                uncompacted = pos;
            }
            readers.insert(i.to_owned(), reader);
        }

        let mut writer = {
            let id = file_list.len() as u64 + 1;
            let write_file = OpenOptions::new()
                .write(true).open(path.join(id.to_string()+".log"))?;
            CmdWriter::new(write_file, id)?
        };
        writer.seek(SeekFrom::End(0))?;

        Ok(KvStore{
            index,
            writer,
            readers,
            dir_path: path,
            uncompacted,
            compacting: false,
        })
    }
}

/// Create a new log file with given id.
///
/// Return a pair of `Reader` and `Writer` of that file.
fn new_log_file(path: PathBuf, id: u64) -> Result<(CmdReader, CmdWriter)> {
    let file_path = path.join(id.to_string()+".log");
    if file_path.exists() {
        return Err(err_msg("Unexpected exist log file"));
    }
    let write_file = OpenOptions::new()
        .create(true).write(true).open(file_path.clone())?;
    let writer = CmdWriter::new(write_file, id.clone())?;
    let read_file = File::open(file_path)?;
    let reader = CmdReader::new(read_file, id)?;
    Ok((reader, writer))
}

impl Cmd {
    /// Create a `Set` command
    pub fn set(k: String, v: String) -> Self {
        return Cmd::Set {
            key: k,
            value: v
        }
    }
    /// Create a `Rm` command
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

impl Seek for CmdWriter {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.pos = self.writer.seek(pos)?;
        Ok(self.pos)
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