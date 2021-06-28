use std::{collections::HashMap, convert::TryInto, fs::{self, OpenOptions}, io::{self, BufWriter, Read, Seek, SeekFrom, Write}, ops::Range, path::PathBuf, sync::{Arc, Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard, atomic::{AtomicU64, Ordering}}};
use std::fs::File;
use dashmap::DashMap;
use serde::{Serialize, Deserialize};
use system_interface::fs::FileIoExt;
use crate::error::*;
use crate::engine::KvsEngine;

const COMPACTION_THRESHOLD: u64 = 1024 * 1024;

/// The `KvStore` stores string key-value pairs
///
/// This engine act as a simple and weak Log-Structued Database.
/// Data will be stored on disk named by id number with `.log` extension.
/// It will keep a `DashMap` in memory for quick indexing.
pub struct KvStore {
    index: Arc<DashMap<String, CmdPos>>,
    writer: Arc<Mutex<CmdWriter>>,
    readers: Arc<DashMap<u64, Arc<RwLock<CmdReader>>>>,
    dir_path: Arc<PathBuf>,
    uncompacted: Arc<AtomicU64>,
    compacting: Arc<Mutex<bool>>,
    writer_id: Arc<AtomicU64>,
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
    reader: File,
    pos: u64,
    id:  u64
}

impl KvsEngine for KvStore {
    fn set(&self, key: String, value: String) -> Result<()> {
        let data = Cmd::set(key.clone(), value.clone());
        self.append(data, |pos| {
            self.index.insert(key, CmdPos::new(self.writer_id.load(Ordering::Relaxed), pos.start, pos.end - pos.start));
        })?;
        Ok(())
    }

    fn get(&self, key: String) -> Result<Option<String>> {
        if let Some(pos) = self.index.get(&key) {
            let reader = self.readers.get(&pos.id)
                .expect("Cannot find log reader");

            let reader = read_lock(&*reader);
            let mut cmd = vec![0u8; pos.len.try_into()?];
            reader.read_exact_at(&mut cmd, pos.pos)?;
            if let Cmd::Set{value, ..} = serde_json::de::from_slice(&mut cmd)? {
                Ok(Some(value))
            } else {
                Err(err_msg("Unexpected Command"))
            }
        } else {
            Ok(None)
        }
    }

    fn remove(&self, key: String) -> Result<()> {
        if self.index.contains_key(&key) {
            let data = Cmd::rm(key.clone());
            self.append(data, |_| {
                self.index.remove(&key).expect("Key not found");
            })?;
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
            let index: DashMap<String, CmdPos> = DashMap::new();
            let readers: DashMap<u64, Arc<RwLock<CmdReader>>> = DashMap::new();
            File::create(path.join(".kvs"))?;

            let (reader, writer) = new_log_file(path.clone(), default_id)?;
            readers.insert(default_id, Arc::new(RwLock::new(reader)));

            Ok(KvStore {
                index: Arc::new(index),
                writer: Arc::new(Mutex::new(writer)),
                readers: Arc::new(readers),
                dir_path: Arc::new(path),
                uncompacted: Arc::new(AtomicU64::new(0)),
                compacting: Arc::new(Mutex::new(false)),
                writer_id: Arc::new(AtomicU64::new(default_id)),
            })
        }
    }

    /// Used by `set` and `remove`, do all of the changes with `self.writer` lock to ensure data consistency
    fn append<F: FnOnce(Range<u64>)>(&self, data: Cmd, update_index: F) -> Result<()> {
        if self.uncompacted.load(Ordering::SeqCst) >= COMPACTION_THRESHOLD {
            self.compact()?;
        }
        let mut writer = self.writer.lock().expect(
            "Can't lock writer"
        );
        let pos = writer.pos;
        serde_json::ser::to_writer(&mut *writer, &data)?;
        writer.flush()?;
        self.uncompacted.fetch_add(writer.pos - pos, Ordering::Relaxed);
        update_index(pos..writer.pos);
        Ok(())
    }

/// Clears stale entries in the log.
///
/// Log files' name should be a continuous number with `.log` extension.
/// The second biggest number of log file is reserved to be the target
/// file of next compaction. And the biggest number of log file is active
/// to write new data.
    fn compact(&self) -> Result<()> {
        // `self.compacting` is used to keep only one thread run compact at a time
        // `.compact-lock` file is used to protect log files from a compact failure
        let compact_lock = self.dir_path.join(".compact-lock");
        {
            let mut compacting = lock(&self.compacting);
            if *compacting {
                return Ok(());
            } else {
                *compacting = true;
                if compact_lock.exists() {
                    return Err(err_msg("Unexpected compact lock file"));
                } else {
                    OpenOptions::new().create(true).write(true).open(compact_lock.clone())?;
                }
            }
        }

        let id = self.writer_id.load(Ordering::Relaxed) - 1;
        // new active log file
        {
            let (reader, writer) = new_log_file(self.dir_path.to_path_buf(), id + 2)?;
            self.readers.insert(id + 2, Arc::new(RwLock::new(reader)));
            let mut self_writer = lock(&self.writer);
            self.uncompacted.swap(0, Ordering::SeqCst);
            *self_writer = writer;
            self.writer_id.fetch_sub(1, Ordering::SeqCst);
        };
        // log file to compact to
        let mut writer = {
            let (reader, writer) = new_log_file(self.dir_path.to_path_buf(), id)?;
            self.readers.insert(id, Arc::new(RwLock::new(reader)));
            writer
        };
        // log file to be compacted and then delete
        let reader = self.readers.get(&(id + 1))
            .expect("Cannot find log reader");
        let mut reader = read_lock(&reader).try_clone()?;
        reader.seek(SeekFrom::Start(0))?;
        
        // replay source file to generate data for compact
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

        // lock `self.writer` to ensure data consistency during compacting
        let _ = lock(&self.writer);
        let mut pos = writer.seek(SeekFrom::Start(0))?;
        for (key, v) in index {
            if let (Some(value), Some(cmdpos)) = (v, self.index.get(&key)) {
                if cmdpos.id == writer.id + 1 {
                    // `DashMap` is not lock-free. drop to release lock
                    drop(cmdpos);
                    let data = Cmd::set(key.clone(), value);
                    serde_json::ser::to_writer(&mut writer, &data)?;
                    self.index.insert(key, CmdPos::new(writer.id, pos, writer.pos - pos));
                    pos = writer.pos;
                }
            } else {
                if !self.index.contains_key(&key) {
                    let data = Cmd::rm(key.clone());
                    serde_json::ser::to_writer(&mut writer, &data)?;
                    pos = writer.pos;
                }
            }
        }
        writer.flush()?;

        // release source file
        let reader = self.readers.get(&(id + 1))
            .expect("Cannot remove log reader");
        let reader = write_lock(&reader);
        let reader_id = reader.id;
        drop(reader);
        fs::remove_file(self.dir_path.join(reader_id.to_string()+".log"))?;

        // unlock
        fs::remove_file(compact_lock)?;
        *lock(&self.compacting) = false;
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
        // TODO: try to resume compact process
        return Err(err_msg("Log files has an uncompleted compact process.(Failure handle unimplemented)"))
    } else {
        if file_list.is_empty() {
            return Err(err_msg(".kvs file exist but no log files"))
        } else if file_list.get(file_list.len() - 1) != Some(&(file_list.len() as u64 + 1)) {
            return Err(err_msg("Unexpected exist log files"))
        }
        let index: DashMap<String, CmdPos> = DashMap::new();
        let readers: DashMap<u64, Arc<RwLock<CmdReader>>> = DashMap::new();
        let mut uncompacted = 0;

        for i in file_list.iter() {
            let mut reader = CmdReader::new(path.join(i.to_string()+".log"), i.to_owned())?;
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
            readers.insert(i.to_owned(), Arc::new(RwLock::new(reader)));
        }

        let (mut writer, id) = {
            let id = file_list.len() as u64 + 1;
            let write_file = OpenOptions::new()
                .write(true).open(path.join(id.to_string()+".log"))?;
            (CmdWriter::new(write_file, id)?
            , id)
        };
        writer.seek(SeekFrom::End(0))?;

        Ok(KvStore{
            index: Arc::new(index),
            writer: Arc::new(Mutex::new(writer)),
            readers: Arc::new(readers),
            dir_path: Arc::new(path),
            uncompacted: Arc::new(AtomicU64::new(uncompacted)),
            compacting: Arc::new(Mutex::new(false)),
            writer_id: Arc::new(AtomicU64::new(id))
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
    let reader = CmdReader::new(file_path, id)?;
    Ok((reader, writer))
}

fn lock<'a, T>(lock: &'a Arc<Mutex<T>>) -> MutexGuard<'a, T> {
    lock.lock().expect(
        format!("Can't get mutex lock, variable type {}", std::any::type_name::<T>()).as_ref()
    )
}
fn read_lock<'a, T>(lock: &'a Arc<RwLock<T>>) -> RwLockReadGuard<'a, T> {
    lock.read().expect(
        format!("Can't get read lock, variable type {}", std::any::type_name::<T>()).as_ref()
    )
}

fn write_lock<'a, T>(lock: &'a Arc<RwLock<T>>) -> RwLockWriteGuard<'a, T> {
    lock.write().expect(
        format!("Can't get write lock, variable type {}", std::any::type_name::<T>()).as_ref()
    )
}

impl Clone for KvStore {
    fn clone(&self) -> Self {
        KvStore{
            index: self.index.clone(),
            writer: self.writer.clone(),
            readers: self.readers.clone(),
            dir_path: self.dir_path.clone(),
            uncompacted: self.uncompacted.clone(),
            compacting: self.compacting.clone(),
            writer_id: self.writer_id.clone(),
        }
    }
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
    fn new(inner: File, id: u64) -> Result<Self> {
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
    fn new(file_path: PathBuf, id: u64) -> Result<Self> {
        let inner = File::open(&file_path)?;
        let pos = inner.seek(SeekFrom::Current(0))?;
        Ok(CmdReader {
            reader: inner,
            pos,
            id
        })
    }

    fn try_clone(&self) -> Result<Self> {
        Ok(CmdReader {
            reader: self.reader.try_clone()?,
            pos: self.pos.clone(),
            id: self.id.clone(),  
        })
    }
 
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> Result<()> {
        Ok(self.reader.read_exact_at(buf, offset)?)
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