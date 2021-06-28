use failure::err_msg;
use structopt::StructOpt;
use thread_pool::ThreadPool;
use std::{fs::OpenOptions, net::Shutdown, str};
use std::env::current_dir;
use serde::{Serialize, Deserialize};
#[macro_use]
extern crate slog;
extern crate slog_term;

use slog::Drain;
use std::net::{TcpListener, TcpStream};
use kvs::*;

const ENGINES: &[&str] = &["kvs", "sled"];

#[derive(StructOpt)]
#[structopt(name = "basic")]
struct Opt {
    #[structopt(long, default_value = "127.0.0.1:4000")]
    addr: String,
    #[structopt(long, default_value = "kvs", possible_values(ENGINES))]
    engine: String,
}

fn main() -> Result<()> {
    let decorator = slog_term::PlainSyncDecorator::new(std::io::stderr());
    let drain = slog_term::FullFormat::new(decorator).build().fuse();

    let log = slog::Logger::root(drain, o!());

    let opt = Opt::from_args();

    info!(log, "{}", env!("CARGO_PKG_VERSION"));

    let listener = TcpListener::bind(opt.addr.clone())?;
    info!(log, "{}", opt.addr);

    if let Ok(conf) = conf_get() {
        if conf.engine != opt.engine {
            return Err(err_msg("wrong engine"));
        }
    } else {
        conf_set(ServerConf {
            engine: opt.engine.clone()
        })?;
    }

    info!(log, "{}", opt.engine);

    match opt.engine.as_str() {
        "kvs" => serve(&log, KvStore::open(current_dir()?)?, thread_pool::SharedQueueThreadPool::new(10)?, listener)?,
        "sled" => serve(&log, SledKvsEngine::open(current_dir()?)?, thread_pool::SharedQueueThreadPool::new(10)?, listener)?,
        _ => unreachable!()
    };

    Ok(())
}

fn serve<E: KvsEngine, T: ThreadPool>(log: &slog::Logger, engine: E, threads: T, listener: TcpListener) -> Result<()> {
    // accept connections and process them serially
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                info!(log, "new client");
                match handle(&log, engine.clone(), &threads, stream) {
                    Ok(_) => 
                        info!(log, "client offline"),
                    Err(e) => 
                        warn!(log, "stream closed: {:?}", e)
                }
            },
            Err(_) => warn!(log, "client connection failed")
        }
    }
    Ok(())
}

fn handle<E: KvsEngine, T: ThreadPool>(_log: &slog::Logger, engine: E, threads: &T, stream: TcpStream) -> Result<()> {
    let mut stream = stream;
    
    threads.spawn(move || {
        Protocol::listen(&mut stream.try_clone().unwrap(), |data: Protocol<Request>| {
            let data = match data.payload {
                Request::Ping(code) =>  Response::Pong(code),
                Request::Shutdown => return Ok(true),
                Request::Set{key,value} => {
                    match engine.set(key, value) {
                        Ok(_) => Response::Success{value: None},
                        Err(e) => Response::Error{msg: e.to_string()}
                    }
                },
                Request::Get{key} => {
                    match engine.get(key) {
                        Ok(v) => Response::Success{value: v},
                        Err(e) => Response::Error{msg: e.to_string()}
                    }
                },
                Request::Rm{key} => {
                    match engine.remove(key) {
                        Ok(_) => Response::Success{value: None},
                        Err(e) => Response::Error{msg: e.to_string()}
                    }
                },
            };
            Protocol::send(&mut stream, Protocol::new(data))?;
    
            Ok(false)
        }).unwrap();
    
        stream.shutdown(Shutdown::Both).unwrap();
    });

    Ok(())
}

#[derive(Serialize, Deserialize)]
struct ServerConf {
    engine: String
}

fn conf_get() -> Result<ServerConf> {
    let file_options = OpenOptions::new()
    .read(true)
    .open("kvs.conf");

    match file_options {
        Ok(file) => {
            let conf: ServerConf = ron::de::from_reader(file)?;
            Ok(conf)
        },
        Err(e) => Err(err_msg(e))
    }
}

fn conf_set(conf: ServerConf) -> Result<()> {
    let file_options = OpenOptions::new()
    .create(true)
    .write(true)
    .open("kvs.conf");
    
    match file_options {
        Ok(file) => {
            ron::ser::to_writer(file, &conf)?;
            Ok(())
        },
        Err(e) => Err(err_msg(e))
    }
}