use failure::err_msg;
use io::{BufRead, Read};
use structopt::StructOpt;
use thread_pool::SharedQueueThreadPool;
use std::{fs::OpenOptions, io::Write, net::Shutdown, str};
use serde::{Serialize, Deserialize};
use std::io;
#[macro_use]
extern crate slog;
extern crate slog_term;

use slog::{Drain, Logger};
use std::net::{TcpListener, TcpStream};
use kvs::{thread_pool::ThreadPool};
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

    let log = slog::Logger::root(drain, o!("version" => env!("CARGO_PKG_VERSION")));

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
        "kvs"  => serve(KvStore::new()?, SharedQueueThreadPool::new(10)?, listener, log),
        "sled" => serve(SledKvsEngine::new()?, SharedQueueThreadPool::new(10)?, listener, log),
        _ => unreachable!()
    }

    Ok(())
}

fn serve<S: KvsEngine, T: ThreadPool>(engine: S, threads: T, listener: TcpListener, log: Logger) {
    // accept connections and process them serially
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                info!(log, "new client");
                match handle(engine.clone(), &threads, stream) {
                    Ok(_) => 
                        info!(log, "client offline"),
                    Err(e) => 
                        warn!(log, "stream closed: {:?}", e)
                }
            },
            Err(_) => warn!(log, "client connection failed")
        }
    }
} 

fn handle<S: KvsEngine, T: ThreadPool>(engine: S, threads: &T,stream: TcpStream) -> Result<()> {
    let mut stream = stream;
    
    threads.spawn(move || {
        loop {
            let mut reader = io::BufReader::new(stream.try_clone().unwrap());
            let mut buffer = String::new();
            reader.read_line(&mut buffer).unwrap();
            
        
            let data: Protocol<Request> = ron::de::from_str(&buffer).unwrap();
        
            
            match data.payload {
                Request::Ping(code) => response(&mut stream, Response::Pong(code)).unwrap(),
                Request::Shutdown(code) => {
                    response(&mut stream, Response::Shutdown(code)).unwrap();
                    break
                },
                Request::Set{key,value} => {
                    match engine.set(key, value) {
                        Ok(_) => response(&mut stream, Response::Success{value: None}).unwrap(),
                        Err(e) => response(&mut stream, Response::Error{msg: e.to_string()}).unwrap()
                    }
                },
                Request::Get{key} => {
                    match engine.get(key) {
                        Ok(v) => response(&mut stream, Response::Success{value: v}).unwrap(),
                        Err(e) => response(&mut stream, Response::Error{msg: e.to_string()}).unwrap()
                    }
                },
                Request::Rm{key} => {
                    match engine.remove(key) {
                        Ok(_) => response(&mut stream, Response::Success{value: None}).unwrap(),
                        Err(e) => response(&mut stream, Response::Error{msg: e.to_string()}).unwrap()
                    }
                },
            };
        }
        stream.shutdown(Shutdown::Both).unwrap();   
    });

    Ok(())
}

fn response(stream: &mut TcpStream, data: Response) -> Result<()> {
    ron::ser::to_writer(stream.try_clone()?, &Protocol::new(data))?;
    stream.write(b"\n")?;
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