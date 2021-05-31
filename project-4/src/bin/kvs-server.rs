use failure::err_msg;
use io::{BufRead, Read};
use structopt::StructOpt;
use std::{fs::OpenOptions, io::Write, net::Shutdown, str};
use serde::{Serialize, Deserialize};
use std::io;
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

    let log = slog::Logger::root(drain, o!("version" => "0.1"));

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

    let mut engine: Box<dyn KvsEngine> = match opt.engine.as_str() {
        "kvs" => Box::new(KvStore::open("")?),
        // "sled" => Box::new(SledKvsEngine::open("")?),
        _ => unreachable!()
    };
    info!(log, "{}", opt.engine);

    // accept connections and process them serially
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                info!(log, "new client");
                match handle(&log, &mut engine, stream) {
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

fn handle(log: &slog::Logger, engine: &mut Box<dyn KvsEngine> ,stream: TcpStream) -> Result<()> {
    let mut stream = stream;
    
    loop {
        let mut reader = io::BufReader::new(stream.try_clone()?);
        let mut buffer = String::new();
        reader.read_line(&mut buffer)?;
        
        debug!(log, "get data from client: {:?}", buffer);
    
        let data: Protocol<Request> = ron::de::from_str(&buffer)?;
    
        debug!(log, "deserialized: {:?}", data);
        
        match data.payload {
            Request::Ping(code) => response(&mut stream, Response::Pong(code))?,
            Request::Shutdown(code) => {
                response(&mut stream, Response::Shutdown(code))?;
                break
            },
            Request::Set{key,value} => {
                match engine.set(key, value) {
                    Ok(v) => response(&mut stream, Response::Success{value: v})?,
                    Err(e) => response(&mut stream, Response::Error{msg: e.to_string()})?
                }
            },
            Request::Get{key} => {
                match engine.get(key) {
                    Ok(v) => response(&mut stream, Response::Success{value: v})?,
                    Err(e) => response(&mut stream, Response::Error{msg: e.to_string()})?
                }
            },
            Request::Rm{key} => {
                match engine.remove(key) {
                    Ok(v) => response(&mut stream, Response::Success{value: v})?,
                    Err(e) => response(&mut stream, Response::Error{msg: e.to_string()})?
                }
            },
        };
    }
    stream.shutdown(Shutdown::Both)?;

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