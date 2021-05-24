use io::Read;
use structopt::StructOpt;
use std::str;
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

    let listener = TcpListener::bind(opt.addr.clone())?;
    info!(log, "{}", opt.addr);

    let mut engine: Box<dyn KvsEngine> = match opt.engine.as_str() {
        "kvs" => Box::new(KvStore::open("")?),
        "sled" => Box::new(SledKvsEngine::open("")?),
        _ => unreachable!()
    };
    info!(log, "{}", opt.engine);

    // accept connections and process them serially
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                debug!(log, "new client");
                handle(&log, &mut engine, stream)?;
            },
            Err(_) => info!(log, "connection failed")
        }
    }

    Ok(())
}

fn handle(log: &slog::Logger, engine: &mut Box<dyn KvsEngine> ,stream: TcpStream) -> Result<()> {
    let mut stream = stream;
    debug!(log, "waiting for data");
    let mut buffer = String::new();
    stream.read_to_string(&mut buffer)?;
    debug!(log, "get data");
    debug!(log, "{:?}", buffer);
    let data: Protocol<Request> = ron::de::from_str(&buffer)?;
    debug!(log, "{:?}", data);
    match data.payload {
        Request::Ping(code) => response(&mut stream, Response::Pong(code))?,
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

    Ok(())
}

fn response(stream: &mut TcpStream, data: Response) -> Result<()> {
    ron::ser::to_writer(stream, &Protocol::new(data))?;
    Ok(())
}