use structopt::StructOpt;
#[macro_use]
extern crate slog;
extern crate slog_term;

use slog::Drain;
use std::net::{TcpListener, TcpStream};
use kvs::*;


#[derive(StructOpt)]
#[structopt(name = "basic")]
struct Opt {
    #[structopt(long)]
    addr: Option<String>,
    #[structopt(long)]
    engine: Option<String>,
}

fn main() -> Result<()> {
    let decorator = slog_term::PlainSyncDecorator::new(std::io::stderr());
    let drain = slog_term::FullFormat::new(decorator).build().fuse();

    let log = slog::Logger::root(drain, o!("version" => "0.1"));

    let opt = Opt::from_args();

    let addr = if let Some(addr) = opt.addr {
        addr
    } else {
        "127.0.0.1:4000".to_owned()
    };
    info!(log, "{}", addr.clone());

    let engine = if let Some(engine) = opt.engine {
        engine
    } else {
        "kvs".to_owned()
    };
    info!(log, "{}", engine.clone());

    let listener = TcpListener::bind(addr)?;

    // accept connections and process them serially
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                debug!(log, "new client");
            },
            Err(e) => info!(log, "connection failed")
        }
    }

    Ok(())
}