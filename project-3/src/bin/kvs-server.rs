use structopt::StructOpt;
#[macro_use]
extern crate slog;
extern crate slog_term;

use slog::Drain;
use kvs::*;

#[derive(StructOpt)]
#[structopt(name = "basic")]
struct Opt {
    #[structopt(long)]
    addr: Option<String>,
    #[structopt(long)]
    engine: Option<String>,
}

fn main() {
    let decorator = slog_term::PlainSyncDecorator::new(std::io::stderr());
    let drain = slog_term::FullFormat::new(decorator).build().fuse();

    let log = slog::Logger::root(drain, o!("version" => "0.5"));

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
}