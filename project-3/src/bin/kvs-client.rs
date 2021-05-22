use structopt::StructOpt;
use std::net::TcpStream;
use kvs::*;

#[derive(StructOpt)]
#[structopt(name = "basic")]
struct Opt {
    #[structopt(subcommand)]
    cmd: Option<OptKvs>,
    #[structopt(long)]
    addr: Option<String>,
}

#[derive(StructOpt)]
#[structopt(name = "main")]
enum OptKvs {
    Set {
        #[structopt(name = "KEY")]
        key: String,
        #[structopt(name = "VALUE")]
        value: String
    },
    Get {
        #[structopt(name = "KEY")]
        key: String
    },
    Rm {
        #[structopt(name = "KEY")]
        key: String
    }
}

fn main() -> Result<()> {
    let opt = Opt::from_args();

    let addr = if let Some(addr) = opt.addr {
        addr
    } else {
        "127.0.0.1:4000".to_owned()
    };

    let mut stream = TcpStream::connect(addr)?;

    

    match opt.cmd {
        Some(OptKvs::Set {key , value}) => {
        },
        Some(OptKvs::Get {key}) => {
        },
        Some(OptKvs::Rm {key}) => {
        },
        None => {
            unreachable!()
        }
    };

    Ok(())
}