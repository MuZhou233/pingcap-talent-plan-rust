use failure::err_msg;
use structopt::StructOpt;
use std::{io::Write, net::TcpStream};
use kvs::*;

#[derive(StructOpt)]
#[structopt(name = "basic")]
struct Opt {
    #[structopt(subcommand)]
    cmd: OptKvs,
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
    println!("start ping");
    match request(&mut stream, Request::Ping(0))? {
        Response::Pong(c) if c == 0 => {},
        _ => return Err(err_msg("protocol error"))
    };

    match opt.cmd {
        OptKvs::Set {key , value} => {
            request(&mut stream, Request::Set{key: key, value: value})?
        },
        OptKvs::Get {key} => {
            request(&mut stream, Request::Get{key: key})?
        },
        OptKvs::Rm {key} => {
            request(&mut stream, Request::Rm{key: key})?
        }
    };

    Ok(())
}

fn request(stream: &mut TcpStream, data: Request) -> Result<Response> {
    println!("{:?}", data);
    let buffer = ron::ser::to_string(&Protocol::new(data))?;
    stream.write(buffer.as_bytes())?;
    
    let res: Protocol<Response> = ron::de::from_reader(stream)?;
    Ok(res.payload)
}