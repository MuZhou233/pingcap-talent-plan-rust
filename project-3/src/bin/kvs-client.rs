use failure::err_msg;
use io::{BufRead, Write};
use structopt::StructOpt;
use std::{io, net::TcpStream, net::Shutdown};
use kvs::*;

#[derive(StructOpt)]
#[structopt(name = "basic")]
struct Opt {
    #[structopt(long, global = true, default_value = "127.0.0.1:4000")]
    addr: String,
    #[structopt(subcommand)]
    cmd: OptKvs,
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

    let mut stream = TcpStream::connect(opt.addr)?;
    
    match request(&mut stream, Request::Ping(0))? {
        Response::Pong(c) if c == 0 => {},
        _ => return Err(err_msg("protocol error"))
    };
    
    match opt.cmd {
        OptKvs::Set {key , value} => {
            match request(&mut stream, Request::Set{key: key, value: value})? {
                Response::Success{value:_} => (),
                Response::Error{msg: e} =>
                    return Err(err_msg(e)),
                _ => return Err(err_msg("Unexpected response"))
            }
        },
        OptKvs::Get {key} => {
            match request(&mut stream, Request::Get{key: key})? {
                Response::Success{value: Some(v)} => 
                    println!("{}", v),
                Response::Success{value: None} =>
                    println!("Key not found"),
                Response::Error{msg: e} =>
                    return Err(err_msg(e)),
                _ => return Err(err_msg("Unexpected response"))
            }
        },
        OptKvs::Rm {key} => {
            match request(&mut stream, Request::Rm{key: key})? {
                Response::Success{value: _} => (),
                Response::Error{msg: e} =>
                    return Err(err_msg(e)),
                _ => return Err(err_msg("Unexpected response"))
            }
        }
    };
    request(&mut stream, Request::Shutdown(0))?;
    stream.shutdown(Shutdown::Both)?;

    Ok(())
}

fn request(stream: &mut TcpStream, data: Request) -> Result<Response> {
    ron::ser::to_writer(stream.try_clone()?, &Protocol::new(data))?;
    stream.write(b"\n")?;
    
    let mut reader = io::BufReader::new(stream.try_clone()?);
    let mut buffer = String::new();
    reader.read_line(&mut buffer)?;
    let res: Protocol<Response> = ron::de::from_str(&buffer)?;
    
    Ok(res.payload)
}