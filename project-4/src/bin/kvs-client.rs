use failure::err_msg;
use structopt::StructOpt;
use std::{net::TcpStream, net::Shutdown};
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

    let mut stream = TcpStream::connect(opt.addr.clone())?;
    
    request_once(&mut stream, Request::Ping(0), |res| 
        match res {
            Response::Pong(c) if c == 0 => Ok(()),
            _ => Err(err_msg("protocol error"))
    })?;

    match opt.cmd {
        OptKvs::Set {key , value} => {
            request_once(&mut stream, Request::Set{key, value}, |res| 
                match res {
                    Response::Success{value:_} => Ok(()),
                    Response::Error{msg: e} =>
                        Err(err_msg(e)),
                    _ => Err(err_msg("Unexpected response"))
            })?;
        },
        OptKvs::Get {key} => {
            request_once(&mut stream, Request::Get{key}, |res| {
                match res {
                    Response::Success{value: Some(v)} => 
                        println!("{}", v),
                    Response::Success{value: None} =>
                        println!("Key not found"),
                    Response::Error{msg: e} =>
                        return Err(err_msg(e)),
                    _ => return Err(err_msg("Unexpected response"))
                }
                Ok(())
            })?;
        },
        OptKvs::Rm {key} => {
            request_once(&mut stream, Request::Rm{key}, |res| 
                match res {
                    Response::Success{value: _} => Ok(()),
                    Response::Error{msg: e} =>
                        return Err(err_msg(e)),
                    _ => return Err(err_msg("Unexpected response"))
            })?;
        }
    };
    Protocol::send(&mut stream, Protocol::new(Request::Shutdown))?;
    stream.shutdown(Shutdown::Both)?;

    Ok(())
}

fn request_once<F: FnMut(Response) -> Result<()>>(stream: &mut TcpStream, req: Request, mut handler: F) -> Result<()> {
    Protocol::send(stream, Protocol::new(req))?;
    Protocol::listen(stream, |data| {
        handler(data.payload)?;
        Ok(true)
    })?;
    Ok(())
}