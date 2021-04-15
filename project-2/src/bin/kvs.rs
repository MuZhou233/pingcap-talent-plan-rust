use structopt::StructOpt;
use kvs::*;

#[derive(StructOpt)]
#[structopt(name = "basic")]
struct Opt {
    #[structopt(subcommand)]
    cmd: Option<OptKvs>
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
    let path = "kvs.log".to_owned();

    match opt.cmd {
        Some(OptKvs::Set {key , value}) => {
            let data = log::Cmd::new(log::CmdName::Set, key, value);
            log::append(data, path)?;
            Ok(())
        },
        Some(_) => {
            eprintln!("unimplemented");
            panic!()
        },
        _ => {
            panic!()
        }
    }
}