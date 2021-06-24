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
    let mut store = KvStore::open("./")?;

    match opt.cmd {
        Some(OptKvs::Set {key , value}) => {
            store.set(key, value)?;
            Ok(())
        },
        Some(OptKvs::Get {key}) => {
            store.get(key)?;
            Ok(())
        },
        Some(OptKvs::Rm {key}) => {
            store.remove(key)?;
            Ok(())
        },
        None => {
            panic!()
        }
    }
}