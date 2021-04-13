use std;
use structopt::StructOpt;

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
        key: Option<String>,
        #[structopt(name = "VALUE")]
        value: Option<String>
    },
    Get {
        #[structopt(name = "KEY")]
        key: Option<String>
    },
    Rm {
        #[structopt(name = "KEY")]
        key: Option<String>
    }
}

fn main() {
    let opt = Opt::from_args();

    match opt.cmd {
        Some(_) => {
            eprintln!("unimplemented");
        },
        _ => {}
    }
    std::process::exit(1);
}