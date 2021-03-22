extern crate clap;
use std;
use clap::{Arg, App, SubCommand};

fn main() {
    let matches = App::new(env!("CARGO_PKG_NAME"))
        .version(env!("CARGO_PKG_VERSION"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .subcommand(SubCommand::with_name("set")
                    .arg(Arg::with_name("set")
                        .long("set"))
                    .arg(Arg::with_name("KEY")
                        .required(true)
                        .index(1))
                    .arg(Arg::with_name("VALUE")
                        .required(true)
                        .index(2))
                    )
        .subcommand(SubCommand::with_name("get")
                    .arg(Arg::with_name("get")
                        .long("get"))
                    .arg(Arg::with_name("KEY")
                        .required(true)
                        .index(1))
                    )
        .subcommand(SubCommand::with_name("rm")
                    .arg(Arg::with_name("rm")
                        .long("rm"))
                    .arg(Arg::with_name("KEY")
                        .required(true)
                        .index(1))
                    )
        .get_matches();

    if let Some(_matches) = matches.subcommand_matches("set") {
        eprintln!("unimplemented");
        std::process::exit(1);
    }else if let Some(_matches) = matches.subcommand_matches("get") {
        eprintln!("unimplemented");
        std::process::exit(1);
    }else if let Some(_matches) = matches.subcommand_matches("rm") {
        eprintln!("unimplemented");
        std::process::exit(1);
    }else {
        std::process::exit(1);
    }
}