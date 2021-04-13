use std::fs::File;
use std::io::prelude::*;
use std::io::Write;
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize)]
pub struct Move {
    direction: Direction,
    length: u8
}

#[derive(Serialize, Deserialize)]
enum Direction {
    Up,
    Down,
    Left,
    Right
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let a = bson::doc! {"direction": "Up", "length": 5};
    
    let mut file = File::create("serde-test.bson")?;
    a.to_writer(&mut file)?;
    file.sync_all()?;

    let mut file = File::open("serde-test.bson")?;
    let b = bson::Document::from_reader(&mut file)?;
    println!("{}", b);

    Ok(())
}