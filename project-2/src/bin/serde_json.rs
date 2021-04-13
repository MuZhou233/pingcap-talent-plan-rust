use std::fs::File;
use std::io::prelude::*;
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize)]
struct Move {
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

fn main() -> std::io::Result<()> {
    let a = Move{direction: Direction::Up, length: 5};

    let mut file = File::create("serde-test.json")?;
    file.write_all(serde_json::to_string(&a).unwrap().as_bytes())?;
    file.sync_all()?;

    let mut buf = String::new();
    let mut file = File::open("serde-test.json")?;
    file.read_to_string(&mut buf)?;
    let b: Move = serde_json::from_slice(buf.as_bytes())?;
    println!("{}", serde_json::to_string(&b).unwrap());

    Ok(())
}