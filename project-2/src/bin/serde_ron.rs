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
    let a = Move{direction: Direction::Up, length: 5};
    let mut buffer: Vec<u8> = Vec::new();

    ron::ser::to_string(&a)?;
    
    buffer.write_all(ron::ser::to_string(&a).unwrap().as_bytes());

    println!("{}", std::str::from_utf8(&buffer).unwrap());

    Ok(())
}