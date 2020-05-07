use std::io::*;
use std::time::Duration;

use serialport::*;

const PORT_NAME: &str = "/dev/ttyUSB0";
const BAUD_RATE: u32 = 115200;
const TIMEOUT: u64 = 10;
const BUFFER_SIZE: usize = 1000;

fn main() {
    let mut settings: SerialPortSettings = Default::default();
    settings.timeout = Duration::from_millis(TIMEOUT);
    settings.baud_rate = BAUD_RATE;

    match serialport::open_with_settings(&PORT_NAME, &settings) {
        Ok(port) => {
            println!("Receiving data on {} at {} baud:", &PORT_NAME, &settings.baud_rate);
            let mut reader = BufReader::new(port);
            let mut lines_iter = reader.by_ref().lines();
            loop {
                match lines_iter.next() {
                    Some(Ok(l)) if l.starts_with("//") => 
                        save_message(lines_iter
                            .by_ref()
                            .map(|c| {
                                let result = c.unwrap();
                                println!("In line {}", result);
                                result
                            })
                            .take_while(|c| !c.starts_with("!"))
                            .collect()),
                    _ => continue
                }
            }
            
            
            // loop {
            //     let message = match lines_iter.next() {
            //         Some(Ok(s)) if s.starts_with("//") => parse_message(lines_iter),
            //         _ => break
            //     };
            //     println!("{}", message);
            // }
        }
        Err(e) => {
            eprintln!("Failed to open \"{}\". Error: {}", PORT_NAME, e);
            ::std::process::exit(1);
        }
    }
}

fn save_message(message: Vec<String>) {
    println!("Received message {:?}", message);
    for line in message {
        println!{"{}", line}
    }
}

