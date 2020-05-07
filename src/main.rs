use std::io::{self, Write};
use std::time::Duration;

use serialport::SerialPortSettings;

const PORT_NAME: &str = "/dev/ttyUSB0";
const BAUD_RATE: u32 = 115200;
const TIMEOUT: u64 = 10;
const BUFFER_SIZE: usize = 1000;

fn main() {
    let mut settings: SerialPortSettings = Default::default();
    settings.timeout = Duration::from_millis(TIMEOUT);
    settings.baud_rate = BAUD_RATE;

    match serialport::open_with_settings(&PORT_NAME, &settings) {
        Ok(mut port) => {
            let mut serial_buf: Vec<u8> = vec![0; BUFFER_SIZE];
            println!("Receiving data on {} at {} baud:", &PORT_NAME, &settings.baud_rate);
            loop {
                match port.read(serial_buf.as_mut_slice()) {
                    Ok(t) => io::stdout().write_all(&serial_buf[..t]).unwrap(),
                    Err(ref e) if e.kind() == io::ErrorKind::TimedOut => (),
                    Err(e) => eprintln!("{:?}", e),
                }
            }
        }
        Err(e) => {
            eprintln!("Failed to open \"{}\". Error: {}", PORT_NAME, e);
            ::std::process::exit(1);
        }
    }

}

fn process_buffer(buf: &Vec<u8>) {
    for byte in buf {
        match byte {
            10 => println!("{}", "carriage return"),
            13 => println!("{}", "newline"),
            _ => ()
        }
    }
}
