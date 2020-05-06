extern crate serial;

use std::io;
use std::time;

use serial::prelude::*;


fn main() {
    let serial_device = String::from("/dev/ttyUSB0");
    let mut port = serial::open(&serial_device).unwrap();
    interact(&mut port).unwrap();
}

fn interact<T: SerialPort>(port: &mut T) -> io::Result<()> {
    port.reconfigure(&|settings| {
        settings.set_baud_rate(serial::Baud115200)?;
        settings.set_char_size(serial::Bits8);
        settings.set_parity(serial::ParityNone);
        settings.set_stop_bits(serial::Stop1);
        settings.set_flow_control(serial::FlowNone);
        Ok(())
    })?;

    port.set_timeout(time::Duration::from_millis(1000))?;

    let mut buf: Vec<u8> = (0..255).collect();

    port.write_all(&buf[..])?;
    port.read_exact(&mut buf[..])?;

    Ok(())
}
