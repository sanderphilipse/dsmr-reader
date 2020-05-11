use std::io::{BufRead, BufReader};
use std::time::Duration;
use std::thread;
use std::sync::mpsc::{self, Sender, Receiver};
use dsmr_reader::*;

use serialport::{SerialPortSettings};
use tokio::prelude::*;
use influx_db_client::Precision;

const PORT_NAME: &str = "/dev/ttyUSB0";
const BAUD_RATE: u32 = 115_200;
const TIMEOUT: u64 = 1000;
const DEFAULT_DATABASE_NAME: &str = "smart_meter";
const BUFFER_SIZE: usize = 60;

#[tokio::main]
async fn main() {
    let influx_db = setup_database(DEFAULT_DATABASE_NAME).await.unwrap();
    println!("Successfully connected to Influx DB");
    let (sender, receiver): (Sender<UsageData>, Receiver<UsageData>) = mpsc::channel();
    let mut settings: SerialPortSettings = Default::default();
    settings.timeout = Duration::from_millis(TIMEOUT);
    settings.baud_rate = BAUD_RATE;
    let port = serialport::open_with_settings(&PORT_NAME, &settings).unwrap();
    println!("Receiving data on {} at {} baud:", &PORT_NAME, &settings.baud_rate);
    let data_iter = BufReader::new(port).lines().map(|l| l.unwrap());
    println!("Created iterator");
    let data_thread = thread::spawn(|| get_meter_data(Box::new(data_iter), sender));
    println!("Created data thread");
    loop {
        let data = receiver.recv();
        match data {
            Ok(d) => {
                influx_db.write_points(usage_to_points(&d).unwrap(), Some(Precision::Seconds), None).await.unwrap();
                println!("{:?}", d);
            },
            Err(_) => continue
        }
        
        data_thread.thread().unpark();
    }

}
