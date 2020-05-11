use std::time::Duration;
use std::thread;
use std::sync::mpsc::{self, Sender, Receiver};
use dsmr_reader::*;

use serialport::{SerialPortSettings};
use tokio::prelude::*;
use futures::try_join;

const PORT_NAME: &str = "/dev/ttyUSB0";
const BAUD_RATE: u32 = 115_200;
const TIMEOUT: u64 = 1000;
const DEFAULT_DATABASE_NAME: &str = "smart_meter";

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
    let data_future = get_meter_data(port, sender);
    let db_future = save_meter_data(influx_db, receiver);
    try_join!(data_future, db_future);
}
