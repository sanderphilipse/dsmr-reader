use std::io::{BufRead, BufReader, Read, ErrorKind};
use std::time::Duration;
use std::thread;
use std::sync::mpsc::{self, Sender, Receiver};
use dsmr_reader::*;

use chrono::{DateTime, NaiveDateTime, FixedOffset};
use influx_db_client::{Point, Points, Value, Client, Precision, points};
use serialport::{SerialPortSettings};
use tokio::prelude::*;

const PORT_NAME: &str = "/dev/ttyUSB0";
const BAUD_RATE: u32 = 115_200;
const TIMEOUT: u64 = 1000;
const ELECTRICITY_READING_LOW_IDENT: &str = "1-0:1.8.1";
const ELECTRICITY_READING_NORMAL_IDENT: &str = "1-0:1.8.2";
const ELECTRICITY_READING_RETURNED_LOW: &str = "1-0:2.8.1";
const ELECTRICITY_READING_RETURNED_NORMAL: &str = "1-0:2.8.2";
const ELECTRICITY_TIMESTAMP: &str = "0-0:1.0.0";
const ELECTRICITY_POWER_DELIVERED: &str = "1-0:1.7.0";
const ELECTRICITY_POWER_RECEIVED: &str = "1-0:2.7.0";
const GAS_READING: &str = "0-1:24.2.1";
const DATE_FORMAT: &str = "%y%m%d%H%M%S";
const HOUR: i32 = 3600;
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
    let meter_thread = thread::spawn(|| get_meter_data(port, sender));
    let db_thread = thread::spawn(||  save_meter_data(influx_db, receiver) );
    meter_thread.join().unwrap();
    db_thread.join().unwrap().await;
}
