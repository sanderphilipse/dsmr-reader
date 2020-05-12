use std::io::{BufRead, BufReader};
use std::time::Duration;
use std::thread;
use std::sync::mpsc::{self, Sender, Receiver};
use dsmr_reader::*;
use clap::{App, Arg};

use serialport::{SerialPortSettings};
use influx_db_client::Precision;

const BAUD_RATE: u32 = 115_200;
const TIMEOUT: u64 = 1000;

#[tokio::main]
async fn main() {
    let matches = App::new("DSMR Reader")
        .version("0.2")
        .author("Sander Philipse <sander.philipse@gmail.com>")
        .about("Reads Dutch smart meter (DSMR) data from a serial device and writes to Influx DB.")
        .arg(Arg::with_name("dbhost")
            .short("h")
            .long("dbhost")
            .help("Sets the database host.")
            .default_value("http://localhost")
            .takes_value(true))
        .arg(Arg::with_name("dbport")
            .short("p")
            .long("dbport")
            .help("Sets the database port.")
            .default_value("8086")
            .takes_value(true))
        .arg(Arg::with_name("dbname")
            .short("n")
            .long("dbname")
            .help("Sets the database name.")
            .default_value("smart_meter")
            .takes_value(true))
        .arg(Arg::with_name("serialdevice")
            .short("s")
            .long("serialdevice")
            .help("Sets the serial device to listen to.")
            .default_value("/dev/ttyUSB0")
            .takes_value(true))
        .get_matches();

    

    // Gets a value for config if supplied by user, or defaults to "default.conf"
    let dbhost = matches.value_of("dbhost").unwrap();
    let dbport = matches.value_of("dbport").unwrap();
    let dbname = matches.value_of("dbname").unwrap();
    let serialdevice = matches.value_of("serialdevice").unwrap();
    
    let influx_db = setup_database(dbhost, dbport, dbname).await.unwrap();
    println!("Successfully connected to Influx DB");
    let (sender, receiver): (Sender<UsageData>, Receiver<UsageData>) = mpsc::channel();
    let mut settings: SerialPortSettings = Default::default();
    settings.timeout = Duration::from_millis(TIMEOUT);
    settings.baud_rate = BAUD_RATE;
    let port = serialport::open_with_settings(&serialdevice, &settings).unwrap();
    println!("Receiving data on {} at {} baud:", &serialdevice, &settings.baud_rate);
    let data_iter = BufReader::new(port).lines().map(|l| l.unwrap());
    let data_thread = thread::spawn(|| get_meter_data(Box::new(data_iter), sender));
    loop {
        let data = receiver.recv();
        match data {
            Ok(data) => {
                influx_db.write_points(usage_to_points(&data).unwrap(), Some(Precision::Seconds), None).await.unwrap();
            },
            Err(_) => continue
        }
        data_thread.thread().unpark();
    }

}
