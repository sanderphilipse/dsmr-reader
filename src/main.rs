use std::io::{BufRead, BufReader, Read, ErrorKind};
use std::time::Duration;
use std::thread;
use std::sync::mpsc::{self, Sender, Receiver};

use chrono::{DateTime, NaiveDateTime, FixedOffset};
use influx_db_client::{Point, Points, Value, Client, Precision, points};
use serialport::{SerialPortSettings};
use tokio::prelude::*;

const PORT_NAME: &str = "/dev/ttyUSB0";
const BAUD_RATE: u32 = 115200;
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

async fn save_meter_data<'a>(db: Client, receiver: mpsc::Receiver::<UsageData>) {
    loop {
        let data = receiver.recv().unwrap();
        println!("Received message with timestamp {}", data.electricity_timestamp);
        let electricity_reading_low_tariff = create_point("electricity_reading_low_tariff", data.electricity_reading_low_tariff, data.electricity_timestamp);
        let electricity_reading_normal_tariff = create_point("electricity_reading_normal_tariff", data.electricity_reading_normal_tariff, data.electricity_timestamp);
        let electricity_returned_reading_low_tariff = create_point("electricity_returned_reading_low_tariff", data.electricity_returned_reading_low_tariff, data.electricity_timestamp);
        let electricity_returned_reading_normal_tariff = create_point("electricity_returned_reading_normal_tariff", data.electricity_returned_reading_normal_tariff, data.electricity_timestamp);
        let power_receiving = create_point("power_receiving", data.power_receiving, data.electricity_timestamp);
        let power_returning = create_point("power_returning", data.power_returning, data.electricity_timestamp);
        let gas_reading = create_point("gas_reading", data.gas_reading, data.gas_timestamp);
        let points = points!(
            electricity_reading_low_tariff,
            electricity_reading_normal_tariff,
            electricity_returned_reading_low_tariff,
            electricity_returned_reading_normal_tariff,
            power_receiving,
            power_returning,
            gas_reading
        );
        match db.write_points(points, Some(Precision::Milliseconds), None).await {
            Ok(_) => { println!("Saved message"); continue },
            Err(_) => continue
        };
    }

}

fn create_point(name: &str, value: Measurement, timestamp: DateTime<FixedOffset>) -> Point {
    Point::new(name)
        .add_timestamp(timestamp.timestamp())
        .add_field("value", Value::Float(value.value))
        .add_tag("unit", Value::String(value.unit))
}

fn get_meter_data(port: Box<dyn serialport::SerialPort>, sender: mpsc::Sender<UsageData> ) {
    let mut reader = BufReader::new(port);
    let mut lines_iter = reader.by_ref().lines();
    loop {
        match lines_iter.next() {
            Some(Ok(l)) if l.starts_with('/') => {
                println!("Received message, parsing now");
                sender.send(parse_message(lines_iter
                    .by_ref()
                    .map(|c| c.unwrap())
                    .take_while(|c| !c.starts_with('!'))
                    .collect()).unwrap()).unwrap()
                },
            _ => continue
        }
    }
}

async fn setup_database(db_name: &str) -> Result<Client, influx_db_client::Error> {
    let mut client = Client::default();
    client.switch_database(db_name);
    if !client.ping().await {
        client.create_database(DEFAULT_DATABASE_NAME).await?;
    }
    Ok(client)
}

fn parse_message(message: Vec<String>) -> Result<UsageData, ErrorKind> {
    let electricity_timestamp = parse_date(find_message(&message, ELECTRICITY_TIMESTAMP)?, DATE_FORMAT)?;
    let electricity_reading_low_tariff = parse_measurement(find_message(&message, ELECTRICITY_READING_LOW_IDENT)?)?;
    let electricity_reading_normal_tariff = parse_measurement(find_message(&message, ELECTRICITY_READING_NORMAL_IDENT)?)?;
    let electricity_returned_reading_low_tariff = parse_measurement(find_message(&message, ELECTRICITY_READING_RETURNED_LOW)?)?;
    let electricity_returned_reading_normal_tariff = parse_measurement(find_message(&message, ELECTRICITY_READING_RETURNED_NORMAL)?)?;
    let power_receiving = parse_measurement(find_message(&message, ELECTRICITY_POWER_DELIVERED)?)?;
    let power_returning = parse_measurement(find_message(&message, ELECTRICITY_POWER_RECEIVED)?)?;
    let gas = find_message(&message, GAS_READING)?;
    let (gas_reading, gas_timestamp) = split_gas(gas)?;
    let result = UsageData {
        electricity_timestamp,
        power_receiving,
        power_returning,
        electricity_reading_low_tariff,
        electricity_reading_normal_tariff,
        electricity_returned_reading_normal_tariff,
        electricity_returned_reading_low_tariff,
        gas_reading,
        gas_timestamp
    };
    Ok(result)
}

fn parse_measurement(value: &str) -> Result<Measurement, ErrorKind> {
    println!("Parsing measurement {}", value);
    let deliminator = value.find('*').ok_or(ErrorKind::InvalidData)?;
    println!("identified deliminator");
    Ok(Measurement {
        value: value[0..deliminator].parse::<f64>().map_err(|_|ErrorKind::InvalidData)?,
        unit: value[deliminator+1..value.len()].to_string()
    })
}

fn parse_date(date: &str, fmt: &str) -> Result<DateTime<FixedOffset>, ErrorKind> {
    println!("Parsing date {}", date);
    let cest: FixedOffset = FixedOffset::east(2 * HOUR);
    let cet: FixedOffset = FixedOffset::east(HOUR);
    if let Ok(naive_date) = NaiveDateTime::parse_from_str(date, fmt) {
        let offset = match date.chars().last(){
            Some('W') => cet,
            Some('S') => cest,
            _ => return Err(ErrorKind::InvalidData)
        };
        Ok(DateTime::from_utc(naive_date, offset))
    } else {
        println!("Error in date parsing");
        Err(ErrorKind::InvalidData)
    }
}


fn split_gas(gas: &str) -> Result<(Measurement, DateTime<FixedOffset>), ErrorKind> {
    println!("Parsing gas {}", gas);
    let gas_offset = gas.find(')').ok_or(ErrorKind::InvalidData)?;
    let gas_timestamp = parse_date(&gas[1..gas_offset], DATE_FORMAT)?;
    let gas_reading = parse_measurement(&gas[gas_offset+2..gas.len()])?;
    println!("Successfully parsed gas");
    Ok((gas_reading, gas_timestamp))
}

fn find_message<'a>(message: &'a [String], ident: &str) -> Result<&'a str, ErrorKind> {
    let mut message_iter = message.iter();
    match message_iter.find(|m| m.starts_with(ident)) {
        Some(s) => {
            if let Some(offset) = &s.find('(') {
                Ok(&s[offset+1..s.len()-1])
            } else {
                Err(ErrorKind::InvalidData)
            }
        },
        None => Err(ErrorKind::InvalidData)
    }
}

struct UsageData {
    electricity_timestamp: DateTime<FixedOffset>,
    power_receiving: Measurement,
    power_returning: Measurement,
    electricity_returned_reading_low_tariff: Measurement,
    electricity_returned_reading_normal_tariff: Measurement,
    electricity_reading_low_tariff: Measurement,
    electricity_reading_normal_tariff: Measurement,
    gas_reading: Measurement,
    gas_timestamp: DateTime<FixedOffset>
}

struct Measurement {
    value: f64,
    unit: String
}