use std::io::{BufRead, BufReader, Read, ErrorKind};
use std::time::Duration;
use chrono::{DateTime, NaiveDateTime, FixedOffset};

use serialport::{SerialPortSettings};

const PORT_NAME: &str = "/dev/ttyUSB0";
const BAUD_RATE: u32 = 115200;
const TIMEOUT: u64 = 1000;
const ELECTRICITY_READING_LOW_IDENT: &str = "1-0:1.8.1";
const ELECTRICITY_READING_NORMAL_IDENT: &str = "1-0:1.8.2";
const ELECTRICITY_READING_TWO_LOW_IDENT: &str = "1-0:2.8.1";
const ELECTRICITY_READING_TWO_NORMAL_IDENT: &str = "1-0:2.8.2";
const ELECTRICITY_TIMESTAMP: &str = "0-0:1.0.0";
const ELECTRICITY_POWER_DELIVERED: &str = "1-0:1.7.0";
const ELECTRICITY_POWER_RECIEVED: &str = "1-0:2.7.0";
const GAS_READING: &str = "0-1:24.2.1";
const DATE_FORMAT: &str = "%y%m%d%H%M%S";
const HOUR: i32 = 3600;
const SUMMER_TIME: FixedOffset = FixedOffset::east(2 * HOUR);
const WINTER_TIME: FixedOffset = FixedOffset::east(1 * HOUR);

fn main() {
    let mut settings: SerialPortSettings = Default::default();
    settings.timeout = Duration::from_millis(TIMEOUT);
    settings.baud_rate = BAUD_RATE;

    let port = serialport::open_with_settings(&PORT_NAME, &settings).unwrap();
    println!("Receiving data on {} at {} baud:", &PORT_NAME, &settings.baud_rate);
    let mut reader = BufReader::new(port);
    let mut lines_iter = reader.by_ref().lines();
    loop {
        match lines_iter.next() {
            Some(Ok(l)) if l.starts_with('/') => 
                save_message(lines_iter
                    .by_ref()
                    .map(|c| c.unwrap())
                    .take_while(|c| !c.starts_with('!'))
                    .collect()).unwrap(),
            _ => continue
        }
    }
}

fn save_message(message: Vec<String>) -> Result<(), ErrorKind> {
    let elec_timestamp = NaiveDateTime::parse_from_str(find_message(&message, ELECTRICITY_TIMESTAMP)?, "");
    let elec_reading_low = find_message(&message, ELECTRICITY_READING_LOW_IDENT);
    let elec_reading_high = find_message(&message, ELECTRICITY_READING_NORMAL_IDENT);
    let elec_reading_two_low = find_message(&message, ELECTRICITY_READING_TWO_LOW_IDENT);
    let elec_reading_two_normal = find_message(&message, ELECTRICITY_READING_TWO_NORMAL_IDENT);
    let elec_power = find_message(&message, ELECTRICITY_POWER_DELIVERED);
    let elec_power_received = find_message(&message, ELECTRICITY_POWER_RECIEVED);
    let gas = find_message(&message, GAS_READING)?;
    let (gas_reading, gas_timestamp) = match split_gas(gas) {
        Some(s) => s,
        None => return Err(ErrorKind::InvalidData)
    };

    Ok(())
}

fn parse_date(date: &str, fmt: &str) -> Result<DateTime<FixedOffset>, ErrorKind> {
    if let Ok(naive_date) = NaiveDateTime::parse_from_str(date, fmt) {
        let offset = match date.chars().last(){
            Some('W') => WINTER_TIME,
            Some('S') => SUMMER_TIME,
            _ => return Err(ErrorKind::InvalidData)
        };
        return Ok(DateTime::from_utc(naive_date, offset))
    }
    Err(ErrorKind::InvalidData)
}


fn split_gas(gas: &str) -> Option<(&str, &str)> {
    let gas_offset = gas.find(')')?;
    let gas_timestamp = &gas[1..gas_offset];
    let gas_reading = &gas[gas_offset+2..gas.len()];
    Some((gas_reading, gas_timestamp))
}

fn find_message<'a>(message: &'a Vec<String>, ident: &str) -> Result<&'a str, ErrorKind> {
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

pub struct UsageData {
    pub power_timestamp: u32,
    pub power_delivered: u32,
    pub power_returned: u32,
    pub energy_delivered_tariff1: u32,
    pub energy_delivered_tariff2: u32,
    pub energy_returned_tariff1: u32,
    pub energy_returned_tariff2: u32,
    pub power_delivered_l1: u32,
    pub power_delivered_l2: u32,
    pub power_delivered_l3: u32,
    pub gas_timestamp: u32,
    pub gas_delivered: u32,
}
