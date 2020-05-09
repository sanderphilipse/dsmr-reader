use std::io::{BufRead, BufReader, Read, ErrorKind};
use std::time::Duration;
use chrono::{DateTime, NaiveDateTime, FixedOffset};

use serialport::{SerialPortSettings};

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

fn main() {
    let mut settings: SerialPortSettings = Default::default();
    settings.timeout = Duration::from_millis(TIMEOUT);
    settings.baud_rate = BAUD_RATE;

    let port = serialport::open_with_settings(&PORT_NAME, &settings).unwrap();
    println!("Receiving data on {} at {} baud:", &PORT_NAME, &settings.baud_rate);
    let mut reader = BufReader::new(port);
    let mut lines_iter = reader.by_ref().lines();
    let mut buffer: Vec<UsageData> = Vec::new();
    loop {
        match lines_iter.next() {
            Some(Ok(l)) if l.starts_with('/') => 
                buffer.push(parse_message(lines_iter
                    .by_ref()
                    .map(|c| c.unwrap())
                    .take_while(|c| !c.starts_with('!'))
                    .collect()).unwrap()),
            _ => continue
        }
    }
}

fn parse_message(message: Vec<String>) -> Result<UsageData, ErrorKind> {
    let electricity_timestamp = parse_date(find_message(&message, ELECTRICITY_TIMESTAMP)?, DATE_FORMAT)?;
    let electricity_reading_low_tariff = str_to_u32(find_message(&message, ELECTRICITY_READING_LOW_IDENT)?)?;
    let electricity_reading_high_tariff = str_to_u32(find_message(&message, ELECTRICITY_READING_NORMAL_IDENT)?)?;
    let electricity_returned_reading_low_tariff = str_to_u32(find_message(&message, ELECTRICITY_READING_RETURNED_LOW)?)?;
    let electricity_returned_reading_high_tariff = str_to_u32(find_message(&message, ELECTRICITY_READING_RETURNED_NORMAL)?)?;
    let power_receiving = str_to_u32(find_message(&message, ELECTRICITY_POWER_DELIVERED)?)?;
    let power_returning = str_to_u32(find_message(&message, ELECTRICITY_POWER_RECEIVED)?)?;
    let gas = find_message(&message, GAS_READING)?;
    let (gas_reading, gas_timestamp) = match split_gas(gas) {
        Some((reading, timestamp)) => (reading.parse::<u32>().map_err(|_| ErrorKind::InvalidData)?, parse_date(timestamp, DATE_FORMAT)?),
        None => return Err(ErrorKind::InvalidData)
    };
    let result = UsageData {
        electricity_timestamp,
        power_receiving,
        power_returning,
        electricity_reading_low_tariff,
        electricity_reading_high_tariff,
        electricity_returned_reading_high_tariff,
        electricity_returned_reading_low_tariff,
        gas_reading,
        gas_timestamp
    };
    Ok(result)
}

fn str_to_u32(value: &str) -> Result<u32, ErrorKind> {
    value.parse::<u32>().map_err(|_| ErrorKind::InvalidData)
}

fn parse_date(date: &str, fmt: &str) -> Result<DateTime<FixedOffset>, ErrorKind> {
    let cest: FixedOffset = FixedOffset::east(2 * HOUR);
    let cet: FixedOffset = FixedOffset::east(HOUR);
    if let Ok(naive_date) = NaiveDateTime::parse_from_str(date, fmt) {
        let offset = match date.chars().last(){
            Some('W') => cet,
            Some('S') => cest,
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
    power_receiving: u32,
    power_returning: u32,
    electricity_returned_reading_low_tariff: u32,
    electricity_returned_reading_high_tariff: u32,
    electricity_reading_low_tariff: u32,
    electricity_reading_high_tariff: u32,
    gas_reading: u32,
    gas_timestamp: DateTime<FixedOffset>
}
