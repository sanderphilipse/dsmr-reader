# Docs

'dsmr_reader' is a crate to process Dutch smart meter readings and save them in [InfluxDB](https://www.influxdata.com/). It was designed to be run on a Raspberry Pi, interacting with a DSMR 4.0 port. It was only tested with a Landis+Gyr E350 (DMSR 4), so any use outside of that context may not work out of the box.

## Usage

Build the binary ('cargo build --release') and run it.  By default the reader will read from /dev/ttyUSB0 and write to an InfluxDB on localhost:8086 with database name 'smart_meter'. You can specify specific configuration options with --dbhost, --dbport, --dbname and --serialdevice.
