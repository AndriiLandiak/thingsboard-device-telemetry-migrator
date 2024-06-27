# ThingsBoard Device Telemetry Migrator

## Overview

The **ThingsBoard Device Telemetry Migrator** is a Python tool designed to facilitate the migration of telemetry data for a specified list of device names in ThingsBoard. This tool provides a seamless way to export telemetry data to a file and then import it back into a ThingsBoard account. It is beneficial for backup, restoration, and data migration tasks.

## Features

- **Export Telemetry Data**: Export telemetry data for a list of device names into a CSV file. The data includes device ID, telemetry key, timestamp, value, and value type.
- **Import Telemetry Data**: Import telemetry data from a CSV file into a ThingsBoard account, ensuring data integrity and consistency.
- **Retry Logic**: Robust retry mechanisms to handle transient errors during export and import operations.
- **Flexible Time Range**: Specify start and end timestamps to export telemetry data for a desired period.
- **Chunked Data Retrieval**: Efficient data retrieval in chunks to handle large datasets without overwhelming the ThingsBoard API.

## Installation

To use this tool, you need to install the `ThingsBoard REST API client` and the `tenacity` library. Follow the instructions below to install these dependencies.

### Installing ThingsBoard REST API Client

The ThingsBoard REST API Client helps you interact with the ThingsBoard REST API from your Python script. With Python Rest Client you can programmatically create assets, devices, customers, users, and other entities and their relations in ThingsBoard.

The recommended method for installing the REST Client is via pip:

```bash
pip3 install tb-rest-client
```
For more information, visit the [ThingsBoard Python REST Client GitHub repository](https://github.com/thingsboard/thingsboard-python-rest-client).

### Installing Tenacity

To install the tenacity library (to process retry logic, otherwise delete module and @retry from the file), run:

```bash
pip3 install tenacity
```
## Usage

### Exporting telemetry data for devices by names

```bash
python3 import_export_tool.py export --host HOST --username USERNAME --password PASSWORD --fileName FILENAME --startTs START_TS --endTs END_TS --deviceNames 'DEVICE_A,DEVICE_B' [--keys 'a,b,c'] [--chunkLimit 1024] [--timeLimit 60]
```

- **keys**: (Optional) Specify telemetry keys to export. If not specified, all keys will be exported.
- **chunkLimit**: (Optional) The maximum number of records to fetch in one chunk. The default is 1024.
- **timeLimit**: (Optional) The time interval, in minutes, for fetching chunkLimit elements. The default is 60 minutes.

**Note**: If `--keys` are not specified, all keys will be used for the export. The `--timeLimit` is used to fetch `--chunkLimit` elements for a specific time interval. For example, if the interval between startTs and endTs is 2 days, the tool will fetch `--chunkLimit` elements in steps defined by `--timeLimit` until it reaches the end of the 2-day period.

### Importing telemetry data for devices by names

```bash
python3 import_export_tool.py import --host HOST --username USERNAME --password PASSWORD --fileName FILENAME
```

## Contributions
Contributions are welcome! If you have suggestions, bug reports, or feature requests, please open an issue or submit a pull request.



