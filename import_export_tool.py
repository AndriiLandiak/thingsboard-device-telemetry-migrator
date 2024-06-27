import argparse
import logging
import csv
import json
from tb_rest_client.rest_client_ce import RestClientCE
from tb_rest_client.rest import ApiException
from tb_rest_client.rest_client_base import EntityId
from tenacity import retry, stop_after_attempt, wait_fixed

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(module)s - %(lineno)d - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

def parse_args():
    parser = argparse.ArgumentParser(description="Export and import time-series data from ThingsBoard")
    parser.add_argument("action", choices=["export", "import"], help="Action to perform")
    parser.add_argument("--host", required=True, help="ThingsBoard host URL")
    parser.add_argument("--username", required=True, help="Username for ThingsBoard")
    parser.add_argument("--password", required=True, help="Password for ThingsBoard")
    parser.add_argument("--fileName", required=True, help="CSV file name")
    parser.add_argument("--startTs", type=int, help="Start timestamp in milliseconds (for export)")
    parser.add_argument("--endTs", type=int, help="End timestamp in milliseconds (for export)")
    parser.add_argument("--deviceNames", help="Comma-separated list of device names (for export)")
    parser.add_argument("--keys", help="Comma-separated list of keys (optional, for export)", default=None)
    parser.add_argument("--chunkLimit", type=int, default=1024, help="Maximum number of records to fetch in each chunk")
    parser.add_argument("--timeLimit", type=int, default=60, help="Time range in minutes for each chunk")
    return parser.parse_args()

def get_all_keys(client, entity_type, entity_id):
    keys_response = client.get_timeseries_keys_v1(EntityId(entity_type=entity_type, id=entity_id))
    return ','.join(keys_response)

def get_device_id(client, device_name):
    try:
        device = client.get_tenant_device(device_name=device_name)
        return device.id.id
    except ApiException as e:
        logging.error(f"Error fetching device ID for {device_name}: {e}")
        return None

def infer_type_and_convert(value):
    try:
        if value.lower() == 'true':
            return True, 'boolean'
        elif value.lower() == 'false':
            return False, 'boolean'
        try:
            int_value = int(value)
            return int_value, 'int'
        except ValueError:
            pass
        try:
            float_value = float(value)
            return float_value, 'double'
        except ValueError:
            pass
        try:
            json_value = json.loads(value.replace("'", '"'))
            return json_value, 'json'
        except json.JSONDecodeError:
            pass
        return value, 'string'
    except Exception as e:
        logging.warning(f"Could not infer type for value: {value}, error: {e}")
        return value, 'string'

@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
def fetch_timeseries(client, entity_id, keys, start_ts, end_ts, chunk_limit):
    return client.get_timeseries(
        entity_id=entity_id, 
        keys=keys, 
        start_ts=start_ts, 
        end_ts=end_ts, 
        limit=chunk_limit,
        order_by='ASC'
    )

def export_timeseries(client, file_name, start_ts, end_ts, device_names, keys=None, chunk_limit=1024, time_limit=60):
    time_limit_ms = time_limit * 60 * 1000
    with open(file_name, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['deviceId', 'key', 'ts', 'value', 'type'])

        for device_name in device_names.split(','):
            device_id = get_device_id(client, device_name)
            logging.info(f"Processing telemetry export for device: {device_name} (ID: {device_id})")

            if not keys:
                keys = get_all_keys(client, 'DEVICE', device_id)
            
            current_start_ts = start_ts
            while current_start_ts < end_ts:
                current_end_ts = min(current_start_ts + time_limit_ms, end_ts)
                try:
                    timeseries_response = fetch_timeseries(client, EntityId(id=device_id, entity_type='DEVICE'), keys, current_start_ts, current_end_ts, chunk_limit)

                    total_fetched = 0
                    for key, data_points in timeseries_response.items():
                        total_fetched += len(data_points)
                        for data_point in data_points:
                            ts = data_point.get('ts')
                            raw_value = data_point.get('value')
                            value, value_type = infer_type_and_convert(raw_value)

                            writer.writerow([device_id, key, ts, value, value_type])

                    if total_fetched > 0:
                        logging.info(f"Fetched {total_fetched} records for device with name {device_name} for period {current_start_ts} to {current_end_ts}")
                except Exception as e:
                    logging.error(f"Failed to fetch timeseries data for device {device_name} from {current_start_ts} to {current_end_ts}: {e}")

                current_start_ts = current_end_ts

            logging.info(f"Finished processing timeseries for device with name {device_name} for the period {start_ts} to {end_ts}")

@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
def save_telemetry(client, entity_id, body):
    client.save_entity_telemetry(entity_id=entity_id, scope='ANY', body=body)

def import_timeseries(client, file_name, log_interval=100):
    with open(file_name, mode='r') as file:
        reader = csv.DictReader(file)
        telemetry_data = {}
        for row in reader:
            device_id = row['deviceId']
            key = row['key']
            ts = int(row['ts'])
            value = row['value']
            value_type = row['type']

            if device_id not in telemetry_data:
                telemetry_data[device_id] = []

            value_converted = value
            if value_type == 'boolean':
                value_converted = (value.lower() == 'true')
            elif value_type == 'int':
                value_converted = int(value)
            elif value_type == 'double':
                value_converted = float(value)
            elif value_type == 'json':
                try:
                    value_converted = json.loads(value.replace("'", '"'))
                except json.JSONDecodeError as e:
                    logging.error(f"Failed to parse JSON value: {value}, error: {e}")
                    continue

            telemetry_data[device_id].append({"ts": ts, "values": {key: value_converted}})

        for device_id, data_points in telemetry_data.items():
            entity_id = EntityId(id=device_id, entity_type='DEVICE')
            counter = 0
            for data_point in data_points:
                body = {
                    "ts": data_point["ts"],
                    "values": data_point["values"]
                }
                try:
                    save_telemetry(client, entity_id, body)
                except Exception as e:
                    logging.error(f"Failed to save telemetry for device {device_id}, data: {body}, error: {e}")
                    continue
                
                counter += 1
                if counter % log_interval == 0:
                    logging.info(f"Saved {counter} telemetry entries for device {device_id}")

            logging.info(f"Finished saving telemetry for device {device_id}, total entries: {counter}")

def main():
    args = parse_args()

    with RestClientCE(base_url=args.host) as client:
        try:
            client.login(username=args.username, password=args.password)
            if args.action == 'export':
                if not args.startTs or not args.endTs or not args.deviceNames:
                    logging.error("startTs, endTs, and deviceNames are required for export")
                    return
                export_timeseries(client, args.fileName, args.startTs, args.endTs, args.deviceNames, args.keys, args.chunkLimit, args.timeLimit)
            elif args.action == 'import':
                import_timeseries(client, args.fileName)
        except ApiException as e:
            logging.exception(e)

if __name__ == '__main__':
    main()

