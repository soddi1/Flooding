import logging
from pybgpstream import BGPStream
import pandas as pd
import ipaddress
import time
from datetime import datetime

# Set up logging
logging.basicConfig(filename='rrc26.txt', level=logging.INFO, format='%(asctime)s %(message)s')

# Function to create a BGPStream instance with filters
def create_bgpstream(start_time, end_time, collectors):
    logging.info("Creating BGPStream instance with filters")
    stream = BGPStream()
    for collector in collectors:
        logging.info(f"Adding collector: {collector}")
        stream.add_filter('collector', collector)
    stream.add_interval_filter(start_time, end_time)
    logging.info(f"Time interval filter added: {start_time} to {end_time}")
    return stream

# Function to load IP ranges from CSV
def load_ip_ranges(csv_file):
    logging.info(f"Loading IP ranges from CSV: {csv_file}")
    df = pd.read_csv(csv_file)
    ip_ranges = []
    for _, row in df.iterrows():
        ip_range = ipaddress.ip_network(row['CIDR'], strict=False)
        logging.info(f"Loaded IP range: {ip_range}")
        ip_ranges.append(ip_range)
    logging.info(f"Total IP ranges loaded: {len(ip_ranges)}")
    return ip_ranges

# Function to process BGP records and save matched records to a file
def process_bgp_records(stream, ip_ranges):
    logging.info("Processing BGP records")
    record_count = 0
    element_count = 0
    match_count = 0
    with open('matched_records.txt', 'a') as match_file:
        for rec in stream.records():
            record_count += 1
            timestamp = datetime.utcfromtimestamp(rec.time).strftime('%Y-%m-%d %H:%M:%S')
            # logging.info(f"Record {record_count} at {timestamp}")
            if record_count % 100 == 0:
                logging.info(f"Processed {record_count} records so far")
            for elem in rec:
                element_count += 1
                if elem.type == "A" or elem.type == "W":
                    prefix_str = elem.fields['prefix']
                    try:
                        prefix = ipaddress.ip_network(prefix_str, strict=False)
                        for ip_range in ip_ranges:
                            if prefix.version == ip_range.version and prefix.subnet_of(ip_range):
                                match_info = {
                                    "timestamp": timestamp,
                                    "type": elem.type,
                                    "peer_address": elem.peer_address,
                                    "prefix": prefix,
                                    "as_path": elem.fields.get('as-path', 'N/A'),
                                    "next_hop": elem.fields.get('next-hop', 'N/A'),
                                    "communities": elem.fields.get('communities', 'N/A')
                                }
                                match_info_str = (
                                    f"{match_info['timestamp']} {match_info['type']} {match_info['peer_address']} "
                                    f"{match_info['prefix']} AS Path: {match_info['as_path']} "
                                    f"Next Hop: {match_info['next_hop']} "
                                    f"Communities: {match_info['communities']}"
                                )
                                logging.info(f"Match found: {match_info_str}")
                                match_file.write(match_info_str + '\n')
                                match_count += 1
                                break
                    except ValueError:
                        continue
    logging.info(f"Total records processed: {record_count}")
    logging.info(f"Total elements processed: {element_count}")
    logging.info(f"Total matches found: {match_count}")

# Retry mechanism
def fetch_bgp_data_with_retries(start_time, end_time, collectors, ip_ranges, max_retries=5):
    for attempt in range(max_retries):
        try:
            logging.info(f"Attempt {attempt + 1} to fetch BGP data")
            stream = create_bgpstream(start_time, end_time, collectors)
            process_bgp_records(stream, ip_ranges)
            return  # Exit after successful data processing
        except Exception as e:
            logging.error(f"Error: {e}. Retrying... ({attempt + 1}/{max_retries})")
            time.sleep(5)  # Wait before retrying
    logging.error("Max retries reached. Exiting.")

# Main function
def main():
    start_time = 1654041600  # June 1, 2022
    end_time = 1667174400    # October 31, 2022
    collectors = ['rrc26']
    logging.info("Starting main function")
    ip_ranges = load_ip_ranges('pakistan_ip_data_with_cidr.csv')
    fetch_bgp_data_with_retries(start_time, end_time, collectors, ip_ranges)
    logging.info("Finished main function")

if __name__ == "__main__":
    main()
