import pandas as pd
import ipaddress

# Load the CSV data
file_path = 'pakistan_ip_data.csv'  # replace with the path to your CSV file
df = pd.read_csv(file_path, header=None, names=['Start IP', 'End IP', 'Country Code', 'Country', 'Region', 'City'])

# Filter the rows relevant to Pakistan
pakistan_data = df[(df['Country Code'] == 'PK') | (df['Country'] == 'Pakistan')]


# Function to calculate CIDR notation from start and end IP addresses
def calculate_cidr(start_ip, end_ip):
    start_ip = ipaddress.IPv4Address(start_ip)
    end_ip = ipaddress.IPv4Address(end_ip)
    network = ipaddress.summarize_address_range(start_ip, end_ip)
    return str(next(network))

# Apply the function to create the CIDR column
pakistan_data['CIDR'] = pakistan_data.apply(lambda row: calculate_cidr(row['Start IP'], row['End IP']), axis=1)

# Save the updated DataFrame to a new CSV file
output_file_path = 'pakistan_ip_data_with_cidr.csv'
pakistan_data.to_csv(output_file_path, index=False)

