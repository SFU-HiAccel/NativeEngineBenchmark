import pandas as pd
import os

# List of CSV files
csv_files = [
    "G1_1e8_1e1_0_0.csv",
    "G1_1e8_1e2_0_0.csv",
    "G1_1e8_1e3_0_0.csv",
    "G1_1e8_1e4_0_0.csv",
    "G1_1e8_1e5_0_0.csv",
    "G1_1e8_1e6_0_0.csv",
    "G1_1e8_1e7_0_0.csv",
    "G1_1e8_1e8_0_0.csv",

]

# Conversion function
def convert_to_parquet(csv_file):
    try:
        # Read the CSV file
        df = pd.read_csv(csv_file)
        
        # Determine Parquet file name
        parquet_file = csv_file.replace(".csv", ".parquet")
        
        # Save as Parquet file
        df.to_parquet(parquet_file, index=False)
        print(f"Converted {csv_file} to {parquet_file}")
    except Exception as e:
        print(f"Error converting {csv_file}: {e}")

# Iterate over the list and convert each file
for csv_file in csv_files:
    if os.path.exists(csv_file):
        convert_to_parquet(csv_file)
    else:
        print(f"File {csv_file} not found.")