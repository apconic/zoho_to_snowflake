# -*- coding: utf-8 -*-
"""
Created on Thu Nov 13 19:12:19 2025

@author: admin
"""

import boto3
import pandas as pd
import json
import os
from datetime import datetime
import re

def extract_table_name(filename):
    # Remove extension
    name = filename.replace(".csv", "")
    # Remove trailing _digits (timestamp)
    cleaned = re.sub(r"_\d+$", "", name)
    # Add .csv back
    return f"{cleaned}"


# S3 setup
bucket = "retail-denmart-zoho-test"
prefix = "zohodata/fullload/"
local_json_path = "max_last_modified_times.json"

# Initialize S3 client
s3 = boto3.client("s3")

# List all CSV files under the prefix
response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
files = [
    obj["Key"]
    for obj in response.get("Contents", [])
    if obj["Key"].endswith(".csv")
    and not any(exclude in obj["Key"] for exclude in ["Full_Incremental_","Full_incremental_test" ,"Users", "reporting_tabs", "salesforce"])
]
# Initialize result dictionary
result = {}

for file_key in files:
    try:
        # Extract table name from filename
        filename = os.path.basename(file_key)
        table_name = extract_table_name(filename)

        # Download file locally
        local_file = f"./datafile/{filename}.csv"
        s3.download_file(bucket, file_key, local_file)

        # Read CSV and check for 'Last Modified Time'
        df = pd.read_csv(local_file)
        if "Last Modified Time" in df.columns:
            df["Last Modified Time"] = pd.to_datetime(df["Last Modified Time"], errors="coerce")
            max_time = df["Last Modified Time"].max()
            result[table_name] = max_time.isoformat() if pd.notnull(max_time) else None
        else:
            result[table_name] = None

        print(f"{table_name}: {result[table_name]}")
    except Exception as e:
        print(f"Error processing {file_key}: {e}")
        continue

# Save JSON locally
with open(local_json_path, "w") as f:
    json.dump(result, f, indent=2)

print(f"\n✅ JSON saved to {local_json_path}")