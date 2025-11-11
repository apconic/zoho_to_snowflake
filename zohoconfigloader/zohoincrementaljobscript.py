# -*- coding: utf-8 -*-
"""
Created on Mon Nov 10 17:22:28 2025

@author: admin
"""

# -*- coding: utf-8 -*-
"""
Created on Thu Oct 16 18:39:47 2025

@author: admin
"""
import time
#import boto3
import json
from datetime import datetime
#from zoho_config_loader import ZohoConfigLoader  # ← Import your class
#AWS Glue Import 
import logging
import sys
from awsglue.utils import getResolvedOptions
import importlib.util
import pandas as pd
from datetime import datetime

def build_s3_key_and_filename(base_prefix, table_name):
    """
    Builds the local filename and full S3 key using the current date.

    Returns:
        tuple: (local_filename, s3_key)
    """
    now = datetime.now()
    local_filename = f"{table_name}.csv"
    s3_key = f"{base_prefix}/{now.year:04d}/{now.month:02d}/{now.day:02d}/{local_filename}"
    return local_filename, s3_key

def get_max_last_modified_time(local_path):
    """
    Reads a CSV file and returns the max 'Last Modified Time' as ISO string.
    
    Parameters:
        local_path (str): Path to the local CSV file.
    
    Returns:
        str or None: ISO 8601 timestamp string if found, else None.
    """
    try:
        df = pd.read_csv(local_path)

        if "Last Modified Time" not in df.columns:
            print(f"Column 'Last Modified Time' not found in {local_path}")
            return None

        df["Last Modified Time"] = pd.to_datetime(df["Last Modified Time"], errors="coerce")
        max_time = df["Last Modified Time"].max()

        if pd.notnull(max_time):
            return max_time.isoformat()
        else:
            print(f"No valid timestamps found in {local_path}")
            return None

    except Exception as e:
        print(f"Error reading {local_path}: {e}")
        return None

#Exclusion of tables which will not to be included in incremental load
excluded_tables = {"Users", "Salesforce_123"}


# Setup structured logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# Check if wheel is present
wheel_loaded = any("zohoconfigloader" in p for p in sys.path)
logger.info(f"Wheel loaded into sys.path? {wheel_loaded}")

# Check if zohoconfigloader package is discoverable
spec = importlib.util.find_spec("zohoconfigloader")
logger.info(f"zohoconfigloader spec found? {'Yes' if spec else 'No'}")

# Confirm AnalyticsClient module is importable
try:
    from AnalyticsClient import AnalyticsClient
    logger.info("AnalyticsClient module loaded successfully.")
except ModuleNotFoundError as e:
    logger.error(f"Failed to import AnalyticsClient: {e}")
    raise

# Import your config loader
try:
    from zohoconfigloader.zoho_config_loader import ZohoConfigLoader
    #from zoho_config_loader import ZohoConfigLoader
    logger.info("ZohoConfigLoader imported successfully.")
except Exception as e:
    logger.error(f"Failed to import ZohoConfigLoader: {e}")
    raise

# Resolve job arguments
try:
    args = getResolvedOptions(sys.argv, [
        "CONFIG_URI",
        "RUNNING_FOR",
        "MANIFEST_KEY"
    ])
    logger.info("Resolved job arguments:")
    for k in args:
        logger.info(f"Glue Parameter - {k}: {args[k]}")
except Exception as e:
    logger.error(f"Failed to resolve job arguments: {e}")
    raise
try:
    # Initialize config loader
    #We need to pass s3 path to ZohoCOnfigLoader and use args 
    # Initialize loader

# =============================================================================
# To Debug Run below line of code and skip line 75 to 79
#     config_loader = ZohoConfigLoader(config_path="s3://zoho-082264365426/zohoconfig/zoho_config.json",runningfor="Zoho")
# =============================================================================
    try:
        config_loader = ZohoConfigLoader(
            config_path=args["CONFIG_URI"],
            runningfor=args["RUNNING_FOR"]
        )
        #config_loader = ZohoConfigLoader()
        logger.info("ZohoConfigLoader initialized.")
    except Exception as e:
        logger.error(f"Failed to initialize ZohoConfigLoader: {e}")
        raise
    
# =============================================================================
#     # Fetch secrets via config loader
#     secrets = config_loader._fetch_secrets(
#         secret_name=config_loader.get("secret_manager_name"),
#         region_name=config_loader.get("region")
#     )
# =============================================================================

    # Extract credentials
    client_id = config_loader.secrets["client_id"]
    client_secret = config_loader.secrets["client_secret"]
    refresh_token = config_loader.secrets["refresh_token"]
    org_id = config_loader.secrets["org_id"]
    workspace_id = config_loader.secrets["workspace_id"]
    # AWS_ACCESS_KEY_ID = secrets["AWS_ACCESS_KEY_ID"]
    # AWS_SECRET_ACCESS_KEY = secrets["AWS_SECRET_ACCESS_KEY"]
    # AWS_REGION = secrets["AWS_REGION"]

    # Initialize AnalyticsClient
    try:
        client = AnalyticsClient(client_id, client_secret, refresh_token)
        logger.info("Zoho AnalyticsClient initialized.")
    except Exception as e:
        logger.error(f"Failed to initialize ZohoConfigLoader: {e}")
        raise
    workspace_api = client.WorkspaceAPI(client, org_id, workspace_id)
    bulk_api = client.get_bulk_instance(org_id, workspace_id)

    # S3 client
    s3_bucket = config_loader.get("bucket") 
    s3_client = config_loader.s3_client
    #bulk_api = client.get_bulk_instance(ORG_ID, WORKSPACE_ID)
    #view_id_list = config_loader.ensure_viewid_mapping()
    #Read viewId and Table Name from manifest_key
    #manifest_key ="tempchunk/zoho_2025-11-05/chunk_1.json"
    manifest_key = args["MANIFEST_KEY"]
    logger.info(f"Path for Zoho table and view Id mapping {manifest_key}")
    response = s3_client.get_object(Bucket=s3_bucket, Key=manifest_key)
    manifest = json.loads(response["Body"].read().decode("utf-8"))


    view_id_list = manifest["viewIds"]
    landing_zone = config_loader.get("zoho_tbl_key") 
    landing_zone =f"{landing_zone}incremental"
    logger.info("FIle loading to path {landing_zone}")
    count=0
    #Load timestamp of tablesfrom max_last_modified_times.json
    mani_key='zohomanifest/max_last_modified_times.json'
    response = s3_client.get_object(Bucket=s3_bucket, Key=mani_key)
    json_content = response['Body'].read().decode('utf-8')
    max_last_modified_times_dict = json.loads(json_content)
    
    for table_name,view_id in view_id_list.items():
        # Dynamically fetch view details and name for each ID
        view_details = client.get_view_details(view_id, {"withInvolvedMetaInfo": True})
        
        view_name = view_details["viewName"].lower().replace(" ","_")
       
        base_prefix = f"{landing_zone}"
       
# → zohodata/incremental/2025/11/10/invoice_items.csv
		#search full load file in s3 with table_name 
		#After searching file load it into pandas dataframe
		#then pick maxdate(LAST_MODIFIED_TIME)
		#store it in variable
        LSMODIFIED_TIME=max_last_modified_times_dict[view_name]
        viewId_or_sql_query = (f'SELECT * FROM "{table_name}" '
                                                     f'WHERE "Last Modified Time" >= \'{LSMODIFIED_TIME}\'')
        # Start async export per view
        print(f"Starting bulk export for {view_name} ({view_id})...")
        #job_id = bulk_api.initiate_bulk_export(view_id, "CSV", config={})
        #job_id = bulk_api.initiate_bulk_export_using_sql(viewId_or_sql_query, "CSV")
        #print(f"Export job started. Job ID: {job_id}")
        try:
            job_id = bulk_api.initiate_bulk_export_using_sql(viewId_or_sql_query, "CSV")
            print(f"Export started for {table_name}. Job ID: {job_id}")
        except Exception as e:
            print(f"Error exporting {table_name}: {e}")
            continue  # Skip to next table

        # Poll job status
        status = "JOB IN PROGRESS"
        while status == "JOB IN PROGRESS":
            details = bulk_api.get_export_job_details(job_id)
            status = details.get("jobStatus")
            print(f"Job Status for {table_name}: {status}")
            if status == "JOB IN PROGRESS":
                time.sleep(120)
        
        # Download + Upload to S3
        if status == "JOB COMPLETED":
            #file_timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
            local_file, s3_key = build_s3_key_and_filename(base_prefix=base_prefix
                                                           ,table_name=view_name)
            print("Local filename:", local_file)
            print("S3 key:", s3_key)
            bulk_api.export_bulk_data(job_id, local_file)
            print(f"Async Export completed. File saved as {local_file}")

            today = datetime.now().strftime("%Y%m%d")
            #s3_key = f"{base_prefix}{local_file}"

            s3_client.upload_file(local_file, s3_bucket, s3_key)
            max_time = get_max_last_modified_time(local_file)
            max_last_modified_times_dict[view_name] =max_time
            print(f"Uploaded file to S3: s3://{s3_bucket}/{s3_key}")
        else:
            print("Export failed:", details)
        if count >=70:
            break
        count+=1
        time.sleep(30)
    json_data = json.dumps(max_last_modified_times_dict, indent=2)
   
    s3_client.put_object(Bucket=s3_bucket, Key=mani_key, Body=json_data.encode('utf-8'))
    print(f"Uploaded JSON to s3://{s3_bucket}/{mani_key}")


except Exception as e:
    print(f"Failed to perform async export: {e}")
    raise