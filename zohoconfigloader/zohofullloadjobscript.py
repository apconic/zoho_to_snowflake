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
    import AnalyticsClient
    logger.info("AnalyticsClient module loaded successfully.")
except ModuleNotFoundError as e:
    logger.error(f"Failed to import AnalyticsClient: {e}")
    raise

# Import your config loader
try:
    from zohoconfigloader.zoho_config_loader import ZohoConfigLoader
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
    CLIENT_ID = config_loader.secrets["CLIENT_ID"]
    CLIENT_SECRET = config_loader.secrets["CLIENT_SECRET"]
    REFRESH_TOKEN = config_loader.secrets["REFRESH_TOKEN"]
    ORG_ID = config_loader.secrets["ORG_ID"]
    WORKSPACE_ID = config_loader.secrets["WORKSPACE_ID"]
    # AWS_ACCESS_KEY_ID = secrets["AWS_ACCESS_KEY_ID"]
    # AWS_SECRET_ACCESS_KEY = secrets["AWS_SECRET_ACCESS_KEY"]
    # AWS_REGION = secrets["AWS_REGION"]

    # Initialize AnalyticsClient
    try:
        client = AnalyticsClient(CLIENT_ID, CLIENT_SECRET, REFRESH_TOKEN)
        logger.info("Zoho AnalyticsClient initialized.")
    except Exception as e:
        logger.error(f"Failed to initialize ZohoConfigLoader: {e}")
        raise
    workspace_api = client.WorkspaceAPI(client, ORG_ID, WORKSPACE_ID)
    bulk_api = client.get_bulk_instance(ORG_ID, WORKSPACE_ID)

    # S3 client
    s3_bucket = config_loader.get("bucket") 
    s3_client = config_loader.s3
    #bulk_api = client.get_bulk_instance(ORG_ID, WORKSPACE_ID)
    #view_id_list = config_loader.ensure_viewid_mapping()
    #Read viewId and Table Name from manifest_key
    #manifest_key ="tempchunk/zoho_2025-10-28/chunk_0.json"
    manifest_key = args["MANIFEST_KEY"]
    logger.info(f"Path for Zoho table and view Id mapping {manifest_key}")
    response = s3_client.get_object(Bucket=s3_bucket, Key=manifest_key)
    manifest = json.loads(response["Body"].read().decode("utf-8"))


# =============================================================================
#     s3_client = boto3.client(
#         "s3",
#         aws_access_key_id=AWS_ACCESS_KEY_ID,
#         aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
#         region_name=AWS_REGION
#     )
# =============================================================================

# =============================================================================
#     # Provide a list of view IDs to process
#     views_list = workspace_api.get_views()
#     view_id_list = [list(obj.values())[0] for obj in views_list if obj.get("viewType") == "Table"]
#     
# =============================================================================
    view_id_list = manifest["viewIds"]
    landing_zone = config_loader.get("zoho_tbl_key") 
    landing_zone =f"{landing_zone}fullload/"
    logger.info("FIle loading to path {landing_zone}")
    count=0
    for table_name,view_id in view_id_list.items():
        # Dynamically fetch view details and name for each ID
        view_details = client.get_view_details(view_id, {"withInvolvedMetaInfo": True})
        
        view_name = view_details["viewName"].lower().replace(" ","_")
       
        base_prefix = f"{landing_zone}{view_name}"

        # Start async export per view
        print(f"Starting bulk export for {view_name} ({view_id})...")
        job_id = bulk_api.initiate_bulk_export(view_id, "CSV", config={})
        print(f"Export job started. Job ID: {job_id}")

        # Poll job status
        status = "IN_PROGRESS"
        while status == "IN_PROGRESS":
            details = bulk_api.get_export_job_details(job_id)
            status = details.get("jobStatus")
            print(f"Job Status: {status}")
            if status == "IN_PROGRESS":
                time.sleep(5)
        
        # Download + Upload to S3
        if status == "JOB COMPLETED":
            file_timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
            local_file = f"{view_name}_{file_timestamp}.csv"
            bulk_api.export_bulk_data(job_id, local_file)
            print(f"Async Export completed. File saved as {local_file}")

            today = datetime.now().strftime("%Y%m%d")
            s3_key = f"{base_prefix}/{local_file}"

            s3_client.upload_file(local_file, s3_bucket, s3_key)
            print(f"Uploaded file to S3: s3://{s3_bucket}/{s3_key}")
        else:
            print("Export failed:", details)
        if count >=15:
            break
        count+=1

except Exception as e:
    print(f"Failed to perform async export: {e}")
    raise