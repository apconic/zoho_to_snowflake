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
	try:
		# config_loader = ZohoConfigLoader()
		config_loader = ZohoConfigLoader(
			config_path=args["CONFIG_URI"],
			runningfor=args["RUNNING_FOR"]
		)
		logger.info("ZohoConfigLoader initialized.")
	except Exception as e:
		logger.error(f"Failed to initialize ZohoConfigLoader: {e}")
		raise

	# Extract credentials
	client_id = config_loader.secrets["client_id"]
	client_secret = config_loader.secrets["client_secret"]
	refresh_token = config_loader.secrets["refresh_token"]
	org_id = config_loader.secrets["org_id"]
	workspace_id = config_loader.secrets["workspace_id"]

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

	delete_key = config_loader.get("tables_allowing_deletion") 
	response = s3_client.get_object(Bucket=s3_bucket, Key=delete_key)
	deletion_enabled_tables = json.loads(response["Body"].read().decode("utf-8"))

	landing_zone = config_loader.get("zoho_tbl_key") 
	landing_zone =f"{landing_zone}delete_enabled_tables"
	logger.info("FIle loading to path {landing_zone}")
	count=0

	for table in deletion_enabled_tables:

		view_id = table["view_id"]
		view_name = table["view_name"].lower().replace(" ","_")
		unique_col = table["unique_col"]

		col_config = {
			"selectedColumns": [unique_col]
		}

		base_prefix = f"{landing_zone}"

		# Start async export per view
		print(f"Starting bulk export for {view_name} ({view_id})...")
		job_id = bulk_api.initiate_bulk_export(view_id, "CSV", col_config)
		print(f"Export job started. Job ID: {job_id}")

		# Poll job status
		status = "JOB IN PROGRESS"
		while status == "JOB IN PROGRESS":
			details = bulk_api.get_export_job_details(job_id)
			status = details.get("jobStatus")
			print(f"Job Status for {view_name}: {status}")
			if status == "JOB IN PROGRESS":
				time.sleep(120)

		# Download + Upload to S3
		if status == "JOB COMPLETED":
			curr_year = datetime.now().strftime("%Y")
			curr_month = datetime.now().strftime("%m")
			curr_day = datetime.now().strftime("%d")
			local_file = f"{view_name}_ids.csv"

			bulk_api.export_bulk_data(job_id, local_file)
			print(f"Async Export completed. File saved as {local_file}")

			s3_key = f"{base_prefix}/{curr_year}/{curr_month}/{curr_day}/{local_file}"

			s3_client.upload_file(local_file, s3_bucket, s3_key)
			print(f"Uploaded file to S3: s3://{s3_bucket}/{s3_key}")
		else:
			print("Export failed:", details)
		if count >=70:
			break
		count+=1
		time.sleep(30)

except Exception as e:
	print(f"Failed to perform async export: {e}")
	raise
