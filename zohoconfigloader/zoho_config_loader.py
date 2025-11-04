# -*- coding: utf-8 -*-
"""
Created on Wed Sep  3 14:02:54 2025

@author: admin
"""

import boto3
import json
import datetime
#import sys
#import datetime
# current_dir = os.path.dirname(os.path.abspath(__file__))
# sys.path.insert(0, current_dir)
# #sys.path.insert(0, r"C:\glue_full_incremental_runner\zohoconfigloader")
from AnalyticsClient import AnalyticsClient
from zoneinfo import ZoneInfo  # available in Python 3.9+

class ZohoConfigLoader:

    def __init__(self, config_path="s3://zoho-082264365426/zohoconfig/zoho_config.json",runningfor="Zoho"):
        #s3 Client 
        self.s3_client = boto3.client("s3")
        self.config_path = config_path
        self.config = self._load_config()
        self.tbl_mapping = self._load_tbl_config()
        region = self.get(key="region")
        secret_name = self._load_config()
        #secret_name = self.get(key="secret_manager_name")
        self.secrets = self._fetch_secrets(secret_name=secret_name, region_name=region)
        #Zoho Analytics Client
        self.ac_client = AnalyticsClient(client_id=self.secrets["CLIENT_ID"]
                                    ,client_secret= self.secrets["CLIENT_SECRET"]
                                    ,refresh_token= self.secrets["REFRESH_TOKEN"])


    def _fetch_secrets(self, secret_name, region_name):
        """
        Fetches secrets from AWS Secrets Manager.
        """
        session = boto3.session.Session()
        client = session.client(service_name='secretsmanager', region_name=region_name)
        response = client.get_secret_value(SecretId=secret_name)
        secret = response.get('SecretString')
        return json.loads(secret)

    def _load_config(self):
        bucket, key = self._parse_s3_path(self.config_path)
        response = self.s3_client.get_object(Bucket=bucket, Key=key)
        # Use strict=False to allow single quotes or non-standard JSON
        return json.loads(response['Body'].read().decode('utf-8'))
    
    def _load_tbl_config(self):
        bucket = self.get(key="bucket")
        tbl_mapping_key = self.get(key="tbl_mapping_key")
        response = self.s3_client.get_object(Bucket=bucket, Key=tbl_mapping_key)
        return json.loads(response['Body'].read().decode('utf-8'))

    def _parse_s3_path(self, s3_path):
        s3_path = s3_path.replace("s3://", "")
        parts = s3_path.split("/", 1)
        return parts[0], parts[1]

    def get(self, key):
        return self.config.get(key)
    
    def get_zoho_url(self):
       zoho_url = self.get(key="zoho_url")
       workspace_id =  self.get(key="workspace_id")
       return f"{zoho_url}{workspace_id}/view/"
   
    def split_mapping_for_jobs(self, runningfor="Zoho", mapping=None):
        """
        Splits the table list into N exclusive index ranges for Python slicing,
        and includes config paths for tbl_mapping and viewId mapping.
    
        Parameters:
            runningfor (str): Label for the job context (e.g., "Zoho" or "Salesforce")
            mapping (dict): Dictionary with keys like 'zoho_tables' or 'salesforce_tables'.
                            If None, fetches via ensure_viewid_mapping()
    
        Returns:
            List of dictionaries with:
                - 'start': start index (inclusive)
                - 'end': end index (exclusive)
                - 'tbl_mapping_key': S3 path for module-table mapping
                - 'zoho_tbl_viewid_key': S3 path for table-viewId mapping
        """
        tbl_key = "zoho_tables" if runningfor == "Zoho" else "salesforce_tables"
    
        if mapping is None:
            mapping = self.tbl_mapping
    
        job_count = int(self.config.get("jobscount", 3))
        table_list = mapping.get(tbl_key, [])
        total_tables = len(table_list)
        base_chunk = total_tables // job_count
        remainder = total_tables % job_count
    
        tbl_mapping_key = self.config.get("tbl_mapping_key")
        zoho_tbl_viewid_key = self.config.get("zoho_tbl_viewid_key")
    
        ranges = []
        start = 0
        for i in range(job_count):
            extra = 1 if i < remainder else 0
            end = start + base_chunk + extra  # end is exclusive
            ranges.append({
                "start": start,
                "end": end,
                "tbl_mapping_key": tbl_mapping_key,
                "zoho_tbl_viewid_key": zoho_tbl_viewid_key
            })
            start = end
    
        return ranges
    
    def ensure_viewid_mapping(self):
        """
        Fetches all views from Zoho Analytics, filters for tables, creates a mapping of
        {table_name: viewId}, and saves it to S3 only if not already present.
        """
        try:
            bucket = self.get(key="bucket")
            config_key = self.get(key="zoho_tbl_viewid_key")
            response = self.s3_client.get_object(Bucket=bucket, Key=config_key)
            mapping = json.loads(response['Body'].read().decode('utf-8'))
        except self.s3_client.exceptions.NoSuchKey:
            mapping = {}

        # Only fetch and save if mapping is empty (i.e., file not found in S3)
        if not mapping:
            analytics_client = AnalyticsClient(
                client_id=self.secrets["CLIENT_ID"],
                client_secret=self.secrets["CLIENT_SECRET"],
                refresh_token=self.secrets["REFRESH_TOKEN"]
            )
            workspace = analytics_client.get_workspace_instance(
                org_id=self.secrets["ORG_ID"],
                workspace_id=self.secrets["WORKSPACE_ID"]
            )
            views = workspace.get_views()  # List of dicts

            # Filter for tables only
            zoho_view_table_mapping = {
                v["viewName"]: v["viewId"]
                for v in views
                if v.get("viewType") == "Table"
            }

            # Save to S3
            self.s3_client.put_object(
                Bucket=bucket,
                Key=config_key,
                Body=json.dumps(zoho_view_table_mapping).encode("utf-8")
            )
           
        else:
            # mapping already exists, just return it
            zoho_view_table_mapping = mapping
            
        
        range = self.split_mapping_for_jobs(runningfor="Zoho",mapping=self.tbl_mapping)
        
        return range

# =============================================================================
#     def get_partitioned_tables_and_viewids(self, job_range):
#         """
#         Given a job range dictionary, returns:
#         - A sliced list of zoho_tables from tbl_mapping_key
#         - The full viewId mapping from zoho_tbl_viewid_key
#     
#         Parameters:
#             job_range (dict): Dictionary with keys:
#                 - 'start': int
#                 - 'end': int
#                 - 'tbl_mapping_key': str (S3 key for table list)
#                 - 'zoho_tbl_viewid_key': str (S3 key for viewId mapping)
#     
#         Returns:
#             tuple: (list_of_tables_in_range, full_viewid_mapping_dict)
#         """
#         bucket = self.get("bucket")
#     
#         # Fetch zoho_tables list from tbl_mapping_key
#         tbl_response = self.s3.get_object(Bucket=bucket, Key=job_range["tbl_mapping_key"])
#         tbl_data = json.loads(tbl_response["Body"].read().decode("utf-8"))
#         zoho_tables = tbl_data.get("zoho_tables", [])
#         sliced_tables = zoho_tables[job_range["start"]:job_range["end"]]
#     
#         # Fetch full viewId mapping from zoho_tbl_viewid_key
#         viewid_response = self.s3.get_object(Bucket=bucket, Key=job_range["zoho_tbl_viewid_key"])
#         viewid_mapping = json.loads(viewid_response["Body"].read().decode("utf-8"))
#     
#         return sliced_tables, viewid_mapping
# =============================================================================
    
    def save_partitioned_tables_and_viewids(self, job_range, chunk_no=0):
        """
        Builds and writes a manifest JSON to S3 for the given job range,
        and returns the manifest object for direct use in downstream jobs.
    
        Parameters:
            job_range (dict): Contains 'start', 'end', 'tbl_mapping_key', 'zoho_tbl_viewid_key'
            chunk_no (int): Chunk number used in the output filename
    
        Returns:
            dict: The manifest JSON object with tables, viewIds, start, end
        """
        bucket = self.get("bucket")
        today = datetime.datetime.now().strftime("%Y-%m-%d")
    
        # Load zoho_tables and slice
        tbl_response = self.s3_client.get_object(Bucket=bucket, Key=job_range["tbl_mapping_key"])
        tbl_data = json.loads(tbl_response["Body"].read().decode("utf-8"))
        zoho_tables = tbl_data.get("zoho_tables", [])
        sliced_tables = zoho_tables[job_range["start"]:job_range["end"]]
    
        # Load full viewId mapping
        viewid_response = self.s3_client.get_object(Bucket=bucket, Key=job_range["zoho_tbl_viewid_key"])
        full_viewid_mapping = json.loads(viewid_response["Body"].read().decode("utf-8"))
    
        # Filter viewIds for sliced tables only
        filtered_viewids = {tbl: full_viewid_mapping[tbl] for tbl in sliced_tables if tbl in full_viewid_mapping}
    
        # Build manifest
        manifest = {
            "tables": sliced_tables,
            "viewIds": filtered_viewids,
            "start": job_range["start"],
            "end": job_range["end"]
        }
    
        # Write to S3
        temp_folder = self.get("temp_storage")
        manifest_key = f"{temp_folder}zoho_{today}/chunk_{chunk_no}.json"
        self.s3_client.put_object(
            Bucket=bucket,
            Key=manifest_key,
            Body=json.dumps(manifest, indent=2).encode("utf-8")
        )
        
        print(f"Manifest written to s3://{bucket}/{manifest_key}")
        return manifest_key

    def utc_now(self):
        #return datetime.datetime.utcnow().replace(microsecond=0).isoformat() + 'Z'
        """Return current timestamp in IST timezone (Asia/Kolkata)"""
        return datetime.now(ZoneInfo("Asia/Kolkata")).replace(microsecond=0).isoformat()
    
    def write_temp_object_manifest(self,run_id: str, object_name: str,
                                   last_success_max_modified: str,
                                   last_success_run_id: str,
                                   load_type: str,
                                   rows_loaded_last_run: int,
                                   zoho_total_rows_now: int ,
                                   s3_data_prefix: str):
        key = f"zoho/_control/temp/{run_id}/object={object_name}.json"
        payload = {
            "object_name": object_name,
            "last_success_max_modified": last_success_max_modified,
            "last_success_run_id": last_success_run_id,
            "last_load_type": load_type,  # FULL or INCREMENTAL
            "zoho_total_rows_now": zoho_total_rows_now,
            "rows_loaded_last_run": rows_loaded_last_run,
            "updated_at": self.utc_now(),
            "watermark_field": "ModifiedTime",
            "s3_data_prefix": s3_data_prefix
        }
        body = json.dumps(payload, separators=(',', ':')).encode('utf-8')
        bucket=self.get("bucket")
        self.s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=body,
            ContentType='application/json',
            ServerSideEncryption='AES256'  # optional but recommended
        )
        return f"s3://{bucket}/{key}"