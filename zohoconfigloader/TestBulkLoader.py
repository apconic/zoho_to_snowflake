# -*- coding: utf-8 -*-
"""
Created on Thu Oct 30 02:23:32 2025

@author: admin
"""

from zoho_config_loader import ZohoConfigLoader
# from AnalyticsClient import AnalyticsClient
from zoho_bulk_job_helper import ZohoBulkJobHelper
# import io
# import pandas as pd
# import requests
from datetime import datetime

view_name ="Expenses"
file_timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
local_file = f"{view_name}_{file_timestamp}.csv"
tmp_path=f"./{local_file}"

config_loader = ZohoConfigLoader(config_path="s3://zoho-082264365426/zohoconfig/zoho_config.json",runningfor="Zoho")

landing_zone = config_loader.get("zoho_tbl_key") 
landing_zone =f"{landing_zone}fullload/"

helper = ZohoBulkJobHelper(analytics_client=config_loader.ac_client
                           , org_id=config_loader.secrets["ORG_ID"]
                           , workspace_id=config_loader.secrets["WORKSPACE_ID"])

zoho_count , zoho_maxdate = helper.get_table_stats_from_zoho(table_name="Expenses")

# =============================================================================
# summary = helper.run_export_job(viewId_or_sql_query="100336000000012191"
#                       ,export_type= "tblLoad"
#                        ,response_format= "CSV")
# =============================================================================
summary = helper.run_export_job(viewId_or_sql_query="100336000000012191"
                                ,export_type="tblLoad"
                                ,response_format="CSV"
                                ,table_name="Expenses"
                                ,load_type="FullLoad")

s3_path,df_count , df_maxdate = helper.upload_to_s3(job_id=summary.job_id
                              ,local_path=tmp_path
                              ,s3_client=config_loader.s3_client
                              ,bucket=config_loader.get("bucket")
                              ,key=f"{landing_zone}{local_file}"
                              ,last_modified_col = "Last Modified Time"
                              ,chunk_rows_default = 200_000
                              ,csv_encoding= "utf-8"
                              )

ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
