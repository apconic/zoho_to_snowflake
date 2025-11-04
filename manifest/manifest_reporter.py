import pandas as pd
from io import StringIO
import boto3
from ZohoConfigLoader.zoho_config_loader import ZohoConfigLoader

class ManifestReporter:
    def __init__(self, config_path=None):
        self.config_loader = ZohoConfigLoader(config_path)
        self.s3 = boto3.client("s3")
        self.manifest_path = self.config_loader.get("manifest_path")

        self.load_report = []
        self.usage_report = []

    def log_load_status(self, module, submodule, table, status, row_count=0, error=None):
        self.load_report.append({
            "Module": module,
            "Submodule": submodule or "root",
            "Table": table,
            "Status": status,
            "RowCount": row_count,
            "Error": error or ""
        })

    def log_api_usage(self, table, row_count):
        units = (row_count // 1000) * 10
        self.usage_report.append({
            "Table": table,
            "RowsProcessed": row_count,
            "EstimatedAPIUnits": units
        })

    def write_manifests(self):
        self._write_csv(self.load_report, "load_manifest.csv")
        self._write_csv(self.usage_report, "api_usage_manifest.csv")

    def _write_csv(self, records, filename):
        if not records:
            print(f"⚠️ No records to write for {filename}")
            return

        df = pd.DataFrame(records)
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)

        bucket, key_prefix = self.config_loader._parse_s3_path(self.manifest_path)
        full_key = f"{key_prefix}{filename}"
        self.s3.put_object(Bucket=bucket, Key=full_key, Body=csv_buffer.getvalue())
        print(f"✅ Manifest written to S3: s3://{bucket}/{full_key}")