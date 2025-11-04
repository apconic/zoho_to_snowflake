# -*- coding: utf-8 -*-
"""
Created on Thu Oct 30 01:56:09 2025

@author: admin
"""

import time
import logging
from typing import Optional, Tuple, Dict, Any
import os 
import pandas as pd
import tempfile
import json
import uuid

from __future__ import annotations
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Any, Dict, Optional

@dataclass
class JobSummary:
    job_id: str
    status: str                      # "JOB COMPLETED" | "FAILED" | "ABORTED" | "JOB ABORTED" | "TIMED OUT"
    details: Dict[str, Any]          # raw SDK details (safe to keep as dict)
    export_started_at_utc: str       # ISO8601 like "2025-10-31T12:34:56Z"
    export_completed_at_utc: str     # ISO8601
    duration_seconds: float

    org_id: Optional[str] = None
    workspace_id: Optional[str] = None
    table_name: Optional[str] = None
    load_type: Optional[str] = None  # "Full" | "Incremental"

    def to_dict(self) -> Dict[str, Any]:
        """Plain dict for CSV/JSON writers."""
        return asdict(self)

    @staticmethod
    def _now_iso() -> str:
        return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")

    @classmethod
    def start(cls, *, job_id: str, org_id: str, workspace_id: str,
              table_name: Optional[str] = None, load_type: Optional[str] = None) -> "JobSummary":
        """Create a summary at job start; caller fills finish info later."""
        return cls(
            job_id=job_id,
            status="IN_PROGRESS",
            details={},
            export_started_at_utc=cls._now_iso(),
            export_completed_at_utc="",   # fill on completion
            duration_seconds=0.0,
            org_id=org_id,
            workspace_id=workspace_id,
            table_name=table_name,
            load_type=load_type,
        )

    def finish(self, *, status: str, details: Dict[str, Any]) -> "JobSummary":
        """Mark completion and compute duration."""
        self.status = status
        self.details = details or {}
        end_iso = self._now_iso()
        self.export_completed_at_utc = end_iso

        # compute duration from the two ISO strings (robust & no tz issues)
        start_dt = datetime.fromisoformat(self.export_started_at_utc.replace("Z", "+00:00"))
        end_dt = datetime.fromisoformat(end_iso.replace("Z", "+00:00"))
        self.duration_seconds = (end_dt - start_dt).total_seconds()
        return self


class ZohoBulkJobHelper:
    """
    Reusable helper to start a Zoho Analytics bulk SQL export job or table load
    and wait for its completion with timeout/backoff.
    """
    def __init__(
        self,
        analytics_client,
        org_id: str,
        workspace_id: str,
        poll_interval_seconds: int = 3,
        max_wait_seconds: int = 300,
        logger: Optional[logging.Logger] = None,
    ):
        self.ac = analytics_client
        self.org_id = org_id
        self.workspace_id = workspace_id
        self.poll_interval_seconds = poll_interval_seconds
        self.max_wait_seconds = max_wait_seconds
        self.log = logger or logging.getLogger(__name__)

# =============================================================================
#     def run_export_job(
#         self,
#         viewId_or_sql_query: str,
#         export_type: str = "tblLoad",
#         response_format: str = "CSV",
#     ) -> Tuple[str, dict]:
#         """
#         Initiates a bulk export job for the given SQL and blocks until completion
#         or failure. Returns (job_id, final_job_details).
# 
#         Raises:
#             TimeoutError: if the job does not complete within max_wait_seconds
#             RuntimeError: if the job finishes in a non-success state
#         """
#         bulk_api = self.ac.get_bulk_instance(self.org_id, self.workspace_id)
#         if export_type == "tblLoad":
#             job_id = bulk_api.initiate_bulk_export(viewId_or_sql_query, response_format, config={})
#         elif export_type == "query":
#             job_id = bulk_api.initiate_bulk_export_using_sql(viewId_or_sql_query, response_format)
#         else:
#             raise ValueError("export_type must be either 'tblLoad' or 'query'")
#         
#         self.log.info("Started bulk export job %s", job_id)
#         job_start_time,job_end_time,job_status,job_details = self._wait_for_job_completion(bulk_api, job_id)
#         return job_id, job_start_time,job_end_time,job_status,job_details
# 
#     # If you want the wait logic reusable on its own, keep it separate:
#     def _wait_for_job_completion(self, bulk_api, job_id: str) -> dict:
#         start_time = time.time()
#         sleep_s = self.poll_interval_seconds
# 
#         while True:
#             details = bulk_api.get_export_job_details(job_id)
#             status = details.get("jobStatus")
#             self.log.debug("Job %s status: %s", job_id, status)
# 
#             if status in {"JOB COMPLETED", "FAILED", "ABORTED", "JOB ABORTED"}:
#                 end_time = time.time()
#                 break
# 
#             if (time.time() - start_time) > self.max_wait_seconds:
#                 raise TimeoutError(
#                     f"TImeoutErrorr : Bulk job {job_id} did not complete within {self.max_wait_seconds}s"
#                 )
# 
#             time.sleep(sleep_s)
#             sleep_s = min(sleep_s + 1, 15)  # gentle backoff
# 
#         if status != "JOB COMPLETED":
#             raise RuntimeError(
#                 f"Bulk job {job_id} ended with status: {status}. Details: {details}"
#             )
# 
#         return start_time,end_time,status,details
# =============================================================================
    def run_export_job(
        self,
        viewId_or_sql_query: str,
        export_type: str = "tblLoad",
        response_format: str = "CSV",
        *,
        table_name: Optional[str] = None,
        load_type: Optional[str] = None,
        ) -> JobSummary:
        bulk_api = self.ac.get_bulk_instance(self.org_id, self.workspace_id)
    
        if export_type == "tblLoad":
            job_id = bulk_api.initiate_bulk_export(viewId_or_sql_query, response_format, config={})
        elif export_type == "query":
            job_id = bulk_api.initiate_bulk_export_using_sql(viewId_or_sql_query, response_format)
        else:
            raise ValueError("export_type must be either 'tblLoad' or 'query'")
    
        summary = JobSummary.start(
            job_id=job_id,
            org_id=self.org_id,
            workspace_id=self.workspace_id,
            table_name=table_name,
            load_type=load_type,
        )
    
        # wait loop
        start_dt = datetime.fromisoformat(summary.export_started_at_utc.replace("Z", "+00:00"))
        sleep_s = self.poll_interval_seconds
        terminal = {"JOB COMPLETED", "FAILED", "ABORTED", "JOB ABORTED"}
    
        while True:
            details = bulk_api.get_export_job_details(job_id)
            status = details.get("jobStatus")
            if status in terminal:
                break
    
            elapsed = (datetime.now(timezone.utc) - start_dt).total_seconds()
            if elapsed > self.max_wait_seconds:
                # finish with timeout status
                return summary.finish(status="TIMED OUT", details={"timeoutSeconds": self.max_wait_seconds})
    
            time.sleep(sleep_s)
            sleep_s = min(sleep_s + 1, 15)
    
        # finalize
        summary.finish(status=status, details=details)
    
        if status != "JOB COMPLETED":
            # you still return the summary (so caller can log), but raise if desired
            raise RuntimeError(f"Bulk job {job_id} ended with status: {status}. Details: {details}")
    
        return summary

 # ---------- NEW: Step 1. Get rowcount & maxdate from Zoho ----------
    def get_table_stats_from_zoho(self, table_name: str, last_modified_col: str = 'Last Modified Time') -> Tuple[int, Optional[str]]:
         sql = f'SELECT COUNT(1) AS rowcount, MAX("{last_modified_col}") AS maxdate FROM "{table_name}"'
# =============================================================================
#          job_id, _ = self.run_export_job(sql, export_type="query", response_format="CSV")
# =============================================================================
         summary = self.run_export_job(viewId_or_sql_query=sql,export_type="query",response_format= "CSV") 
    
         # small one-row CSV → read to pandas via /tmp
         bulk_api = self.ac.get_bulk_instance(self.org_id, self.workspace_id)
         fd, tmp_path = tempfile.mkstemp(prefix="zoho_stats_", suffix=".csv", dir=".")
         os.close(fd)
         try:
             bulk_api.export_bulk_data(summary.job_id, tmp_path)
             df = pd.read_csv(tmp_path)
             rc = int(df.loc[0, "rowcount"])
             md = df.loc[0, "maxdate"] if "maxdate" in df.columns else None
             return rc, md
         finally:
             try: os.remove(tmp_path)
             except OSError: pass
# ---------- helpers used to get maxdate and rowcount from local file ----------
    def _profile_csv_locally(
        self,
        local_csv_path: str,
        last_modified_col: str,
        *,
        chunk_rows: int,
        encoding: str,
    ) -> Tuple[int, Optional[str]]:
        """
        Reads a CSV from local path in pandas CHUNKS and computes:
          - total rowcount
          - max(last_modified_col)
        Uses `usecols` to only load the needed column for efficiency.
        """
        total = 0
        max_dt = None
    
        for chunk in pd.read_csv(
            local_csv_path,
            chunksize=chunk_rows,
            encoding=encoding,
            dtype=str,
            usecols=[last_modified_col],   #only loadind date column
        ):
            total += len(chunk)
    
            # Handle missing or invalid dates gracefully
            dt_series = pd.to_datetime(chunk[last_modified_col], errors="coerce", utc=True)
            chunk_max = dt_series.max()
            if pd.notna(chunk_max):
                max_dt = max_dt if (max_dt and max_dt > chunk_max) else chunk_max
    
        max_iso = max_dt.isoformat().replace("+00:00", "Z") if max_dt is not None else None
        return total, max_iso
 
  # ---------- NEW: Step 3. Save file to s3 ----------    
    def upload_to_s3(self,job_id,local_path,s3_client,bucket,key
                     ,*
                     ,last_modified_col: str = "Last Modified Time"
                     ,chunk_rows_default: int = 200_000
                     ,csv_encoding: str = "utf-8"
                    ) -> Tuple[str, int, str]:
        
        
        bulk_api = self.ac.get_bulk_instance(self.org_id, self.workspace_id)
        bulk_api.export_bulk_data(job_id, local_path)
        s3_client.upload_file(local_path
                       ,bucket
                       ,key)
        # 5) Profile with pandas in CHUNKS to avoid memory pressure
        df_rowcount, df_maxdate = self._profile_csv_locally(
            local_csv_path=local_path,
            last_modified_col=last_modified_col,
            chunk_rows=chunk_rows_default,
            encoding=csv_encoding)

        return f"s3://{bucket}/{key}",df_rowcount,df_maxdate
    
    def _write_manifest_record(
        self,
        record: Dict[str, Any],
        ts,
        *,
        s3_bucket: str,
        s3_prefix_manifest: str,
        table_name: str,
    ) -> str:
        """
        Writes one JSON line per run. No in-place append (S3 has no append),
        so create a unique object per record keyed by date/job.
        """
        #ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        key = f"{s3_prefix_manifest}{table_name}/manifest_{ts}_{uuid.uuid4().hex}.json"
        body = (json.dumps(record, ensure_ascii=False) + "\n").encode("utf-8")
        self.s3.put_object(Bucket=s3_bucket, Key=key, Body=body, ContentType="application/json")
        return f"s3://{s3_bucket}/{key}"
    