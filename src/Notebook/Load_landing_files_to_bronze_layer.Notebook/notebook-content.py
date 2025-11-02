# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "4ea4f0aa-201f-4f2b-89ac-c06461102d1e",
# META       "default_lakehouse_name": "raqsconsole_LH",
# META       "default_lakehouse_workspace_id": "6df05e67-30af-48c3-8841-b1aa8f82e95e",
# META       "known_lakehouses": [
# META         {
# META           "id": "4ea4f0aa-201f-4f2b-89ac-c06461102d1e"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

import os
import traceback
import datetime
import pyspark.sql.functions as F
from pyspark.sql.utils import AnalysisException
from notebookutils import mssparkutils

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 🔧 Config

RAW_PATH = "Files/raw"
BRONZE_SCHEMA = "bronze"
AUDIT_TABLE = "meta_lakehouse.bronze_load_audit"
APPEND_TIMESTAMP = True
OVERWRITE_MODE = "overwrite"

# Ensure RAW_PATH ends without a slash for consistent string manipulation
RAW_PATH_CLEANED = RAW_PATH.rstrip("/")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 🚀 DISCOVERY STAGE

print("🔍 Scanning raw zone for parquet files...")

# List all files in the raw zone recursively
all_files = []
def list_files_recursive(path):
    try:
        # NOTE: mssparkutils.fs.ls() returns paths that include the full mount point/container
        for item in mssparkutils.fs.ls(path):
            if item.isDir:
                list_files_recursive(item.path)
            elif item.name.endswith(".parquet"):
                all_files.append(item.path)
    except Exception as e:
        print(f"⚠️ Failed to access {path}: {e}")

list_files_recursive(RAW_PATH_CLEANED)

print(f"✅ Found {len(all_files)} parquet files.")

# Group files by logical table (assume folder right above parquet)
table_files = {}
for f in all_files:
    # Relative path should be: AdventureWorks/DimAccount/Year=2025/.../file.parquet
    
    relative_path = f.split(RAW_PATH_CLEANED, 1)[-1].strip("/")
    
    # The logical table name is the first part of the relative path, assuming RAW_PATH is /Files/raw
    # and the immediate subfolders define the table/dataset.
    parts = relative_path.split("/")
    
    # Heuristic for table name: Take the first two non-partition folders (e.g., /AdventureWorks/DimAccount)
    # or just the top folder if there's no deeper structure. Adjust this logic based on your exact file convention.
    if len(parts) > 1 and not ("=" in parts[1]): # Check if the second part is likely not a partition folder (e.g. Year=2025)
        # Assuming two-level table structure: Dataset/Table (e.g., AdventureWorks/DimAccount)
        table_name = "/".join(parts[0:2])
    else:
        # Assuming one-level table structure: Table (e.g., DimAccount)
        table_name = parts[0]
        
    table_files.setdefault(table_name, []).append(f)

print(f"📂 Found {len(table_files)} tables to process.\n")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# 🧱 Processing Files

audit_entries = []

for table_name, files in table_files.items():
    print(f"▶️ Processing {table_name} ({len(files)} files)")

    start_time = datetime.datetime.utcnow()
    valid_dfs = []
    total_rows = 0
    status = "SUCCESS"
    error_message = None

    # Step 1: Try reading each parquet file
    # NOTE: The spark.read.parquet(files_list) approach is usually more efficient
    # but reading individually handles bad files better, as implemented here.
    for f in files:
        try:
            df = (
                spark.read
                .option("mergeSchema", "true")
                .parquet(f)
            )
            valid_dfs.append(df)
        except Exception as e:
            print(f"⚠️ Skipping bad file: {f}")
            print(f"   Reason: {str(e)[:200]}")

    # Step 2: Skip table if no valid files
    if not valid_dfs:
        print(f"❌ No valid data found for {table_name}, skipping.\n")
        # Ensure the error_message fits the schema
        audit_entries.append((table_name, len(files), 0, start_time, datetime.datetime.utcnow(), "FAILED", "No valid files"))
        continue

    # Step 3: Union all DataFrames safely
    try:
        df_final = valid_dfs[0]
        for next_df in valid_dfs[1:]:
            df_final = df_final.unionByName(next_df, allowMissingColumns=True)
    except Exception as e:
        status = "FAILED"
        error_message = f"Union failed: {str(e)[:200]}"
        print(f"❌ Union failed for {table_name}: {error_message}")
        audit_entries.append((table_name, len(files), 0, start_time, datetime.datetime.utcnow(), status, error_message))
        continue

    # Step 4: Add ingestion timestamp
    if APPEND_TIMESTAMP:
        df_final = df_final.withColumn("load_timestamp_utc_bronze", F.current_timestamp())
    
    # 🌟 CORRECTION: Calculate row count *before* writing to avoid a second expensive action
    try:
        total_rows = df_final.count()
    except Exception as e:
        print(f"⚠️ Could not count rows for {table_name}. Proceeding with write (0 count logged).")
        total_rows = 0 # Log 0 if count fails, but still attempt write

    # Step 5: Write to Bronze Table
    table_clean_name = table_name.replace("/", "_").lower()
    print(f"💾 Writing {table_name} → {BRONZE_SCHEMA}.{table_clean_name} ({total_rows} rows)")

    try:
        # Use Delta format for Bronze tables for schema evolution, ACID properties
        df_final.write.format("delta").mode(OVERWRITE_MODE).saveAsTable(f"{BRONZE_SCHEMA}.{table_clean_name}")
        print(f"✅ {table_name} successfully loaded.\n")
    except AnalysisException as e:
        status = "FAILED"
        error_message = f"Write failed (Analysis): {str(e)[:200]}"
        print(f"❌ Write failed for {table_name}: {error_message}")
    except Exception as e:
        status = "FAILED"
        # Truncate traceback to fit in the audit table column
        formatted_traceback = traceback.format_exc().replace('\n', ' // ')
        # Now, use the temporary variable in the f-string
        error_message = f"Unexpected: {formatted_traceback[:300]}"
        
        print(f"❌ Unexpected error for {table_name}: {error_message}")
        
    # Step 6: Log the result (total_rows calculated earlier)
    # total_rows is 0 if write failed, but the pre-count is a better reflection of what was attempted.
    # We re-evaluate total_rows to 0 if the write was a hard fail.
    if status == "FAILED":
         total_rows = 0
         
    audit_entries.append((table_name, len(files), total_rows, start_time, datetime.datetime.utcnow(), status, error_message))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 🧾 Audit Logging 

if audit_entries:
    print(f"\n🪵 Writing load audit ({len(audit_entries)} entries)...")

    audit_schema = (
        "table_name STRING, "
        "file_count INT, "
        "row_count BIGINT, "
        "start_time TIMESTAMP, "
        "end_time TIMESTAMP, "
        "status STRING, "
        "error_message STRING"
    )

    df_audit = spark.createDataFrame(audit_entries, schema=audit_schema)

    try:
        # Use Delta format for the Audit table
        df_audit.write.format("delta").mode("append").saveAsTable(AUDIT_TABLE)
        print(f"✅ Audit successfully logged to {AUDIT_TABLE}")
    except Exception as e:
        print(f"⚠️ Could not write audit table: {e}")
else:
    print("ℹ️ No audit entries to log.")

print("\n🏁 Bronze load process completed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
