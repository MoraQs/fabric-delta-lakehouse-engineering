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

# Define which tables are Dimensions (Snapshot/Overwrite) vs. Facts (Incremental/Append)
# IMPORTANT: Adjust this list to match your actual table names (e.g., AdventureWorks/DimPromotion)
DIMENSION_TABLES = [
    "AdventureWorks/DimPromotion",
    "AdventureWorks/DimCustomer",
    "AdventureWorks/DimAccount"
]

# Get the current date to target only today's partition
TODAY_PATH_FRAGMENT = f"Year={datetime.datetime.utcnow().strftime('%Y')}/Month={datetime.datetime.utcnow().strftime('%m')}/Day={datetime.datetime.utcnow().strftime('%d')}"

# Ensure RAW_PATH ends without a slash for consistent string manipulation
RAW_PATH_CLEANED = RAW_PATH.rstrip("/")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 3: Discovery Stage - Find and Group Today's Files

print(f"🔍 Scanning raw zone for files in today's partition: {TODAY_PATH_FRAGMENT}")

# List all files, but only those matching today's partition path for efficiency.
all_files = []
def list_current_day_files(path):
    """
    Lists files only in the current day's partition path for all tables.
    Handles the structure: RAW_PATH / SourceSystem / TableName / Year=...
    """
    try:
        # 1. List the source system folders (e.g., AdventureWorks) directly under RAW_PATH
        for source_folder in mssparkutils.fs.ls(path):
            if source_folder.isDir:
                # 2. List the table folders (e.g., DimPromotion, DimCustomer) inside the source system
                for table_folder in mssparkutils.fs.ls(source_folder.path):
                    if table_folder.isDir:
                        # Construct the expected daily snapshot path (e.g., .../DimPromotion/Year=2025/Month=11/Day=03)
                        daily_path = f"{table_folder.path.rstrip('/')}/{TODAY_PATH_FRAGMENT}"
                        
                        try:
                            # 3. Check the specific daily partition path
                            if mssparkutils.fs.exists(daily_path):
                                # 4. List the files (e.g., snapshot.parquet or multiple time-stamped files)
                                for item in mssparkutils.fs.ls(daily_path):
                                    if not item.isDir and item.name.endswith(".parquet"):
                                        all_files.append(item.path)
                        except Exception as e:
                            # Ignoring file access exceptions for non-existent daily paths
                            pass

    except Exception as e:
        print(f"⚠️ Failed to access {path}: {e}")

list_current_day_files(RAW_PATH_CLEANED)

print(f"✅ Found {len(all_files)} parquet files targeted for today's load.")


# Group files by logical table
table_files = {}
for f in all_files:
    # Example f: Files/raw/AdventureWorks/DimPromotion/Year=2025/.../snapshot.parquet
    relative_path = f.split(RAW_PATH_CLEANED, 1)[-1].strip("/")
    
    # Extract the clean table name (e.g., "AdventureWorks/DimPromotion")
    if "Year=" in relative_path:
        table_path_only = relative_path.split("Year=")[0].rstrip("/")
    else:
        parts = relative_path.split("/")
        table_path_only = "/".join(parts[0:2])
        
    table_files.setdefault(table_path_only, []).append(f)

print(f"📂 Found {len(table_files)} tables to process.\n")

# Exit early if no files were found to process
if not table_files:
    print("❌ Critical: No tables found for today's load. Check pipeline output or RAW_PATH.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 4: Processing Setup

audit_entries = []

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 5: Core Processing Loop - Read, Union, and Write to Bronze Delta Table

# Skip loop if no tables were found in Cell 3
if table_files:
    for table_name, files in table_files.items():
        
        # Determine Write Mode
        is_dimension = table_name in DIMENSION_TABLES
        write_mode = "overwrite" if is_dimension else "append"
        
        print(f"▶️ Processing {table_name} ({len(files)} files) - Mode: {write_mode.upper()}")

        start_time = datetime.datetime.utcnow()
        valid_dfs = []
        total_rows = 0
        status = "SUCCESS"
        error_message = None

        # Step 1: Try reading each parquet file
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
                print(f"   Reason: {str(e)[:200]}")

        # Step 2: Skip table if no valid files
        if not valid_dfs:
            print(f"❌ No valid data found for {table_name}, skipping.\n")
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
        
        # Calculate row count
        try:
            total_rows = df_final.count()
        except Exception as e:
            print(f"⚠️ Could not count rows for {table_name}. Proceeding with write (0 count logged).")
            total_rows = 0

        # Step 5: Write to Bronze Delta Table
        table_clean_name = table_name.replace("/", "_").lower()
        full_table_name = f"{BRONZE_SCHEMA}.{table_clean_name}"
        
        print(f"💾 Writing {table_name} → {full_table_name} ({total_rows} rows) using {write_mode.upper()} mode.")

        try:
            df_final.write.format("delta").mode(write_mode).saveAsTable(full_table_name)
            print(f"✅ {table_name} successfully loaded.\n")
        except AnalysisException as e:
            status = "FAILED"
            # 🐛 DEBUG: Print full analysis exception message
            print(f"❌ WRITE FAILED (AnalysisException) for {table_name}: {e}")
            error_message = f"Write failed (Analysis): {str(e)}"
        except Exception as e:
            status = "FAILED"
            # 🐛 DEBUG: Print full traceback for unexpected errors
            print(f"❌ WRITE FAILED (Unexpected Error) for {table_name}: {e}")
            formatted_traceback = traceback.format_exc().replace('\n', ' // ')
            error_message = f"Unexpected: {formatted_traceback[:300]}"
            
        # Step 6: Log the result
        if status == "FAILED":
            total_rows = 0
                
        audit_entries.append((table_name, len(files), total_rows, start_time, datetime.datetime.utcnow(), status, error_message))
else:
    print("ℹ️ Skipping Core Processing Loop because no files were found in the discovery stage (Cell 3).")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 6: Audit Logging

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
        # Ensure the Audit table always appends
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
