# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "4ea4f0aa-201f-4f2b-89ac-c06461102d1e",
# META       "default_lakehouse_name": "dev_raqsconsole_LH",
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

import datetime
import pyspark.sql.functions as F
from notebookutils import mssparkutils

# CONFIG
RAW_PATH = "Files/raw"
BRONZE_SCHEMA = "bronze"
AUDIT_TABLE = "meta_lakehouse.bronze_load_audit"

# Define snapshot-based vs incremental tables
DIMENSION_TABLES = [
    "AdventureWorks/DimPromotion",
    "AdventureWorks/DimCustomer",
    "AdventureWorks/DimAccount",
    "AdventureWorks/DimGeography",
    "AdventureWorks/DimProduct",
    "AdventureWorks/DimProductCategory",
    "AdventureWorks/DimProductSubcategory"
]

# Get today's partition
today = datetime.datetime.utcnow()
TODAY_PATH_FRAGMENT = f"Year={today:%Y}/Month={today:%m}/Day={today:%d}"

print(f"🔍 Scanning for new raw data in: {TODAY_PATH_FRAGMENT}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def discover_tables(raw_path, today_fragment):

    """Return dict: {table_name: [file_paths]}"""
    
    table_files = {}
    for source_folder in mssparkutils.fs.ls(raw_path):
        if not source_folder.isDir:
            continue
        for table_folder in mssparkutils.fs.ls(source_folder.path):
            if not table_folder.isDir:
                continue
            day_path = f"{table_folder.path.rstrip('/')}/{today_fragment}"
            if not mssparkutils.fs.exists(day_path):
                continue

            files = [
                f.path for f in mssparkutils.fs.ls(day_path)
                if f.name.endswith(".parquet")
            ]
            if files:
                table_key = f"{source_folder.name}/{table_folder.name}"
                table_files[table_key] = files
    return table_files

# Run discovery
table_files = discover_tables(RAW_PATH, TODAY_PATH_FRAGMENT)

if not table_files:
    print("⚠️ No new raw files found for today. Exiting early.")
else:
    print(f"✅ Found {len(table_files)} tables to process.\n")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 4: Processing Setup

audit_entries = []

for table_name, files in table_files.items():

    start_time = datetime.datetime.utcnow()
    is_dim = table_name in DIMENSION_TABLES
    write_mode = "overwrite" if is_dim else "append"

    print(f"▶️ {table_name} ({len(files)} files) → {write_mode.upper()}")

    try:
        # Read parquet files
        df = spark.read.option("mergeSchema", "true").parquet(*files)
        df = df.withColumn("load_timestamp_utc_bronze", F.current_timestamp()) \
               .withColumn("snapshot_date", F.to_date(F.current_timestamp()))

        row_count = df.count()

        # Prepare target table name
        clean_name = table_name.replace("/", "_").lower()
        full_table = f"{BRONZE_SCHEMA}.{clean_name}"

        # Write to Bronze schema
        df.write.format("delta") \
            .mode(write_mode) \
            .option("mergeSchema", "true") \
            .saveAsTable(full_table)

        status, error_message = "SUCCESS", None
        print(f"✅ Loaded {row_count:,} rows into {full_table}")
        
    except Exception as e:
        status, error_message = "FAILED", str(e)
        row_count = 0
        print(f"❌ Failed for {table_name}: {e}")

    audit_entries.append((
        table_name, len(files), row_count,
        start_time, datetime.datetime.utcnow(),
        status, error_message
    ))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if audit_entries:
    df_audit = spark.createDataFrame(audit_entries, schema="""
        table_name STRING,
        file_count INT,
        row_count BIGINT,
        start_time TIMESTAMP,
        end_time TIMESTAMP,
        status STRING,
        error_message STRING
    """)

    df_audit.write.format("delta").mode("append").saveAsTable(AUDIT_TABLE)
    print(f"🪵 Audit written to {AUDIT_TABLE}")
else:
    print("ℹ️ No audit entries to log.")

print("\n🏁 Bronze load complete.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
