# Fabric AdventureWorks Data Platform

A **production-ready, end-to-end data engineering platform** built on Microsoft Fabric. This project demonstrates modern ELT practices with dynamic data ingestion from SQL databases into OneLake, intelligent incremental loads for facts, and automated CI/CD deployment.

## 📋 Quick Navigation

- [What It Does](#what-it-does)
- [Key Capabilities](#key-capabilities)
- [Tech Stack](#tech-stack)
- [Getting Started](#getting-started)
- [Project Structure](#project-structure)
- [Learn More](#learn-more)

## What It Does

Automatically ingests data from an **AdventureWorks SQL database** into a **medallion lake architecture** using Microsoft Fabric. The platform:

1. **Extracts** data dynamically from source systems via intelligent pipeline orchestration
2. **Loads** data into Bronze layer with incremental facts & snapshot dimensions  
3. **Transforms** into analytics-ready Mart tables for business intelligence
4. **Automates** everything with CI/CD for UAT & PROD deployments

## Key Capabilities

✅ **Dynamic Table Discovery** - Discovers & processes tables automatically from metadata  
✅ **Incremental Loads** - Facts load only changed data using watermark-based tracking  
✅ **Snapshot Dimensions** - Daily full captures for slowly-changing dimensions  
✅ **Data Quality** - Audit logging for every load (file count, rows, errors)  
✅ **Automated CI/CD** - Azure Pipelines with UAT → PROD promotion gates  
✅ **Delta Lake** - ACID transactions, schema evolution, time travel  
✅ **Error Resilience** - Skips bad files, continues processing, logs all failures  
✅ **Multi-Environment** - Environment-specific configs (UAT/PROD) via parameters  

## Tech Stack

- **Data Platform**: Microsoft Fabric (Lakehouse, SQL Database, Pipelines)
- **Storage**: OneLake with Delta Lake format
- **Orchestration**: Fabric DataPipeline (dynamic lookup + ForEach loop)
- **Transformation**: PySpark & Spark SQL Notebooks
- **Deployment**: fabric-cicd Python SDK (Microsoft's official Infrastructure-as-Code tool)
- **CI/CD**: Azure Pipelines with Service Principal authentication
- **Language**: Python, SQL, YAML

## Project Structure

```bash
fabric-adventureworks-data-platform/
├── README.md                                 # This file
├── deploy.py                                 # Python script for fabric-cicd deployment
├── fabric-pipeline.yml                       # Azure Pipelines CI/CD configuration
│
├── src/
│   ├── parameter.yml                         # Environment-specific parameters (find/replace)
│   │
│   ├── Lakehouse/
│   │   └── raqsconsole_LH.Lakehouse/        # OneLake storage container
│   │       ├── lakehouse.metadata.json
│   │       └── shortcuts.metadata.json
│   │
│   ├── Notebook/
│   │   ├── Load_landing_files_to_bronze_layer.Notebook/
│   │   │   └── notebook-content.py          # Bronze layer loading (PySpark)
│   │   │       - Discovers today's parquet files in raw zone
│   │   │       - Unions data per table
│   │   │       - Writes to Bronze Delta tables
│   │   │       - Logs audit trail
│   │   │
│   │   └── transformed to mart.Notebook/
│   │       └── notebook-content.sql         # Transformation to Mart (SQL/PySpark)
│   │           - Transforms Bronze to Gold/Mart
│   │           - Business logic & aggregations
│   │
│   └── Pipeline/
│       └── ELT_pipeline.DataPipeline/
│           └── pipeline-content.json        # Main orchestration pipeline
│               - Lookup metadata from SQL
│               - ForEach loop per table
│               - Copy activity (Fabric SQL → Lakehouse)
│               - Watermark tracking
│               - Conditional incremental/snapshot logic
```

## Getting Started

### Prerequisites

- Microsoft Fabric Workspace with Admin access
- Fabric SQL Database (AdventureWorks sample)
- Azure DevOps (for CI/CD)
- Python 3.8+ and git

### Quick Setup

## Project Structure

```bash
fabric-adventureworks-data-platform/
├── README.md                           # This file
├── deploy.py                           # Deployment script (fabric-cicd)
├── fabric-pipeline.yml                 # CI/CD pipeline (Azure Pipelines)
├── src/parameter.yml                   # Environment configs (UAT/PROD)
│
├── src/Lakehouse/                      # OneLake storage
│   └── raqsconsole_LH/                 # Landing zone
│
├── src/Notebook/                       # Transformation logic
│   ├── Load_landing_files_to_bronze_layer.Notebook/
│   │   └── notebook-content.py         # Raw → Bronze ingestion
│   └── transform_to_mart.Notebook/
│       └── notebook-content.sql        # Bronze → Analytics-ready Mart tables
│
└── src/Pipeline/                       # Data orchestration
    └── ELT_pipeline.DataPipeline/
        └── pipeline-content.json       # Extract from SQL → Lakehouse
```

## How It Works

**Phase 1: Extract** → Fabric Pipeline queries metadata, discovers tables, applies watermarks for incremental loads

**Phase 2: Load** → Notebooks ingest raw files into Bronze Delta tables (dimensions as snapshots, facts incrementally)

**Phase 3: Transform** → Business logic transforms Bronze → Mart for analytics consumption

**Phase 4: Deploy** → CI/CD automatically pushes code to UAT, then PROD after validation

## Core Features Explained

### Dynamic Discovery

Instead of hardcoding table names, the pipeline queries a metadata table to discover which tables to load. Enables scalability as new tables are added.

### Intelligent Incremental Loads  

Fact tables track the last load timestamp (watermark). Only new/changed data gets loaded, reducing data transfer & compute costs.

### Snapshot Dimensions

Dimensions are fully reloaded daily as snapshots with `snapshot_date` partitioning. Each table stores the full dimension state for each business day, enabling time-travel analysis and SCD Type 1 (overwrite) patterns efficiently. Partitioned tables accelerate queries and simplify retention policies.

### Audit Trail

Every load is logged: file count, row count, success/failure status. Critical for data governance & troubleshooting.

### Multi-Environment Support

Same code deploys to UAT & PROD with different parameters (IDs, names). Validate in UAT before promoting to PROD.

### Dynamic Folder Partitioning

Tables are automatically partitioned in OneLake by date hierarchy:

```bash
Files/raw/{SourceSystem}/{TableName}/Year=YYYY/Month=MM/Day=DD/{filename}.parquet
```

Example:

```bash
Files/raw/AdventureWorks/FactInternetSales/Year=2026/Month=05/Day=15/dbo_FactInternetSales_20260515143022.parquet
Files/raw/AdventureWorks/DimCustomer/Year=2026/Month=05/Day=15/dbo_DimCustomer.parquet
```

**Benefits**:

- **Partition Pruning** - Queries scan only today's data, reducing I/O & cost
- **Easy Retention** - Delete old partitions by date range  
- **Parallelization** - Partition-aware processing speeds up transformations
- **Data Organization** - Clear folder hierarchy for data discovery & governance

The pipeline dynamically constructs partition paths using `formatDateTime()` functions, so new dates are handled automatically without code changes.

## Deployment Pipeline

**Infrastructure-as-Code with fabric-cicd Python SDK** (Microsoft's official Fabric deployment tool):

1. **Git-Stored Code** - All Fabric items (Notebooks, Pipelines, Lakehouse) version controlled
2. **Automated Deploy** - `deploy.py` script publishes items to Fabric via fabric-cicd SDK
3. **Environment Promotion** - Parameter find/replace handles UAT → PROD config changes
4. **CI/CD Orchestration** - Azure Pipelines automates validation & promotion gates

**Flow**: Git push → UAT deployment → ✅ Validation → PROD deployment (main branch only)

Environment-specific configs in `parameter.yml` ensure code reusability across environments.

## Why This Approach?

**Scalable** — Add new tables to metadata, they're automatically discovered  
**Cost-Efficient** — Incremental loads reduce data transfer; cloud-native caching  
**Reliable** — Error handling & audit logging catch & document issues  
**Maintainable** — Infrastructure-as-code (Git); reproducible across environments  
**Business-Ready** — Delta Lake ensures data quality & consistency  

## Learn More

- 📚 [Microsoft Fabric Documentation](https://learn.microsoft.com/fabric)
- 📚 [Delta Lake Guide](https://docs.databricks.com/en/delta/index.html)
- 📚 [Azure Pipelines](https://learn.microsoft.com/en-us/azure/devops/pipelines)