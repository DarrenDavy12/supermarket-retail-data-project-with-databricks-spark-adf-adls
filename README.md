# 🧱 Supermarket Retail Sales Data Pipeline - Azure Databricks 

## Project Overview

This project demonstrates an end-to-end **Medallion Architecture** pipeline on **Azure Databricks**, processing retail sales data from raw ingestion to clean and aggregated insights. The pipeline follows the Bronze → Silver → Gold layers to ensure clean, structured, and analysis-ready data.

### 💼  Business Scenario:

A retail business sells across multiple regions, states, and product categories, generating daily transactional sales data (orders, sales value, profit, discounts). This data arrives as raw CSV exports with no consistent structure — different date formats, unvalidated types, and no separation between "raw as received" and "safe to report on." Analysts and regional/category managers need trustworthy, aggregated sales and profit figures to support decisions like stocking, discounting, and regional performance reviews, but currently there's no repeatable pipeline turning raw exports into reporting-ready data.

### 💼 Business use case:

Regional/Category managers need reliable Sales, Profit, and Discount figures broken down by Region, Category, and State to inform stocking and pricing decisions.
The pipeline should ingest raw sales exports, enforce schema and clean known formatting issues, and produce curated, query-ready aggregates — without manual intervention once built.
Orchestration (ADF) would eventually run this on a schedule, so new sales data flows through Bronze → Silver → Gold automatically, and failures are visible and traceable to a specific stage rather than a mystery in a spreadsheet.
Credentials should be moved to Key Vault, access controlled via RBAC/Managed Identity, so the pipeline could run against production data without hard-coded secrets.

### 🎯 Solution: 

A PySpark pipeline ingests raw CSVs to Bronze, enforces schema and cleans known formatting issues in Silver (with detailed logging), and produces query-ready aggregates in Gold. Azure Data Factory orchestration (planned) would run this on a schedule, making data flow automatic and failures visible. The trade-off: using Databricks Parquet instead of a warehouse database, because it integrates tightly with Azure ADLS Gen2 and keeps compute flexible for experiments alongside production pipelines.

---

## 🏗️ Architecture

`
Raw CSV (Bronze) -> Cleaned & Typed (Silver) -> Aggregated Metrics (Gold) -> Analysis / BI / Reporting
`

**Azure Services & Tools Used:**

- **Azure Databricks** – Notebook environment and Spark cluster for ETL processing.
- **Azure Data Lake Storage Gen2 (ADLS)** – Storage for Bronze, Silver, Gold layers.
- **Azure Key Vault** (planned) – Secure management of credentials.
- **Azure Data Factory (ADF)** – Orchestration and scheduling of pipeline workflows. (Couldn't utilize orchestration/scheduling because of databricks tier choice, I made a conceptual approach in **ADF Architecture Diagram** section below.)
- **Parquet** – Columnar storage format for Silver and Gold layers.
- **PySpark / Spark SQL** – Data transformation and aggregation.
- **Databricks Notebooks** – Organize and run ETL logic.
- **GitHub** – Version control for notebooks and project files.
- **VS Code / IDE** – Optional editing and debugging environment.
- **Python Libraries** – Used within Databricks for additional transformations.

---

## 🌊 Data Flow

1. 🥉 **Bronze Layer (Raw)**
   - Ingest raw CSV file from ADLS.
   - Handle inconsistent formatting issues (e.g., different date formats).

2. 🥈 **Silver Layer (Cleaned)**
   - Apply schema enforcement and type conversion:
     - `Order Date` converted to proper timestamp.
     - `Sales`, `Profit`, `Discount` ensured to be numeric.
   - Resolve common data issues:
     - **Date parsing errors** (`CANNOT_PARSE_TIMESTAMP`) handled using `to_date` with proper format.
     - Ignored invalid or inconsistent rows.
   - Store cleaned data in **Parquet** format in the Silver container.

3. 🥇 **Gold Layer (Aggregated)**
   - Aggregate metrics by `Region`, `Category`, or `State`.
   - Produce analysis-ready datasets for BI or reporting.
   - Store in **Parquet** format in the Gold container.

4. **Pipeline Orchestration**
   - Use **Azure Data Factory** to schedule and automate the pipeline.
   - Orchestrate Bronze → Silver → Gold processing.
   - Integrate Databricks notebooks as pipeline activities.

---

## 🌋 🎯 Challenges/Errors Resolved

During development, the following issues were encountered and resolved:

1. **File format issues**
   - Error: `FAILED_READ_FILE.CANNOT_READ_FILE_FOOTER`
   - Cause: Trying to read CSV as Parquet.
   - Solution: Ensure proper file format (`spark.read.format("csv")` for raw files, Parquet for processed layers).

2. **Timestamp parsing errors**
   - Error: `CANNOT_PARSE_TIMESTAMP`
   - Cause: Mixed date formats in the CSV (`MM-dd-yyyy` vs `M/d/yyyy`).
   - Solution: Used PySpark `to_date()` with correct `format` parameter and nullable handling.

3. **Column resolution errors**
   - Error: `column1 cannot be resolved`
   - Cause: Column names referenced did not exist.
   - Solution: Checked schema and referenced correct column names (e.g., `City`, `Region`, `Sales`).

4. **Storage access errors**
   - Error: `KeyProviderException` / `Invalid configuration value for fs.azure.account.key`
   - Cause: Hard-coded or missing ADLS credentials.
   - Solution: Future improvement: use **Azure Key Vault** for secret management instead of hard-coded keys.

---

## Walkthrough 

1. **Setup**
   - Configure Databricks cluster.
   - Ensure access to ADLS containers: `bronze`, `silver`, `gold`.

2. **Run Notebooks**
   - `bronze_silver.ipynb` → ingests raw CSV and writes to Silver layer.
   - `gold.ipynb` → reads Silver layer and produces Gold layer.

3. **Verify Output**
   - Silver layer: Cleaned Parquet files.
   - Gold layer: Aggregated metrics in Parquet.

4. **Pipeline Automation**
   - Use **ADF** to schedule notebooks as activities.
   - Monitor pipeline runs and failures via ADF.

    ADF Pipeline: Supermarket Sales ETL

    Pipeline steps:

    a. Bronze → Silver notebook (Databricks)
    - Reads raw CSV from Bronze layer
    - Cleans data and writes to Silver layer

    b. Silver → Gold notebook (Databricks)
    - Reads Silver layer
    - Aggregates and writes Gold layer

    Activities are connected via Success dependency (Gold runs after Silver completes)

    **ADF Architecture Diagram**

                 ┌─────────────┐
                 │  Bronze CSV │
                 │  Raw Data   │
                 └─────┬───────┘
                       │
                       ▼
         ┌─────────────────────────┐
         │ Bronze → Silver Notebook│
         │ Databricks PySpark      │
         │ - Cleans dates          │
         │ - Removes nulls         │
         │ - Standardizes columns  │
         └─────────┬──────────────┘
                   │
                   ▼
            ┌─────────────┐
            │ Silver Layer│
            │ (Parquet)   │
            └─────┬───────┘
                  │
                  ▼
         ┌────────────────────┐
         │ Silver → Gold       │
         │ Notebook (Databricks)│
         │ - Aggregations       │
         │ - Enrichments        │
         │ - Final prep         │
         └─────┬───────────────┘
               │
               ▼
          ┌───────────┐
          │ Gold Layer│
          │ (Parquet) │
          └───────────┘

---

## Future Improvements

- Integrate **Azure Key Vault** to store and retrieve ADLS credentials securely.
- Security using RBAC for user scope.
- Managed Identity for in house applications to access ADLS.
- Service Principal for any connection to third party tools, or external sources to ADLS.
- Expand **ADF orchestration** for error handling and notifications.
- Add **BI dashboards** on top of Gold layer data.
- Implement **data quality checks** at each Medallion stage.


---
 
**Project Status:** Complete Bronze → Silver → Gold pipeline with cleaned and aggregated datasets, ready for orchestration via ADF.  
**Tools & Services Used:** Azure Databricks, ADLS Gen2, Azure Data Factory, PySpark, Python, Spark SQL, Parquet, Azure Key Vault (planned, couuld not apply because of databricks tier 14-day-trial doesnt allow secrets/scope), GitHub, VS Code (optional, I did for project structure).
