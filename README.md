# Enterprise Batch E-Commerce ETL Pipeline with Apache Airflow & Great Expectations

[![Python](https://img.shields.io/badge/Python-3.10%2B-blue.svg)](https://www.python.org/)
[![Apache Airflow](https://img.shields.io/badge/Airflow-2.7.0-017CEE.svg)](https://airflow.apache.org/)
[![Great Expectations](https://img.shields.io/badge/Great%20Expectations-0.18%2B-FF6B6B.svg)](https://greatexpectations.io/)
[![Docker](https://img.shields.io/badge/Docker-Compose%20v3.8-2496ED.svg)](https://www.docker.com/)
[![Tests](https://img.shields.io/badge/Pytest-100%25%20Passing-brightgreen.svg)](https://pytest.org/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

An end-to-end, production-grade batch data engineering solution implementing the **Medallion Architecture** (Raw &rarr; Bronze &rarr; Silver &rarr; Analytical Serving Store) for multi-national e-commerce transaction data. Orchestrated via **Apache Airflow** in a containerized environment, this pipeline integrates automated **Great Expectations** quality gates at each storage tier, operating as an active **Circuit Breaker** to prevent data corruption, anomalies, or schema drift from propagating downstream.

---

## Table of Contents

1. [Executive Summary & Motivation](#1-executive-summary--motivation)
2. [System Architecture & Medallion Data Flow](#2-system-architecture--medallion-data-flow)
3. [Airflow DAG Orchestration](#3-airflow-dag-orchestration)
4. [Great Expectations & Data Quality Circuit Breakers](#4-great-expectations--data-quality-circuit-breakers)
5. [Transformation Rules & Business Logic](#5-transformation-rules--business-logic)
6. [Analytical Serving Store (SQLite) & Idempotency](#6-analytical-serving-store-sqlite--idempotency)
7. [Repository File Layout](#7-repository-file-layout)
8. [Configuration & Environment Variables](#8-configuration--environment-variables)
9. [Quickstart & Execution Guide](#9-quickstart--execution-guide)
10. [Automated Testing Suite](#10-automated-testing-suite)
11. [Data Docs & Verification Evidence](#11-data-docs--verification-evidence)
12. [Troubleshooting & FAQ](#12-troubleshooting--faq)
13. [Final Submission Checklist](#13-final-submission-checklist)

---

## 1. Executive Summary & Motivation

In global e-commerce operations, real-time and historical transactional data drives core business decisions: revenue forecasting, inventory management, regional promotion strategies, and cohort lifetime value (LTV) models. However, raw retail transaction streams are notoriously dirty:
- **Missing Identifiers:** Guest checkouts frequently emit records without customer accounts (`CustomerID` is null). Dropping these records prematurely silently destroys legitimate top-line revenue; keeping them as nulls breaks downstream dimensional joins.
- **Anomalous Values:** Returns, cancellations, and system errors generate negative quantities or zero/negative unit prices.
- **Data Volume & Schema Drift:** Ingestion pipelines relying on raw CSVs suffer from poor I/O performance, lack of compression, and absence of compile-time schema validation.
- **Silent Failures:** Traditional cron scripts fail silently, writing corrupted or partial data into business intelligence (BI) data warehouses.

### Project Solution
This project establishes a resilient, automated data engineering pipeline:
1. **Containerized Architecture:** Fully dockerized stack orchestrating Airflow Webserver, Scheduler, PostgreSQL metadata database, and a specialized `etl-service` worker container.
2. **Medallion Data Layering:** Segregates data into Raw (immutable CSV ingestion), Bronze (typed, columnar, Snappy-compressed Apache Parquet partitioned by `Year` and `Month`), Silver (cleansed, deduplicated, and business-aggregated Parquet), and Gold/Serving (indexed relational SQLite database).
3. **Automated Quality Assurance:** Deploys Great Expectations validation suites immediately after each ingestion and transformation stage. Any schema violation or invariant breach trips a **Circuit Breaker**, failing the Airflow task and halting all downstream operations.
4. **Idempotency by Design:** Guarantees that re-running the pipeline on identical or backfilled dates will always produce the exact same analytical state without duplicate rows.

---

## 2. System Architecture & Medallion Data Flow

```
                                +---------------------------------------------+
                                |             EXTERNAL ENVIRONMENT            |
                                |     UCI Online Retail E-Commerce (CSV)      |
                                +---------------------------------------------+
                                                       |
                                                       | (HTTP/HTTPS Stream)
                                                       v
+---------------------------------------------------------------------------------------------------------+
|                                    AIRFLOW ORCHESTRATION (DOCKER)                                       |
|                                                                                                         |
|   [Task 1: download_raw_data]                                                                           |
|                |                                                                                        |
|                v                                                                                        |
|   [Task 2: bronze_layer_processing]                                                                      |
|                |                                                                                        |
|                v                                                                                        |
|   [Task 3: bronze_data_validation]  --- (Great Expectations Suite: bronze_expectations)                 |
|                |                               |                                                        |
|                | [Pass]                        | [Fail] ---> [CIRCUIT BREAKER: HALT PIPELINE]            |
|                v                                                                                        |
|   [Task 4: silver_layer_transformation]                                                                 |
|                |                                                                                        |
|                v                                                                                        |
|   [Task 5: silver_data_validation]  --- (Great Expectations Suite: silver_expectations)                 |
|                |                               |                                                        |
|                | [Pass]                        | [Fail] ---> [CIRCUIT BREAKER: HALT PIPELINE]            |
|                v                                                                                        |
|   [Task 6: load_to_analytical_store]                                                                    |
+---------------------------------------------------------------------------------------------------------+
            |                          |                          |                          |
            | Writes                   | Writes                   | Writes                   | Writes
            v                          v                          v                          v
    +---------------+          +------------------+       +------------------+       +------------------+
    |   data/raw/   |          |   data/bronze/   |       |   data/silver/   |       | data/analytics.db|
    | (Raw CSV File)|          | (Parquet Format) |       | (Parquet Format) |       | (SQLite Serving) |
    |               |          | Partitioned by:  |       | Partitioned by:  |       | Table:           |
    |               |          |  - Year          |       |  - Year          |       |  daily_country_  |
    |               |          |  - Month         |       |  - Month         |       |  sales           |
    +---------------+          +------------------+       +------------------+       +------------------+
```

### Medallion Layer Specifications

| Layer | Storage Path | Serialization / Engine | Partitioning | Cleansing & Purpose |
| :--- | :--- | :--- | :--- | :--- |
| **Raw** | `data/raw/ecommerce_data.csv` | Plain CSV / UTF-8 fallback ISO-8859-1 | None | Untampered external extraction landing zone. Preserves raw historical ground truth. |
| **Bronze** | `data/bronze/` | Apache Parquet (Snappy compressed, PyArrow) | `Year`, `Month` | Enforced strict primitive types (`Quantity: int64`, `UnitPrice: float64`, `InvoiceDate: datetime64`). Preserves raw transaction granularity. |
| **Silver** | `data/silver/` | Apache Parquet (Snappy compressed, PyArrow) | `Year`, `Month` | Imputes missing `CustomerID` to `'UNKNOWN'`. Filters returns (`Quantity < 0`) and zero prices. Computes `TotalPrice`. Aggregates to daily country grain. |
| **Serving (Gold)** | `data/analytics.db` | SQLite Relational Database (WAL mode enabled) | B-tree Indexes on `(InvoiceDate, Country)` and `(Year, Month)` | Final consumption layer. Structured table `daily_country_sales` optimized for BI dashboards, SQL analysts, and executive reporting. |

---

## 3. Airflow DAG Orchestration

The workflow is authored as an explicit DAG inside [`dags/ecommerce_analytics_pipeline.py`](dags/ecommerce_analytics_pipeline.py). 

### DAG Parameters
- **DAG ID:** `ecommerce_analytics_pipeline`
- **Schedule Interval:** `@daily` (midnight UTC cadence)
- **Catchup:** `False` (prevents uncontrolled historical backfill cascades)
- **Max Active Runs:** `1` (prevents concurrent write contention on SQLite file locks)
- **Default Arguments:**
  - `retries`: `2`
  - `retry_delay`: `timedelta(minutes=2)`
  - `execution_timeout`: `timedelta(minutes=30)`

### Dependency Graph (Strict Sequential Chain)
```python
download_raw_data >> bronze_layer_processing >> bronze_data_validation >> silver_layer_transformation >> silver_data_validation >> load_to_analytical_store
```

### Task Manifest

| Step | Task ID | Operator | Python Callable | Description |
| :---: | :--- | :--- | :--- | :--- |
| **1** | `download_raw_data` | `PythonOperator` | `etl_scripts.raw_downloader.download_raw_data` | Streams the CSV dataset over HTTPS with chunked buffer writes and atomic rename. |
| **2** | `bronze_layer_processing` | `PythonOperator` | `etl_scripts.bronze_processor.process_to_bronze` | Casts schemas, extracts temporal partitions, and writes Snappy Parquet files. |
| **3** | `bronze_data_validation` | `PythonOperator` | `etl_scripts.gx_validator.run_validation_checkpoint` | Executes `bronze_expectations` suite. Halts on critical schema deviations. |
| **4** | `silver_layer_transformation` | `PythonOperator` | `etl_scripts.silver_transformer.transform_to_silver` | Imputes nulls, filters returns, derives `TotalPrice`, and computes `DailyTotalSales`. |
| **5** | `silver_data_validation` | `PythonOperator` | `etl_scripts.gx_validator.run_validation_checkpoint` | Executes `silver_expectations` suite. Validates composite keys and non-negative sales. |
| **6** | `load_to_analytical_store` | `PythonOperator` | `etl_scripts.analytics_loader.load_to_sqlite` | Idempotently overwrites table `daily_country_sales` and builds performance indexes. |

---

## 4. Great Expectations & Data Quality Circuit Breakers

Data pipelines without automated quality gates suffer from **silent data corruption**. This project enforces automated quality gates via Great Expectations (`great_expectations/`).

### How the Circuit Breaker Operates
1. In tasks 3 and 5, `run_validation_checkpoint` initializes a Great Expectations `FileDataContext` and constructs a `RuntimeBatchRequest` over the newly serialized Parquet partitions.
2. The dataset is evaluated against the designated Expectation Suite (`bronze_expectations` or `silver_expectations`).
3. If **any** non-tolerated expectation fails:
   - The validation runner logs detailed failure diagnostics (failing column, observed vs. expected distribution, offending values).
   - An explicit `RuntimeError("CIRCUIT BREAKER TRIGGERED: ...")` is raised.
   - Airflow catches the exception, terminates the task with exit code `1`, and marks all downstream tasks as `upstream_failed`.
   - **Result:** Bad data is quarantined before reaching the serving database or downstream business dashboards.
4. Static HTML **Data Docs** are automatically re-rendered to [`great_expectations/uncommitted/data_docs/local_site/index.html`](great_expectations/uncommitted/data_docs/local_site/index.html).

### Expectation Suite Matrix

#### Bronze Suite (`great_expectations/expectations/bronze_expectations.json`)
- `expect_column_to_exist` for: `InvoiceNo`, `StockCode`, `Quantity`, `InvoiceDate`, `UnitPrice`, `CustomerID`, `Country`.
- `expect_column_values_to_not_be_null` on `InvoiceNo` (`mostly: 1.0`).
- `expect_column_values_to_not_be_null` on `CustomerID` (`mostly: 0.70` &mdash; accounts for guest checkout reality before Silver imputation).
- `expect_column_values_to_be_of_type` on `Quantity` (`int64`).
- `expect_column_values_to_be_of_type` on `UnitPrice` (`float64`).
- `expect_column_values_to_be_between` on `Quantity` (`min_value: 1`, `max_value: 100000`, `mostly: 0.85` &mdash; permits returns in raw bronze).

#### Silver Suite (`great_expectations/expectations/silver_expectations.json`)
- `expect_column_to_exist` for: `InvoiceDate`, `Country`, `DailyTotalSales`.
- `expect_column_values_to_not_be_null` on `DailyTotalSales` (`mostly: 1.0`).
- `expect_compound_columns_to_be_unique` on composite key `["InvoiceDate", "Country"]`.
- `expect_column_values_to_be_between` on `DailyTotalSales` (`min_value: 0.0` &mdash; strictly non-negative after return filtering).
- `expect_table_row_count_to_be_between` (`min_value: 1`, `max_value: 10000000` &mdash; prevents empty table publishing).

---

## 5. Transformation Rules & Business Logic

All data transformation logic is encapsulated inside [`etl_scripts/silver_transformer.py`](etl_scripts/silver_transformer.py) under the testable function `clean_and_transform_dataframe(df)`.

```
Raw / Bronze Transaction Record:
{
  InvoiceNo: "536365",
  StockCode: "85123A",
  Quantity: 6,
  InvoiceDate: "2023-11-01 08:26:00",
  UnitPrice: 2.55,
  CustomerID: NaN,
  Country: "United Kingdom"
}

Transformation Step 1 (Imputation):
  CustomerID: NaN -> "UNKNOWN"

Transformation Step 2 (Quality Filter):
  Assert Quantity >= 0 and UnitPrice > 0 -> PASS

Transformation Step 3 (Arithmetic):
  TotalPrice = 6 * 2.55 = 15.30

Transformation Step 4 (Date Truncation):
  InvoiceDate = "2023-11-01"

Transformation Step 5 (Group & Sum):
  GROUP BY InvoiceDate, Country
  DailyTotalSales = SUM(TotalPrice)
```

### Business Rules Rationale
1. **CustomerID Imputation:** In retail e-commerce, dropping rows where `CustomerID` is null discards legitimate revenue from guest checkouts. Replacing nulls with `'UNKNOWN'` preserves financial totals while allowing segmentation between registered users and anonymous shoppers.
2. **Negative Quantity Filtering:** Negative quantities in retail datasets represent order cancellations or customer product returns (often prefixed with `'C'` in `InvoiceNo`). For top-line sales reporting, returns are isolated to prevent corrupting gross revenue aggregates.
3. **Price Non-Zero Validation:** Unit prices equal to or less than $0.00 represent administrative write-offs or sample giveaways and are filtered out.
4. **Day-Level Grain Aggregation:** High-frequency timestamp granularity (`YYYY-MM-DD HH:MM:SS`) is normalized to ISO calendar day (`YYYY-MM-DD`). Grouping by `(InvoiceDate, Country)` reduces raw millions of records into manageable daily summaries.

---

## 6. Analytical Serving Store (SQLite) & Idempotency

The final serving layer is an SQLite database located at `data/analytics.db`.

### Database Schema
```sql
CREATE TABLE IF NOT EXISTS daily_country_sales (
    InvoiceDate TEXT NOT NULL,
    Country TEXT NOT NULL,
    DailyTotalSales REAL NOT NULL,
    Year INTEGER NOT NULL,
    Month INTEGER NOT NULL
);

-- Analytical Optimization Indexes
CREATE INDEX IF NOT EXISTS idx_daily_country_sales_date_country 
ON daily_country_sales (InvoiceDate, Country);

CREATE INDEX IF NOT EXISTS idx_daily_country_sales_year_month 
ON daily_country_sales (Year, Month);
```

### Idempotency Guarantee
In batch pipelines, retries or manual re-runs must not duplicate data. In [`etl_scripts/analytics_loader.py`](etl_scripts/analytics_loader.py), loading uses `if_exists='replace'` within an atomic transaction. Re-running the pipeline 100 times produces the exact same row count and sales totals as running it once.

### Verification SQL Query
```sql
-- Query revenue breakdown by country
SELECT 
    Country, 
    COUNT(*) AS ActiveSalesDays, 
    ROUND(SUM(DailyTotalSales), 2) AS TotalRevenue
FROM daily_country_sales
GROUP BY Country
ORDER BY TotalRevenue DESC;
```

---

## 7. Repository File Layout

```
ecommerce-analytics-pipeline/
│
├── .env.example                               # Explicit documentation for all environment variables
├── .gitignore                                 # Git ignore covering Python, Airflow logs, and SQLite DBs
├── docker-compose.yml                         # Container stack: Postgres, Airflow Web/Scheduler, ETL Service
├── Dockerfile.etl                             # Custom ETL image with Airflow, Pandas, PyArrow, GX, Pytest
├── requirements.txt                           # Locked Python production and testing dependencies
├── README.md                                  # Complete technical architecture and execution manual
├── PROJECT_REPORT.md                          # Detailed academic & industry project engineering report
│
├── dags/
│   └── ecommerce_analytics_pipeline.py       # Master Airflow DAG (6-task sequential Medallion chain)
│
├── etl_scripts/
│   ├── __init__.py                            # Modular package exports
│   ├── raw_downloader.py                      # Task 1: HTTP streaming ingestion into data/raw/
│   ├── bronze_processor.py                    # Task 2: Schema cast & Year/Month Parquet partitioner
│   ├── gx_validator.py                        # Tasks 3 & 5: Great Expectations runner & Circuit Breaker
│   ├── silver_transformer.py                  # Task 4: Cleansing, UNKNOWN imputation & sales aggregation
│   └── analytics_loader.py                    # Task 6: Idempotent SQLite loader & index builder
│
├── great_expectations/
│   ├── great_expectations.yml                 # Master GX v3 project configuration
│   ├── checkpoints/
│   │   ├── bronze_checkpoint.yml              # Checkpoint specification for Bronze layer
│   │   └── silver_checkpoint.yml              # Checkpoint specification for Silver layer
│   ├── expectations/
│   │   ├── bronze_expectations.json           # 12 schema and type expectations for Bronze
│   │   └── silver_expectations.json           # 7 business rule expectations for Silver
│   ├── plugins/                               # Custom GX plugins directory
│   └── uncommitted/                           # Local Data Docs and validation logs (gitignored)
│
├── tests/
│   ├── __init__.py                            # Pytest test package
│   ├── test_silver_transformer.py             # Unit tests for imputation, filters, and aggregations
│   ├── test_circuit_breaker.py                # Tests verifying failure when invariants are breached
│   ├── test_analytics_loader.py               # Tests for SQLite table creation and idempotency
│   └── test_dag_integrity.py                 # AST and DagBag structural tests for Airflow DAG
│
└── data/
    ├── raw/                                   # Raw CSV landing zone (contains sample ecommerce_data.csv)
    ├── bronze/                                # Partitioned Bronze Parquet store (Year=YYYY/Month=MM)
    ├── silver/                                # Partitioned Silver Parquet store (Year=YYYY/Month=MM)
    └── analytics.db                           # Final SQLite serving database file
```

---

## 8. Configuration & Environment Variables

All settings are parameterized via `.env`. A complete template is provided in [`.env.example`](.env.example).

| Variable Name | Default Value | Description |
| :--- | :--- | :--- |
| `AIRFLOW_UID` | `50000` | Host UID mapped to the container user to prevent permission issues. |
| `AIRFLOW_GID` | `0` | Host GID (root group) for shared volume writes. |
| `_AIRFLOW_WWW_USER_USERNAME` | `airflow` | Default username for the Airflow Web UI. |
| `_AIRFLOW_WWW_USER_PASSWORD` | `airflow` | Default password for the Airflow Web UI. |
| `AIRFLOW__WEBSERVER__SECRET_KEY` | `462e0861a98cd65178c7d6184b8da536` | 32-byte hexadecimal key for securing web sessions. |
| `POSTGRES_USER` | `airflow` | PostgreSQL metadata user. |
| `POSTGRES_PASSWORD` | `airflow` | PostgreSQL metadata password. |
| `POSTGRES_DB` | `airflow` | PostgreSQL metadata database name. |
| `DATA_SOURCE_URL` | Databricks Retail Dataset | Remote HTTPS URL from which the raw transactions CSV is fetched. |
| `RAW_DATA_PATH` | `/opt/airflow/data/raw/ecommerce_data.csv` | Absolute path inside container for the raw CSV. |
| `BRONZE_DATA_PATH` | `/opt/airflow/data/bronze` | Absolute path inside container for Bronze Parquet partitions. |
| `SILVER_DATA_PATH` | `/opt/airflow/data/silver` | Absolute path inside container for Silver Parquet partitions. |
| `ANALYTICS_DB_PATH` | `/opt/airflow/data/analytics.db` | Absolute path inside container for SQLite serving database. |
| `GREAT_EXPECTATIONS_DIR` | `/opt/airflow/great_expectations` | Path to Great Expectations configuration directory. |

To initialize your environment:
```bash
cp .env.example .env
```

---

## 9. Quickstart & Execution Guide

### Option A: Running via Docker Compose (Recommended Production Setup)

#### 1. Clone the repository and configure environment
```bash
git clone https://github.com/Adarsh12325/ecommerce-analytics-pipeline.git
cd ecommerce-analytics-pipeline
cp .env.example .env
```

#### 2. Build and start the services
```bash
docker-compose up -d --build
```

This launches 4 core containers:
- `airflow_postgres`: PostgreSQL 15 metadata store.
- `airflow_init`: Performs `airflow db init` and registers the admin user.
- `airflow_webserver`: Accessible at `http://localhost:8080`.
- `airflow_scheduler`: Parses DAGs and executes tasks according to schedule.
- `airflow_etl_service`: Dedicated testing and ad-hoc runner container.

#### 3. Access the Airflow Web UI
1. Open your browser and navigate to `http://localhost:8080`.
2. Login with credentials:
   - **Username:** `airflow`
   - **Password:** `airflow`
3. Locate `ecommerce_analytics_pipeline` in the DAGs list.
4. Toggle the DAG switch to **Active**, then click the **Trigger DAG** (Play) button.

#### 4. Trigger via Command Line (Alternative)
```bash
docker-compose exec airflow-scheduler airflow dags trigger ecommerce_analytics_pipeline
```

#### 5. Monitor Task Execution Logs
```bash
docker-compose logs -f airflow-scheduler
```

---

### Option B: Running Standalone Locally (Offline / Development)

If you prefer testing the pipeline directly on your host machine without running Docker:

#### 1. Create and activate a Python virtual environment
```bash
python -m venv .venv
# On Windows (PowerShell):
.venv\Scripts\Activate.ps1
# On Linux/macOS:
source .venv/bin/activate
```

#### 2. Install requirements
```bash
pip install --upgrade pip
pip install -r requirements.txt
```

#### 3. Execute the full ETL pipeline end-to-end
```bash
python -c "
from etl_scripts.bronze_processor import process_to_bronze
from etl_scripts.gx_validator import run_validation_checkpoint
from etl_scripts.silver_transformer import transform_to_silver
from etl_scripts.analytics_loader import load_to_sqlite

print('>>> Ingesting Raw CSV into Bronze Parquet...')
process_to_bronze('data/raw/ecommerce_data.csv', 'data/bronze')

print('>>> Executing Bronze Data Quality Assertions...')
run_validation_checkpoint('bronze', 'data/bronze')

print('>>> Cleansing & Transforming into Silver Parquet...')
transform_to_silver('data/bronze', 'data/silver')

print('>>> Executing Silver Data Quality Assertions...')
run_validation_checkpoint('silver', 'data/silver')

print('>>> Loading Refined Dataset into SQLite Analytical Store...')
load_to_sqlite('data/silver', 'data/analytics.db')

print('>>> Batch Pipeline Finished Successfully!')
"
```

#### 4. Query the generated database
```bash
python -c "
import sqlite3
conn = sqlite3.connect('data/analytics.db')
cursor = conn.cursor()
cursor.execute('SELECT Country, COUNT(*), ROUND(SUM(DailyTotalSales), 2) FROM daily_country_sales GROUP BY Country ORDER BY 3 DESC;')
for row in cursor.fetchall():
    print(f'Country: {row[0]:<16} | Days: {row[1]:<3} | Total Revenue: ${row[2]:,.2f}')
conn.close()
"
```

---

## 10. Automated Testing Suite

The repository contains an automated test suite across 4 dedicated test modules.

### Executing Tests inside Docker
As specified in the submission requirements:
```bash
docker-compose exec etl-service pytest tests/ -v
```

### Executing Tests Locally
```bash
pytest tests/ -v
```

### Test Suite Coverage Breakdown

```
tests/
├── test_silver_transformer.py
│   ├── test_customer_id_imputation                     [PASSED] -> Validates null CustomerIDs become 'UNKNOWN'
│   ├── test_negative_quantity_and_invalid_price_filter [PASSED] -> Validates Quantity < 0 and UnitPrice <= 0 filtered
│   ├── test_total_price_arithmetic_and_aggregation     [PASSED] -> Asserts TotalPrice = Qty * Price & daily grouping
│   ├── test_empty_dataframe_handling                   [PASSED] -> Validates graceful schema return on empty input
│   └── test_transform_to_silver_disk_io               [PASSED] -> End-to-end integration reading/writing Parquet
│
├── test_circuit_breaker.py
│   ├── test_bronze_circuit_breaker_on_corrupt_data     [PASSED] -> Asserts RuntimeError raised on schema breach
│   ├── test_silver_circuit_breaker_on_negative_sales   [PASSED] -> Asserts RuntimeError raised if DailyTotalSales < 0
│   └── test_silver_circuit_breaker_on_duplicates       [PASSED] -> Asserts failure if (Date, Country) is not unique
│
├── test_analytics_loader.py
│   └── test_sqlite_loader_and_idempotency              [PASSED] -> Verifies replace mode, indexes & idempotency
│
└── test_dag_integrity.py
    ├── test_dag_file_syntax_and_ast                    [PASSED] -> Static AST parsing verifies 6 tasks & sequence
    └── test_dag_dagbag_runtime                         [PASSED] -> DagBag verification in container environment
```

**Result: 11 tests executed, 100% passing.**

---

## 11. Data Docs & Verification Evidence

### 1. Great Expectations Data Docs
Great Expectations generates static HTML reports illustrating dataset profiling and assertion results:
- **Location:** `great_expectations/uncommitted/data_docs/local_site/index.html`
- **How to view:** Open the file directly in any web browser, or serve it via Python:
  ```bash
  python -m http.server 8000 --directory great_expectations/uncommitted/data_docs/local_site
  ```
  Navigate to `http://localhost:8000` to inspect expectation metrics, column profiles, and pass/fail distributions.

### 2. Sample Terminal Output: Bronze Layer Validation
```
2026-10-01 14:03:11 [INFO] etl_scripts.gx_validator - TASK START: bronze_data_validation
2026-10-01 14:03:11 [INFO] etl_scripts.gx_validator - Loaded 71 records for bronze validation.
2026-10-01 14:03:11 [INFO] etl_scripts.gx_validator - Evaluating suite 'bronze_expectations'...
2026-10-01 14:03:12 [INFO] etl_scripts.gx_validator - ------------------------------------------------
2026-10-01 14:03:12 [INFO] etl_scripts.gx_validator - VALIDATION REPORT FOR STAGE: BRONZE
2026-10-01 14:03:12 [INFO] etl_scripts.gx_validator - Overall Success Status: True
2026-10-01 14:03:12 [INFO] etl_scripts.gx_validator - Total Expectations Evaluated: 12
2026-10-01 14:03:12 [INFO] etl_scripts.gx_validator - Successful Assertions: 12
2026-10-01 14:03:12 [INFO] etl_scripts.gx_validator - Unsuccessful Assertions: 0
2026-10-01 14:03:12 [INFO] etl_scripts.gx_validator - SUCCESS: All 12 bronze data quality assertions passed.
```

### 3. Sample Terminal Output: Silver Layer Validation
```
2026-10-01 14:03:13 [INFO] etl_scripts.gx_validator - TASK START: silver_data_validation
2026-10-01 14:03:13 [INFO] etl_scripts.gx_validator - Loaded 19 records for silver validation.
2026-10-01 14:03:13 [INFO] etl_scripts.gx_validator - Evaluating suite 'silver_expectations'...
2026-10-01 14:03:13 [INFO] etl_scripts.gx_validator - ------------------------------------------------
2026-10-01 14:03:13 [INFO] etl_scripts.gx_validator - VALIDATION REPORT FOR STAGE: SILVER
2026-10-01 14:03:13 [INFO] etl_scripts.gx_validator - Overall Success Status: True
2026-10-01 14:03:13 [INFO] etl_scripts.gx_validator - Total Expectations Evaluated: 7
2026-10-01 14:03:13 [INFO] etl_scripts.gx_validator - Successful Assertions: 7
2026-10-01 14:03:13 [INFO] etl_scripts.gx_validator - Unsuccessful Assertions: 0
2026-10-01 14:03:13 [INFO] etl_scripts.gx_validator - SUCCESS: All 7 silver data quality assertions passed.
```

### 4. Sample Serving Layer Verification Output
```
Verification: Table 'daily_country_sales' has 19 records and total sales of $2,410.40

Top Countries by Sales:
1. United Kingdom   | 8 Active Days | $1,495.10
2. France           | 4 Active Days | $  372.60
3. Australia        | 2 Active Days | $  180.60
4. Netherlands      | 1 Active Days | $  177.60
5. Switzerland      | 1 Active Days | $   76.50
6. Germany          | 1 Active Days | $   52.50
7. Belgium          | 1 Active Days | $   45.00
8. Norway           | 1 Active Days | $   10.50
```

---

## 12. Troubleshooting & FAQ

### Q1: Why do we use Parquet instead of keeping intermediate files in CSV?
**A:** Parquet is a columnar storage format with dictionary encoding and Snappy compression. It enforces typed schemas at the file level and supports partition pruning (only reading `Year=2023/Month=11`). This reduces analytical query I/O by up to 85% compared to raw CSVs.

### Q2: How does the Circuit Breaker prevent data corruption?
**A:** When an expectation in `bronze_expectations` or `silver_expectations` is breached (such as negative sales or schema missing `InvoiceNo`), `gx_validator.py` raises an unhandled `RuntimeError`. Airflow captures this non-zero exit, marks the task `FAILED`, and marks subsequent tasks as `UPSTREAM_FAILED`. No corrupted data ever reaches the SQLite database.

### Q3: What happens if Docker reports port 8080 is already allocated?
**A:** You can rebind the Airflow Webserver port in `docker-compose.yml`:
```yaml
ports:
  - "8085:8080"
```
Then access the Web UI at `http://localhost:8085`.

### Q4: Why impute `CustomerID` with `'UNKNOWN'` instead of dropping rows?
**A:** Dropping records with missing customer IDs destroys valid financial transaction volume originating from guest checkouts. Using `'UNKNOWN'` preserves 100% of top-line revenue metrics while distinguishing guest checkouts from authenticated user transactions.

### Q5: How is idempotency guaranteed when re-running the pipeline?
**A:** 
- In Bronze & Silver stages: Parquet output directories are wiped and atomically rewritten for each batch run.
- In the SQLite Loader: The pandas `to_sql()` operation executes with `if_exists='replace'`, dropping and recreating the target table within a transaction.

---
