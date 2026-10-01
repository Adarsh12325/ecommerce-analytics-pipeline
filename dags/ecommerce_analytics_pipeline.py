"""
Apache Airflow DAG: E-Commerce Analytics Batch Pipeline.

Orchestrates the Medallion batch architecture (Raw -> Bronze -> Silver -> Analytical SQLite)
with automated Great Expectations data quality gates (Circuit Breakers) at each stage.

DAG ID: ecommerce_analytics_pipeline
Schedule: @daily
Sequence:
    download_raw_data
    >> bronze_layer_processing
    >> bronze_data_validation
    >> silver_layer_transformation
    >> silver_data_validation
    >> load_to_analytical_store
"""

import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# Ensure root workspace is on PYTHONPATH for module imports
WORKSPACE_DIR = os.getenv("AIRFLOW_HOME", "/opt/airflow")
if WORKSPACE_DIR not in sys.path:
    sys.path.insert(0, WORKSPACE_DIR)

# Import modular ETL functions directly from etl_scripts package
from etl_scripts.raw_downloader import download_raw_data
from etl_scripts.bronze_processor import process_to_bronze
from etl_scripts.silver_transformer import transform_to_silver
from etl_scripts.analytics_loader import load_to_sqlite
from etl_scripts.gx_validator import run_validation_checkpoint

# Pipeline paths configurable via environment variables with safe defaults
DATA_SOURCE_URL = os.getenv(
    "DATA_SOURCE_URL",
    "https://raw.githubusercontent.com/databricks/Spark-The-Definitive-Guide/master/data/retail-data/all/online-retail-dataset.csv",
)
RAW_DATA_PATH = os.getenv("RAW_DATA_PATH", os.path.join(WORKSPACE_DIR, "data", "raw", "ecommerce_data.csv"))
BRONZE_DATA_PATH = os.getenv("BRONZE_DATA_PATH", os.path.join(WORKSPACE_DIR, "data", "bronze"))
SILVER_DATA_PATH = os.getenv("SILVER_DATA_PATH", os.path.join(WORKSPACE_DIR, "data", "silver"))
ANALYTICS_DB_PATH = os.getenv("ANALYTICS_DB_PATH", os.path.join(WORKSPACE_DIR, "data", "analytics.db"))
GREAT_EXPECTATIONS_DIR = os.getenv(
    "GREAT_EXPECTATIONS_DIR", os.path.join(WORKSPACE_DIR, "great_expectations")
)

# Standard Airflow enterprise default task configuration
default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "start_date": datetime(2023, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=30),
}

# Define the Master Directed Acyclic Graph
with DAG(
    dag_id="ecommerce_analytics_pipeline",
    default_args=default_args,
    description="Batch Medallion ETL pipeline with automated Great Expectations quality gates",
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["ecommerce", "medallion", "great_expectations", "production", "etl"],
) as dag:

    # -------------------------------------------------------------------------
    # Task 1: Download Raw Data
    # -------------------------------------------------------------------------
    download_raw_data_task = PythonOperator(
        task_id="download_raw_data",
        python_callable=download_raw_data,
        op_kwargs={
            "url": DATA_SOURCE_URL,
            "dest_path": RAW_DATA_PATH,
        },
        doc_md="""
        ### Ingest External Raw E-Commerce Transactions
        Streams the raw online retail CSV dataset over HTTPS into the local Raw layer (`data/raw/`).
        Enforces strict HTTP status verification; non-200 responses immediately fail the task.
        """,
    )

    # -------------------------------------------------------------------------
    # Task 2: Bronze Layer Processing
    # -------------------------------------------------------------------------
    bronze_layer_processing_task = PythonOperator(
        task_id="bronze_layer_processing",
        python_callable=process_to_bronze,
        op_kwargs={
            "input_csv": RAW_DATA_PATH,
            "output_dir": BRONZE_DATA_PATH,
        },
        doc_md="""
        ### Standardize & Partition to Bronze Parquet Layer
        Parses `InvoiceDate` to timestamps, enforces primitive types, extracts `Year` and `Month`,
        and writes partitioned Snappy-compressed Apache Parquet files to `data/bronze/`.
        """,
    )

    # -------------------------------------------------------------------------
    # Task 3: Bronze Data Quality Validation (Great Expectations Circuit Breaker)
    # -------------------------------------------------------------------------
    bronze_data_validation_task = PythonOperator(
        task_id="bronze_data_validation",
        python_callable=run_validation_checkpoint,
        op_kwargs={
            "stage": "bronze",
            "data_dir": BRONZE_DATA_PATH,
            "context_root_dir": GREAT_EXPECTATIONS_DIR,
            "fail_on_error": True,
        },
        doc_md="""
        ### Bronze Layer Data Quality Gate
        Evaluates the `bronze_expectations` suite against the newly created Bronze Parquet dataset.
        Validates column existence, nullity constraints, numeric types, and value bounds.
        Failure immediately raises an exception to halt downstream execution (Circuit Breaker).
        """,
    )

    # -------------------------------------------------------------------------
    # Task 4: Silver Layer Transformation
    # -------------------------------------------------------------------------
    silver_layer_transformation_task = PythonOperator(
        task_id="silver_layer_transformation",
        python_callable=transform_to_silver,
        op_kwargs={
            "bronze_dir": BRONZE_DATA_PATH,
            "silver_dir": SILVER_DATA_PATH,
        },
        doc_md="""
        ### Cleanse & Aggregate to Silver Analytical Layer
        Imputes missing `CustomerID` with `'UNKNOWN'`, filters out negative quantities and non-positive prices,
        calculates `TotalPrice = Quantity * UnitPrice`, and aggregates daily country revenues.
        Writes partitioned Parquet files to `data/silver/`.
        """,
    )

    # -------------------------------------------------------------------------
    # Task 5: Silver Data Quality Validation (Great Expectations Circuit Breaker)
    # -------------------------------------------------------------------------
    silver_data_validation_task = PythonOperator(
        task_id="silver_data_validation",
        python_callable=run_validation_checkpoint,
        op_kwargs={
            "stage": "silver",
            "data_dir": SILVER_DATA_PATH,
            "context_root_dir": GREAT_EXPECTATIONS_DIR,
            "fail_on_error": True,
        },
        doc_md="""
        ### Silver Layer Business Invariant Gate
        Asserts analytical correctness on aggregated data using `silver_expectations`.
        Verifies `DailyTotalSales >= 0`, composite uniqueness across `(InvoiceDate, Country)`,
        and valid row count boundaries. Halts loading if invariants are breached.
        """,
    )

    # -------------------------------------------------------------------------
    # Task 6: Load to Analytical Store (SQLite Serving Layer)
    # -------------------------------------------------------------------------
    load_to_analytical_store_task = PythonOperator(
        task_id="load_to_analytical_store",
        python_callable=load_to_sqlite,
        op_kwargs={
            "silver_dir": SILVER_DATA_PATH,
            "db_path": ANALYTICS_DB_PATH,
            "table_name": "daily_country_sales",
        },
        doc_md="""
        ### Idempotent Serving Load into SQLite
        Reads the verified Silver Parquet dataset and executes an idempotent write
        (`if_exists='replace'`) into table `daily_country_sales` in `data/analytics.db`.
        Creates analytical B-tree indexes for low-latency BI and analyst queries.
        """,
    )

    # -------------------------------------------------------------------------
    # Master Pipeline Execution Graph (Exact Sequential Dependency Order)
    # -------------------------------------------------------------------------
    (
        download_raw_data_task
        >> bronze_layer_processing_task
        >> bronze_data_validation_task
        >> silver_layer_transformation_task
        >> silver_data_validation_task
        >> load_to_analytical_store_task
    )
