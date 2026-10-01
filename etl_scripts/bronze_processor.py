"""
Bronze Layer Processing Module for E-Commerce Batch Pipeline.

Reads raw uncompressed CSV data, parses timestamps, standardizes schema types,
extracts temporal partition keys ('Year', 'Month'), and writes partitioned
columnar Parquet files to data/bronze/.
"""

import logging
import os
import shutil
import sys
import pandas as pd

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s (%(filename)s:%(lineno)d) - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


def process_to_bronze(input_csv: str, output_dir: str) -> None:
    """
    Ingest raw CSV transactions, parse types, and partition into Bronze Parquet store.

    Args:
        input_csv (str): Filesystem path to the raw e-commerce CSV.
        output_dir (str): Filesystem path to the destination Bronze Parquet directory.

    Raises:
        FileNotFoundError: If input_csv does not exist.
        ValueError: If required columns are missing from the raw dataset.
        Exception: On data reading or serialization failures.
    """
    logger.info("================================================================================")
    logger.info("TASK START: bronze_layer_processing")
    logger.info(f"Input Raw CSV: {input_csv}")
    logger.info(f"Output Bronze Directory: {output_dir}")
    logger.info("================================================================================")

    if not os.path.exists(input_csv):
        err_msg = f"Raw CSV source file does not exist at path: '{input_csv}'"
        logger.error(err_msg)
        raise FileNotFoundError(err_msg)

    # Read raw CSV with fallback encodings to support international characters
    logger.info(f"Reading raw CSV data from {input_csv}...")
    try:
        try:
            df = pd.read_csv(input_csv, encoding="utf-8")
        except UnicodeDecodeError:
            logger.warning("UTF-8 decoding failed; falling back to ISO-8859-1 (Latin1) encoding.")
            df = pd.read_csv(input_csv, encoding="ISO-8859-1")
    except Exception as e:
        logger.error(f"Failed to load CSV file: {str(e)}", exc_info=True)
        raise

    initial_row_count = len(df)
    logger.info(f"Loaded {initial_row_count:,} records from raw CSV.")
    logger.info(f"Raw columns detected: {list(df.columns)}")

    # Verify essential columns exist in the incoming source
    required_cols = ["InvoiceNo", "StockCode", "Description", "Quantity", "InvoiceDate", "UnitPrice", "CustomerID", "Country"]
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        err_msg = f"Schema validation failed: Missing required columns in raw CSV: {missing_cols}"
        logger.error(err_msg)
        raise ValueError(err_msg)

    # Standardize data types
    logger.info("Standardizing schemas and parsing datetime fields...")
    df["InvoiceNo"] = df["InvoiceNo"].astype(str)
    df["StockCode"] = df["StockCode"].astype(str)
    df["Description"] = df["Description"].astype(str)
    df["Country"] = df["Country"].astype(str)

    # Parse InvoiceDate with flexible datetime parser
    df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"], errors="coerce")
    null_dates = df["InvoiceDate"].isna().sum()
    if null_dates > 0:
        logger.warning(f"Detected {null_dates} records with unparseable InvoiceDate; dropping them.")
        df = df.dropna(subset=["InvoiceDate"])

    # Ensure Quantity is integer and UnitPrice is float
    df["Quantity"] = pd.to_numeric(df["Quantity"], errors="coerce").fillna(0).astype("int64")
    df["UnitPrice"] = pd.to_numeric(df["UnitPrice"], errors="coerce").fillna(0.0).astype("float64")

    # Keep CustomerID intact for Bronze (numeric string or NaN)
    # We do NOT impute 'UNKNOWN' here; business cleansing is deferred to Silver
    df["CustomerID"] = df["CustomerID"].astype(object)

    # Extract temporal partition columns: Year and Month
    logger.info("Extracting 'Year' and 'Month' partition columns from InvoiceDate...")
    df["Year"] = df["InvoiceDate"].dt.year.astype(int)
    df["Month"] = df["InvoiceDate"].dt.month.astype(int)

    logger.info(f"Extracted partitions: Years={sorted(df['Year'].unique().tolist())}, Months={sorted(df['Month'].unique().tolist())}")

    # Prepare Bronze destination directory (Idempotent overwrite)
    if os.path.exists(output_dir):
        logger.info(f"Target bronze directory exists. Overwriting content in: {output_dir}")
        try:
            shutil.rmtree(output_dir)
        except OSError as e:
            logger.warning(f"Could not purge directory cleanly: {e}")
    os.makedirs(output_dir, exist_ok=True)

    # Write to Parquet with Year/Month partitioning
    logger.info(f"Serializing {len(df):,} records to Parquet partitioned by ['Year', 'Month']...")
    try:
        df.to_parquet(
            output_dir,
            engine="pyarrow",
            partition_cols=["Year", "Month"],
            index=False,
            compression="snappy",
        )
        logger.info(f"Successfully saved partitioned Bronze Parquet files to {output_dir}")
    except Exception as e:
        logger.error(f"Failed to write Parquet dataset: {str(e)}", exc_info=True)
        raise

    logger.info("TASK COMPLETE: bronze_layer_processing successfully finished.")


if __name__ == "__main__":
    sample_csv = os.getenv("RAW_DATA_PATH", "data/raw/ecommerce_data.csv")
    sample_bronze = os.getenv("BRONZE_DATA_PATH", "data/bronze")
    process_to_bronze(sample_csv, sample_bronze)
