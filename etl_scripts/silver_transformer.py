"""
Silver Layer Transformation Module for E-Commerce Batch Pipeline.

Reads partitioned Bronze Parquet data, executes data cleansing rules (null imputation,
filtering anomalies/returns), computes derived business metrics, aggregates daily
country sales, and writes partitioned Parquet files to data/silver/.
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


def clean_and_transform_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Core transformation logic isolated for unit testing and pipeline execution.

    Business Rules:
    1. Impute missing/null CustomerID values with the string literal 'UNKNOWN'.
    2. Filter out non-positive transactions: Quantity < 0 or UnitPrice <= 0.
    3. Calculate derived metric: TotalPrice = Quantity * UnitPrice.
    4. Truncate InvoiceDate to the day level (YYYY-MM-DD).
    5. Group by (InvoiceDate, Country) and calculate DailyTotalSales = sum(TotalPrice).
    6. Derive Year and Month partition columns.

    Args:
        df (pd.DataFrame): Raw or Bronze DataFrame.

    Returns:
        pd.DataFrame: Refined, aggregated Silver DataFrame.
    """
    if df.empty:
        logger.warning("Received empty DataFrame for transformation.")
        return pd.DataFrame(
            columns=["InvoiceDate", "Country", "DailyTotalSales", "Year", "Month"]
        )

    # Make an explicit working copy
    transformed_df = df.copy()

    # Rule 1: Impute missing/null CustomerID with string literal 'UNKNOWN'
    logger.info("Applying Rule 1: Imputing missing CustomerID values with 'UNKNOWN'...")
    # Handle NaN, None, and empty/whitespace strings
    transformed_df["CustomerID"] = transformed_df["CustomerID"].fillna("UNKNOWN")
    transformed_df["CustomerID"] = transformed_df["CustomerID"].apply(
        lambda x: "UNKNOWN" if (pd.isna(x) or str(x).strip() in ("", "nan", "None", "<NA>")) else str(x).split(".")[0] if isinstance(x, float) else str(x)
    )

    # Ensure Quantity and UnitPrice are numeric
    transformed_df["Quantity"] = pd.to_numeric(transformed_df["Quantity"], errors="coerce").fillna(0)
    transformed_df["UnitPrice"] = pd.to_numeric(transformed_df["UnitPrice"], errors="coerce").fillna(0.0)

    # Rule 2: Filter out records where Quantity < 0 or UnitPrice <= 0
    initial_rows = len(transformed_df)
    logger.info(f"Applying Rule 2: Filtering anomalous records (Quantity >= 0 and UnitPrice > 0)... Initial rows: {initial_rows}")
    valid_filter = (transformed_df["Quantity"] >= 0) & (transformed_df["UnitPrice"] > 0)
    transformed_df = transformed_df[valid_filter].copy()
    retained_rows = len(transformed_df)
    filtered_out = initial_rows - retained_rows
    logger.info(f"Filtered out {filtered_out:,} invalid/return rows ({filtered_out/initial_rows*100:.2f}%). Retained: {retained_rows:,} rows.")

    if transformed_df.empty:
        logger.warning("No rows remained after applying filtering rules.")
        return pd.DataFrame(
            columns=["InvoiceDate", "Country", "DailyTotalSales", "Year", "Month"]
        )

    # Rule 3: Compute derived metric TotalPrice = Quantity * UnitPrice
    logger.info("Applying Rule 3: Calculating derived metric TotalPrice = Quantity * UnitPrice...")
    transformed_df["TotalPrice"] = transformed_df["Quantity"] * transformed_df["UnitPrice"]
    transformed_df["TotalPrice"] = transformed_df["TotalPrice"].round(2)

    # Rule 4: Truncate InvoiceDate to the day level
    logger.info("Applying Rule 4: Normalizing InvoiceDate to calendar day (YYYY-MM-DD)...")
    transformed_df["InvoiceDate"] = pd.to_datetime(transformed_df["InvoiceDate"], errors="coerce")
    transformed_df = transformed_df.dropna(subset=["InvoiceDate"])
    # Format as ISO date string YYYY-MM-DD for standard analytical grouping and storage
    transformed_df["InvoiceDate"] = transformed_df["InvoiceDate"].dt.strftime("%Y-%m-%d")

    # Clean Country column
    transformed_df["Country"] = transformed_df["Country"].astype(str).str.strip()

    # Rule 5: Group by (InvoiceDate, Country) and calculate DailyTotalSales
    logger.info("Applying Rule 5: Grouping by [InvoiceDate, Country] and aggregating DailyTotalSales...")
    aggregated_df = (
        transformed_df.groupby(["InvoiceDate", "Country"], as_index=False)["TotalPrice"]
        .sum()
        .rename(columns={"TotalPrice": "DailyTotalSales"})
    )
    aggregated_df["DailyTotalSales"] = aggregated_df["DailyTotalSales"].round(2)

    # Rule 6: Derive Year and Month partition keys from the day-level InvoiceDate
    date_series = pd.to_datetime(aggregated_df["InvoiceDate"])
    aggregated_df["Year"] = date_series.dt.year.astype(int)
    aggregated_df["Month"] = date_series.dt.month.astype(int)

    # Sort deterministically for idempotent outputs
    aggregated_df = aggregated_df.sort_values(by=["InvoiceDate", "Country"]).reset_index(drop=True)
    logger.info(f"Transformation complete. Aggregated output contains {len(aggregated_df):,} daily country records.")

    return aggregated_df


def transform_to_silver(bronze_dir: str, silver_dir: str) -> None:
    """
    Read partitioned Bronze Parquet data, cleanse, aggregate, and serialize to Silver Parquet.

    Args:
        bronze_dir (str): Source directory containing Bronze Parquet partitions.
        silver_dir (str): Target directory for saving partitioned Silver Parquet files.

    Raises:
        FileNotFoundError: If bronze_dir does not exist or has no parquet files.
        Exception: On transformation or writing failures.
    """
    logger.info("================================================================================")
    logger.info("TASK START: silver_layer_transformation")
    logger.info(f"Source Bronze Directory: {bronze_dir}")
    logger.info(f"Target Silver Directory: {silver_dir}")
    logger.info("================================================================================")

    if not os.path.exists(bronze_dir):
        err_msg = f"Bronze data directory does not exist: '{bronze_dir}'"
        logger.error(err_msg)
        raise FileNotFoundError(err_msg)

    # Read the full partitioned Bronze dataset using PyArrow
    logger.info(f"Reading partitioned Parquet dataset from: {bronze_dir}...")
    try:
        bronze_df = pd.read_parquet(bronze_dir, engine="pyarrow")
    except Exception as e:
        logger.error(f"Failed to read Bronze Parquet dataset: {str(e)}", exc_info=True)
        raise

    logger.info(f"Successfully loaded {len(bronze_df):,} records from Bronze layer.")

    # Apply core cleansing and business aggregations
    silver_df = clean_and_transform_dataframe(bronze_df)

    # Prepare Silver destination directory (Idempotent overwrite)
    if os.path.exists(silver_dir):
        logger.info(f"Target silver directory exists. Overwriting content in: {silver_dir}")
        try:
            shutil.rmtree(silver_dir)
        except OSError as e:
            logger.warning(f"Could not purge directory cleanly: {e}")
    os.makedirs(silver_dir, exist_ok=True)

    # Serialize to partitioned Parquet by ['Year', 'Month']
    logger.info(f"Serializing {len(silver_df):,} records to Silver Parquet partitioned by ['Year', 'Month']...")
    try:
        silver_df.to_parquet(
            silver_dir,
            engine="pyarrow",
            partition_cols=["Year", "Month"],
            index=False,
            compression="snappy",
        )
        logger.info(f"Successfully wrote partitioned Silver Parquet to: {silver_dir}")
    except Exception as e:
        logger.error(f"Failed to write Silver Parquet dataset: {str(e)}", exc_info=True)
        raise

    logger.info("TASK COMPLETE: silver_layer_transformation successfully finished.")


if __name__ == "__main__":
    sample_bronze = os.getenv("BRONZE_DATA_PATH", "data/bronze")
    sample_silver = os.getenv("SILVER_DATA_PATH", "data/silver")
    transform_to_silver(sample_bronze, sample_silver)
