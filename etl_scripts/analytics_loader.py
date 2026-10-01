"""
Analytical Store Loader Module for E-Commerce Batch Pipeline.

Reads refined partitioned Silver Parquet data and loads it idempotently into an
analytical serving layer (SQLite database) table 'daily_country_sales'.
"""

import logging
import os
import sqlite3
import sys
import time
import pandas as pd

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s (%(filename)s:%(lineno)d) - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


def load_to_sqlite(
    silver_dir: str,
    db_path: str,
    table_name: str = "daily_country_sales",
) -> None:
    """
    Load Silver Parquet dataset into SQLite analytical serving table idempotently.

    Args:
        silver_dir (str): Path to the Silver Parquet dataset directory.
        db_path (str): Filesystem path to the SQLite database file (e.g. data/analytics.db).
        table_name (str): Target table name in the analytical database.

    Raises:
        FileNotFoundError: If silver_dir does not exist.
        sqlite3.Error: If SQLite connection or write operation fails.
        Exception: On general pipeline loading errors.
    """
    start_time = time.time()
    logger.info("================================================================================")
    logger.info("TASK START: load_to_analytical_store")
    logger.info(f"Source Silver Directory: {silver_dir}")
    logger.info(f"Target Database File: {db_path}")
    logger.info(f"Target Table Name: {table_name}")
    logger.info("================================================================================")

    if not os.path.exists(silver_dir):
        err_msg = f"Silver data directory not found: '{silver_dir}'"
        logger.error(err_msg)
        raise FileNotFoundError(err_msg)

    # Read Silver Parquet dataset
    logger.info(f"Reading Silver Parquet data from: {silver_dir}...")
    try:
        silver_df = pd.read_parquet(silver_dir, engine="pyarrow")
    except Exception as e:
        logger.error(f"Failed to read Silver Parquet dataset: {str(e)}", exc_info=True)
        raise

    row_count = len(silver_df)
    logger.info(f"Read {row_count:,} records from Silver layer.")
    if row_count == 0:
        logger.warning("Silver dataset is empty. Proceeding with empty table creation.")

    # Ensure parent directory of database exists
    db_dir = os.path.dirname(os.path.abspath(db_path))
    os.makedirs(db_dir, exist_ok=True)
    logger.info(f"Verified target database directory: {db_dir}")

    # Establish SQLite connection
    conn = None
    try:
        logger.info(f"Connecting to SQLite database at: {db_path}...")
        conn = sqlite3.connect(db_path, timeout=30.0)

        # Enable SQLite write-ahead logging (WAL) for concurrency & performance
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")

        # Write DataFrame to SQLite using if_exists='replace' to guarantee idempotency
        logger.info(f"Writing {row_count:,} rows into table '{table_name}' (mode: REPLACE)...")
        silver_df.to_sql(
            name=table_name,
            con=conn,
            if_exists="replace",
            index=False,
            dtype={
                "InvoiceDate": "TEXT",
                "Country": "TEXT",
                "DailyTotalSales": "REAL",
                "Year": "INTEGER",
                "Month": "INTEGER",
            },
        )

        # Create indexes for optimal analytical query performance
        logger.info(f"Creating analytical indexes on table '{table_name}'...")
        conn.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{table_name}_date_country ON {table_name} (InvoiceDate, Country);"
        )
        conn.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{table_name}_year_month ON {table_name} (Year, Month);"
        )
        conn.commit()

        # Verification query
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*), SUM(DailyTotalSales) FROM {table_name};")
        loaded_count, total_sales = cursor.fetchone()
        logger.info(
            f"Verification: Table '{table_name}' has {loaded_count:,} records and total sales of ${total_sales:,.2f}"
        )

    except sqlite3.Error as sql_err:
        logger.error(f"SQLite database error occurred: {sql_err}", exc_info=True)
        if conn:
            conn.rollback()
        raise
    except Exception as e:
        logger.error(f"Unexpected error loading to SQLite: {str(e)}", exc_info=True)
        raise
    finally:
        if conn:
            conn.close()
            logger.info("SQLite connection closed gracefully.")

    elapsed = time.time() - start_time
    logger.info(f"TASK COMPLETE: load_to_analytical_store finished in {elapsed:.2f} seconds.")


if __name__ == "__main__":
    sample_silver = os.getenv("SILVER_DATA_PATH", "data/silver")
    sample_db = os.getenv("ANALYTICS_DB_PATH", "data/analytics.db")
    load_to_sqlite(sample_silver, sample_db)
