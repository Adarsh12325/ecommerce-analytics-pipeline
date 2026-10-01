"""
Analytical Store Loader Test Suite.

Verifies:
1. Loading Silver Parquet into SQLite database table 'daily_country_sales'.
2. Idempotency: Multiple runs cleanly replace table contents without duplicate accumulation.
3. Database indexes are created on (InvoiceDate, Country) and (Year, Month).
4. Analytical queries execute successfully against the loaded SQLite store.
"""

import os
import shutil
import sqlite3
import tempfile
import pandas as pd
import pytest

from etl_scripts.analytics_loader import load_to_sqlite


@pytest.fixture
def sample_silver_data():
    """
    Provides mock Silver data ready for analytical store ingestion.
    """
    return pd.DataFrame(
        {
            "InvoiceDate": ["2023-11-01", "2023-11-01", "2023-11-02", "2023-11-02"],
            "Country": ["France", "United Kingdom", "Germany", "United Kingdom"],
            "DailyTotalSales": [245.50, 1890.75, 430.00, 1520.25],
            "Year": [2023, 2023, 2023, 2023],
            "Month": [11, 11, 11, 11],
        }
    )


def test_sqlite_loader_and_idempotency(sample_silver_data):
    """
    Tests that load_to_sqlite creates the database, writes table 'daily_country_sales',
    and re-running the function replaces existing rows rather than duplicating them.
    """
    temp_dir = tempfile.mkdtemp()
    try:
        silver_dir = os.path.join(temp_dir, "silver")
        db_path = os.path.join(temp_dir, "test_analytics.db")

        # Write sample silver Parquet dataset
        sample_silver_data.to_parquet(
            silver_dir,
            partition_cols=["Year", "Month"],
            engine="pyarrow",
        )

        # Run 1: Initial load
        load_to_sqlite(silver_dir=silver_dir, db_path=db_path, table_name="daily_country_sales")

        # Verify initial load
        assert os.path.exists(db_path)
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        cursor.execute("SELECT COUNT(*), SUM(DailyTotalSales) FROM daily_country_sales;")
        count1, sum_sales1 = cursor.fetchone()
        assert count1 == 4
        assert sum_sales1 == pytest.approx(4086.50, rel=1e-2)

        # Verify indexes exist
        cursor.execute("SELECT name FROM sqlite_master WHERE type='index';")
        index_names = [row[0] for row in cursor.fetchall()]
        assert "idx_daily_country_sales_date_country" in index_names
        assert "idx_daily_country_sales_year_month" in index_names

        # Run 2: Re-run load to verify idempotency (MUST NOT duplicate rows)
        load_to_sqlite(silver_dir=silver_dir, db_path=db_path, table_name="daily_country_sales")

        cursor.execute("SELECT COUNT(*), SUM(DailyTotalSales) FROM daily_country_sales;")
        count2, sum_sales2 = cursor.fetchone()
        assert count2 == 4, f"Idempotency violated! Expected 4 rows, found {count2}"
        assert sum_sales2 == pytest.approx(4086.50, rel=1e-2)

        # Run analytical SQL query
        cursor.execute("""
            SELECT Country, SUM(DailyTotalSales) as TotalRevenue
            FROM daily_country_sales
            GROUP BY Country
            ORDER BY TotalRevenue DESC;
        """)
        results = cursor.fetchall()
        assert results[0][0] == "United Kingdom"
        assert results[0][1] == pytest.approx(3411.00, rel=1e-2)

        conn.close()

    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)
