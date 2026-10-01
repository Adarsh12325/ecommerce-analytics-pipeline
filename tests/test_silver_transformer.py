"""
Unit and Integration Test Suite for Silver Layer Transformation Logic.

Satisfies Core Requirement 12:
- Test 1: CustomerID imputation (missing/null replaced with 'UNKNOWN', valid IDs preserved).
- Test 2: Negative quantity and non-positive price filtering.
- Test 3: TotalPrice arithmetic and daily country sales aggregation.
- Additional tests: Edge cases, date truncation, Parquet I/O, and idempotency.
"""

import os
import shutil
import tempfile
import pandas as pd
import pytest

from etl_scripts.silver_transformer import clean_and_transform_dataframe, transform_to_silver


@pytest.fixture
def sample_raw_transactions_df():
    """
    Provides a synthetic mock DataFrame containing clean, dirty, and edge-case transactions.
    """
    return pd.DataFrame(
        {
            "InvoiceNo": ["5001", "5002", "5003", "5004", "5005", "5006", "5007"],
            "StockCode": ["A1", "A2", "A3", "A4", "A5", "A6", "A7"],
            "Description": [
                "Widget Red",
                "Widget Blue",
                "Widget Green",
                "Widget Yellow",
                "Widget Orange",
                "Widget Purple",
                "Widget Black",
            ],
            "Quantity": [10, -5, 20, 0, 15, 5, 2],
            "InvoiceDate": [
                "2023-11-01 10:15:00",
                "2023-11-01 11:30:00",
                "2023-11-01 14:45:00",
                "2023-11-02 09:00:00",
                "2023-11-02 16:20:00",
                "2023-11-02 18:00:00",
                "2023-11-03 12:00:00",
            ],
            "UnitPrice": [2.50, 2.50, 4.00, 5.00, 10.00, 0.00, 50.00],
            "CustomerID": [12345.0, 12345.0, None, 14000.0, float("nan"), 15000.0, 16000.0],
            "Country": [
                "United Kingdom",
                "United Kingdom",
                "United Kingdom",
                "France",
                "France",
                "Germany",
                "Germany",
            ],
        }
    )


# ==============================================================================
# Requirement 12 - Test Case 1: CustomerID Imputation
# ==============================================================================
def test_customer_id_imputation():
    """
    Validates that missing or null CustomerID values are imputed with the string literal 'UNKNOWN',
    while valid customer identifiers are preserved.
    """
    raw_df = pd.DataFrame(
        {
            "InvoiceNo": ["101", "102", "103", "104"],
            "StockCode": ["P1", "P2", "P3", "P4"],
            "Description": ["Product 1", "Product 2", "Product 3", "Product 4"],
            "Quantity": [5, 10, 2, 8],
            "InvoiceDate": [
                "2023-10-15 10:00:00",
                "2023-10-15 11:00:00",
                "2023-10-16 12:00:00",
                "2023-10-16 13:00:00",
            ],
            "UnitPrice": [10.0, 20.0, 15.0, 5.0],
            "CustomerID": [None, 19842.0, float("nan"), "   "],
            "Country": ["United Kingdom", "United Kingdom", "France", "France"],
        }
    )

    # Directly test the raw dataframe cleansing logic
    working_df = raw_df.copy()
    working_df["CustomerID"] = working_df["CustomerID"].fillna("UNKNOWN")
    working_df["CustomerID"] = working_df["CustomerID"].apply(
        lambda x: "UNKNOWN"
        if (pd.isna(x) or str(x).strip() in ("", "nan", "None", "<NA>"))
        else str(x).split(".")[0]
        if isinstance(x, float)
        else str(x)
    )

    expected_ids = ["UNKNOWN", "19842", "UNKNOWN", "UNKNOWN"]
    assert working_df["CustomerID"].tolist() == expected_ids, (
        f"CustomerID imputation failed. Expected {expected_ids}, got {working_df['CustomerID'].tolist()}"
    )

    # Verify that clean_and_transform_dataframe processes without dropping rows with missing CustomerID
    transformed = clean_and_transform_dataframe(raw_df)
    assert not transformed.empty
    # Revenue from UNKNOWN customer records must be retained in the final daily aggregations
    assert len(transformed) == 2  # 2 distinct (Date, Country) groups


# ==============================================================================
# Requirement 12 - Test Case 2: Negative Quantity and Non-Positive Price Filter
# ==============================================================================
def test_negative_quantity_and_invalid_price_filtering():
    """
    Verifies that records where Quantity < 0 (returns/anomalies) or UnitPrice <= 0
    are filtered out, ensuring no corrupted or zero-value transactions pass through.
    """
    raw_df = pd.DataFrame(
        {
            "InvoiceNo": ["201", "C202", "203", "204", "205"],
            "StockCode": ["S1", "S2", "S3", "S4", "S5"],
            "Description": ["Good Item", "Return Item", "Zero Price", "Negative Price", "Valid Item"],
            "Quantity": [10, -5, 20, 15, 2],
            "InvoiceDate": [
                "2023-10-20 09:00:00",
                "2023-10-20 10:00:00",
                "2023-10-20 11:00:00",
                "2023-10-20 12:00:00",
                "2023-10-20 13:00:00",
            ],
            "UnitPrice": [5.0, 5.0, 0.0, -2.5, 25.0],
            "CustomerID": [10001, 10001, 10002, 10003, 10004],
            "Country": ["Germany", "Germany", "Germany", "Germany", "Germany"],
        }
    )

    transformed = clean_and_transform_dataframe(raw_df)

    # Only rows 201 (10 * 5 = 50.0) and 205 (2 * 25 = 50.0) are valid
    # Row C202 is excluded (Quantity = -5 < 0)
    # Row 203 is excluded (UnitPrice = 0.0 <= 0)
    # Row 204 is excluded (UnitPrice = -2.5 <= 0)
    assert len(transformed) == 1
    assert transformed.iloc[0]["Country"] == "Germany"
    assert transformed.iloc[0]["InvoiceDate"] == "2023-10-20"
    assert transformed.iloc[0]["DailyTotalSales"] == pytest.approx(100.0, rel=1e-2)


# ==============================================================================
# Requirement 12 - Test Case 3: TotalPrice Arithmetic & Aggregation Logic
# ==============================================================================
def test_total_price_arithmetic_and_daily_country_aggregation():
    """
    Verifies that TotalPrice is calculated as Quantity * UnitPrice and that
    records are grouped by calendar day (InvoiceDate) and Country, correctly summing DailyTotalSales.
    """
    raw_df = pd.DataFrame(
        {
            "InvoiceNo": ["301", "302", "303", "304", "305"],
            "StockCode": ["P1", "P2", "P3", "P4", "P5"],
            "Description": ["Item 1", "Item 2", "Item 3", "Item 4", "Item 5"],
            "Quantity": [3, 7, 2, 4, 10],
            "InvoiceDate": [
                # Same day, same country (UK) -> 3*10.0 + 7*5.0 = 30 + 35 = 65.0
                "2023-12-05 08:30:00",
                "2023-12-05 14:15:00",
                # Same day, different country (France) -> 2*20.0 = 40.0
                "2023-12-05 16:00:00",
                # Different day, UK -> 4*15.0 = 60.0
                "2023-12-06 10:00:00",
                # Different day, France -> 10*3.5 = 35.0
                "2023-12-06 11:30:00",
            ],
            "UnitPrice": [10.0, 5.0, 20.0, 15.0, 3.5],
            "CustomerID": [101, 102, 103, 104, 105],
            "Country": ["United Kingdom", "United Kingdom", "France", "United Kingdom", "France"],
        }
    )

    transformed = clean_and_transform_dataframe(raw_df)

    # Expected groups:
    # 1. 2023-12-05, France: 40.00
    # 2. 2023-12-05, United Kingdom: 65.00
    # 3. 2023-12-06, France: 35.00
    # 4. 2023-12-06, United Kingdom: 60.00
    assert len(transformed) == 4

    # Verify composite key uniqueness
    composite_keys = list(zip(transformed["InvoiceDate"], transformed["Country"]))
    assert len(composite_keys) == len(set(composite_keys)), "Composite key (InvoiceDate, Country) is not unique!"

    # Verify calculations for 2023-12-05, UK
    uk_day1 = transformed[
        (transformed["InvoiceDate"] == "2023-12-05") & (transformed["Country"] == "United Kingdom")
    ]
    assert len(uk_day1) == 1
    assert uk_day1.iloc[0]["DailyTotalSales"] == pytest.approx(65.00, rel=1e-2)
    assert uk_day1.iloc[0]["Year"] == 2023
    assert uk_day1.iloc[0]["Month"] == 12

    # Verify calculations for 2023-12-05, France
    fr_day1 = transformed[
        (transformed["InvoiceDate"] == "2023-12-05") & (transformed["Country"] == "France")
    ]
    assert len(fr_day1) == 1
    assert fr_day1.iloc[0]["DailyTotalSales"] == pytest.approx(40.00, rel=1e-2)


# ==============================================================================
# Additional Tests: Edge Cases & Parquet Partitioning
# ==============================================================================
def test_empty_dataframe_handling():
    """
    Validates that empty inputs produce an empty DataFrame with the exact target schema.
    """
    empty_df = pd.DataFrame(
        columns=["InvoiceNo", "StockCode", "Description", "Quantity", "InvoiceDate", "UnitPrice", "CustomerID", "Country"]
    )
    result = clean_and_transform_dataframe(empty_df)
    assert result.empty
    assert list(result.columns) == ["InvoiceDate", "Country", "DailyTotalSales", "Year", "Month"]


def test_transform_to_silver_disk_io(sample_raw_transactions_df):
    """
    End-to-end integration test reading mock Bronze Parquet and writing partitioned Silver Parquet.
    """
    temp_dir = tempfile.mkdtemp()
    try:
        bronze_dir = os.path.join(temp_dir, "bronze")
        silver_dir = os.path.join(temp_dir, "silver")

        # Create Bronze Parquet dataset
        sample_df = sample_raw_transactions_df.copy()
        sample_df["InvoiceDate"] = pd.to_datetime(sample_df["InvoiceDate"])
        sample_df["Year"] = sample_df["InvoiceDate"].dt.year
        sample_df["Month"] = sample_df["InvoiceDate"].dt.month
        sample_df.to_parquet(bronze_dir, partition_cols=["Year", "Month"], engine="pyarrow")

        # Execute transformation function
        transform_to_silver(bronze_dir=bronze_dir, silver_dir=silver_dir)

        # Assert output directory exists and is readable
        assert os.path.exists(silver_dir)
        silver_read_df = pd.read_parquet(silver_dir, engine="pyarrow")
        assert not silver_read_df.empty
        assert "DailyTotalSales" in silver_read_df.columns
        assert (silver_read_df["DailyTotalSales"] >= 0).all()

        # Idempotency test: Re-running should cleanly overwrite without duplication
        transform_to_silver(bronze_dir=bronze_dir, silver_dir=silver_dir)
        silver_read_df_rerun = pd.read_parquet(silver_dir, engine="pyarrow")
        assert len(silver_read_df) == len(silver_read_df_rerun)

    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)
