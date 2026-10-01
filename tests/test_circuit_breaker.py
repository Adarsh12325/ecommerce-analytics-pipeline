"""
Data Quality Circuit Breaker Test Suite.

Satisfies Core Requirement 10:
Verifies that when Great Expectations assertions are violated,
the validation runner raises an explicit exception / halts with non-zero exit,
proving that invalid data cannot propagate downstream in the pipeline.
"""

import os
import shutil
import tempfile
import pandas as pd
import pytest

from etl_scripts.gx_validator import run_validation_checkpoint


@pytest.fixture
def temp_gx_workdir():
    """
    Sets up a temporary working directory with bronze and silver test directories.
    """
    temp_dir = tempfile.mkdtemp()
    bronze_dir = os.path.join(temp_dir, "bronze")
    silver_dir = os.path.join(temp_dir, "silver")
    os.makedirs(bronze_dir, exist_ok=True)
    os.makedirs(silver_dir, exist_ok=True)

    yield {"root": temp_dir, "bronze": bronze_dir, "silver": silver_dir}

    shutil.rmtree(temp_dir, ignore_errors=True)


def test_bronze_circuit_breaker_on_corrupt_data(temp_gx_workdir):
    """
    Tests that a Bronze dataset missing essential columns or violating types
    triggers a RuntimeError circuit breaker.
    """
    bronze_dir = temp_gx_workdir["bronze"]

    # Create invalid bronze dataset missing required columns (e.g. missing InvoiceDate, StockCode)
    corrupted_bronze_df = pd.DataFrame(
        {
            "InvoiceNo": ["1001", "1002"],
            "WrongColumnA": [1, 2],
            "Quantity": [10, 20],
            "Year": [2023, 2023],
            "Month": [11, 11],
        }
    )
    corrupted_bronze_df.to_parquet(
        bronze_dir,
        partition_cols=["Year", "Month"],
        engine="pyarrow",
    )

    # Validation must raise RuntimeError (Circuit Breaker)
    with pytest.raises(RuntimeError) as exc_info:
        run_validation_checkpoint(
            stage="bronze",
            data_dir=bronze_dir,
            fail_on_error=True,
        )

    assert "CIRCUIT BREAKER TRIGGERED" in str(exc_info.value)


def test_silver_circuit_breaker_on_negative_sales(temp_gx_workdir):
    """
    Tests that a Silver dataset containing negative DailyTotalSales
    (which violates silver_expectations) trips the circuit breaker.
    """
    silver_dir = temp_gx_workdir["silver"]

    # Create invalid silver dataset containing negative DailyTotalSales
    corrupted_silver_df = pd.DataFrame(
        {
            "InvoiceDate": ["2023-11-01", "2023-11-02"],
            "Country": ["France", "France"],
            "DailyTotalSales": [150.00, -50.00],  # Negative sale violates invariant!
            "Year": [2023, 2023],
            "Month": [11, 11],
        }
    )
    corrupted_silver_df.to_parquet(
        silver_dir,
        partition_cols=["Year", "Month"],
        engine="pyarrow",
    )

    # Validation must raise RuntimeError (Circuit Breaker)
    with pytest.raises(RuntimeError) as exc_info:
        run_validation_checkpoint(
            stage="silver",
            data_dir=silver_dir,
            fail_on_error=True,
        )

    assert "CIRCUIT BREAKER TRIGGERED" in str(exc_info.value)


def test_silver_circuit_breaker_on_duplicate_composite_keys(temp_gx_workdir):
    """
    Tests that a Silver dataset containing duplicate composite keys (InvoiceDate, Country)
    trips the circuit breaker.
    """
    silver_dir = temp_gx_workdir["silver"]

    # Create invalid silver dataset with duplicate (InvoiceDate, Country) entries
    duplicate_silver_df = pd.DataFrame(
        {
            "InvoiceDate": ["2023-11-01", "2023-11-01"],  # Duplicate date & country!
            "Country": ["United Kingdom", "United Kingdom"],
            "DailyTotalSales": [100.00, 200.00],
            "Year": [2023, 2023],
            "Month": [11, 11],
        }
    )
    duplicate_silver_df.to_parquet(
        silver_dir,
        partition_cols=["Year", "Month"],
        engine="pyarrow",
    )

    with pytest.raises(RuntimeError) as exc_info:
        run_validation_checkpoint(
            stage="silver",
            data_dir=silver_dir,
            fail_on_error=True,
        )

    assert "CIRCUIT BREAKER TRIGGERED" in str(exc_info.value)
