"""
Great Expectations Automated Quality Assurance & Circuit Breaker Module.

Executes Expectation Suites against Bronze and Silver data layers, renders
comprehensive Data Docs, and acts as a strict pipeline circuit breaker.
If data quality assertions fail, this module logs violation specifics and raises
a non-zero exit / RuntimeError, forcing Airflow to halt downstream execution.
"""

import argparse
import logging
import os
import sys
from typing import Dict, Any, Optional

import pandas as pd
import great_expectations as gx
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.data_context import FileDataContext

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s (%(filename)s:%(lineno)d) - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


def get_gx_context(context_root_dir: Optional[str] = None) -> FileDataContext:
    """
    Locates and returns the Great Expectations FileDataContext.

    Args:
        context_root_dir (Optional[str]): Explicit path to great_expectations directory.

    Returns:
        FileDataContext: Loaded GX data context.
    """
    if not context_root_dir:
        candidates = [
            os.getenv("GREAT_EXPECTATIONS_DIR"),
            os.path.join(os.getcwd(), "great_expectations"),
            os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "great_expectations")),
            "/opt/airflow/great_expectations",
        ]
        for candidate in candidates:
            if candidate and os.path.exists(os.path.join(candidate, "great_expectations.yml")):
                context_root_dir = candidate
                break

    if not context_root_dir or not os.path.exists(context_root_dir):
        raise FileNotFoundError(
            f"Could not locate great_expectations directory. Checked candidates: {candidates}"
        )

    logger.info(f"Initializing Great Expectations DataContext from: {context_root_dir}")
    return FileDataContext(context_root_dir=context_root_dir)


def run_validation_checkpoint(
    stage: str,
    data_dir: str,
    context_root_dir: Optional[str] = None,
    fail_on_error: bool = True,
) -> Dict[str, Any]:
    """
    Executes a Great Expectations validation checkpoint against the specified storage layer.

    Args:
        stage (str): Pipeline stage to validate ('bronze' or 'silver').
        data_dir (str): Directory containing partitioned Parquet data.
        context_root_dir (Optional[str]): Root directory of great_expectations config.
        fail_on_error (bool): If True, raises RuntimeError when validation fails (Circuit Breaker).

    Returns:
        Dict[str, Any]: Summary dictionary with success status and statistics.

    Raises:
        RuntimeError: When validation fails and fail_on_error is True.
        FileNotFoundError: When data_dir does not exist.
    """
    stage = stage.lower().strip()
    if stage not in ("bronze", "silver"):
        raise ValueError(f"Invalid validation stage '{stage}'. Must be 'bronze' or 'silver'.")

    checkpoint_name = f"{stage}_checkpoint"
    suite_name = f"{stage}_expectations"

    logger.info("================================================================================")
    logger.info(f"TASK START: {stage}_data_validation")
    logger.info(f"Target Data Directory: {data_dir}")
    logger.info(f"Checkpoint Name: {checkpoint_name}")
    logger.info(f"Expectation Suite: {suite_name}")
    logger.info("================================================================================")

    if not os.path.exists(data_dir):
        err_msg = f"Data directory for validation does not exist: {data_dir}"
        logger.error(err_msg)
        raise FileNotFoundError(err_msg)

    # Read Parquet dataset into memory for RuntimeBatchRequest
    logger.info(f"Reading {stage} Parquet data for validation profiling from: {data_dir}...")
    try:
        df = pd.read_parquet(data_dir, engine="pyarrow")
    except Exception as e:
        logger.error(f"Failed to load Parquet data from {data_dir}: {str(e)}", exc_info=True)
        raise

    logger.info(f"Loaded {len(df):,} records for {stage} validation. Columns: {list(df.columns)}")

    # Initialize GX context
    context = get_gx_context(context_root_dir)

    # Build RuntimeBatchRequest
    batch_request = RuntimeBatchRequest(
        datasource_name="pandas_datasource",
        data_connector_name="runtime_data_connector",
        data_asset_name=f"{stage}_dataset",
        runtime_parameters={"batch_data": df},
        batch_identifiers={
            "pipeline_stage": stage,
            "run_id": f"{stage}_validation_run",
        },
    )

    # Execute validation via validator
    logger.info(f"Evaluating suite '{suite_name}' against {stage} dataset...")
    try:
        validator = context.get_validator(
            batch_request=batch_request,
            expectation_suite_name=suite_name,
        )
        validation_result = validator.validate()
    except Exception as e:
        logger.error(f"Exception during validation execution: {str(e)}", exc_info=True)
        raise

    # Extract results and statistics
    success = validation_result.success
    stats = validation_result.statistics
    evaluated_expectations = stats.get("evaluated_expectations", 0)
    successful_expectations = stats.get("successful_expectations", 0)
    unsuccessful_expectations = stats.get("unsuccessful_expectations", 0)

    failed_details = []
    for res in validation_result.results:
        if not res.success:
            failed_details.append({
                "expectation_type": res.expectation_config.expectation_type,
                "kwargs": res.expectation_config.kwargs,
                "result": res.result,
            })

    # Render HTML Data Docs
    try:
        context.build_data_docs()
        logger.info("Great Expectations Data Docs rendered successfully.")
    except Exception as doc_err:
        logger.warning(f"Note: Data Docs generation skipped or encountered minor notice: {doc_err}")

    logger.info("--------------------------------------------------------------------------------")
    logger.info(f"VALIDATION REPORT FOR STAGE: {stage.upper()}")
    logger.info(f"Overall Success Status: {success}")
    logger.info(f"Total Expectations Evaluated: {evaluated_expectations}")
    logger.info(f"Successful Assertions: {successful_expectations}")
    logger.info(f"Unsuccessful Assertions: {unsuccessful_expectations}")
    logger.info("--------------------------------------------------------------------------------")

    if not success:
        logger.error(f"CRITICAL: Data quality validation FAILED for {stage} layer!")
        for idx, failure in enumerate(failed_details, 1):
            logger.error(
                f"  Failure #{idx}: Expectation '{failure['expectation_type']}' "
                f"with kwargs {failure['kwargs']} failed. "
                f"Observed result: {failure['result']}"
            )

        if fail_on_error:
            circuit_breaker_msg = (
                f"CIRCUIT BREAKER TRIGGERED: {stage}_data_validation failed with "
                f"{unsuccessful_expectations} expectation violations out of {evaluated_expectations}. "
                f"Halting pipeline execution to prevent corrupted data propagation."
            )
            logger.critical(circuit_breaker_msg)
            raise RuntimeError(circuit_breaker_msg)
    else:
        logger.info(f"SUCCESS: All {evaluated_expectations} {stage} data quality assertions passed.")
        logger.info("Data Docs updated. Pipeline integrity verified.")

    logger.info(f"TASK COMPLETE: {stage}_data_validation finished.")
    return {
        "stage": stage,
        "success": success,
        "evaluated_expectations": evaluated_expectations,
        "successful_expectations": successful_expectations,
        "unsuccessful_expectations": unsuccessful_expectations,
        "failed_details": failed_details,
    }


def main():
    parser = argparse.ArgumentParser(description="Great Expectations Validation Runner")
    parser.add_argument(
        "--stage",
        required=True,
        choices=["bronze", "silver"],
        help="Storage layer to validate",
    )
    parser.add_argument(
        "--data-dir",
        required=False,
        help="Path to partitioned Parquet directory",
    )
    parser.add_argument(
        "--gx-dir",
        required=False,
        help="Path to great_expectations directory",
    )
    args = parser.parse_args()

    if not args.data_dir:
        default_dir = os.getenv(
            f"{args.stage.upper()}_DATA_PATH",
            os.path.join("data", args.stage),
        )
        args.data_dir = default_dir

    try:
        run_validation_checkpoint(
            stage=args.stage,
            data_dir=args.data_dir,
            context_root_dir=args.gx_dir,
            fail_on_error=True,
        )
        sys.exit(0)
    except Exception as exc:
        logger.error(f"Execution terminated with error: {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()
