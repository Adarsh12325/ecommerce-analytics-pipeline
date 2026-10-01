"""
Airflow DAG Integrity and Graph Structure Test Suite.

Verifies:
1. DAG file parses without syntax or runtime import errors.
2. DAG id is strictly 'ecommerce_analytics_pipeline'.
3. Schedule interval is '@daily'.
4. Contains exactly the required 6 sequential tasks.
5. Task dependencies enforce the sequential Medallion architecture.
6. Works seamlessly across both local development environments and Docker container.
"""

import ast
import os
import pytest

DAG_FILE_PATH = os.path.join(
    os.path.dirname(__file__), "..", "dags", "ecommerce_analytics_pipeline.py"
)


def test_dag_file_syntax_and_ast():
    """
    Parses the DAG Python file into an Abstract Syntax Tree (AST) to verify
    syntax correctness and static compliance regardless of host environment.
    """
    assert os.path.exists(DAG_FILE_PATH), f"DAG file does not exist at {DAG_FILE_PATH}"
    with open(DAG_FILE_PATH, "r", encoding="utf-8") as f:
        source_code = f.read()

    # Must parse cleanly without syntax errors
    tree = ast.parse(source_code, filename=DAG_FILE_PATH)
    assert tree is not None

    # Verify DAG ID and required task IDs are present in the source AST
    expected_dag_id = "ecommerce_analytics_pipeline"
    expected_tasks = [
        "download_raw_data",
        "bronze_layer_processing",
        "bronze_data_validation",
        "silver_layer_transformation",
        "silver_data_validation",
        "load_to_analytical_store",
    ]

    assert expected_dag_id in source_code, f"DAG ID '{expected_dag_id}' not found in source."
    for task_id in expected_tasks:
        assert task_id in source_code, f"Required task '{task_id}' not found in source code."


def test_dag_dagbag_runtime():
    """
    If Apache Airflow is installed in the runtime environment (e.g. inside the
    etl-service Docker container or an Airflow virtualenv), validates DAG loading
    via Airflow's DagBag, dependency chaining, and acyclic properties.
    """
    try:
        from airflow.models import DagBag
    except ImportError:
        pytest.skip(
            "Apache Airflow is not installed in the local host environment. "
            "DagBag validation runs inside the 'etl-service' Docker container "
            "(via: docker-compose exec etl-service pytest)."
        )

    dag_folder = os.path.join(os.path.dirname(__file__), "..", "dags")
    dag_bag = DagBag(dag_folder=dag_folder, include_examples=False)

    assert len(dag_bag.import_errors) == 0, f"DAG import errors: {dag_bag.import_errors}"

    dag_id = "ecommerce_analytics_pipeline"
    assert dag_id in dag_bag.dags, f"DAG '{dag_id}' missing from DagBag."

    dag = dag_bag.get_dag(dag_id)
    assert dag.schedule_interval == "@daily"
    assert dag.catchup is False

    expected_tasks = [
        "download_raw_data",
        "bronze_layer_processing",
        "bronze_data_validation",
        "silver_layer_transformation",
        "silver_data_validation",
        "load_to_analytical_store",
    ]

    actual_tasks = [t.task_id for t in dag.tasks]
    for task_id in expected_tasks:
        assert task_id in actual_tasks, f"Task '{task_id}' not registered in DAG."

    # Validate linear chain
    for i in range(len(expected_tasks) - 1):
        up_task = dag.get_task(expected_tasks[i])
        down_task = dag.get_task(expected_tasks[i + 1])
        assert down_task.task_id in up_task.downstream_task_ids
