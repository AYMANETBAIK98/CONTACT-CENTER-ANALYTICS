"""The Airflow DAG loads and runs its steps in the right order (step 8).

    pytest tests/test_dag.py        (skipped where Airflow is not installed, e.g. the tools container)
"""
import pytest

pytest.importorskip("airflow.models")   # the real Airflow package

from airflow.models import DagBag  # noqa: E402


@pytest.fixture(scope="module")
def dag():
    bag = DagBag(dag_folder="orchestration/dags", include_examples=False)
    assert bag.import_errors == {}
    return bag.get_dag("contact_center_daily")


def test_dag_loads(dag):
    assert dag is not None
    assert dag.max_active_runs == 1 and not dag.catchup


def test_steps_run_in_order(dag):
    order = ["generate_day", "ingest_bronze", "silver", "gold", "dbt_build", "eod_report"]
    for up, down in zip(order, order[1:]):
        assert dag.get_task(down).upstream_task_ids == {up}


def test_dbt_failure_stops_the_report(dag):
    """The EOD report only runs if dbt (and its data tests) succeeded."""
    assert dag.get_task("eod_report").trigger_rule == "all_success"
