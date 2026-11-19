"""The daily batch of the Contact Center Analytics Platform, as an Airflow DAG (step 8).

    generate_day -> ingest_bronze -> silver -> gold -> dbt_build -> eod_report

One run = one simulated day (the run's logical date). To replay a given day:
    Trigger DAG w/ config  {"date": "2026-11-04"}

Every step is idempotent (same day twice = same result), so a failed run is simply re-run:
  - the generator rewrites the same files (same seed),
  - bronze skips files already uploaded (same MD5),
  - silver and gold overwrite only the partitions they write,
  - dbt rebuilds the tables, and its 41 data tests fail the run if the data is wrong (nothing downstream runs).
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT = "/opt/airflow/project"
DAY = "{{ dag_run.conf.get('date', ds) if dag_run and dag_run.conf else ds }}"
SPARK = ("spark-submit --properties-file spark/conf/spark-defaults.conf "
         "--conf spark.jars.ivy=/home/airflow/.ivy2 --conf spark.driver.memory=1g")
DBT = "/home/airflow/dbt-venv/bin/dbt"

default_args = {
    "owner": "ayman",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "cwd": PROJECT,                 # every command runs from the repository root
}

with DAG(
    dag_id="contact_center_daily",
    description="Simulated contact center day: landing -> bronze -> silver -> gold -> dbt star schema -> EOD report",
    start_date=datetime(2026, 11, 2),
    schedule="0 23 * * *",          # every night at 23:00 UTC, after the last market closes
    catchup=False,
    max_active_runs=1,              # Spark jobs rewrite shared tables: one day at a time
    default_args=default_args,
    tags=["contact-center", "batch", "spark", "dbt"],
) as dag:

    generate_day = BashOperator(
        task_id="generate_day",
        bash_command=f"python -m generator.generate --start {DAY} --days 1",
    )
    ingest_bronze = BashOperator(
        task_id="ingest_bronze",
        bash_command="python scripts/ingest_bronze.py",
    )
    silver = BashOperator(
        task_id="silver",
        bash_command=f"{SPARK} spark/silver.py",
        execution_timeout=timedelta(minutes=30),
    )
    gold = BashOperator(
        task_id="gold",
        bash_command=f"{SPARK} spark/gold.py",
        execution_timeout=timedelta(minutes=30),
    )
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"{DBT} build",           # models + 41 data tests: a failed test stops the run here
    )
    eod_report = BashOperator(
        task_id="eod_report",
        bash_command=f"python -m rta.status_message --date {DAY} --at 21:00 --source files",
    )

    generate_day >> ingest_bronze >> silver >> gold >> dbt_build >> eod_report
