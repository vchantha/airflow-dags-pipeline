"""Demo DAG for the Airflow 3 POC.

Shows: TaskFlow API, dynamic task mapping, branching, a bash task,
task dependencies, retries, and a schedule. No external connections needed.

Copy this file into the `dags/` folder of the git-sync repo
(airflow-dags-pipeline) and push -- Airflow picks it up within `period`.
"""
from __future__ import annotations

import random
from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import dag, task

default_args = {
    "owner": "devsecops",
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
}


@dag(
    dag_id="demo_etl",
    description="Demo: extract -> transform (mapped) -> branch -> load",
    schedule="@daily",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    default_args=default_args,
    tags=["demo", "poc"],
)
def demo_etl():
    start = BashOperator(
        task_id="start",
        bash_command='echo "Run {{ run_id }} for {{ ds }} on $(hostname)"',
    )

    @task
    def extract() -> list[dict]:
        """Pretend to pull records from a source system."""
        records = [{"id": i, "amount": random.randint(10, 500)} for i in range(1, 6)]
        print(f"Extracted {len(records)} records")
        return records

    @task
    def transform(record: dict) -> dict:
        """Runs once per record (dynamic task mapping)."""
        record["amount_with_tax"] = round(record["amount"] * 1.1, 2)
        return record

    @task
    def summarize(records: list[dict]) -> float:
        total = sum(r["amount_with_tax"] for r in records)
        print(f"Total (with tax) = {total}")
        return total

    @task.branch
    def check_threshold(total: float) -> str:
        return "load_high_value" if total > 1000 else "load_normal"

    @task
    def load_high_value(total: float):
        print(f"High-value batch: {total} -> flagging for review")

    @task
    def load_normal(total: float):
        print(f"Normal batch: {total} -> loading")

    done = EmptyOperator(task_id="done", trigger_rule="none_failed_min_one_success")

    extracted = extract()
    transformed = transform.expand(record=extracted)
    total = summarize(transformed)
    branch = check_threshold(total)

    start >> extracted
    branch >> [load_high_value(total), load_normal(total)] >> done


demo_etl()
