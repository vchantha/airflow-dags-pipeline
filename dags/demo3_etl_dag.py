"""
Demo 2 DAG for Airflow 3 POC.

Demonstrates:
- TaskFlow API
- BashOperator
- Dynamic task mapping
- XCom
- Branching
- Retries
- Task dependencies
- Daily scheduling
- No external connections required

Copy this file into the `dags/` folder of the git-sync repository.
Airflow will automatically discover it.
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
    dag_id="demo3_etl",
    description="Demo 3: extract -> validate -> transform -> aggregate -> branch -> load",
    schedule="@daily",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    default_args=default_args,
    tags=["demo", "demo2", "poc", "etl"],
)
def demo2_etl():

    # ---------------------------------------------------------
    # START
    # ---------------------------------------------------------

    start = BashOperator(
        task_id="start",
        bash_command=(
            'echo "========================================"; '
            'echo "Starting Demo2 ETL"; '
            'echo "Run ID: {{ run_id }}"; '
            'echo "Execution date: {{ ds }}"; '
            'echo "Hostname: $(hostname)"; '
            'echo "========================================"'
        ),
    )

    # ---------------------------------------------------------
    # EXTRACT
    # ---------------------------------------------------------

    @task
    def extract() -> list[dict]:
        """
        Simulate extracting records from a source system.
        """

        records = [
            {
                "id": i,
                "customer": f"CUST-{1000 + i}",
                "amount": random.randint(50, 1000),
            }
            for i in range(1, 11)
        ]

        print(f"Extracted {len(records)} records")

        for record in records:
            print(record)

        return records

    # ---------------------------------------------------------
    # VALIDATE
    # ---------------------------------------------------------

    @task
    def validate(record: dict) -> dict:
        """
        Validate individual records.

        Runs once for each record using dynamic task mapping.
        """

        if record["amount"] <= 0:
            raise ValueError(
                f"Invalid amount for record {record['id']}"
            )

        record["valid"] = True

        print(
            f"Validated record={record['id']} "
            f"customer={record['customer']} "
            f"amount={record['amount']}"
        )

        return record

    # ---------------------------------------------------------
    # TRANSFORM
    # ---------------------------------------------------------

    @task
    def transform(record: dict) -> dict:
        """
        Transform each validated record.
        """

        tax_rate = 0.10

        record["tax"] = round(
            record["amount"] * tax_rate,
            2,
        )

        record["amount_with_tax"] = round(
            record["amount"] + record["tax"],
            2,
        )

        record["status"] = (
            "HIGH_VALUE"
            if record["amount_with_tax"] >= 500
            else "NORMAL"
        )

        print(
            f"Transformed record={record['id']} "
            f"total={record['amount_with_tax']} "
            f"status={record['status']}"
        )

        return record

    # ---------------------------------------------------------
    # AGGREGATE
    # ---------------------------------------------------------

    @task
    def aggregate(records: list[dict]) -> dict:
        """
        Aggregate all transformed records.
        """

        total_amount = sum(
            record["amount"]
            for record in records
        )

        total_tax = sum(
            record["tax"]
            for record in records
        )

        total_with_tax = sum(
            record["amount_with_tax"]
            for record in records
        )

        high_value_count = sum(
            1
            for record in records
            if record["status"] == "HIGH_VALUE"
        )

        summary = {
            "record_count": len(records),
            "total_amount": round(total_amount, 2),
            "total_tax": round(total_tax, 2),
            "total_with_tax": round(total_with_tax, 2),
            "high_value_count": high_value_count,
        }

        print("========================================")
        print("ETL SUMMARY")
        print("========================================")

        for key, value in summary.items():
            print(f"{key}: {value}")

        return summary

    # ---------------------------------------------------------
    # BRANCH
    # ---------------------------------------------------------

    @task.branch
    def check_batch(summary: dict) -> str:
        """
        Branch based on total transaction value.
        """

        total = summary["total_with_tax"]

        print(f"Total batch value: {total}")

        if total >= 5000:
            return "load_high_value"

        return "load_normal"

    # ---------------------------------------------------------
    # LOAD - HIGH VALUE
    # ---------------------------------------------------------

    @task
    def load_high_value(summary: dict):
        print("========================================")
        print("HIGH VALUE LOAD")
        print("========================================")

        print(
            f"Loading high-value batch: "
            f"{summary['total_with_tax']}"
        )

        print(
            f"Records: {summary['record_count']}"
        )

        print("Batch flagged for additional review.")

    # ---------------------------------------------------------
    # LOAD - NORMAL
    # ---------------------------------------------------------

    @task
    def load_normal(summary: dict):
        print("========================================")
        print("NORMAL LOAD")
        print("========================================")

        print(
            f"Loading normal batch: "
            f"{summary['total_with_tax']}"
        )

        print(
            f"Records: {summary['record_count']}"
        )

        print("Batch loaded successfully.")

    # ---------------------------------------------------------
    # END
    # ---------------------------------------------------------

    done = EmptyOperator(
        task_id="done",
        trigger_rule="none_failed_min_one_success",
    )

    # ---------------------------------------------------------
    # TASK GRAPH
    # ---------------------------------------------------------

    extracted = extract()

    validated = validate.expand(
        record=extracted
    )

    transformed = transform.expand(
        record=validated
    )

    summary = aggregate(transformed)

    branch = check_batch(summary)

    high_value = load_high_value(summary)

    normal = load_normal(summary)

    # ---------------------------------------------------------
    # DEPENDENCIES
    # ---------------------------------------------------------

    start >> extracted

    branch >> [high_value, normal]

    [high_value, normal] >> done


demo2_etl()
