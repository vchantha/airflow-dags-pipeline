"""Shared helpers for the lakehouse PySpark jobs.

Catalog wiring (Iceberg REST -> Lakekeeper, OAuth2 client-credentials via
Keycloak) is passed as Spark conf by the SparkApplication (see the Airflow
DAG), not hard-coded here, so the same job runs in any environment.
"""
import argparse
import logging
import os

from pyspark.sql import SparkSession

CATALOG = "lakehouse"
log = logging.getLogger("lakehouse")


def base_parser(desc: str) -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=desc)
    p.add_argument("--ds", required=True, help="Logical date, YYYY-MM-DD (Airflow {{ ds }})")
    return p


def get_spark(app: str) -> SparkSession:
    builder = SparkSession.builder.appName(app)
    # OAuth2 client-credentials for the Lakekeeper REST catalog. Injected as
    # driver env from a k8s Secret, NOT via sparkConf, so the secret never
    # appears in the SparkApplication object. Executors don't need it: the
    # driver plans, and table FileIO properties travel with the tasks.
    cid, secret = os.environ.get("LAKEKEEPER_CLIENT_ID"), os.environ.get("LAKEKEEPER_CLIENT_SECRET")
    if cid and secret:
        builder = builder.config(f"spark.sql.catalog.{CATALOG}.credential", f"{cid}:{secret}")
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def ensure_namespaces(spark: SparkSession, *names: str) -> None:
    for n in names:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{n}")
