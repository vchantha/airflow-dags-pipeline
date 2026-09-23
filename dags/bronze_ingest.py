"""raw -> bronze: append-only, schema-on-read landing of source orders.

Idempotent per logical date: re-running a date atomically replaces that
date's partition (dynamic partition overwrite), never duplicates rows.

--source-path  JSON files in S3 (s3a://landing/orders/dt=<ds>/). If omitted,
               synthetic sample orders are generated (demo / smoke test).
"""
from pyspark.sql import functions as F

from common import CATALOG, base_parser, ensure_namespaces, get_spark, log

TABLE = f"{CATALOG}.bronze.orders"
STATUSES = ["NEW", "PAID", "SHIPPED", "CANCELLED"]


def sample_orders(spark, ds: str, n: int = 10_000):
    return (
        spark.range(n)
        .withColumn("order_id", F.concat(F.lit(ds.replace("-", "")), F.lit("-"), F.col("id").cast("string")))
        .withColumn("customer_id", (F.rand(seed=1) * 500).cast("int"))
        .withColumn("product_id", (F.rand(seed=2) * 50).cast("int"))
        .withColumn("quantity", (F.rand(seed=3) * 5 + 1).cast("int"))
        .withColumn("unit_price", F.round(F.rand(seed=4) * 100 + 1, 2))
        .withColumn("status", F.element_at(F.array(*[F.lit(s) for s in STATUSES]),
                                           (F.rand(seed=5) * len(STATUSES)).cast("int") + 1))
        .withColumn("order_ts", (F.unix_timestamp(F.lit(f"{ds} 00:00:00")) + F.rand(seed=6) * 86399)
                    .cast("timestamp"))
        .drop("id")
    )


def main():
    p = base_parser("bronze ingest")
    p.add_argument("--source-path", default=None)
    a = p.parse_args()
    a.source_path = a.source_path or None  # Airflow passes "" when the param is unset
    spark = get_spark(f"bronze-ingest-{a.ds}")
    ensure_namespaces(spark, "bronze", "silver", "gold")

    df = spark.read.json(a.source_path) if a.source_path else sample_orders(spark, a.ds)
    df = (df.withColumn("ingest_ds", F.lit(a.ds).cast("date"))
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_source", F.lit(a.source_path or "synthetic")))

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {TABLE} (
            order_id string, customer_id int, product_id int, quantity int,
            unit_price double, status string, order_ts timestamp,
            ingest_ds date, _ingested_at timestamp, _source string)
        USING iceberg
        PARTITIONED BY (ingest_ds)
        TBLPROPERTIES ('format-version'='2', 'write.target-file-size-bytes'='134217728')""")

    df.select(*spark.table(TABLE).columns).writeTo(TABLE).overwritePartitions()
    log.warning("bronze rows for %s: %d", a.ds, spark.table(TABLE).where(f"ingest_ds = DATE'{a.ds}'").count())
    spark.stop()


if __name__ == "__main__":
    main()
