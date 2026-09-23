"""bronze -> silver: typed, de-duplicated, validated orders (MERGE upsert).

Idempotent: MERGE on order_id, so replays and late corrections update rows
instead of duplicating them. Rows failing basic validity go to a quarantine
table rather than being silently dropped.
"""
from pyspark.sql import Window, functions as F

from common import CATALOG, base_parser, get_spark, log

SRC = f"{CATALOG}.bronze.orders"
TGT = f"{CATALOG}.silver.orders"
BAD = f"{CATALOG}.silver.orders_quarantine"


def main():
    a = base_parser("silver transform").parse_args()
    spark = get_spark(f"silver-transform-{a.ds}")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {TGT} (
            order_id string, customer_id int, product_id int, quantity int,
            unit_price double, amount double, status string, order_ts timestamp,
            order_date date, updated_at timestamp)
        USING iceberg PARTITIONED BY (order_date)
        TBLPROPERTIES ('format-version'='2')""")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {BAD} (
            order_id string, customer_id int, product_id int, quantity int,
            unit_price double, status string, order_ts timestamp,
            ingest_ds date, reason string, _quarantined_at timestamp)
        USING iceberg PARTITIONED BY (ingest_ds)""")

    src = spark.table(SRC).where(f"ingest_ds = DATE'{a.ds}'").cache()
    valid = (F.col("order_id").isNotNull() & F.col("customer_id").isNotNull()
             & (F.col("quantity") > 0) & (F.col("unit_price") >= 0) & F.col("order_ts").isNotNull())

    bad = (src.where(~valid).withColumn("reason", F.lit("failed validation"))
              .withColumn("_quarantined_at", F.current_timestamp())
              .select("order_id", "customer_id", "product_id", "quantity", "unit_price", "status",
                      "order_ts", "ingest_ds", "reason", "_quarantined_at"))
    bad.writeTo(BAD).overwritePartitions()

    latest = Window.partitionBy("order_id").orderBy(F.col("_ingested_at").desc())
    good = (src.where(valid)
               .withColumn("_rn", F.row_number().over(latest)).where("_rn = 1").drop("_rn")
               .withColumn("amount", F.round(F.col("quantity") * F.col("unit_price"), 2))
               .withColumn("order_date", F.to_date("order_ts"))
               .withColumn("updated_at", F.current_timestamp())
               .select("order_id", "customer_id", "product_id", "quantity", "unit_price", "amount",
                       "status", "order_ts", "order_date", "updated_at"))
    good.createOrReplaceTempView("_silver_updates")

    spark.sql(f"""
        MERGE INTO {TGT} t USING _silver_updates s ON t.order_id = s.order_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *""")
    log.warning("silver merged for %s; quarantined=%d", a.ds, bad.count())
    spark.stop()


if __name__ == "__main__":
    main()
