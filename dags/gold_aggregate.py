"""silver -> gold: daily sales KPIs per product (business-facing mart).

Recomputes only the order_date partitions touched by this run's silver
updates, so late-arriving data corrects history without a full rebuild.
"""
from pyspark.sql import functions as F

from common import CATALOG, base_parser, get_spark, log

SRC = f"{CATALOG}.silver.orders"
TGT = f"{CATALOG}.gold.daily_product_sales"


def main():
    a = base_parser("gold aggregate").parse_args()
    spark = get_spark(f"gold-aggregate-{a.ds}")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {TGT} (
            order_date date, product_id int, orders bigint, units bigint,
            revenue double, avg_order_value double, cancelled_orders bigint,
            computed_at timestamp)
        USING iceberg PARTITIONED BY (order_date)
        TBLPROPERTIES ('format-version'='2')""")

    dates = [r[0] for r in spark.table(SRC).where(f"updated_at >= TIMESTAMP'{a.ds} 00:00:00'")
             .select("order_date").distinct().collect()]
    if not dates:
        log.warning("no silver changes for %s; nothing to aggregate", a.ds)
        spark.stop()
        return

    agg = (spark.table(SRC).where(F.col("order_date").isin(dates))
           .groupBy("order_date", "product_id")
           .agg(F.count("*").alias("orders"),
                F.sum("quantity").alias("units"),
                F.round(F.sum(F.when(F.col("status") != "CANCELLED", F.col("amount")).otherwise(0)), 2)
                .alias("revenue"),
                F.round(F.avg("amount"), 2).alias("avg_order_value"),
                F.sum(F.when(F.col("status") == "CANCELLED", 1).otherwise(0)).alias("cancelled_orders"))
           .withColumn("computed_at", F.current_timestamp()))
    agg.writeTo(TGT).overwritePartitions()
    log.warning("gold refreshed for %d date(s)", len(dates))
    spark.stop()


if __name__ == "__main__":
    main()
