"""Data-quality gate. Exits non-zero (failing the Airflow task and blocking
downstream maintenance) if any check fails."""
import sys

from common import CATALOG, base_parser, get_spark, log


def main():
    a = base_parser("data quality").parse_args()
    spark = get_spark(f"dq-{a.ds}")
    b = f"{CATALOG}.bronze.orders"
    s = f"{CATALOG}.silver.orders"
    q = f"{CATALOG}.silver.orders_quarantine"
    g = f"{CATALOG}.gold.daily_product_sales"

    def one(sql):
        return spark.sql(sql).first()[0]

    bronze_n = one(f"SELECT count(*) FROM {b} WHERE ingest_ds = DATE'{a.ds}'")
    quarantine_n = one(f"SELECT count(*) FROM {q} WHERE ingest_ds = DATE'{a.ds}'")
    checks = {
        "bronze has rows for ds": bronze_n > 0,
        "silver order_id unique": one(f"SELECT count(*) - count(DISTINCT order_id) FROM {s}") == 0,
        "silver no null keys": one(f"SELECT count(*) FROM {s} WHERE order_id IS NULL OR customer_id IS NULL") == 0,
        "silver amount >= 0": one(f"SELECT count(*) FROM {s} WHERE amount < 0") == 0,
        "quarantine < 5% of bronze": bronze_n > 0 and quarantine_n / bronze_n < 0.05,
        "gold not empty": one(f"SELECT count(*) FROM {g}") > 0,
        "gold revenue reconciles with silver": one(
            f"SELECT abs((SELECT coalesce(sum(revenue), 0) FROM {g}) - "
            f"(SELECT coalesce(sum(amount), 0) FROM {s} WHERE status <> 'CANCELLED')) < 1.0"),
    }
    for k, ok in checks.items():
        log.warning("DQ %-40s %s", k, "PASS" if ok else "FAIL")
    spark.stop()
    failed = [k for k, ok in checks.items() if not ok]
    if failed:
        sys.exit(f"data quality failed: {failed}")


if __name__ == "__main__":
    main()
