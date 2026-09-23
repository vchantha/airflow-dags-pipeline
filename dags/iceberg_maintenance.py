"""Iceberg table maintenance through the Lakekeeper-backed catalog: compact
small files, rewrite manifests, expire old snapshots, remove orphan files."""
from common import CATALOG, base_parser, get_spark, log

TABLES = ["bronze.orders", "silver.orders", "silver.orders_quarantine", "gold.daily_product_sales"]


def main():
    p = base_parser("iceberg maintenance")
    p.add_argument("--retain-days", type=int, default=7)
    p.add_argument("--orphan-older-than-days", type=int, default=3)
    a = p.parse_args()
    spark = get_spark(f"maintenance-{a.ds}")
    ts = f"TIMESTAMP '{a.ds} 00:00:00'"

    for t in TABLES:
        # Order matters: compact -> manifests -> expire -> orphans.
        spark.sql(f"CALL {CATALOG}.system.rewrite_data_files(table => '{t}', "
                  f"options => map('min-input-files', '5'))").show()
        spark.sql(f"CALL {CATALOG}.system.rewrite_manifests('{t}')").show()
        spark.sql(f"CALL {CATALOG}.system.expire_snapshots(table => '{t}', "
                  f"older_than => {ts} - INTERVAL {a.retain_days} DAYS, retain_last => 5)").show()
        # Orphan age must exceed the longest-running writer, or live files get deleted.
        spark.sql(f"CALL {CATALOG}.system.remove_orphan_files(table => '{t}', "
                  f"older_than => {ts} - INTERVAL {a.orphan_older_than_days} DAYS)").show()
        log.warning("maintained %s.%s", CATALOG, t)
    spark.stop()


if __name__ == "__main__":
    main()
