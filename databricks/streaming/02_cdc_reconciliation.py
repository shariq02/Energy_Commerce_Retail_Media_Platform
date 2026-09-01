# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # CDC RECONCILIATION
# MAGIC
# MAGIC **Energy Commerce and Retail Media Analytics Platform**  
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** August 2026
# MAGIC
# MAGIC **Purpose:** Compare the CDC current-state Bronze tables against the row
# MAGIC counts exported from PostgreSQL after the change run
# MAGIC (`_manifest`/`post_change_row_counts.json` uploaded to the landing Volume).
# MAGIC A non-deleted current row is expected to exist for every live operational
# MAGIC row. Results are written to `quality.quality_audit_log`; a mismatch fails
# MAGIC the notebook.

# COMMAND ----------

# DBTITLE 1,Configuration
import datetime as _dt

from pyspark.sql import functions as F

CATALOG = "energy_commerce_retail_media"
BRONZE_SCHEMA = "bronze"
QUALITY_TABLE = f"{CATALOG}.quality.quality_audit_log"
CDC_VOLUME = "cdc_operational_landing"
VOLUME_ROOT = f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/{CDC_VOLUME}"

TABLES = [
    "tariffs",
    "products",
    "customers",
    "customer_contracts",
    "meters",
    "advertisers",
    "campaigns",
    "campaign_budgets",
    "orders",
    "order_items",
]

RUN_ID = _dt.datetime.now(_dt.UTC).strftime("cdc-recon-%Y%m%dT%H%M%SZ")

# COMMAND ----------

# DBTITLE 1,Read the PostgreSQL baseline snapshot
snapshot_path = f"{VOLUME_ROOT}/snapshots/post_change_row_counts.json"
try:
    snap = spark.read.option("multiLine", "true").json(snapshot_path).collect()[0]
    pg_counts = snap["row_counts"].asDict()
    print(f"OK  baseline captured_at={snap['captured_at']}")
except Exception as exc:
    raise RuntimeError(
        f"FAIL  cannot read {snapshot_path} -- upload the change-driver snapshot first: {exc}"
    ) from exc

# COMMAND ----------

# DBTITLE 1,Compare current-state row counts
rows = []
for table in TABLES:
    current = f"{CATALOG}.{BRONZE_SCHEMA}.cdc_{table}_current"
    live = spark.table(current).filter("_deleted = false").count()
    deleted = spark.table(current).filter("_deleted = true").count()
    expected = int(pg_counts.get(table, -1))
    ok = expected == live
    rows.append(
        (
            RUN_ID,
            _dt.datetime.now(_dt.UTC).date().isoformat(),
            "operational_cdc",
            "bronze",
            f"cdc_{table}_current",
            "row_count_vs_postgres",
            float(live),
            float(expected),
            "PASS" if ok else "FAIL",
            None
            if ok
            else f"expected {expected}, current {live}, tombstones {deleted}",
        )
    )
    print(
        f"  {table:<22} postgres={expected:<7} current_live={live:<7} "
        f"tombstones={deleted:<5} {'OK' if ok else 'MISMATCH'}"
    )

audit = (
    spark.createDataFrame(
        rows,
        "run_id string, run_date string, source string, stage string, component string, "
        "metric_name string, metric_value double, threshold double, status string, "
        "error_detail string",
    )
    .withColumn("run_date", F.to_date("run_date"))
    .withColumn("recorded_at", F.current_timestamp())
)
audit.write.mode("append").saveAsTable(QUALITY_TABLE)
print(f"OK  {audit.count()} row(s) written to {QUALITY_TABLE}")

# COMMAND ----------

# DBTITLE 1,Result
failed = [r for r in rows if r[8] == "FAIL"]
print("=" * 70)
print("CDC RECONCILIATION SUMMARY")
print("=" * 70)
print(f"Run id       : {RUN_ID}")
print(f"Tables checked: {len(TABLES)}   Mismatches: {len(failed)}")
print(f"Overall      : {'PASS' if not failed else 'FAIL'}")
print("=" * 70)
if failed:
    raise RuntimeError(
        f"FAIL  CDC reconciliation mismatch on: {', '.join(r[4] for r in failed)}"
    )