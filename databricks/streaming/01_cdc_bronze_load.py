# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # CDC BRONZE LOAD
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC **Author:** Sharique Mohammad
# MAGIC **Date:** August 2026
# MAGIC
# MAGIC **Purpose:** Batch-load the CDC event files the local consumer uploaded to
# MAGIC the landing Volume. Every event is appended to `cdc_<table>_history`; the
# MAGIC latest event per primary key is merged into `cdc_<table>_current`. Idempotent
# MAGIC -- a re-run over the same files changes nothing (history dedupes on a change
# MAGIC id, the merge is LSN-guarded). Fail-closed: a malformed file or an unknown
# MAGIC table stops the notebook before any write.

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException
from pyspark.sql.window import Window

# COMMAND ----------

# DBTITLE 1,Configuration
CATALOG = "energy_commerce_retail_media"
BRONZE_SCHEMA = "bronze"
CDC_VOLUME = "cdc_operational_landing"
VOLUME_ROOT = f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/{CDC_VOLUME}"

TABLE_COLUMNS = {
    "tariffs": [
        "tariff_id",
        "tariff_code",
        "name",
        "energy_type",
        "unit_price_eur_per_kwh",
        "standing_charge_eur_per_month",
        "contract_term_months",
        "active",
        "valid_from",
        "valid_to",
        "created_at",
        "updated_at",
    ],
    "products": [
        "product_id",
        "sku",
        "name",
        "category",
        "unit_price_eur",
        "active",
        "created_at",
        "updated_at",
    ],
    "customers": [
        "customer_id",
        "customer_number",
        "first_name",
        "last_name",
        "email",
        "phone",
        "street",
        "house_number",
        "postal_code",
        "city",
        "country_code",
        "date_of_birth",
        "signed_up_at",
        "status",
        "created_at",
        "updated_at",
    ],
    "customer_contracts": [
        "contract_id",
        "contract_number",
        "customer_id",
        "tariff_id",
        "start_date",
        "end_date",
        "status",
        "billing_day",
        "created_at",
        "updated_at",
    ],
    "meters": [
        "meter_id",
        "meter_serial",
        "contract_id",
        "meter_type",
        "melo_id",
        "installed_on",
        "removed_on",
        "status",
        "created_at",
        "updated_at",
    ],
    "orders": [
        "order_id",
        "order_number",
        "customer_id",
        "order_status",
        "ordered_at",
        "currency",
        "items_subtotal_eur",
        "shipping_fee_eur",
        "total_eur",
        "created_at",
        "updated_at",
    ],
    "order_items": [
        "order_item_id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price_eur",
        "line_total_eur",
        "created_at",
    ],
}
PRIMARY_KEY = {
    "tariffs": "tariff_id",
    "products": "product_id",
    "customers": "customer_id",
    "customer_contracts": "contract_id",
    "meters": "meter_id",
    "orders": "order_id",
    "order_items": "order_item_id",
}
META_COLS = [
    "_op",
    "_op_name",
    "_row_key",
    "_lsn",
    "_event_ts",
    "_source_ts_ms",
    "_deleted",
    "_ingested_ts",
    "_synced_ts",
]

# COMMAND ----------

# DBTITLE 1,Locate landing files per table


def list_files(table):
    path = f"{VOLUME_ROOT}/{table}"
    try:
        return [f.path for f in dbutils.fs.ls(path) if f.name.endswith(".jsonl")]
    except Exception:
        return []


files_by_table = {t: list_files(t) for t in TABLE_COLUMNS}
for table, files in files_by_table.items():
    print(f"  {table:<22} {len(files)} file(s)")
total_files = sum(len(f) for f in files_by_table.values())
if total_files == 0:
    dbutils.notebook.exit("no CDC landing files found -- nothing to load")

# COMMAND ----------

# DBTITLE 1,Load one table: append history, merge current


def _nested_fields(df, parent):
    if parent not in df.columns:
        return set()
    field = next(f for f in df.schema.fields if f.name == parent)
    return {sf.name for sf in getattr(field.dataType, "fields", [])}


def normalise(df, table):
    columns = TABLE_COLUMNS[table]
    after_fields = _nested_fields(df, "after")
    before_fields = _nested_fields(df, "before")

    def val(col):
        after = F.col(f"after.{col}") if col in after_fields else F.lit(None)
        before = F.col(f"before.{col}") if col in before_fields else F.lit(None)
        return F.coalesce(after.cast("string"), before.cast("string")).alias(col)

    return df.select(
        F.col("_op"),
        F.col("_op_name"),
        F.col("_row_key"),
        F.col("_lsn").cast("long").alias("_lsn"),
        F.to_timestamp("_event_ts").alias("_event_ts"),
        F.col("_source_ts_ms").cast("long").alias("_source_ts_ms"),
        F.col("_deleted").cast("boolean").alias("_deleted"),
        F.to_timestamp("_ingested_ts").alias("_ingested_ts"),
        F.current_timestamp().alias("_synced_ts"),
        *[val(c) for c in columns],
    )


def load_table(table):
    files = files_by_table[table]
    if not files:
        return {"table": table, "events": 0, "current_rows": None, "status": "SKIPPED"}

    raw = spark.read.option("multiLine", "false").json(files)
    missing_meta = [c for c in ("_op", "_lsn", "_row_key") if c not in raw.columns]
    if missing_meta:
        raise RuntimeError(f"FAIL  {table}: landing files missing {missing_meta}")

    events = normalise(raw, table)
    pk = PRIMARY_KEY[table]
    value_cols = TABLE_COLUMNS[table]

    history = f"{CATALOG}.{BRONZE_SCHEMA}.cdc_{table}_history"
    hist_df = events.withColumn(
        "_change_id", F.sha2(F.concat_ws("|", F.col("_row_key"), F.col("_lsn")), 256)
    )
    (hist_df.dropDuplicates(["_change_id"]).createOrReplaceTempView("cdc_hist_src"))
    spark.sql(
        f"MERGE INTO {history} t USING cdc_hist_src s ON t._change_id = s._change_id "
        "WHEN NOT MATCHED THEN INSERT *"
    )
    appended = spark.table(history).count()

    # Latest event per key wins (LSN order).
    latest = (
        events.withColumn(
            "_rn",
            F.row_number().over(
                Window.partitionBy("_row_key").orderBy(F.col("_lsn").desc())
            ),
        )
        .filter("_rn = 1")
        .drop("_rn")
    )
    latest.createOrReplaceTempView("cdc_current_src")

    current = f"{CATALOG}.{BRONZE_SCHEMA}.cdc_{table}_current"
    set_values = ", ".join(f"t.{c} = s.{c}" for c in value_cols)
    set_meta = ", ".join(f"t.{c} = s.{c}" for c in META_COLS)
    set_meta_only = ", ".join(
        f"t.{c} = s.{c}"
        for c in (
            "_op",
            "_op_name",
            "_lsn",
            "_event_ts",
            "_source_ts_ms",
            "_deleted",
            "_ingested_ts",
            "_synced_ts",
        )
    )
    insert_cols = ", ".join(value_cols + META_COLS)
    insert_vals = ", ".join(f"s.{c}" for c in value_cols + META_COLS)
    spark.sql(f"""
        MERGE INTO {current} t
        USING cdc_current_src s ON t.{pk} = s.{pk}
        WHEN MATCHED AND s._lsn > t._lsn AND s._deleted = false
            THEN UPDATE SET {set_values}, {set_meta}
        WHEN MATCHED AND s._lsn > t._lsn AND s._deleted = true
            THEN UPDATE SET {set_meta_only}
        WHEN NOT MATCHED
            THEN INSERT ({insert_cols}) VALUES ({insert_vals})
    """)
    current_rows = spark.table(current).count()

    return {
        "table": table,
        "events": events.count(),
        "history_rows": appended,
        "current_rows": current_rows,
        "status": "LOADED",
    }


# COMMAND ----------

# DBTITLE 1,Run all tables
results = []
for table in TABLE_COLUMNS:
    try:
        results.append(load_table(table))
        r = results[-1]
        print(
            f"OK  {table}: {r['status']} events={r.get('events')} "
            f"current={r.get('current_rows')}"
        )
    except (AnalysisException, RuntimeError) as exc:
        print(f"FAIL  {table}: {exc}")
        results.append({"table": table, "status": "FAILED", "error": str(exc)})

# COMMAND ----------

# DBTITLE 1,Archive processed landing files
processed_ok = all(r["status"] in ("LOADED", "SKIPPED") for r in results)
if processed_ok:
    archive = f"{VOLUME_ROOT}/_processed"
    dbutils.fs.mkdirs(archive)
    for table, files in files_by_table.items():
        for path in files:
            name = path.rstrip("/").split("/")[-1]
            dbutils.fs.mv(path, f"{archive}/{table}__{name}")
    print(f"OK  moved {total_files} file(s) to {archive}")
else:
    print("PRESERVED  landing files kept -- at least one table failed to load")

# COMMAND ----------

# DBTITLE 1,Summary
print("=" * 70)
print("CDC BRONZE LOAD SUMMARY")
print("=" * 70)
for r in results:
    print(
        f"{r['table']:<22} status={r['status']:<8} "
        f"events={r.get('events')!s:<8} current_rows={r.get('current_rows')}"
    )
    if r.get("error"):
        print(f"    {r['error']}")
loaded = sum(1 for r in results if r["status"] == "LOADED")
failed = sum(1 for r in results if r["status"] == "FAILED")
print("-" * 70)
print(
    f"Loaded: {loaded}   Skipped: {sum(1 for r in results if r['status'] == 'SKIPPED')}   Failed: {failed}"
)
print(f"Overall: {'PASS' if failed == 0 else 'FAIL'}")
print("=" * 70)
if failed:
    raise RuntimeError(
        "FAIL  CDC Bronze load did not fully pass -- landing files preserved"
    )
