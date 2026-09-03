# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # CREATE CDC UNITY CATALOG OBJECTS
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC **Author:** Sharique Mohammad
# MAGIC **Date:** August 2026
# MAGIC
# MAGIC **Purpose:** Create the landing Volume the local CDC consumer uploads to,
# MAGIC and the two Bronze representations per operational table -- an append-only
# MAGIC event history and a current-state table maintained by merge. Structure
# MAGIC only; the load notebook fills them. Safe to run repeatedly.

# COMMAND ----------

# DBTITLE 1,Configuration
CATALOG = "energy_commerce_retail_media"
BRONZE_SCHEMA = "bronze"
CDC_VOLUME = "cdc_operational_landing"

# Operational table -> ordered column list (mirrors postgres/ddl/). Every value
# column is stored as STRING at Bronze, matching the batch Bronze convention.
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

# CDC metadata columns present on both representations.
CDC_META_DDL = [
    "_op STRING",
    "_op_name STRING",
    "_row_key STRING",
    "_lsn BIGINT",
    "_event_ts TIMESTAMP",
    "_source_ts_ms BIGINT",
    "_deleted BOOLEAN",
    "_ingested_ts TIMESTAMP",
    "_synced_ts TIMESTAMP",
]

# COMMAND ----------

# DBTITLE 1,Create the landing Volume
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}.{CDC_VOLUME}")
volume_root = f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/{CDC_VOLUME}"
print(f"OK  volume ready: {volume_root}")
for table in TABLE_COLUMNS:
    dbutils.fs.mkdirs(f"{volume_root}/{table}")
print(f"OK  {len(TABLE_COLUMNS)} per-table landing folders present")

# COMMAND ----------

# DBTITLE 1,Create per-table history and current-state tables
created = []
for table, columns in TABLE_COLUMNS.items():
    value_cols = [f"{c} STRING" for c in columns]
    meta_cols = list(CDC_META_DDL)

    history = f"{CATALOG}.{BRONZE_SCHEMA}.cdc_{table}_history"
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {history} ("
        + ", ".join(["_change_id STRING", *meta_cols, *value_cols])
        + ") USING DELTA"
    )

    current = f"{CATALOG}.{BRONZE_SCHEMA}.cdc_{table}_current"
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {current} ("
        + ", ".join([*value_cols, *meta_cols])
        + ") USING DELTA"
    )
    created.extend([history, current])
    print(f"OK  {table}: history + current")

# COMMAND ----------

# DBTITLE 1,Verify
found = {
    row.tableName
    for row in spark.sql(f"SHOW TABLES IN {CATALOG}.{BRONZE_SCHEMA}").collect()
}
missing = [t.split(".")[-1] for t in created if t.split(".")[-1] not in found]
if missing:
    raise RuntimeError(f"FAIL  missing CDC tables after creation: {missing}")

print("=" * 70)
print("CDC OBJECT SETUP SUMMARY")
print("=" * 70)
print(f"Catalog        : {CATALOG}")
print(f"Landing volume : {volume_root}")
print(
    f"Tables created : {len(created)}  (history + current for {len(TABLE_COLUMNS)} tables)"
)
print("RESULT: PASS")
print("=" * 70)
