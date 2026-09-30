# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # ANALYTICS SETUP
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** create the Analytics and Semantic schemas (one pair per
# MAGIC ecosystem). Idempotent -- safe to re-run. Also asserts every Gold table
# MAGIC this build's Analytics notebooks read already exists, before any
# MAGIC Analytics processing starts.

# COMMAND ----------

# DBTITLE 1,Analytics shared library
# MAGIC %run ./_analytics_common

# COMMAND ----------

# DBTITLE 1,Configuration
ANALYTICS_SCHEMAS = (
    "energy_analytics",
    "commerce_analytics",
    "energy_semantic",
    "commerce_semantic",
)

# Every Gold table this build's Analytics notebooks read -- (table, source, schema).
_REQUIRED_GOLD = (
    ("energy_balance_component", "smard", None),
    ("market_price", "smard", None),
    ("channel_reading", "honda_iot", None),
    ("weather_observation", "dwd", None),
    ("ga4_session", "ga4", None),
    ("rees46_event", "rees46", None),
)

# COMMAND ----------

# DBTITLE 1,Create the Analytics and Semantic schemas
for schema in ANALYTICS_SCHEMAS:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}")
    print(f"OK  schema ready: {CATALOG}.{schema}")

# COMMAND ----------

# DBTITLE 1,Preflight -- every required Gold table exists
_missing = []
for table, source, schema in _REQUIRED_GOLD:
    full = f"{CATALOG}.{schema or gold_schema_for(source)}.{table}"
    if not spark.catalog.tableExists(full):
        _missing.append(full)
if _missing:
    raise RuntimeError(
        f"Analytics reads Gold table(s) that don't exist yet: {_missing} -- "
        "run the corresponding Gold notebook(s) first"
    )
print(f"OK  {len(_REQUIRED_GOLD)} required Gold table(s) all present")

# COMMAND ----------

# DBTITLE 1,Summary
print("=" * 70)
print("ANALYTICS SETUP SUMMARY")
print("=" * 70)
for schema in ANALYTICS_SCHEMAS:
    print(f"Schema : {CATALOG}.{schema}")
print("RESULT: PASS")
print("=" * 70)
