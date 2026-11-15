# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # GOLD SETUP
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** November 2026
# MAGIC
# MAGIC **Purpose:** create the Gold schemas (`energy_gold`, `commerce_gold`,
# MAGIC `genomics_gold`, `shared_conformed`) and the analytics schemas
# MAGIC (`energy_analytics`, `commerce_analytics`, `genomics_analytics`), and
# MAGIC assert that the `quality` tables exist. Preflight: every Silver table named in a
# MAGIC Gold `read_silver()` call must be registered in `quality.field_class_registry`;
# MAGIC a missing one fails here, before any Gold run. `dim_date` comes from
# MAGIC `setup/02_create_shared_conformed.py`. Safe to re-run.

# COMMAND ----------

# DBTITLE 1,Gold shared library
# MAGIC %run ./_gold_common

# COMMAND ----------

# DBTITLE 1,Configuration
GOLD_SCHEMAS = (
    "energy_gold",
    "commerce_gold",
    GENOMICS_GOLD_SCHEMA,
    SHARED_CONFORMED_SCHEMA,
    *ANALYTICS_SCHEMAS.values(),
)

# COMMAND ----------

# DBTITLE 1,Create the Gold and analytics schemas
for schema in GOLD_SCHEMAS:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}")
    print(f"OK  schema ready: {CATALOG}.{schema}")

# COMMAND ----------

# DBTITLE 1,Assert dim_date exists
if not spark.catalog.tableExists(f"{CATALOG}.{SHARED_CONFORMED_SCHEMA}.dim_date"):
    raise RuntimeError(
        f"{CATALOG}.{SHARED_CONFORMED_SCHEMA}.dim_date missing -- run "
        "databricks/setup/02_create_shared_conformed.py first"
    )
print(f"OK  present: {CATALOG}.{SHARED_CONFORMED_SCHEMA}.dim_date")

# COMMAND ----------

# DBTITLE 1,Assert the shared quality tables exist
for t in (AUDIT_TABLE, WATERMARK_TABLE):
    if not spark.catalog.tableExists(t):
        raise RuntimeError(
            f"{t} missing -- run databricks/setup/00_create_schemas.py first"
        )
    print(f"OK  present: {t}")

# COMMAND ----------

# DBTITLE 1,Imports
import sys as _sys
from pathlib import Path as _Path

# COMMAND ----------

# DBTITLE 1,Fast preflight -- every Gold-required Silver table is registered
if repo_root() not in _sys.path:
    _sys.path.insert(0, repo_root())
from src.schemas._silver_notebook_scan import all_read_silver_tables

_gold_root = _Path(repo_root()) / "databricks" / "gold"
_required = all_read_silver_tables(_gold_root)
if not spark.catalog.tableExists(FIELD_CLASS_TABLE):
    raise RuntimeError(
        f"{FIELD_CLASS_TABLE} missing -- run databricks/silver/00_silver_setup.py first"
    )
_registered_rows = (
    spark.table(FIELD_CLASS_TABLE).select("table_name").distinct().collect()
)
_registered = {r["table_name"] for r in _registered_rows}
_unregistered = sorted(_required - _registered)
if _unregistered:
    raise RuntimeError(
        f"Gold reads Silver table(s) not registered in {FIELD_CLASS_TABLE}: "
        f"{_unregistered} -- run databricks/silver/00_silver_setup.py (and the "
        "Silver notebook that writes each one) before any Gold notebook"
    )
print(f"OK  {len(_required)} Gold-required Silver table(s) all registered")

# COMMAND ----------

# DBTITLE 1,Summary
print("=" * 70)
print("GOLD SETUP SUMMARY")
print("=" * 70)
for schema in GOLD_SCHEMAS:
    print(f"Schema           : {CATALOG}.{schema}")
print("RESULT: PASS")
print("=" * 70)
