# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # GOLD SETUP
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** create the two Gold schemas (`energy_gold`, `commerce_gold`).
# MAGIC Gold has no per-table registry like Silver's field_class_registry -- one
# MAGIC schema per ecosystem is the whole rule. `shared_conformed` is
# MAGIC created separately by `databricks/setup/03_create_shared_conformed.py`.
# MAGIC Idempotent -- safe to re-run.
# MAGIC
# MAGIC The `quality.pipeline_watermarks` and `quality.quality_audit_log` tables
# MAGIC already exist (`databricks/setup/00_create_schemas.py`); this notebook
# MAGIC only asserts them, same as `databricks/silver/00_silver_setup.py`.

# COMMAND ----------

# DBTITLE 1,Shared Silver library (CATALOG, AUDIT_TABLE, WATERMARK_TABLE, ...)
# MAGIC %run ../silver/_silver_common

# COMMAND ----------

# DBTITLE 1,Gold shared library
# MAGIC %run ./_gold_common

# COMMAND ----------

# DBTITLE 1,Configuration
GOLD_SCHEMAS = (
    "energy_gold",
    "commerce_gold",
)

# COMMAND ----------

# DBTITLE 1,Create the Gold schemas
for schema in GOLD_SCHEMAS:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}")
    print(f"OK  schema ready: {CATALOG}.{schema}")

# COMMAND ----------

# DBTITLE 1,Assert shared_conformed exists
if not spark.catalog.databaseExists(f"{CATALOG}.{SHARED_CONFORMED_SCHEMA}"):
    raise RuntimeError(
        f"{CATALOG}.{SHARED_CONFORMED_SCHEMA} missing -- run "
        "databricks/setup/03_create_shared_conformed.py first"
    )
print(f"OK  present: {CATALOG}.{SHARED_CONFORMED_SCHEMA}")

# COMMAND ----------

# DBTITLE 1,Assert the shared quality tables exist
for t in (AUDIT_TABLE, WATERMARK_TABLE):
    if not spark.catalog.tableExists(t):
        raise RuntimeError(
            f"{t} missing -- run databricks/setup/00_create_schemas.py first"
        )
    print(f"OK  present: {t}")

# COMMAND ----------

# DBTITLE 1,Summary
print("=" * 70)
print("GOLD SETUP SUMMARY")
print("=" * 70)
for schema in GOLD_SCHEMAS:
    print(f"Schema           : {CATALOG}.{schema}")
print(f"Shared conformed : {CATALOG}.{SHARED_CONFORMED_SCHEMA}")
print("RESULT: PASS")
print("=" * 70)
