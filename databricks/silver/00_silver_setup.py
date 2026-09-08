# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER SETUP
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** create the `energy_silver` schema, the slim `quarantine`
# MAGIC table, and load the central `field_class_registry` from its seed file
# MAGIC (`src/schemas/field_classes/energy_silver_field_classes.csv`, produced by
# MAGIC `src/schemas/_generate_field_classes.py`). Idempotent -- safe to re-run.
# MAGIC
# MAGIC The `quality.pipeline_watermarks` and `quality.quality_audit_log` tables
# MAGIC already exist (`databricks/setup/00_create_schemas.py`); this notebook
# MAGIC only asserts them.

# COMMAND ----------

# DBTITLE 1,Shared library
# MAGIC %run ./_silver_common

# COMMAND ----------

# DBTITLE 1,Imports
import csv as _csv
import os as _os

# COMMAND ----------

# DBTITLE 1,Create the energy_silver schema
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SILVER_SCHEMA}")
print(f"OK  schema ready: {CATALOG}.{SILVER_SCHEMA}")

# COMMAND ----------

# DBTITLE 1,Create the slim quarantine table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {QUARANTINE_TABLE} (
    source_system    STRING,
    bronze_table     STRING,
    source_record_id STRING,
    rule_id          STRING,
    reason           STRING,
    field_name       STRING,
    offending_value  STRING,
    run_id           STRING,
    quarantined_at   TIMESTAMP
)
USING DELTA
COMMENT 'Slim Silver quarantine -- diagnostic fields only. Bronze is the authoritative raw record; complete rows are never duplicated here.'
""")
print(f"OK  table ready: {QUARANTINE_TABLE}")

# COMMAND ----------

# DBTITLE 1,Create the central field-class registry
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FIELD_CLASS_TABLE} (
    table_name      STRING NOT NULL,
    column_name     STRING NOT NULL,
    field_class     STRING NOT NULL,
    derivation_rule STRING,
    source_reference STRING
)
USING DELTA
COMMENT 'One central registry for energy_silver. Seeded from the source contracts + mappings + the fixed Silver governance column set. Silver notebooks validate against it; they never author it.'
""")
print(f"OK  table ready: {FIELD_CLASS_TABLE}")

# COMMAND ----------

# DBTITLE 1,Load the field-class registry from its seed
SEED = _os.path.join(
    repo_root(), "src", "schemas", "field_classes", "energy_silver_field_classes.csv"
)
if not _os.path.exists(SEED):
    raise RuntimeError(
        f"seed not found: {SEED} -- run `python src/schemas/_generate_field_classes.py`"
    )

with open(SEED, encoding="utf-8", newline="") as fh:
    rows = [
        (
            r["table_name"],
            r["column_name"],
            r["field_class"],
            r.get("derivation_rule") or None,
            r.get("source_reference") or None,
        )
        for r in _csv.DictReader(fh)
    ]

bad_class = sorted(
    {c for _, _, c, _, _ in rows} - {"source_provided", "derived", "synthetic"}
)
if bad_class:
    raise RuntimeError(f"seed carries unknown field_class value(s): {bad_class}")

seed_df = spark.createDataFrame(
    rows,
    "table_name string, column_name string, field_class string, "
    "derivation_rule string, source_reference string",
)
seed_df.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(FIELD_CLASS_TABLE)
print(f"OK  {FIELD_CLASS_TABLE}: loaded {len(rows)} rows from the seed")

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
print("SILVER SETUP SUMMARY")
print("=" * 70)
print(f"Schema           : {CATALOG}.{SILVER_SCHEMA}")
print(f"Quarantine table : {QUARANTINE_TABLE}")
print(f"Field-class rows : {spark.table(FIELD_CLASS_TABLE).count()}")
distinct_tables = spark.table(FIELD_CLASS_TABLE).select("table_name").distinct().count()
print(f"Tables covered   : {distinct_tables}")
print("RESULT: PASS")
print("=" * 70)
