# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # CREATE UNITY CATALOG SCHEMAS
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** August 2026
# MAGIC
# MAGIC **Purpose:** Create the project schemas and the two quality tables. It creates
# MAGIC no Volume and no data table, and it drops nothing. Safe to run again.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Imports

# COMMAND ----------

from pyspark.sql.utils import AnalysisException

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Configuration
# MAGIC
# MAGIC `CATALOG` is the single Unity Catalog catalog for this project.
# MAGIC `SCHEMAS` are the schemas created below.

# COMMAND ----------

CATALOG = "energy_commerce_retail_media"

SCHEMAS = [
    "bronze",
    "energy_silver",
    "energy_silver_reference",
    "commerce_silver",
    "commerce_silver_reference",
    "genomics_silver",
    "genomics_silver_reference",
    "energy_gold",
    "shared_conformed",
    "quality",
    "genomics_bronze",
]

QUALITY_SCHEMA = "quality"
QUALITY_TABLES = ["pipeline_watermarks", "quality_audit_log"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Create Schemas

# COMMAND ----------

# DBTITLE 1,Create schemas
for schema in SCHEMAS:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}")
    print(f"OK  schema ready: {CATALOG}.{schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Create Quality / Control Tables
# MAGIC
# MAGIC Both tables live in `energy_commerce_retail_media.quality` and are
# MAGIC generic control tables, not source-specific -- every pipeline run
# MAGIC across every source/stage/component writes into the same two tables.

# COMMAND ----------

# DBTITLE 1,Create pipeline_watermarks
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{QUALITY_SCHEMA}.pipeline_watermarks (
    run_id              STRING      NOT NULL,
    source              STRING      NOT NULL,
    stage               STRING      NOT NULL,
    component           STRING      NOT NULL,
    last_processed_ts   TIMESTAMP,
    last_processed_row  BIGINT,
    rows_written        BIGINT,
    status              STRING      NOT NULL,
    started_at          TIMESTAMP   NOT NULL,
    completed_at        TIMESTAMP
)
USING DELTA
""")
print(f"OK  table ready: {CATALOG}.{QUALITY_SCHEMA}.pipeline_watermarks")

# COMMAND ----------

# DBTITLE 1,Create quality_audit_log
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{QUALITY_SCHEMA}.quality_audit_log (
    run_id          STRING      NOT NULL,
    run_date        DATE        NOT NULL,
    source          STRING      NOT NULL,
    stage           STRING      NOT NULL,
    component       STRING      NOT NULL,
    metric_name     STRING      NOT NULL,
    metric_value    DOUBLE,
    threshold       DOUBLE,
    status          STRING      NOT NULL,
    error_detail    STRING,
    recorded_at     TIMESTAMP   NOT NULL
)
USING DELTA
""")
print(f"OK  table ready: {CATALOG}.{QUALITY_SCHEMA}.quality_audit_log")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Verify Schemas

# COMMAND ----------

# DBTITLE 1,Verify schemas
found_schemas = {
    row.databaseName for row in spark.sql(f"SHOW SCHEMAS IN {CATALOG}").collect()
}
missing_schemas = [s for s in SCHEMAS if s not in found_schemas]

print(f"Catalog: {CATALOG}")
print(f"Schemas found: {sorted(found_schemas)}")

if missing_schemas:
    raise RuntimeError(f"FAIL  missing schemas after creation: {missing_schemas}")
print(f"OK  all {len(SCHEMAS)} required schemas present")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Verify Quality Tables

# COMMAND ----------

# DBTITLE 1,Verify quality tables
found_tables = {
    row.tableName
    for row in spark.sql(f"SHOW TABLES IN {CATALOG}.{QUALITY_SCHEMA}").collect()
}
missing_tables = [t for t in QUALITY_TABLES if t not in found_tables]

print(f"Tables found in {CATALOG}.{QUALITY_SCHEMA}: {sorted(found_tables)}")

if missing_tables:
    raise RuntimeError(f"FAIL  missing quality tables after creation: {missing_tables}")

for table in QUALITY_TABLES:
    try:
        spark.sql(f"DESCRIBE TABLE {CATALOG}.{QUALITY_SCHEMA}.{table}")
        print(f"OK  describable: {CATALOG}.{QUALITY_SCHEMA}.{table}")
    except AnalysisException as exc:
        raise RuntimeError(
            f"FAIL  could not describe {CATALOG}.{QUALITY_SCHEMA}.{table}: {exc}"
        )

print(f"OK  all {len(QUALITY_TABLES)} required quality tables present")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Final Summary

# COMMAND ----------

# DBTITLE 1,Final PASS/FAIL summary
print("=" * 70)
print("UNITY CATALOG SETUP SUMMARY")
print("=" * 70)
print(f"Catalog             : {CATALOG}")
print(f"Schemas created     : {len(SCHEMAS)}  -> {SCHEMAS}")
print(f"Quality tables ready: {len(QUALITY_TABLES)}  -> {QUALITY_TABLES}")
print("-" * 70)
print("RESULT: PASS -- Unity Catalog structure created and verified.")
print("=" * 70)
