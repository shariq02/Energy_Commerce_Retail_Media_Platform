# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # CREATE UNITY CATALOG SCHEMAS AND VOLUMES
# MAGIC
# MAGIC **Energy Commerce and Retail Media Analytics Platform**
# MAGIC **Author:** Sharique Mohammad
# MAGIC **Date:** August 2026
# MAGIC
# MAGIC **Purpose:** Create and verify the Unity Catalog structure required
# MAGIC for the project.
# MAGIC
# MAGIC This notebook creates structure only:
# MAGIC - the 5 project schemas (`bronze`, `silver`, `gold`, `quality`, `eda`)
# MAGIC - the 11 frozen Bronze upload-unit Volumes (staging -> Volume -> Bronze)
# MAGIC - the 2 quality/control Delta tables (`pipeline_watermarks`, `quality_audit_log`)
# MAGIC
# MAGIC It does **not** create any Bronze business/data table. Bronze tables are
# MAGIC created later by the Bronze loading notebooks, which read chunks out of
# MAGIC these Volumes.
# MAGIC
# MAGIC Three counts are never the same number and none derives from another:
# MAGIC the staging dataset count, the Volume upload-unit count (11, fixed
# MAGIC below), and the Bronze table count.
# MAGIC
# MAGIC This notebook is safe to run repeatedly -- every statement uses
# MAGIC `IF NOT EXISTS`. It never drops a schema/volume/table and never deletes
# MAGIC Volume contents.

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
# MAGIC `SCHEMAS` are the 5 schemas created below.
# MAGIC `VOLUME_UNITS` are the 11 frozen Bronze upload units -- one Volume per
# MAGIC upload unit, never one Volume per physical chunk. Chunks are files
# MAGIC stored inside their upload unit's Volume.

# COMMAND ----------

CATALOG = "energy_commerce_retail_media"

SCHEMAS = ["bronze", "silver", "gold", "quality", "eda"]

# 11 Volume upload units, fixed by the frozen staging -> Volume -> Bronze
# architecture. Grouped here by source for readability only -- the schema
# each Volume lives under is `bronze` in every case.
VOLUME_UNITS = [
    "dwd_analytical",
    "dwd_metadata",
    "smard_analytical",
    "honda_iot_analytical",
    "kddcup2012_analytical",
    "criteo_attribution_events",
    "search_visibility_events",
    "search_visibility_reference",
    "ipinyou_analytical",
    "ipinyou_reference",
    "rees46_events",
]

VOLUME_SCHEMA = "bronze"

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
# MAGIC ## 4. Create Volume Upload Units
# MAGIC
# MAGIC One managed Volume per Bronze upload unit, under
# MAGIC `energy_commerce_retail_media.bronze`. This is the physical target for
# MAGIC the manual upload step (`data/staging/{source}/{dataset}/` -> here),
# MAGIC not a data table.

# COMMAND ----------

# DBTITLE 1,Create Bronze upload-unit volumes
for volume in VOLUME_UNITS:
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{VOLUME_SCHEMA}.{volume}")
    print(f"OK  volume ready: /Volumes/{CATALOG}/{VOLUME_SCHEMA}/{volume}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Create Quality / Control Tables
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
# MAGIC ## 6. Verify Schemas

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
# MAGIC ## 7. Verify Volumes

# COMMAND ----------

# DBTITLE 1,Verify volumes
found_volumes = {
    row.volume_name
    for row in spark.sql(f"SHOW VOLUMES IN {CATALOG}.{VOLUME_SCHEMA}").collect()
}
missing_volumes = [v for v in VOLUME_UNITS if v not in found_volumes]

print(f"Volumes found in {CATALOG}.{VOLUME_SCHEMA}: {sorted(found_volumes)}")

if missing_volumes:
    raise RuntimeError(f"FAIL  missing volumes after creation: {missing_volumes}")
print(f"OK  all {len(VOLUME_UNITS)} required volumes present")

# COMMAND ----------

# DBTITLE 1,Verify volume accessibility
inaccessible_volumes = []
for volume in VOLUME_UNITS:
    volume_path = f"/Volumes/{CATALOG}/{VOLUME_SCHEMA}/{volume}"
    try:
        dbutils.fs.ls(volume_path)
        print(f"OK  accessible: {volume_path}")
    except Exception as exc:
        inaccessible_volumes.append((volume_path, str(exc)))

if inaccessible_volumes:
    detail = "; ".join(f"{path} -> {err}" for path, err in inaccessible_volumes)
    raise RuntimeError(f"FAIL  inaccessible volumes: {detail}")
print(f"OK  all {len(VOLUME_UNITS)} volumes accessible")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Verify Quality Tables

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
# MAGIC ## 9. Final Summary

# COMMAND ----------

# DBTITLE 1,Final PASS/FAIL summary
print("=" * 70)
print("UNITY CATALOG SETUP SUMMARY")
print("=" * 70)
print(f"Catalog             : {CATALOG}")
print(f"Schemas created     : {len(SCHEMAS)}  -> {SCHEMAS}")
print(f"Volumes created     : {len(VOLUME_UNITS)}  (under {CATALOG}.{VOLUME_SCHEMA})")
for volume in VOLUME_UNITS:
    print(f"                       /Volumes/{CATALOG}/{VOLUME_SCHEMA}/{volume}")
print(f"Quality tables ready: {len(QUALITY_TABLES)}  -> {QUALITY_TABLES}")
print("-" * 70)
print("RESULT: PASS -- Unity Catalog structure created and verified.")
print("=" * 70)
