# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # CREATE GENOMICS BRONZE UPLOAD VOLUMES
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** November 2026
# MAGIC
# MAGIC **Purpose:** create one upload volume per genomics source under
# MAGIC `genomics_bronze`. The sources come from `_genomics_bronze_common.py`. Structure
# MAGIC only, no Bronze table. Safe to run again: `IF NOT EXISTS`, never drops.

# COMMAND ----------

# DBTITLE 1,Shared config
# MAGIC %run ../bronze/_genomics_bronze_common

# COMMAND ----------

# DBTITLE 1,Create the volumes
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
for source in SOURCES:
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{source}")
    print(f"OK  volume ready: {volume_path(source)}")

# COMMAND ----------

# DBTITLE 1,Verify the volumes
found_volumes = {
    row.volume_name
    for row in spark.sql(f"SHOW VOLUMES IN {CATALOG}.{SCHEMA}").collect()
}
missing_volumes = [s for s in SOURCES if s not in found_volumes]
if missing_volumes:
    raise RuntimeError(f"FAIL  missing volumes after creation: {missing_volumes}")

inaccessible = []
for source in SOURCES:
    try:
        dbutils.fs.ls(volume_path(source))
    except Exception as exc:
        inaccessible.append((volume_path(source), str(exc)))
if inaccessible:
    raise RuntimeError(f"FAIL  inaccessible volumes: {inaccessible}")

print(f"OK  all {len(SOURCES)} volumes present and accessible")
