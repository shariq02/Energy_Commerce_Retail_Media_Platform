# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # CREATE THE BRONZE LANDING VOLUME
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** November 2026
# MAGIC
# MAGIC **Purpose:** create the one landing Volume that holds the staged files of one
# MAGIC source at a time (`<source>/` folder), for both Bronze loaders. Structure only,
# MAGIC no Bronze table. Safe to run again: `IF NOT EXISTS`, never drops.

# COMMAND ----------

# DBTITLE 1,Shared library
# MAGIC %run ../bronze/_bronze_common

# COMMAND ----------

# DBTITLE 1,Create the landing Volume
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{LANDING_SCHEMA}.{LANDING_VOLUME}")
print(f"OK  volume ready: {landing_root()}")

# COMMAND ----------

# DBTITLE 1,Verify the Volume
found_volumes = {
    row.volume_name
    for row in spark.sql(f"SHOW VOLUMES IN {CATALOG}.{LANDING_SCHEMA}").collect()
}
if LANDING_VOLUME not in found_volumes:
    raise RuntimeError(f"FAIL  missing volume after creation: {LANDING_VOLUME}")
dbutils.fs.ls(landing_root())
print(f"OK  volume present and accessible: {landing_root()}")
