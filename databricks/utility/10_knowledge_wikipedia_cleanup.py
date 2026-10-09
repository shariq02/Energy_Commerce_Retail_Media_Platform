# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # KNOWLEDGE WIKIPEDIA CLEANUP -- table and volume
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** remove the objects of an earlier version of the Wikipedia
# MAGIC notebooks that the current notebooks no longer use: the table
# MAGIC `knowledge.wikipedia_build_report` and the volume
# MAGIC `knowledge.wikipedia_corpus` (shard files and JSON reports). The tables
# MAGIC `wikipedia_article` and `wikipedia_selected` stay.
# MAGIC
# MAGIC Dropping is switched off (`DROP = False`); read the plan, then switch it on
# MAGIC and re-run only the drop cell. Dropping a managed volume deletes its files.

# COMMAND ----------

# DBTITLE 1,Configuration
CATALOG = "energy_commerce_retail_media"
SCHEMA = "knowledge"
TABLES = ["wikipedia_build_report"]
VOLUMES = ["wikipedia_corpus"]
DROP = False  # True

# COMMAND ----------

# DBTITLE 1,Find what exists
existing_tables = [
    t for t in TABLES if spark.catalog.tableExists(f"{CATALOG}.{SCHEMA}.{t}")
]
volume_rows = spark.sql(f"SHOW VOLUMES IN {CATALOG}.{SCHEMA}").collect()
existing_volumes = [
    r["volume_name"] for r in volume_rows if r["volume_name"] in VOLUMES
]
print(f"tables to drop:  {existing_tables or 'none found'}")
print(f"volumes to drop: {existing_volumes or 'none found'}")

# COMMAND ----------

# DBTITLE 1,List the files in each volume
for volume in existing_volumes:
    root = f"/Volumes/{CATALOG}/{SCHEMA}/{volume}"
    print(f"{root}:")
    for entry in dbutils.fs.ls(root):
        print(f"  {entry.name}  {entry.size} bytes")

# COMMAND ----------

# DBTITLE 1,Drop
if DROP:
    for table in existing_tables:
        spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.{table}")
        print(f"OK  dropped table {CATALOG}.{SCHEMA}.{table}")
    for volume in existing_volumes:
        spark.sql(f"DROP VOLUME IF EXISTS {CATALOG}.{SCHEMA}.{volume}")
        print(f"OK  dropped volume {CATALOG}.{SCHEMA}.{volume}")
else:
    print("DROP is False -- nothing dropped. Review the plan above first.")

# COMMAND ----------

# DBTITLE 1,Verify
left_tables = [
    t for t in TABLES if spark.catalog.tableExists(f"{CATALOG}.{SCHEMA}.{t}")
]
left_volumes = [
    r["volume_name"]
    for r in spark.sql(f"SHOW VOLUMES IN {CATALOG}.{SCHEMA}").collect()
    if r["volume_name"] in VOLUMES
]
print(f"tables left:  {left_tables or 'none'}")
print(f"volumes left: {left_volumes or 'none'}")
