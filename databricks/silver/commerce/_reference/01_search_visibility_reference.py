# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER -- SEARCH VISIBILITY REPOSITORY (REFERENCE)
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** `search_visibility_repository` -- the repository reference
# MAGIC table the events notebook's `repository_id` resolves against -- into
# MAGIC `commerce_silver_reference` at its source grain. Events are handled by
# MAGIC `commerce/search_signal/01_search_visibility_events.py`.

# COMMAND ----------

# DBTITLE 1,Shared library
# MAGIC %run ../../_silver_common

# COMMAND ----------

# DBTITLE 1,Imports + config
from pyspark.sql import functions as F

SOURCE = "search_visibility"
# See 01_search_visibility_events.py for why SOURCE != SOURCE_SYSTEM.
SOURCE_SYSTEM = "search_visibility_ramp_dryad"
COMPONENT = "silver/commerce/_reference/01_search_visibility_reference"
RID = run_id()

REPO_BT = "search_visibility_repository"

# COMMAND ----------

# DBTITLE 1,search_visibility_repository -> Silver
repo = read_bronze(REPO_BT).withColumn("_srid", F.col("repository_id").cast("string"))
repo = add_provenance(repo, SOURCE_SYSTEM, "_srid", RID)
write_silver(repo, REPO_BT, source=SOURCE_SYSTEM, component=COMPONENT, rid=RID)

# COMMAND ----------

# DBTITLE 1,Summary
print("=" * 70)
print(f"SEARCH VISIBILITY REPOSITORY -- COMPLETE  (run_id {RID})")
print("=" * 70)
