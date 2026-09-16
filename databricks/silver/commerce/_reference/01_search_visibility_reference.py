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

# DBTITLE 1,Inspection library
# MAGIC %run ../../_silver_inspect

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "search_visibility"
# See 01_search_visibility_events.py for why SOURCE != SOURCE_SYSTEM.
SOURCE_SYSTEM = "search_visibility_ramp_dryad"
COMPONENT = "silver/commerce/_reference/01_search_visibility_reference"
RID = run_id()

REPO_BT = "search_visibility_repository"

# COMMAND ----------

# DBTITLE 1,Read Bronze -- search_visibility_repository
bronze_repo = read_bronze(REPO_BT)

# COMMAND ----------

# DBTITLE 1,Transform -- search_visibility_repository
repo = bronze_repo.withColumn("_srid", F.col("repository_id").cast("string"))

# Rename the reference side's `country` too -- it means the repository's home
# country, a different concept from the events table's traffic-geography
# `country`.
repo = repo.withColumnRenamed("country", "repository_home_country")

repo = add_provenance(repo, SOURCE_SYSTEM, "_srid", RID)

# COMMAND ----------

# DBTITLE 1,Write Silver -- search_visibility_repository
write_silver(repo, REPO_BT, source=SOURCE_SYSTEM, component=COMPONENT, rid=RID)

# COMMAND ----------

# DBTITLE 1,Inspect search_visibility_repository + export findings
_findings_blocks = inspect_table(
    repo,
    REPO_BT,
    source=SOURCE_SYSTEM,
    component=COMPONENT,
    rid=RID,
    key_cols=["repository_id"],
    df_before=bronze_repo,
)
write_silver_findings(
    SOURCE_SYSTEM,
    f"{COMPONENT.split('/')[-1]}__{REPO_BT}",
    REPO_BT,
    _findings_blocks,
)
