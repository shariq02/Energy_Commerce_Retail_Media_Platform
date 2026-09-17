# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # GOLD -- DIM REPOSITORY
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Grain:** one row per `repository_id`.
# MAGIC
# MAGIC **Sources:** `search_visibility_repository` (Silver,
# MAGIC commerce_silver_reference).
# MAGIC
# MAGIC **Serves use case:** any Commerce use case needing the repository
# MAGIC reference `fact_search_visibility` resolves against.
# MAGIC
# MAGIC **Purpose:** promote `search_visibility_repository` to Gold.

# COMMAND ----------

# DBTITLE 1,Shared Silver library
# MAGIC %run ../../../silver/_silver_common

# COMMAND ----------

# DBTITLE 1,Gold shared library
# MAGIC %run ../../_gold_common

# COMMAND ----------

# DBTITLE 1,Inspection library
# MAGIC %run ../../_gold_inspect

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "search_visibility_ramp_dryad"
COMPONENT = "gold/commerce/_reference/01_dim_repository"
RID = gold_run_id()
GOLD_TABLE = "dim_repository"

# COMMAND ----------

# DBTITLE 1,Read Silver -- search_visibility_repository
_repository_silver = read_silver("search_visibility_repository")

# COMMAND ----------

# DBTITLE 1,Transform -- surrogate key + Gold provenance
dim = _repository_silver.withColumn("repository_key", surrogate_key("repository_id"))
dim = add_gold_provenance(dim, SOURCE, RID)

# COMMAND ----------

# DBTITLE 1,Grain assertion -- one row per repository_id
assert_unique_grain(dim, ["repository_id"], component=COMPONENT, source=SOURCE, rid=RID)

# COMMAND ----------

# DBTITLE 1,Write Gold -- dim_repository
write_gold(dim, GOLD_TABLE, source=SOURCE, component=COMPONENT, rid=RID)

# COMMAND ----------

# DBTITLE 1,Inspect dim_repository + export findings
_findings_blocks = inspect_gold_table(
    dim,
    GOLD_TABLE,
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["repository_id"],
    df_before=_repository_silver,
)
write_gold_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__{GOLD_TABLE}",
    GOLD_TABLE,
    _findings_blocks,
)
