# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # GOLD -- FACT SEARCH VISIBILITY
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Grain:** one row per `source_record_id`.
# MAGIC
# MAGIC **Sources:** `search_visibility_events` (Silver, commerce_silver);
# MAGIC `dim_repository` (Gold, this run).
# MAGIC
# MAGIC **Serves use case:** any Commerce use case needing search-console
# MAGIC visibility metrics (clicks, impressions, position) by repository.
# MAGIC
# MAGIC **Purpose:** promote `search_visibility_events` to Gold and resolve
# MAGIC `repository_key`, the FK to `dim_repository`.

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

# DBTITLE 1,Imports
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "search_visibility_ramp_dryad"
COMPONENT = "gold/commerce/search_signal/01_fact_search_visibility"
RID = gold_run_id()
GOLD_TABLE = "fact_search_visibility"

# COMMAND ----------

# DBTITLE 1,Read Silver -- search_visibility_events
_events_silver = read_silver("search_visibility_events")

# COMMAND ----------

# DBTITLE 1,Read Gold -- dim_repository
_repository = read_gold("dim_repository", source="search_visibility_ramp_dryad")

# COMMAND ----------

# DBTITLE 1,Transform -- resolve the repository key
fact = resolve_fk(
    _events_silver,
    _repository,
    fact_key_cols=["repository_id"],
    dim_key_cols=["repository_id"],
    dim_surrogate_col="repository_key",
    output_col="repository_key",
)

# COMMAND ----------

# DBTITLE 1,Transform -- surrogate key + Gold provenance
fact = fact.withColumn("search_visibility_key", surrogate_key("source_record_id"))
fact = add_gold_provenance(fact, SOURCE, RID)

# COMMAND ----------

# DBTITLE 1,Grain assertion -- one row per source_record_id
assert_unique_grain(
    fact, ["source_record_id"], component=COMPONENT, source=SOURCE, rid=RID
)

# COMMAND ----------

# DBTITLE 1,Write Gold -- fact_search_visibility
write_gold(fact, GOLD_TABLE, source=SOURCE, component=COMPONENT, rid=RID)

# COMMAND ----------

# DBTITLE 1,Inspect fact_search_visibility + export findings
_findings_blocks = inspect_gold_table(
    fact,
    GOLD_TABLE,
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["source_record_id"],
    df_before=_events_silver,
    extra_checks={
        "repository_key_unmatched_count": fact.filter(
            F.col("repository_key").isNull()
        ).count(),
    },
)
write_gold_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__{GOLD_TABLE}",
    GOLD_TABLE,
    _findings_blocks,
)
