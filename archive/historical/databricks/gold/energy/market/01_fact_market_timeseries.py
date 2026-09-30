# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # GOLD -- FACT MARKET TIMESERIES
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Grain:** one row per `source_record_id`.
# MAGIC
# MAGIC **Sources:** `smard_energy_timeseries` (Silver, energy_silver).
# MAGIC
# MAGIC **Serves use case:** any Energy use case needing SMARD market/generation
# MAGIC time series, long format.
# MAGIC
# MAGIC **Purpose:** promote `smard_energy_timeseries` to Gold, near pass-through
# MAGIC -- no dimension to resolve at this grain. `metric_semantic_status` /
# MAGIC `semantic_issue_ref` (the PV-forecast sign-mirror flag) and the 5-sigma
# MAGIC outlier flag carry through unaltered.

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
SOURCE = "smard"
COMPONENT = "gold/energy/market/01_fact_market_timeseries"
RID = gold_run_id()
GOLD_TABLE = "fact_market_timeseries"

# COMMAND ----------

# DBTITLE 1,Read Silver -- smard_energy_timeseries
_timeseries_silver = read_silver("smard_energy_timeseries")

# COMMAND ----------

# DBTITLE 1,Transform -- surrogate key + Gold provenance
fact = _timeseries_silver.withColumn(
    "market_timeseries_key", surrogate_key("source_record_id")
)
fact = add_gold_provenance(fact, SOURCE, RID)

# COMMAND ----------

# DBTITLE 1,Grain assertion -- one row per source_record_id
assert_unique_grain(
    fact, ["source_record_id"], component=COMPONENT, source=SOURCE, rid=RID
)

# COMMAND ----------

# DBTITLE 1,Write Gold -- fact_market_timeseries
write_gold(fact, GOLD_TABLE, source=SOURCE, component=COMPONENT, rid=RID)

# COMMAND ----------

# DBTITLE 1,Inspect fact_market_timeseries + export findings
_findings_blocks = inspect_gold_table(
    fact,
    GOLD_TABLE,
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["source_record_id"],
    df_before=_timeseries_silver,
)
write_gold_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__{GOLD_TABLE}",
    GOLD_TABLE,
    _findings_blocks,
)
