# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # GOLD -- DIM MARKET
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Grain:** one row per `market_zone`. Static reference, no SCD -- a
# MAGIC fixed, small set (DE-LU + 4 control areas), no reconfiguration observed.
# MAGIC
# MAGIC **Sources:** `smard_energy_timeseries` (Silver, energy_silver).
# MAGIC
# MAGIC **Serves use case:** resolving `fact_market_timeseries` to a governed zone.
# MAGIC
# MAGIC **Purpose:** promote the zone identity to a proper dimension.

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
COMPONENT = "gold/energy/market/02_dim_market"
RID = gold_run_id()
GOLD_TABLE = "dim_market"

# COMMAND ----------

# DBTITLE 1,Read Silver -- smard_energy_timeseries
_timeseries_silver = read_silver("smard_energy_timeseries")

# COMMAND ----------

# DBTITLE 1,Transform -- distinct market zones
dim = _timeseries_silver.select("market_zone").distinct()

# COMMAND ----------

# DBTITLE 1,Transform -- surrogate key + Gold provenance
dim = dim.withColumn("market_key", surrogate_key("market_zone"))
dim = add_gold_provenance(dim, SOURCE, RID)

# COMMAND ----------

# DBTITLE 1,Grain assertion -- one row per market_zone
assert_unique_grain(dim, ["market_zone"], component=COMPONENT, source=SOURCE, rid=RID)

# COMMAND ----------

# DBTITLE 1,Write Gold -- dim_market
write_gold(dim, GOLD_TABLE, source=SOURCE, component=COMPONENT, rid=RID)

# COMMAND ----------

# DBTITLE 1,Inspect dim_market + export findings
_findings_blocks = inspect_gold_table(
    dim,
    GOLD_TABLE,
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["market_zone"],
)
write_gold_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__{GOLD_TABLE}",
    GOLD_TABLE,
    _findings_blocks,
)
