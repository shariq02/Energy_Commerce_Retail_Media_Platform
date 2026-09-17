# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # GOLD -- DIM_NETWORK
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Grain:** one row per `network_id`.
# MAGIC
# MAGIC **Sources:** `mastr_netze` (Silver, energy_silver).
# MAGIC
# MAGIC **Serves use case:** any Energy use case needing this MaStR
# MAGIC grid-topology entity. One of 4 sibling notebooks in this folder, split
# MAGIC from a single `01_dim_location.py` -- see the folder's other files for
# MAGIC the rest. These are four distinct entity types at four distinct grains,
# MAGIC never unioned into one dimension.
# MAGIC
# MAGIC **Purpose:** promote `mastr_netze` to Gold -- natural key renamed `MastrNummer` -> `network_id`.

# COMMAND ----------

# DBTITLE 1,Shared Silver library
# MAGIC %run ../../../../silver/_silver_common

# COMMAND ----------

# DBTITLE 1,Gold shared library
# MAGIC %run ../../../_gold_common

# COMMAND ----------

# DBTITLE 1,Inspection library
# MAGIC %run ../../../_gold_inspect

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "mastr"
COMPONENT = "gold/energy/grid/dim_location/03_dim_network"
RID = gold_run_id()
GOLD_TABLE = "dim_network"

# COMMAND ----------

# DBTITLE 1,Read Silver -- mastr_netze
_silver = read_silver("mastr_netze")

# COMMAND ----------

# DBTITLE 1,Transform -- surrogate key + Gold provenance
dim = _silver.withColumnRenamed("MastrNummer", "network_id")
dim = dim.withColumn("network_key", surrogate_key("network_id"))
dim = add_gold_provenance(dim, SOURCE, RID)

# COMMAND ----------

# DBTITLE 1,Grain assertion -- one row per network_id
assert_unique_grain(dim, ["network_id"], component=COMPONENT, source=SOURCE, rid=RID)

# COMMAND ----------

# DBTITLE 1,Write Gold -- dim_network
write_gold(dim, GOLD_TABLE, source=SOURCE, component=COMPONENT, rid=RID)

# COMMAND ----------

# DBTITLE 1,Inspect dim_network + export findings
_findings_blocks = inspect_gold_table(
    dim,
    GOLD_TABLE,
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["network_id"],
    df_before=_silver,
)
write_gold_findings(
    SOURCE,
    "dim_location__dim_network",
    GOLD_TABLE,
    _findings_blocks,
)
