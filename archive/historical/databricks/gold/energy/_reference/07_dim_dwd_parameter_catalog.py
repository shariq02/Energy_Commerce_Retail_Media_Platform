# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # GOLD -- DIM DWD PARAMETER CATALOG
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Grain:** one row per `parameter_source_code`. Static reference, no SCD.
# MAGIC
# MAGIC **Sources:** `dwd_parameter_catalog` (Silver, energy_silver_reference).
# MAGIC
# MAGIC **Serves use case:** the DWD parameter vocabulary the 15
# MAGIC `fact_weather_*` tables' source parameters resolve against.
# MAGIC
# MAGIC **Purpose:** promote `dwd_parameter_catalog` to Gold.

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
SOURCE = "dwd"
COMPONENT = "gold/energy/_reference/07_dim_dwd_parameter_catalog"
RID = gold_run_id()
GOLD_TABLE = "dim_dwd_parameter_catalog"

# COMMAND ----------

# DBTITLE 1,Read Silver -- dwd_parameter_catalog
_parameter_catalog_silver = read_silver("dwd_parameter_catalog")

# COMMAND ----------

# DBTITLE 1,Transform -- surrogate key + Gold provenance
dim = _parameter_catalog_silver.withColumn(
    "dwd_parameter_catalog_key", surrogate_key("parameter_source_code")
)
dim = add_gold_provenance(dim, SOURCE, RID)

# COMMAND ----------

# DBTITLE 1,Grain assertion -- one row per parameter_source_code
assert_unique_grain(
    dim, ["parameter_source_code"], component=COMPONENT, source=SOURCE, rid=RID
)

# COMMAND ----------

# DBTITLE 1,Write Gold -- dim_dwd_parameter_catalog
write_gold(dim, GOLD_TABLE, source=SOURCE, component=COMPONENT, rid=RID)

# COMMAND ----------

# DBTITLE 1,Inspect dim_dwd_parameter_catalog + export findings
_findings_blocks = inspect_gold_table(
    dim,
    GOLD_TABLE,
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["parameter_source_code"],
    df_before=_parameter_catalog_silver,
)
write_gold_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__{GOLD_TABLE}",
    GOLD_TABLE,
    _findings_blocks,
)
