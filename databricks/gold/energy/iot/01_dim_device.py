# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # GOLD -- DIM DEVICE
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Grain:** one row per (`site_name`, `subsystem`, `measurement_type`,
# MAGIC `channel_code`). Static reference, no SCD -- one fixed known site with a
# MAGIC fixed known channel set, no evidence of change.
# MAGIC
# MAGIC **Sources:** `honda_channel_catalog` (Silver, energy_silver_reference).
# MAGIC
# MAGIC **Serves use case:** resolving a `fact_site_*` reading to a device
# MAGIC identity via `channel_code` = column name, no physical unpivot.
# MAGIC
# MAGIC **Purpose:** promote `honda_channel_catalog` to Gold.

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
SOURCE = "honda_iot"
COMPONENT = "gold/energy/iot/01_dim_device"
RID = gold_run_id()
GOLD_TABLE = "dim_device"

# COMMAND ----------

# DBTITLE 1,Read Silver -- honda_channel_catalog
_channel_catalog_silver = read_silver("honda_channel_catalog")

# COMMAND ----------

# DBTITLE 1,Transform -- surrogate key + Gold provenance
dim = _channel_catalog_silver.withColumn(
    "device_key",
    surrogate_key("site_name", "subsystem", "measurement_type", "channel_code"),
)
dim = add_gold_provenance(dim, SOURCE, RID)

# COMMAND ----------

# DBTITLE 1,Grain assertion -- one row per (site_name, subsystem, measurement_type, channel_code)
assert_unique_grain(
    dim,
    ["site_name", "subsystem", "measurement_type", "channel_code"],
    component=COMPONENT,
    source=SOURCE,
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Write Gold -- dim_device
write_gold(dim, GOLD_TABLE, source=SOURCE, component=COMPONENT, rid=RID)

# COMMAND ----------

# DBTITLE 1,Inspect dim_device + export findings
_findings_blocks = inspect_gold_table(
    dim,
    GOLD_TABLE,
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["site_name", "subsystem", "measurement_type", "channel_code"],
    df_before=_channel_catalog_silver,
)
write_gold_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__{GOLD_TABLE}",
    GOLD_TABLE,
    _findings_blocks,
)
