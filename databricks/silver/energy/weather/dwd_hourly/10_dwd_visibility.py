# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER -- DWD_VISIBILITY
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** `dwd_visibility` (one of the 14 DWD hourly-historical measurement
# MAGIC Bronze tables) into source-scoped Silver at its (station, hour) grain.
# MAGIC One of 14 sibling notebooks in this folder, split from a single
# MAGIC `01_dwd_hourly_measurements.py` -- see the folder's other files for the
# MAGIC other 13 parameters, and `15_dwd_missingness_reconciliation.py` for the
# MAGIC cross-table missingness check that runs after all 14 have written.
# MAGIC Runs after the `_reference/dwd_reference/` notebooks.

# COMMAND ----------

# DBTITLE 1,Shared library
# MAGIC %run ../../../_silver_common

# COMMAND ----------

# DBTITLE 1,Inspection library
# MAGIC %run ../../../_silver_inspect

# COMMAND ----------

# DBTITLE 1,DWD hourly measurement shared helper
# MAGIC %run ./_dwd_hourly_common

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "dwd"
COMPONENT = "silver/energy/weather/dwd_hourly/10_dwd_visibility"
RID = run_id()

CONTRACT = load_contract(SOURCE)
MAPPING = load_mapping(SOURCE)
TABLES = contract_tables(CONTRACT)
NAME_MAP = flatten_business_names(MAPPING, SOURCE)
CODED = coded_columns(MAPPING, SOURCE)

# COMMAND ----------

# DBTITLE 1,Read Bronze -- dwd_visibility
_bronze = read_bronze("dwd_visibility")

# COMMAND ----------

# DBTITLE 1,Transform -- dwd_visibility
df = transform_dwd_measurement(_bronze, "dwd_visibility")

# COMMAND ----------

# DBTITLE 1,Write Silver -- dwd_visibility
write_silver(df, "dwd_visibility", source=SOURCE, component=COMPONENT, rid=RID)

# COMMAND ----------

# DBTITLE 1,Inspect -- dwd_visibility
_findings_blocks = inspect_table(
    df,
    "dwd_visibility",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["STATIONS_ID", "observation_ts"],
    df_before=_bronze,
)
write_silver_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__dwd_visibility",
    "dwd_visibility",
    _findings_blocks,
)
