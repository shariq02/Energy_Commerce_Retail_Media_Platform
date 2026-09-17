# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # GOLD -- FACT_SITE_ELECTRICITY_P
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Grain:** one row per (`frequency`, `datetime_utc`).
# MAGIC
# MAGIC **Sources:** `honda_electricity_p` (Silver, energy_silver).
# MAGIC
# MAGIC **Serves use case:** any Energy use case needing the Honda `honda_electricity_p` channel.
# MAGIC There is one physical site and no source-provided location dimension to
# MAGIC resolve against.
# MAGIC
# MAGIC **Purpose:** one of 7 Honda site-energy facts, split -- see the sibling
# MAGIC notebooks in this folder for the other 6. Never conformed into a single
# MAGIC `fact_site_energy`. Near pass-through -- the 5-sigma/stuck-reading/
# MAGIC monotonicity flags Silver already computed carry through unaltered.

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
SOURCE = "honda_iot"
COMPONENT = "gold/energy/iot/fact_site_energy/01_fact_site_electricity_p"
RID = gold_run_id()
GOLD_TABLE = "fact_site_electricity_p"

# COMMAND ----------

# DBTITLE 1,Read Silver -- honda_electricity_p
_silver = read_silver("honda_electricity_p")

# COMMAND ----------

# DBTITLE 1,Transform -- surrogate key + Gold provenance
fact = _silver.withColumn(
    "electricity_p_key", surrogate_key("frequency", "datetime_utc")
)
fact = add_gold_provenance(fact, SOURCE, RID)

# COMMAND ----------

# DBTITLE 1,Grain assertion -- one row per (frequency, datetime_utc)
assert_unique_grain(
    fact,
    ["frequency", "datetime_utc"],
    component=COMPONENT,
    source=SOURCE,
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Write Gold -- fact_site_electricity_p
write_gold(fact, GOLD_TABLE, source=SOURCE, component=COMPONENT, rid=RID)

# COMMAND ----------

# DBTITLE 1,Inspect fact_site_electricity_p + export findings
_findings_blocks = inspect_gold_table(
    fact,
    GOLD_TABLE,
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["frequency", "datetime_utc"],
    df_before=_silver,
)
write_gold_findings(
    SOURCE,
    "fact_site_energy__fact_site_electricity_p",
    GOLD_TABLE,
    _findings_blocks,
)
