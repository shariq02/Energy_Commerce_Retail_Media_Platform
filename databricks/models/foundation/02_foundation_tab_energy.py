# Databricks notebook source
# /// script
# [tool.databricks.environment]
# base_environment = "databricks_ai_v5"
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # FOUNDATION TABULAR ENERGY
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** in-context tabular models (TabPFN v2, TabPFN-2.5, TabICL v2) as extra
# MAGIC candidates for the energy one-row-one-example tasks, scored on the validation
# MAGIC partition with the baselines and rows of the accepted candidates. The context
# MAGIC rows are train rows only. Run on serverless GPU compute.

# COMMAND ----------

# DBTITLE 1,Model shared library
# MAGIC %run ../lib/_model_common

# COMMAND ----------

# DBTITLE 1,Model metrics library
# MAGIC %run ../lib/_model_metrics

# COMMAND ----------

# DBTITLE 1,Tabular model library
# MAGIC %run ../lib/_fit_tabular

# COMMAND ----------

# DBTITLE 1,Foundation time-series library
# MAGIC %run ../lib/_fit_foundation_ts

# COMMAND ----------

# DBTITLE 1,Foundation tabular library
# MAGIC %run ../lib/_fit_foundation_tab

# COMMAND ----------

# DBTITLE 1,Smoke parameter
dbutils.widgets.text("smoke", "false")
SMOKE = dbutils.widgets.get("smoke").lower() == "true"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = model_run_id()

# COMMAND ----------

# DBTITLE 1,Task context price_quarter_hour.negative_price
ctx = FoundationContext("price_quarter_hour.negative_price", rid, SMOKE)

# COMMAND ----------

# DBTITLE 1,Read the frozen dataset
df = read_frozen(ctx)

# COMMAND ----------

# DBTITLE 1,Run the in-context candidates
run_foundation_tab(ctx, df)

# COMMAND ----------

# DBTITLE 1,Task context redispatch.any_event
ctx = FoundationContext("redispatch.any_event", rid, SMOKE)

# COMMAND ----------

# DBTITLE 1,Read the frozen dataset
df = read_frozen(ctx)

# COMMAND ----------

# DBTITLE 1,Run the in-context candidates
run_foundation_tab(ctx, df)

# COMMAND ----------

# DBTITLE 1,Task context redispatch.event_energy
ctx = FoundationContext("redispatch.event_energy", rid, SMOKE)

# COMMAND ----------

# DBTITLE 1,Read the frozen dataset
df = read_frozen(ctx)

# COMMAND ----------

# DBTITLE 1,Run the in-context candidates
run_foundation_tab(ctx, df)

# COMMAND ----------

# DBTITLE 1,Task context ccpp.output
ctx = FoundationContext("ccpp.output", rid, SMOKE)

# COMMAND ----------

# DBTITLE 1,Read the frozen dataset
df = read_frozen(ctx)

# COMMAND ----------

# DBTITLE 1,Run the in-context candidates
run_foundation_tab(ctx, df)

# COMMAND ----------

# DBTITLE 1,Task context capacity_additions.additions
ctx = FoundationContext("capacity_additions.additions", rid, SMOKE)

# COMMAND ----------

# DBTITLE 1,Read the frozen dataset
df = read_frozen(ctx)

# COMMAND ----------

# DBTITLE 1,Run the in-context candidates
run_foundation_tab(ctx, df)

# COMMAND ----------

# DBTITLE 1,Write the findings of this notebook
write_foundation_findings(
    "energy",
    "02_foundation_tab_energy",
    "Foundation Tabular Energy",
    [
        "price_quarter_hour.negative_price",
        "redispatch.any_event",
        "redispatch.event_energy",
        "ccpp.output",
        "capacity_additions.additions",
    ],
    rid,
    SMOKE,
)
