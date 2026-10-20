# Databricks notebook source
# /// script
# [tool.databricks.environment]
# base_environment = "databricks_ai_v5"
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # FOUNDATION TIME SERIES ENERGY
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** pretrained forecasting models (Chronos-2 zero-shot and LoRA, TimesFM
# MAGIC 2.5, Granite TTM) as extra candidates for the energy time-series tasks, scored on
# MAGIC the validation partition with the baselines and rows of the accepted candidates.
# MAGIC Run on serverless GPU compute.

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

# DBTITLE 1,Smoke parameter
dbutils.widgets.text("smoke", "false")
SMOKE = dbutils.widgets.get("smoke").lower() == "true"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = model_run_id()

# COMMAND ----------

# DBTITLE 1,Task context price_daily.price
ctx = FoundationContext("price_daily.price", rid, SMOKE)

# COMMAND ----------

# DBTITLE 1,Read the frozen dataset
df = read_frozen(ctx)

# COMMAND ----------

# DBTITLE 1,Run the foundation candidates
run_foundation_ts(ctx, df)

# COMMAND ----------

# DBTITLE 1,Task context price_quarter_hour.price
ctx = FoundationContext("price_quarter_hour.price", rid, SMOKE)

# COMMAND ----------

# DBTITLE 1,Read the frozen dataset
df = read_frozen(ctx)

# COMMAND ----------

# DBTITLE 1,Run the foundation candidates
run_foundation_ts(ctx, df)

# COMMAND ----------

# DBTITLE 1,Task context load.load
ctx = FoundationContext("load.load", rid, SMOKE)

# COMMAND ----------

# DBTITLE 1,Read the frozen dataset
df = read_frozen(ctx)

# COMMAND ----------

# DBTITLE 1,Run the foundation candidates
run_foundation_ts(ctx, df)

# COMMAND ----------

# DBTITLE 1,Task context bias.bias
ctx = FoundationContext("bias.bias", rid, SMOKE)

# COMMAND ----------

# DBTITLE 1,Read the frozen dataset
df = read_frozen(ctx)

# COMMAND ----------

# DBTITLE 1,Run the foundation candidates
run_foundation_ts(ctx, df)

# COMMAND ----------

# DBTITLE 1,Task context zone_generation.generation
ctx = FoundationContext("zone_generation.generation", rid, SMOKE)

# COMMAND ----------

# DBTITLE 1,Read the frozen dataset
df = read_frozen(ctx)

# COMMAND ----------

# DBTITLE 1,Run the foundation candidates
run_foundation_ts(ctx, df)

# COMMAND ----------

# DBTITLE 1,Task context honda_forecast.increment
ctx = FoundationContext("honda_forecast.increment", rid, SMOKE)

# COMMAND ----------

# DBTITLE 1,Read the frozen dataset
df = read_frozen(ctx)

# COMMAND ----------

# DBTITLE 1,Run the foundation candidates
run_foundation_ts(ctx, df)

# COMMAND ----------

# DBTITLE 1,Write the findings of this notebook
write_foundation_findings(
    "energy",
    "01_foundation_ts_energy",
    "Foundation Time Series Energy",
    [
        "price_daily.price",
        "price_quarter_hour.price",
        "load.load",
        "bias.bias",
        "zone_generation.generation",
        "honda_forecast.increment",
    ],
    rid,
    SMOKE,
)
