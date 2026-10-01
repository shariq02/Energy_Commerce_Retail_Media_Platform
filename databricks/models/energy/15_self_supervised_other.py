# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # MODEL SELF SUPERVISED SERIES
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** self-supervised reconstruction of masked blocks in the daily load and Honda hourly
# MAGIC series, scored against a causal seasonal-carry baseline.

# COMMAND ----------

# DBTITLE 1,Model shared library
# MAGIC %run ../_model_common

# COMMAND ----------

# DBTITLE 1,Model metrics library
# MAGIC %run ../_model_metrics

# COMMAND ----------

# DBTITLE 1,Tabular model library
# MAGIC %run ../_fit_tabular

# COMMAND ----------

# DBTITLE 1,Reconstruction model library
# MAGIC %run ../_fit_reconstruction

# COMMAND ----------

# DBTITLE 1,Smoke parameter
dbutils.widgets.text("smoke", "false")
SMOKE = dbutils.widgets.get("smoke").lower() == "true"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = model_run_id()

# COMMAND ----------

# DBTITLE 1,Task context (frozen version pinned)
ctx = TaskContext("self_supervised_other.reconstruction", rid, SMOKE)

# COMMAND ----------

# DBTITLE 1,Read the frozen dataset
df = read_frozen(ctx, apply_smoke=False)

# COMMAND ----------

# DBTITLE 1,Task specification
SPEC = {
    "sets": [
        {
            "name": "daily_load",
            "filter_col": "series_family",
            "filter_value": "daily_load",
            "id_col": "series_id",
            "ts_col": "ts",
            "variables": ["series_value"],
            "freq": "D",
            "period": 7,
            "step_hours": 24,
            "baseline": "seasonal_carry",
        },
        {
            "name": "honda_hourly",
            "filter_col": "series_family",
            "filter_value": "honda_hourly",
            "id_col": "series_id",
            "ts_col": "ts",
            "variables": ["series_value"],
            "freq": "h",
            "period": 24,
            "step_hours": 1,
            "baseline": "seasonal_carry",
        },
    ],
    "models": [
        "ridge_cross_variable",
        "gbt_cross_variable_lightgbm",
        "gbt_cross_variable_sklearn",
        "masked_sequence_model",
    ],
}

# COMMAND ----------

# DBTITLE 1,Run the reconstruction candidates
run_reconstruction(ctx, df, SPEC)
