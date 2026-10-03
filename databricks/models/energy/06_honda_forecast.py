# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # MODEL HONDA FORECAST
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** baseline and candidates for the Honda site energy increment on the frozen dataset.

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

# DBTITLE 1,Smoke parameter
dbutils.widgets.text("smoke", "false")
SMOKE = dbutils.widgets.get("smoke").lower() == "true"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = model_run_id()

# COMMAND ----------

# DBTITLE 1,Task context (frozen version pinned)
ctx = TaskContext("honda_forecast.increment", rid, SMOKE)

# COMMAND ----------

# DBTITLE 1,Read the frozen dataset
df = read_frozen(ctx)

# COMMAND ----------

# DBTITLE 1,Task specification
SPEC = {
    "target": "target_increment",
    "key_cols": ["location_key", "channel", "interval_seconds", "interval_start_utc"],
    "id_features": ["location_key", "channel", "interval_seconds"],
    "baseline": {"kind": "column", "column": "increment_lag2d", "name": "persistence"},
    "models": ["ridge", "gbt_lightgbm", "gbt_sklearn"],
    "fold_mode": "rolling",
    "date_col": "local_date",
    "gap_days": 1,
}

# COMMAND ----------

# DBTITLE 1,Run the regression candidates
run_regression(ctx, df, SPEC)
