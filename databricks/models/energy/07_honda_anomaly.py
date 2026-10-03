# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # MODEL HONDA ANOMALY
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** anomaly scores for the Honda hourly series, evaluated on anomalies injected into the
# MAGIC validation period from the stored mask specification.

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

# DBTITLE 1,Anomaly model library
# MAGIC %run ../lib/_fit_anomaly

# COMMAND ----------

# DBTITLE 1,Smoke parameter
dbutils.widgets.text("smoke", "false")
SMOKE = dbutils.widgets.get("smoke").lower() == "true"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = model_run_id()

# COMMAND ----------

# DBTITLE 1,Task context (frozen version pinned)
ctx = TaskContext("honda_anomaly.anomaly", rid, SMOKE)

# COMMAND ----------

# DBTITLE 1,Read the frozen dataset
df = read_frozen(ctx)

# COMMAND ----------

# DBTITLE 1,Task specification
SPEC = {
    "series_cols": ["location_key", "channel"],
    "time_col": "interval_start_utc",
    "target": "target_increment",
    "key_cols": ["location_key", "channel", "interval_seconds", "interval_start_utc"],
    "drop": [],
    "models": [
        "forecast_residual_lightgbm",
        "forecast_residual_sklearn",
        "isolation_forest",
        "autoencoder",
    ],
}

# COMMAND ----------

# DBTITLE 1,Run the anomaly candidates
run_anomaly(ctx, df, SPEC)
