# Databricks notebook source
# /// script
# [tool.databricks.environment]
# base_environment = "databricks_ai_v5"
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # FOUNDATION ANOMALY ENERGY
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** pretrained masked-reconstruction models (MOMENT, Energy-TSPulse) as extra
# MAGIC candidates for the anomaly task and the weather reconstruction task, scored on the
# MAGIC validation partition with the rows, injected anomalies and masked blocks of the
# MAGIC accepted candidates. Run on serverless GPU compute.

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

# DBTITLE 1,Reconstruction model library
# MAGIC %run ../lib/_fit_reconstruction

# COMMAND ----------

# DBTITLE 1,Foundation time-series library
# MAGIC %run ../lib/_fit_foundation_ts

# COMMAND ----------

# DBTITLE 1,Foundation anomaly and reconstruction library
# MAGIC %run ../lib/_fit_foundation_anomaly

# COMMAND ----------

# DBTITLE 1,Smoke parameter
dbutils.widgets.text("smoke", "false")
SMOKE = dbutils.widgets.get("smoke").lower() == "true"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = model_run_id()

# COMMAND ----------

# DBTITLE 1,Task context honda_anomaly.anomaly
ctx = FoundationContext("honda_anomaly.anomaly", rid, SMOKE)

# COMMAND ----------

# DBTITLE 1,Read the frozen dataset
df = read_frozen(ctx)

# COMMAND ----------

# DBTITLE 1,Run the foundation candidates
run_foundation_anomaly(ctx, df)

# COMMAND ----------

# DBTITLE 1,Task context weather_imputation.reconstruction
ctx = FoundationContext("weather_imputation.reconstruction", rid, SMOKE)

# COMMAND ----------

# DBTITLE 1,Read the frozen dataset
df = read_frozen(ctx, apply_smoke=False)

# COMMAND ----------

# DBTITLE 1,Run the foundation candidates
run_foundation_reconstruction(ctx, df)

# COMMAND ----------

# DBTITLE 1,Write the findings of this notebook
write_foundation_findings(
    "energy",
    "04_foundation_anomaly_energy",
    "Foundation Anomaly Energy",
    [
        "honda_anomaly.anomaly",
        "weather_imputation.reconstruction",
    ],
    rid,
    SMOKE,
)
