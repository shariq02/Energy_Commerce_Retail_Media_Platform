# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # REFERENCE PROFILES AND MONITORING
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** November 2026
# MAGIC
# MAGIC **Purpose:** profile the train partition of each registered model of every
# MAGIC ecosystem at its frozen version (the reference profile of each model column and
# MAGIC of the predictions, and the predictions of the train window), then replay the
# MAGIC validation and held-out partitions: batch scoring into the prediction table and
# MAGIC the comparison of each window with the recorded reference (feature drift, input
# MAGIC quality, prediction drift). The references of all ecosystems are recorded before
# MAGIC any other window is read. No target value is scored. A breach is a record,
# MAGIC never a status change.

# COMMAND ----------

# DBTITLE 1,Model shared library
# MAGIC %run ../lib/_model_common

# COMMAND ----------

# DBTITLE 1,Model fit library
# MAGIC %run ../lib/_model_fit

# COMMAND ----------

# DBTITLE 1,Model evaluation library
# MAGIC %run ../lib/_model_evaluate

# COMMAND ----------

# DBTITLE 1,Model governance library
# MAGIC %run ../lib/_model_govern

# COMMAND ----------

# DBTITLE 1,Model operate library
# MAGIC %run ../lib/_model_operate

# COMMAND ----------

# DBTITLE 1,Configuration
COMPONENT = "models/operate/01_reference_and_monitor"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = model_run_id()

# COMMAND ----------

# DBTITLE 1,Environment of this compute
print(environment_record())

# COMMAND ----------

# DBTITLE 1,Profile every registered model (reference windows)
profile_failed = {eco: operate_ecosystem(eco, rid, "profile") for eco in ECOSYSTEMS}

# COMMAND ----------

# DBTITLE 1,Every registered model was profiled
for eco, failed in profile_failed.items():
    guard(
        COMPONENT,
        rid,
        f"every_registered_model_profiled:{eco}",
        failed,
        "failed",
    )

# COMMAND ----------

# DBTITLE 1,Monitor every registered model (validation and held-out windows)
monitor_failed = {eco: operate_ecosystem(eco, rid, "monitor") for eco in ECOSYSTEMS}

# COMMAND ----------

# DBTITLE 1,Every registered model was monitored
for eco, failed in monitor_failed.items():
    guard(
        COMPONENT,
        rid,
        f"every_registered_model_monitored:{eco}",
        failed,
        "failed",
    )

# COMMAND ----------

# DBTITLE 1,Write the findings of this notebook
write_model_findings(
    ECOSYSTEMS,
    "operate__01_reference_and_monitor",
    "Reference Profiles And Monitoring",
    [
        "reference_profile",
        "monitoring_results",
        "monitoring_flags",
        "model_predictions",
    ],
    rid,
)
