# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # MONITOR ENERGY
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** replay the validation and held-out partitions of each registered energy model:
# MAGIC batch scoring into the prediction table and the comparison of each window with
# MAGIC the recorded reference (feature drift, input quality, prediction drift). No
# MAGIC target value is scored. A breach is a record, never a status change.

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

# DBTITLE 1,Survival model library
# MAGIC %run ../lib/_fit_survival

# COMMAND ----------

# DBTITLE 1,Anomaly model library
# MAGIC %run ../lib/_fit_anomaly

# COMMAND ----------

# DBTITLE 1,Reconstruction model library
# MAGIC %run ../lib/_fit_reconstruction

# COMMAND ----------

# DBTITLE 1,Weak supervision model library
# MAGIC %run ../lib/_fit_weak

# COMMAND ----------

# DBTITLE 1,Reinforcement learning model library
# MAGIC %run ../lib/_fit_rl

# COMMAND ----------

# DBTITLE 1,Evaluation shared library
# MAGIC %run ../lib/_eval_common

# COMMAND ----------

# DBTITLE 1,Tabular evaluation library
# MAGIC %run ../lib/_eval_tabular

# COMMAND ----------

# DBTITLE 1,Survival evaluation library
# MAGIC %run ../lib/_eval_survival

# COMMAND ----------

# DBTITLE 1,Anomaly evaluation library
# MAGIC %run ../lib/_eval_anomaly

# COMMAND ----------

# DBTITLE 1,Reconstruction evaluation library
# MAGIC %run ../lib/_eval_reconstruction

# COMMAND ----------

# DBTITLE 1,Weak supervision evaluation library
# MAGIC %run ../lib/_eval_weak

# COMMAND ----------

# DBTITLE 1,Reinforcement learning evaluation library
# MAGIC %run ../lib/_eval_rl

# COMMAND ----------

# DBTITLE 1,Evaluation task specifications
# MAGIC %run ../lib/_eval_specs

# COMMAND ----------

# DBTITLE 1,Registry library
# MAGIC %run ../lib/_registry_rules

# COMMAND ----------

# DBTITLE 1,Operate rules library
# MAGIC %run ../lib/_operate_rules

# COMMAND ----------

# DBTITLE 1,Operate windows library
# MAGIC %run ../lib/_operate_windows

# COMMAND ----------

# DBTITLE 1,Configuration
COMPONENT = "models/operate/02_monitor_energy"
SOURCE = "models"
ECOSYSTEM = "energy"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = model_run_id()

# COMMAND ----------

# DBTITLE 1,Environment of this compute
print(environment_record())

# COMMAND ----------

# DBTITLE 1,Monitor every registered energy model
failed = operate_ecosystem(ECOSYSTEM, rid, "monitor")

# COMMAND ----------

# DBTITLE 1,Every registered model ran
check(
    COMPONENT,
    SOURCE,
    f"every_registered_model_monitored:{ECOSYSTEM}",
    not failed,
    detail=f"failed: {failed}",
    metric_value=float(len(failed)),
    rid=rid,
)
