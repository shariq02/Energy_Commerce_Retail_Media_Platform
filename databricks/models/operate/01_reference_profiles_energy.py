# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # REFERENCE PROFILES ENERGY
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** profile the train partition of each registered energy model at its frozen
# MAGIC version: the reference profile of each model column and of the predictions, and
# MAGIC the predictions of the train window. The reference is recorded before any
# MAGIC other window is read.

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
COMPONENT = "models/operate/01_reference_profiles_energy"
SOURCE = "models"
ECOSYSTEM = "energy"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = model_run_id()

# COMMAND ----------

# DBTITLE 1,Environment of this compute
print(environment_record())

# COMMAND ----------

# DBTITLE 1,Profile every registered energy model
failed = operate_ecosystem(ECOSYSTEM, rid, "profile")

# COMMAND ----------

# DBTITLE 1,Every registered model ran
check(
    COMPONENT,
    SOURCE,
    f"every_registered_model_profiled:{ECOSYSTEM}",
    not failed,
    detail=f"failed: {failed}",
    metric_value=float(len(failed)),
    rid=rid,
)