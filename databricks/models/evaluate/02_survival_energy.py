# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # EVALUATE SURVIVAL MODELS ENERGY
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** score the stored unit-lifetime survival models and the Kaplan-Meier baseline on the
# MAGIC held-out partition, exactly as stored.

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

# DBTITLE 1,Survival model library
# MAGIC %run ../_fit_survival

# COMMAND ----------

# DBTITLE 1,Evaluation shared library
# MAGIC %run ../_eval_common

# COMMAND ----------

# DBTITLE 1,Survival evaluation library
# MAGIC %run ../_eval_survival

# COMMAND ----------

# DBTITLE 1,Evaluation task specifications
# MAGIC %run ../_eval_specs

# COMMAND ----------

# DBTITLE 1,Smoke parameter
dbutils.widgets.text("smoke", "false")
SMOKE = dbutils.widgets.get("smoke").lower() == "true"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = model_run_id()

# COMMAND ----------

# DBTITLE 1,Evaluate survival.unit_lifetime
evaluate_by_id("survival.unit_lifetime", rid, SMOKE)
