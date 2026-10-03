# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # EVALUATE MATCHING MODELS ENERGY
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** score the stored match-tier classifiers on the held-out partition of the matching
# MAGIC split.

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

# DBTITLE 1,Weak supervision and matching model library
# MAGIC %run ../_fit_weak

# COMMAND ----------

# DBTITLE 1,Evaluation shared library
# MAGIC %run ../_eval_common

# COMMAND ----------

# DBTITLE 1,Weak supervision and matching evaluation library
# MAGIC %run ../_eval_weak

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

# DBTITLE 1,Evaluate redispatch_matching.tier
evaluate_by_id("redispatch_matching.tier", rid, SMOKE)
