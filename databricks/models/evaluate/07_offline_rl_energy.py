# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # EVALUATE OFFLINE POLICIES ENERGY
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** score the stored pumped-storage and redispatch policies on the held-out partition.

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

# DBTITLE 1,Offline policy model library
# MAGIC %run ../lib/_fit_rl

# COMMAND ----------

# DBTITLE 1,Evaluation shared library
# MAGIC %run ../lib/_eval_common

# COMMAND ----------

# DBTITLE 1,Offline policy evaluation library
# MAGIC %run ../lib/_eval_rl

# COMMAND ----------

# DBTITLE 1,Evaluation task specifications
# MAGIC %run ../lib/_eval_specs

# COMMAND ----------

# DBTITLE 1,Smoke parameter
dbutils.widgets.text("smoke", "false")
SMOKE = dbutils.widgets.get("smoke").lower() == "true"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = model_run_id()

# COMMAND ----------

# DBTITLE 1,Evaluate rl_pumped_storage.policy
evaluate_by_id("rl_pumped_storage.policy", rid, SMOKE)

# COMMAND ----------

# DBTITLE 1,Evaluate rl_redispatch.policy
evaluate_by_id("rl_redispatch.policy", rid, SMOKE)
