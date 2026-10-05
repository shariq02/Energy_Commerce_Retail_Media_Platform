# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # MONITOR COMMERCE
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** replay the validation and held-out partitions of each registered commerce model:
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

# DBTITLE 1,Ranking model library
# MAGIC %run ../lib/_fit_ranking

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

# DBTITLE 1,Ranking evaluation library
# MAGIC %run ../lib/_eval_ranking

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
COMPONENT = "models/operate/02_monitor_commerce"
SOURCE = "models"
ECOSYSTEM = "commerce"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = model_run_id()

# COMMAND ----------

# DBTITLE 1,Environment of this compute
print(environment_record())

# COMMAND ----------

# DBTITLE 1,Monitor every registered commerce model
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
