# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # MODEL WEAK SUPERVISION
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** majority vote and label model over the labelling-function votes on the evaluation
# MAGIC split (grouped by place or unit); the frozen dataset is unchanged.

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

# DBTITLE 1,Weak supervision and matching library
# MAGIC %run ../_fit_weak

# COMMAND ----------

# DBTITLE 1,Smoke parameter
dbutils.widgets.text("smoke", "false")
SMOKE = dbutils.widgets.get("smoke").lower() == "true"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = model_run_id()

# COMMAND ----------

# DBTITLE 1,Task context (frozen version pinned)
ctx = TaskContext("weak_supervision.labels", rid, SMOKE)

# COMMAND ----------

# DBTITLE 1,Read the frozen dataset
df = read_frozen(ctx, apply_smoke=False)

# COMMAND ----------

# DBTITLE 1,Attach the evaluation split
df = attach_evaluation_partition(df, ctx, ["entity_type", "entity_id", "lf_name"])

# COMMAND ----------

# DBTITLE 1,Task specification
SPEC = {"key_cols": ["entity_type", "entity_id", "lf_name"], "models": ["label_model"]}

# COMMAND ----------

# DBTITLE 1,Run the weak supervision candidates
run_weak_supervision(ctx, df, SPEC)
