# Databricks notebook source
# /// script
# [tool.databricks.environment]
# base_environment = "databricks_ai_v5"
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # MODEL UNIT SURVIVAL
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** Kaplan-Meier baseline and Cox, survival forest and boosted Cox candidates for wind
# MAGIC unit lifetimes on the frozen dataset.

# COMMAND ----------

# DBTITLE 1,Model shared library
# MAGIC %run ../lib/_model_common

# COMMAND ----------

# DBTITLE 1,Model metrics library
# MAGIC %run ../lib/_model_metrics

# COMMAND ----------

# DBTITLE 1,Survival model library
# MAGIC %run ../lib/_fit_survival

# COMMAND ----------

# DBTITLE 1,Smoke parameter
dbutils.widgets.text("smoke", "false")
SMOKE = dbutils.widgets.get("smoke").lower() == "true"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = model_run_id()

# COMMAND ----------

# DBTITLE 1,Task context (frozen version pinned)
ctx = TaskContext("survival.unit_lifetime", rid, SMOKE)

# COMMAND ----------

# DBTITLE 1,Read the frozen dataset
df = read_frozen(ctx)

# COMMAND ----------

# DBTITLE 1,Task specification
SPEC = {
    "duration_col": "target_duration_years",
    "event_col": "target_event",
    "entry_col": "entry_years",
    "key_cols": ["unit_id"],
    "group_col": "is_offshore",
    "diagnostic_feature": "commissioning_year",
    "drop": ["entry_years", "left_truncated", "entry_date"],
    "models": ["cox_lifelines", "survival_forest", "boosted_cox_xgboost"],
}

# COMMAND ----------

# DBTITLE 1,Run the survival candidates
run_survival(ctx, df, SPEC)
