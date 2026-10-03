# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # MODEL CAPACITY ADDITIONS
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** baseline and count-aware candidates for monthly capacity additions per carrier on the
# MAGIC frozen dataset.

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

# DBTITLE 1,Smoke parameter
dbutils.widgets.text("smoke", "false")
SMOKE = dbutils.widgets.get("smoke").lower() == "true"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = model_run_id()

# COMMAND ----------

# DBTITLE 1,Task context (frozen version pinned)
ctx = TaskContext("capacity_additions.additions", rid, SMOKE)

# COMMAND ----------

# DBTITLE 1,Read the frozen dataset
df = read_frozen(ctx)

# COMMAND ----------

# DBTITLE 1,Task specification
SPEC = {
    "target": "target_capacity_added_mw",
    "key_cols": ["carrier_key", "commissioning_month"],
    "id_features": ["carrier_key"],
    "baseline": {
        "kind": "group_mean",
        "cols": ["carrier_key", "month"],
        "name": "seasonal_mean",
    },
    "models": ["ridge", "poisson_gbt_lightgbm", "poisson_gbt_sklearn"],
    "fold_mode": "rolling",
    "date_col": "commissioning_month",
    "gap_days": 0,
}

# COMMAND ----------

# DBTITLE 1,Run the regression candidates
run_regression(ctx, df, SPEC)
