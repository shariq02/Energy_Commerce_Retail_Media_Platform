# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # MODEL ZONE GENERATION
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** baseline and candidates for zone wind and PV generation from weather on the frozen
# MAGIC dataset.

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
ctx = TaskContext("zone_generation.generation", rid, SMOKE)

# COMMAND ----------

# DBTITLE 1,Read the frozen dataset
df = read_frozen(ctx)

# COMMAND ----------

# DBTITLE 1,Task specification
SPEC = {
    "target": "target_generation_mwh",
    "key_cols": ["market_area_code", "local_date", "carrier"],
    "id_features": ["market_area_code", "carrier"],
    "baseline": {
        "kind": "group_ratio",
        "cols": ["market_area_code", "carrier", "month"],
        "denominator": "capacity_net_mw",
        "name": "capacity_factor",
    },
    "models": ["ridge", "gbt_lightgbm", "gbt_sklearn"],
    "fold_mode": "rolling",
    "date_col": "local_date",
    "gap_days": 2,
}

# COMMAND ----------

# DBTITLE 1,Run the regression candidates
run_regression(ctx, df, SPEC)
