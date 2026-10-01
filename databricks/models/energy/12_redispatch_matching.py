# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # MODEL REDISPATCH MATCHING
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** match-tier classifiers for redispatch asset texts on the evaluation split (grouped by
# MAGIC normalised asset identity); the frozen dataset is unchanged.

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
ctx = TaskContext("redispatch_matching.tier", rid, SMOKE)

# COMMAND ----------

# DBTITLE 1,Read the frozen dataset
df = read_frozen(ctx, apply_smoke=False)

# COMMAND ----------

# DBTITLE 1,Attach the evaluation split
df = attach_evaluation_partition(df, ctx, ["affected_asset_text"])

# COMMAND ----------

# DBTITLE 1,Task specification
SPEC = {
    "text_col": "affected_asset_text",
    "target": "target_match_tier",
    "key_cols": ["affected_asset_text"],
    "models": [
        "logistic_numeric",
        "gbt_lightgbm",
        "gbt_sklearn",
        "char_ngram_logistic",
    ],
}

# COMMAND ----------

# DBTITLE 1,Run the matching candidates
run_matching(ctx, df, SPEC)
