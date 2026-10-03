# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # MODEL NEXT ITEM GA4
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** next-item ranking on the frozen dataset: popularity, co-occurrence and sequence
# MAGIC candidates.

# COMMAND ----------

# DBTITLE 1,Model shared library
# MAGIC %run ../lib/_model_common

# COMMAND ----------

# DBTITLE 1,Model metrics library
# MAGIC %run ../lib/_model_metrics

# COMMAND ----------

# DBTITLE 1,Ranking model library
# MAGIC %run ../lib/_fit_ranking

# COMMAND ----------

# DBTITLE 1,Smoke parameter
dbutils.widgets.text("smoke", "false")
SMOKE = dbutils.widgets.get("smoke").lower() == "true"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = model_run_id()

# COMMAND ----------

# DBTITLE 1,Task context (frozen version pinned)
ctx = TaskContext("next_item_ga4.next_item", rid, SMOKE)

# COMMAND ----------

# DBTITLE 1,Read the frozen dataset
df = read_frozen(ctx)

# COMMAND ----------

# DBTITLE 1,Task specification
SPEC = {
    "session_col": "session_key",
    "step_col": "step",
    "src_col": "seq_product_id",
    "truth_col": "target_next_product_id",
    "key_cols": ["session_key", "step"],
    "models": ["cooccurrence", "sequence_gru", "sequence_transformer"],
}

# COMMAND ----------

# DBTITLE 1,Run the ranking candidates
run_ranking(ctx, df, SPEC)
