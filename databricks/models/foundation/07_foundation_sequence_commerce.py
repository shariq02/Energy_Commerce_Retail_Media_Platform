# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # FOUNDATION SEQUENCE COMMERCE
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** a SASRec-type next-item model and a Decision Transformer trained on the
# MAGIC train sessions, as extra candidates for the next-item and session policy tasks, scored
# MAGIC on the validation partition with the rows of the accepted candidates. Users are
# MAGIC sampled as in the accepted notebooks. Run on serverless GPU compute.

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

# DBTITLE 1,Offline policy library
# MAGIC %run ../lib/_fit_rl

# COMMAND ----------

# DBTITLE 1,Foundation time-series library
# MAGIC %run ../lib/_fit_foundation_ts

# COMMAND ----------

# DBTITLE 1,Foundation ranking library
# MAGIC %run ../lib/_fit_foundation_rank

# COMMAND ----------

# DBTITLE 1,Foundation offline policy library
# MAGIC %run ../lib/_fit_foundation_rl

# COMMAND ----------

# DBTITLE 1,Smoke parameter
dbutils.widgets.text("smoke", "false")
SMOKE = dbutils.widgets.get("smoke").lower() == "true"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = model_run_id()

# COMMAND ----------

# DBTITLE 1,Task context next_item_ga4.next_item
ctx = FoundationContext("next_item_ga4.next_item", rid, SMOKE)

# COMMAND ----------

# DBTITLE 1,Read the frozen dataset
df = read_frozen(ctx)

# COMMAND ----------

# DBTITLE 1,Run the foundation candidate
run_foundation_ranking(ctx, df)

# COMMAND ----------

# DBTITLE 1,Task context next_item_rees46.next_item
ctx = FoundationContext("next_item_rees46.next_item", rid, SMOKE)

# COMMAND ----------

# DBTITLE 1,Read the frozen dataset
df = sample_users(read_frozen(ctx))

# COMMAND ----------

# DBTITLE 1,Run the foundation candidate
run_foundation_ranking(ctx, df)

# COMMAND ----------

# DBTITLE 1,Task context rl_session_sequences.policy
ctx = FoundationContext("rl_session_sequences.policy", rid, SMOKE)

# COMMAND ----------

# DBTITLE 1,Read the frozen dataset
df = sample_users(read_frozen(ctx))

# COMMAND ----------

# DBTITLE 1,Run the foundation candidate
run_foundation_policy(ctx, df)

# COMMAND ----------

# DBTITLE 1,Write the findings of this notebook
write_foundation_findings(
    "commerce",
    "07_foundation_sequence_commerce",
    "Foundation Sequence Commerce",
    [
        "next_item_ga4.next_item",
        "next_item_rees46.next_item",
        "rl_session_sequences.policy",
    ],
    rid,
    SMOKE,
)
