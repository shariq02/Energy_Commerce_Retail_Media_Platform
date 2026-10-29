# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # FOUNDATION POLICY ENERGY
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** a Decision Transformer trained offline on the logged trajectories, as an
# MAGIC extra candidate for the pumped storage and redispatch policy tasks, scored on the
# MAGIC validation partition with the rows and the metrics of the accepted policies. Run on
# MAGIC serverless GPU compute.

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

# DBTITLE 1,Offline policy library
# MAGIC %run ../lib/_fit_rl

# COMMAND ----------

# DBTITLE 1,Foundation time-series library
# MAGIC %run ../lib/_fit_foundation_ts

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

# DBTITLE 1,Task context rl_pumped_storage.policy
ctx = FoundationContext("rl_pumped_storage.policy", rid, SMOKE)

# COMMAND ----------

# DBTITLE 1,Read the frozen dataset
df = read_frozen(ctx)

# COMMAND ----------

# DBTITLE 1,Run the foundation candidate
run_foundation_policy(ctx, df)

# COMMAND ----------

# DBTITLE 1,Task context rl_redispatch.policy
ctx = FoundationContext("rl_redispatch.policy", rid, SMOKE)

# COMMAND ----------

# DBTITLE 1,Read the frozen dataset
df = read_frozen(ctx)

# COMMAND ----------

# DBTITLE 1,Run the foundation candidate
run_foundation_policy(ctx, df)

# COMMAND ----------

# DBTITLE 1,Write the findings of this notebook
write_foundation_findings(
    "energy",
    "06_foundation_rl_energy",
    "Foundation Policy Energy",
    [
        "rl_pumped_storage.policy",
        "rl_redispatch.policy",
    ],
    rid,
    SMOKE,
)
