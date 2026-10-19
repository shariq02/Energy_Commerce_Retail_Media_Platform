# Databricks notebook source
# /// script
# [tool.databricks.environment]
# base_environment = "databricks_ai_v5"
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # FOUNDATION SURVIVAL AND MATCHING ENERGY
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** in-context models (SurvPFN, a discrete-time hazard model on TabICL, and
# MAGIC the TabPFN and TabICL classifiers) as extra candidates for the unit lifetime task and
# MAGIC the match tier task, scored on the validation partition with the rows of the accepted
# MAGIC candidates. The context rows are train rows only. Run on serverless GPU compute.

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

# DBTITLE 1,Survival model library
# MAGIC %run ../lib/_fit_survival

# COMMAND ----------

# DBTITLE 1,Weak supervision and matching library
# MAGIC %run ../lib/_fit_weak

# COMMAND ----------

# DBTITLE 1,Foundation time-series library
# MAGIC %run ../lib/_fit_foundation_ts

# COMMAND ----------

# DBTITLE 1,Foundation tabular library
# MAGIC %run ../lib/_fit_foundation_tab

# COMMAND ----------

# DBTITLE 1,Foundation survival library
# MAGIC %run ../lib/_fit_foundation_survival

# COMMAND ----------

# DBTITLE 1,Smoke parameter
dbutils.widgets.text("smoke", "false")
SMOKE = dbutils.widgets.get("smoke").lower() == "true"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = model_run_id()

# COMMAND ----------

# DBTITLE 1,Task context survival.unit_lifetime
ctx = FoundationContext("survival.unit_lifetime", rid, SMOKE)

# COMMAND ----------

# DBTITLE 1,Read the frozen dataset
df = read_frozen(ctx)

# COMMAND ----------

# DBTITLE 1,Run the foundation candidates
run_foundation_survival(ctx, df)

# COMMAND ----------

# DBTITLE 1,Task context redispatch_matching.tier
ctx = FoundationContext("redispatch_matching.tier", rid, SMOKE)

# COMMAND ----------

# DBTITLE 1,Read the frozen dataset
df = read_frozen(ctx, apply_smoke=False)

# COMMAND ----------

# DBTITLE 1,Attach the evaluation split
df = attach_evaluation_partition(df, ctx, ["affected_asset_text"])

# COMMAND ----------

# DBTITLE 1,Run the in-context candidates
run_foundation_tier(ctx, df)
