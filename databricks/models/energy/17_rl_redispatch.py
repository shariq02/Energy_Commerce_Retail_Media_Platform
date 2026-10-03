# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # MODEL REDISPATCH POLICY
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** offline action policies for redispatch interventions on the frozen trajectories:
# MAGIC majority baseline, behaviour cloning and reward-weighted classifiers.

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

# DBTITLE 1,Smoke parameter
dbutils.widgets.text("smoke", "false")
SMOKE = dbutils.widgets.get("smoke").lower() == "true"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = model_run_id()

# COMMAND ----------

# DBTITLE 1,Task context (frozen version pinned)
ctx = TaskContext("rl_redispatch.policy", rid, SMOKE)

# COMMAND ----------

# DBTITLE 1,Read the frozen dataset
df = read_frozen(ctx)

# COMMAND ----------

# DBTITLE 1,Task specification
SPEC = {
    "action_col": "action_direction",
    "reward_col": "reward",
    "key_cols": ["episode_id", "step"],
    "drop": ["action_direction", "action_reason", "action_energy_mwh", "reward"],
    "id_features": ["market_area_code"],
    "models": [
        "behaviour_cloning_logistic",
        "behaviour_cloning_gbt_lightgbm",
        "behaviour_cloning_gbt_sklearn",
        "reward_weighted_gbt_lightgbm",
        "reward_weighted_gbt_sklearn",
    ],
}

# COMMAND ----------

# DBTITLE 1,Run the policy candidates
run_action_policy(ctx, df, SPEC)
