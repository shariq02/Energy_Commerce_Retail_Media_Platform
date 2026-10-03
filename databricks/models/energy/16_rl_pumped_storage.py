# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # MODEL PUMPED STORAGE POLICY
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** offline policies for pumped-storage arbitrage on the frozen trajectories: rule
# MAGIC baseline, behaviour cloning, reward-weighted regression and conservative Q-learning.

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

# DBTITLE 1,Offline policy library
# MAGIC %run ../_fit_rl

# COMMAND ----------

# DBTITLE 1,Smoke parameter
dbutils.widgets.text("smoke", "false")
SMOKE = dbutils.widgets.get("smoke").lower() == "true"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = model_run_id()

# COMMAND ----------

# DBTITLE 1,Task context (frozen version pinned)
ctx = TaskContext("rl_pumped_storage.policy", rid, SMOKE)

# COMMAND ----------

# DBTITLE 1,Read the frozen dataset
df = read_frozen(ctx)

# COMMAND ----------

# DBTITLE 1,Task specification
SPEC = {
    "state_cols": [
        "state_residual_load_mwh",
        "state_price_eur_per_mwh",
        "hour_of_day",
        "day_of_week",
        "month",
    ],
    "action_col": "action_net_mwh",
    "reward_col": "reward_eur",
    "price_col": "state_price_eur_per_mwh",
    "key_cols": ["episode_id", "step"],
    "models": [
        "behaviour_cloning_lightgbm",
        "behaviour_cloning_sklearn",
        "reward_weighted_regression_lightgbm",
        "reward_weighted_regression_sklearn",
        "conservative_q",
    ],
}

# COMMAND ----------

# DBTITLE 1,Run the policy candidates
run_pumped_storage(ctx, df, SPEC)
