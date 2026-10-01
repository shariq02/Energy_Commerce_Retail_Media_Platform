# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # MODEL SESSION POLICY
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** offline event-type policies on the frozen REES46 session trajectories (10% user
# MAGIC sample): frequent-event baseline, Markov, boosted and GRU behaviour models.

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
ctx = TaskContext("rl_session_sequences.policy", rid, SMOKE)

# COMMAND ----------

# DBTITLE 1,Read the frozen dataset and sample users
df = sample_users(read_frozen(ctx))

# COMMAND ----------

# DBTITLE 1,Task specification
SPEC = {
    "action_col": "target_action_event_type",
    "reward_col": "reward",
    "session_col": "session_key",
    "step_col": "step",
    "prev_col": "seq_previous_event_type",
    "key_cols": ["episode_id", "step"],
    "drop": [
        "seq_event_type",
        "seq_product_id",
        "seq_category",
        "seq_brand",
        "seq_price",
        "seq_price_imputed",
        "seq_price_is_missing",
        "reward",
        "target_action_event_type",
        "target_action_product_id",
    ],
    "id_features": ["step"],
    "models": [
        "markov",
        "behaviour_cloning_gbt_lightgbm",
        "behaviour_cloning_gbt_sklearn",
        "reward_weighted_gbt_lightgbm",
        "reward_weighted_gbt_sklearn",
        "sequence_gru",
        "sequence_gru_reward_weighted",
    ],
}

# COMMAND ----------

# DBTITLE 1,Run the policy candidates
run_session_policy(ctx, df, SPEC)
