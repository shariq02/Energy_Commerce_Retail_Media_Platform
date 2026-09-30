# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SESSION SEQUENCE REWARD
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** step records for REES46 sessions with a reward per event for reward-
# MAGIC weighted sequence modelling: purchase counts fully, cart partly, view not
# MAGIC at all. The logged action is the user's own choice, so this is not policy
# MAGIC learning.

# COMMAND ----------

# DBTITLE 1,ML shared library
# MAGIC %run ../../_ml_common

# COMMAND ----------

# DBTITLE 1,ML inspection library
# MAGIC %run ../../_ml_inspect

# COMMAND ----------

# DBTITLE 1,Configuration
ECO = "commerce"
SOURCE = "rees46"
COMPONENT = "ml/commerce/targets/rl_session_reward"
TABLE = "target_rl_session_rees46"
# reward weight of each event type times the item price
EVENT_REWARD_WEIGHT = {"view": 0.0, "cart": 0.1, "purchase": 1.0}

# COMMAND ----------

# DBTITLE 1,Run identity
rid = ml_run_id()

# COMMAND ----------

# DBTITLE 1,Read the sequence steps
_seq = read_ml("features_item_sequence_rees46", ecosystem=ECO).select(
    "session_key",
    "user_id",
    "step",
    "event_key",
    "local_date",
    "seq_event_type",
    "seq_product_id",
    "seq_price",
)

# COMMAND ----------

# DBTITLE 1,Reward per step
_weight = F.lit(None).cast("double")
for _t, _w in EVENT_REWARD_WEIGHT.items():
    _weight = F.when(F.col("seq_event_type") == _t, F.lit(float(_w))).otherwise(_weight)
out = (
    _seq.withColumn("episode_id", F.col("session_key"))
    .withColumn(
        "reward",
        F.coalesce(_weight, F.lit(0.0)) * F.coalesce(F.col("seq_price"), F.lit(0.0)),
    )
    .withColumn("target_action_product_id", F.col("seq_product_id"))
    .withColumn("target_action_event_type", F.col("seq_event_type"))
    .withColumn("provenance_tier", F.lit("simulated"))
    .select(
        "episode_id",
        "step",
        "session_key",
        "user_id",
        "event_key",
        "local_date",
        "target_action_event_type",
        "target_action_product_id",
        "reward",
        "provenance_tier",
    )
)
out = add_ml_provenance(out, TABLE, ECO, rid)

# COMMAND ----------

# DBTITLE 1,Grain and forbidden-column gate
_GRAIN = ["episode_id", "step"]
assert_unique_grain(out, _GRAIN, component=COMPONENT, source=SOURCE, rid=rid)
assert_no_forbidden_columns(out, component=COMPONENT, source=SOURCE, rid=rid)
check(
    COMPONENT,
    SOURCE,
    "non_empty",
    out.limit(1).count() > 0,
    detail="no rows produced; check the input filters",
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,Write
write_ml(out, TABLE, ecosystem=ECO, source=SOURCE, component=COMPONENT, rid=rid)

# COMMAND ----------

# DBTITLE 1,Inspect and export findings
_blocks = inspect_ml_table(
    read_ml(TABLE, ecosystem=ECO), TABLE, ecosystem=ECO, key_cols=_GRAIN
)
write_ml_findings(ECO, "targets__" + TABLE, TABLE, _blocks)
