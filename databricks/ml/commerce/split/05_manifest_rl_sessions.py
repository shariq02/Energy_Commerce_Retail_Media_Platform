# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # MANIFEST RL SESSIONS
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** user-disjoint partition of the session sequences, split by episode: every
# MAGIC step of a session inherits the split of its user, with the REES46 temporal
# MAGIC rule.

# COMMAND ----------

# DBTITLE 1,ML shared library
# MAGIC %run ../../_ml_common

# COMMAND ----------

# DBTITLE 1,Configuration
ECO = "commerce"
SOURCE = "ml"
COMPONENT = "ml/commerce/split/manifest_rl_sessions"
TABLE = "partition_manifest"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = ml_run_id()

# COMMAND ----------

# DBTITLE 1,Manifest -- rl_session_sequences
_keys = ["episode_id", "step"]
_base = read_ml("assembled_rl_session_sequences", ecosystem=ECO).select(
    *_keys, "user_id", "local_date"
)

_p = user_partition(["user_id"])
_d = F.col("local_date")
_cut = F.lit(REES46_EVALUATION_START).cast("date")
_partition = (
    F.when((_p == "train") & (_d >= _cut), "excluded")
    .when((_p != "train") & (_d < _cut), "excluded")
    .otherwise(_p)
)
_diag = F.lit(None).cast("string")

_keyed = (
    _base.withColumn("partition", _partition)
    .withColumn(
        "fold_id", F.when(F.col("partition") == "train", grouped_fold(["user_id"]))
    )
    .withColumn("group_key", F.col("user_id"))
    .withColumn("diagnostic_group", _diag)
)
write_partition_manifest(
    _keyed,
    "rl_session_sequences",
    ECO,
    key_cols=_keys,
    fold_col="fold_id",
    group_col="group_key",
    rule_id="hash:user_id_with_temporal_ordering",
    diagnostic_col="diagnostic_group",
)
