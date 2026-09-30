# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # MANIFEST REDISPATCH
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** partition manifest of the old-regime redispatch datasets: train 2013-04-02
# MAGIC to 2018-12-31, validation 2019, test 2020.

# COMMAND ----------

# DBTITLE 1,ML shared library
# MAGIC %run ../../_ml_common

# COMMAND ----------

# DBTITLE 1,Configuration
ECO = "energy"
SOURCE = "ml"
COMPONENT = "ml/energy/split/manifest_redispatch"
TABLE = "partition_manifest"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = ml_run_id()

# COMMAND ----------

# DBTITLE 1,Manifest -- redispatch
_m = (
    read_ml("dataset_manifest", ecosystem=ECO)
    .filter(F.col("dataset_id") == "redispatch")
    .first()
)
_keys = list(_m["grain_key"])
_base = read_ml("assembled_redispatch", ecosystem=ECO).select(*_keys, "local_date")
_keyed = _base.withColumn(
    "partition", calendar_partition("local_date", "redispatch")
).withColumn(
    "fold_id",
    F.when(F.col("partition") == "train", rolling_fold("local_date", "redispatch")),
)
write_partition_manifest(
    _keyed,
    "redispatch",
    ECO,
    key_cols=_keys,
    fold_col="fold_id",
    rule_id="calendar:redispatch",
)

# COMMAND ----------

# DBTITLE 1,Manifest -- rl_redispatch (episode = operator and day)
_keys = ["episode_id", "step"]
_base = read_ml("assembled_rl_redispatch", ecosystem=ECO).select(*_keys, "local_date")
_keyed = (
    _base.withColumn("partition", calendar_partition("local_date", "redispatch"))
    .withColumn(
        "fold_id",
        F.when(F.col("partition") == "train", rolling_fold("local_date", "redispatch")),
    )
    .withColumn("group_key", F.col("episode_id"))
)
write_partition_manifest(
    _keyed,
    "rl_redispatch",
    ECO,
    key_cols=_keys,
    fold_col="fold_id",
    group_col="group_key",
    rule_id="calendar:redispatch",
)
