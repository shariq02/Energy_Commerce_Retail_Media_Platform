# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # MANIFEST QUARTER-HOUR
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** partition manifest of the quarter-hour price dataset and the pumped-
# MAGIC storage trajectories on the quarter-hour calendar; trajectories are split
# MAGIC by episode.

# COMMAND ----------

# DBTITLE 1,ML shared library
# MAGIC %run ../../_ml_common

# COMMAND ----------

# DBTITLE 1,Configuration
ECO = "energy"
SOURCE = "ml"
COMPONENT = "ml/energy/split/manifest_quarter_hour"
TABLE = "partition_manifest"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = ml_run_id()

# COMMAND ----------

# DBTITLE 1,Manifest -- price_quarter_hour
_m = (
    read_ml("dataset_manifest", ecosystem=ECO)
    .filter(F.col("dataset_id") == "price_quarter_hour")
    .first()
)
_keys = list(_m["grain_key"])
_base = read_ml("assembled_price_quarter_hour", ecosystem=ECO).select(
    *_keys, "local_date"
)
_keyed = _base.withColumn(
    "partition", calendar_partition("local_date", "quarter_hour")
).withColumn(
    "fold_id",
    F.when(F.col("partition") == "train", rolling_fold("local_date", "quarter_hour")),
)
write_partition_manifest(
    _keyed,
    "price_quarter_hour",
    ECO,
    key_cols=_keys,
    fold_col="fold_id",
    rule_id="calendar:quarter_hour",
)

# COMMAND ----------

# DBTITLE 1,Manifest -- rl_pumped_storage (one partition per episode)
_keys = ["episode_id", "step"]
_base = read_ml("assembled_rl_pumped_storage", ecosystem=ECO).select(
    *_keys, "local_date"
)
_keyed = (
    _base.withColumn("partition", calendar_partition("local_date", "quarter_hour"))
    .withColumn(
        "fold_id",
        F.when(
            F.col("partition") == "train", rolling_fold("local_date", "quarter_hour")
        ),
    )
    .withColumn("group_key", F.col("episode_id"))
)
write_partition_manifest(
    _keyed,
    "rl_pumped_storage",
    ECO,
    key_cols=_keys,
    fold_col="fold_id",
    group_col="group_key",
    rule_id="calendar:quarter_hour",
)
