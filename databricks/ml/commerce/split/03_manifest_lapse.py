# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # MANIFEST LAPSE
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** user-disjoint partition of the lapse datasets. GA4: training users only at
# MAGIC the 2020-11-20 cutoff and evaluation users only at 2020-12-01; the other
# MAGIC row of a user is excluded so a user is in one partition. REES46: user-
# MAGIC disjoint at its single cutoff.

# COMMAND ----------

# DBTITLE 1,ML shared library
# MAGIC %run ../../_ml_common

# COMMAND ----------

# DBTITLE 1,Configuration
ECO = "commerce"
SOURCE = "ml"
COMPONENT = "ml/commerce/split/manifest_lapse"
TABLE = "partition_manifest"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = ml_run_id()

# COMMAND ----------

# DBTITLE 1,Manifest -- lapse_rees46
_keys = ["user_id", "cutoff_date"]
_keyed = (
    read_ml("assembled_lapse_rees46", ecosystem=ECO)
    .select(*_keys)
    .withColumn("partition", user_partition(["user_id"]))
    .withColumn(
        "fold_id", F.when(F.col("partition") == "train", grouped_fold(["user_id"]))
    )
    .withColumn("group_key", F.col("user_id"))
)
write_partition_manifest(
    _keyed,
    "lapse_rees46",
    ECO,
    key_cols=_keys,
    fold_col="fold_id",
    group_col="group_key",
    rule_id="hash:user_id",
)

# COMMAND ----------

# DBTITLE 1,Manifest -- lapse_ga4
_keys = ["user_id", "cutoff_date"]
_p = user_partition(["user_id"])
_first = F.lit(GA4_LAPSE_TRAIN_CUTOFF).cast("date")
_second = F.lit(GA4_LAPSE_EVALUATION_CUTOFF).cast("date")
_partition = (
    F.when((_p == "train") & (F.col("cutoff_date") != _first), "excluded")
    .when((_p != "train") & (F.col("cutoff_date") != _second), "excluded")
    .otherwise(_p)
)
_keyed = (
    read_ml("assembled_lapse_ga4", ecosystem=ECO)
    .select(*_keys)
    .withColumn("partition", _partition)
    .withColumn(
        "fold_id", F.when(F.col("partition") == "train", grouped_fold(["user_id"]))
    )
    .withColumn("group_key", F.col("user_id"))
)
write_partition_manifest(
    _keyed,
    "lapse_ga4",
    ECO,
    key_cols=_keys,
    fold_col="fold_id",
    group_col="group_key",
    rule_id="hash:user_id_with_cutoffs",
)
