# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # MANIFEST GROUPED SMALL
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** grouped k-fold assignment for the small entity datasets (weak supervision
# MAGIC labels and redispatch asset matching). Everything is one training
# MAGIC partition; folds carry the evaluation.

# COMMAND ----------

# DBTITLE 1,ML shared library
# MAGIC %run ../../_ml_common

# COMMAND ----------

# DBTITLE 1,Configuration
ECO = "energy"
SOURCE = "ml"
COMPONENT = "ml/energy/split/manifest_grouped_small"
TABLE = "partition_manifest"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = ml_run_id()

# COMMAND ----------

# DBTITLE 1,Manifest -- weak_supervision
_keys = ["entity_type", "entity_id", "lf_name"]
_keyed = (
    read_ml("assembled_weak_supervision", ecosystem=ECO)
    .select(*_keys)
    .withColumn("partition", F.lit("train"))
    .withColumn("fold_id", grouped_fold(["entity_id"]))
    .withColumn("group_key", F.col("entity_id"))
)
write_partition_manifest(
    _keyed,
    "weak_supervision",
    ECO,
    key_cols=_keys,
    fold_col="fold_id",
    group_col="group_key",
    rule_id="hash:entity_id",
)

# COMMAND ----------

# DBTITLE 1,Manifest -- redispatch_matching
_keyed = (
    read_ml("assembled_redispatch_matching", ecosystem=ECO)
    .select("affected_asset_text")
    .withColumn("partition", F.lit("train"))
    .withColumn("fold_id", grouped_fold(["affected_asset_text"]))
    .withColumn("group_key", F.col("affected_asset_text"))
)
write_partition_manifest(
    _keyed,
    "redispatch_matching",
    ECO,
    key_cols=["affected_asset_text"],
    fold_col="fold_id",
    group_col="group_key",
    rule_id="hash:affected_asset_text",
)
