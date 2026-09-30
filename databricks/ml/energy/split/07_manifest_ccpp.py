# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # MANIFEST CCPP
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** random partition of the de-duplicated combined-cycle plant rows by a
# MAGIC stable hash of the row key. There is no time column.

# COMMAND ----------

# DBTITLE 1,ML shared library
# MAGIC %run ../../_ml_common

# COMMAND ----------

# DBTITLE 1,Configuration
ECO = "energy"
SOURCE = "ml"
COMPONENT = "ml/energy/split/manifest_ccpp"
TABLE = "partition_manifest"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = ml_run_id()

# COMMAND ----------

# DBTITLE 1,Hash partition
_base = read_ml("assembled_ccpp", ecosystem=ECO).select("sample_key")
_keyed = (
    _base.withColumn("partition", user_partition(["sample_key"]))
    .withColumn(
        "fold_id", F.when(F.col("partition") == "train", grouped_fold(["sample_key"]))
    )
    .withColumn("group_key", F.col("sample_key"))
)

# COMMAND ----------

# DBTITLE 1,Write the manifest
write_partition_manifest(
    _keyed,
    "ccpp",
    ECO,
    key_cols=["sample_key"],
    fold_col="fold_id",
    group_col="group_key",
    rule_id="hash:sample_key",
)
