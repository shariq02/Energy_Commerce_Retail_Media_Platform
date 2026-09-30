# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # MANIFEST HONDA
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** partition manifest of the Honda site datasets: train 2018 to 2021,
# MAGIC validation 2022, test 2023, per resolution.

# COMMAND ----------

# DBTITLE 1,ML shared library
# MAGIC %run ../../_ml_common

# COMMAND ----------

# DBTITLE 1,Configuration
ECO = "energy"
SOURCE = "ml"
COMPONENT = "ml/energy/split/manifest_honda"
TABLE = "partition_manifest"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = ml_run_id()

# COMMAND ----------

# DBTITLE 1,Manifest -- honda_forecast
_m = (
    read_ml("dataset_manifest", ecosystem=ECO)
    .filter(F.col("dataset_id") == "honda_forecast")
    .first()
)
_keys = list(_m["grain_key"])
_base = read_ml("assembled_honda_forecast", ecosystem=ECO).select(*_keys, "local_date")
_keyed = _base.withColumn(
    "partition", calendar_partition("local_date", "honda")
).withColumn(
    "fold_id",
    F.when(F.col("partition") == "train", rolling_fold("local_date", "honda")),
)
write_partition_manifest(
    _keyed,
    "honda_forecast",
    ECO,
    key_cols=_keys,
    fold_col="fold_id",
    rule_id="calendar:honda",
)

# COMMAND ----------

# DBTITLE 1,Manifest -- honda_anomaly
_m = (
    read_ml("dataset_manifest", ecosystem=ECO)
    .filter(F.col("dataset_id") == "honda_anomaly")
    .first()
)
_keys = list(_m["grain_key"])
_base = read_ml("assembled_honda_anomaly", ecosystem=ECO).select(*_keys, "local_date")
_keyed = _base.withColumn(
    "partition", calendar_partition("local_date", "honda")
).withColumn(
    "fold_id",
    F.when(F.col("partition") == "train", rolling_fold("local_date", "honda")),
)
write_partition_manifest(
    _keyed,
    "honda_anomaly",
    ECO,
    key_cols=_keys,
    fold_col="fold_id",
    rule_id="calendar:honda",
)
