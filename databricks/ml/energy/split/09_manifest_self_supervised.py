# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # MANIFEST SELF-SUPERVISED
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** partition manifest of the self-supervised datasets: weather windows on the
# MAGIC energy daily calendar with a station group, and the mixed series windows
# MAGIC with one calendar per series family.

# COMMAND ----------

# DBTITLE 1,ML shared library
# MAGIC %run ../../_ml_common

# COMMAND ----------

# DBTITLE 1,Configuration
ECO = "energy"
SOURCE = "ml"
COMPONENT = "ml/energy/split/manifest_self_supervised"
TABLE = "partition_manifest"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = ml_run_id()

# COMMAND ----------

# DBTITLE 1,Manifest -- weather_imputation
_keys = ["location_key", "observation_timestamp_utc"]
_keyed = (
    read_ml("assembled_weather_imputation", ecosystem=ECO)
    .select(*_keys, "local_date")
    .withColumn("partition", calendar_partition("local_date", "energy_daily"))
    .withColumn(
        "fold_id",
        F.when(
            F.col("partition") == "train", rolling_fold("local_date", "energy_daily")
        ),
    )
    .withColumn("group_key", F.col("location_key"))
)
write_partition_manifest(
    _keyed,
    "weather_imputation",
    ECO,
    key_cols=_keys,
    fold_col="fold_id",
    group_col="group_key",
    rule_id="calendar:energy_daily",
)

# COMMAND ----------

# DBTITLE 1,Manifest -- self_supervised_other (calendar per series family)
_keys = ["series_family", "series_id", "ts"]
_base = read_ml("assembled_self_supervised_other", ecosystem=ECO).select(
    *_keys, "local_date"
)
_part = F.when(
    F.col("series_family") == "honda_hourly", calendar_partition("local_date", "honda")
).otherwise(calendar_partition("local_date", "energy_daily"))
_fold = F.when(
    F.col("series_family") == "honda_hourly", rolling_fold("local_date", "honda")
).otherwise(rolling_fold("local_date", "energy_daily"))
_keyed = _base.withColumn("partition", _part).withColumn(
    "fold_id", F.when(F.col("partition") == "train", _fold)
)
write_partition_manifest(
    _keyed,
    "self_supervised_other",
    ECO,
    key_cols=_keys,
    fold_col="fold_id",
    rule_id="calendar:per_series_family",
)
