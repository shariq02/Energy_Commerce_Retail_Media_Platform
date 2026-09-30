# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # MANIFEST ENERGY DAILY
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** partition manifest of the datasets on the energy daily calendar: train to
# MAGIC 2023-12-31, validation 2024, test 2025 onward, with the embargo purged and
# MAGIC rolling-origin folds inside the training partition.

# COMMAND ----------

# DBTITLE 1,ML shared library
# MAGIC %run ../../_ml_common

# COMMAND ----------

# DBTITLE 1,Configuration
ECO = "energy"
SOURCE = "ml"
COMPONENT = "ml/energy/split/manifest_energy_daily"
TABLE = "partition_manifest"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = ml_run_id()

# COMMAND ----------

# DBTITLE 1,Manifest -- price_daily
_m = (
    read_ml("dataset_manifest", ecosystem=ECO)
    .filter(F.col("dataset_id") == "price_daily")
    .first()
)
_keys = list(_m["grain_key"])
_base = read_ml("assembled_price_daily", ecosystem=ECO).select(*_keys, "local_date")
_keyed = _base.withColumn(
    "partition", calendar_partition("local_date", "energy_daily")
).withColumn(
    "fold_id",
    F.when(F.col("partition") == "train", rolling_fold("local_date", "energy_daily")),
)
write_partition_manifest(
    _keyed,
    "price_daily",
    ECO,
    key_cols=_keys,
    fold_col="fold_id",
    rule_id="calendar:energy_daily",
)

# COMMAND ----------

# DBTITLE 1,Manifest -- bias
_m = (
    read_ml("dataset_manifest", ecosystem=ECO)
    .filter(F.col("dataset_id") == "bias")
    .first()
)
_keys = list(_m["grain_key"])
_base = read_ml("assembled_bias", ecosystem=ECO).select(*_keys, "local_date")
_keyed = _base.withColumn(
    "partition", calendar_partition("local_date", "energy_daily")
).withColumn(
    "fold_id",
    F.when(F.col("partition") == "train", rolling_fold("local_date", "energy_daily")),
)
write_partition_manifest(
    _keyed,
    "bias",
    ECO,
    key_cols=_keys,
    fold_col="fold_id",
    rule_id="calendar:energy_daily",
)

# COMMAND ----------

# DBTITLE 1,Manifest -- load
_m = (
    read_ml("dataset_manifest", ecosystem=ECO)
    .filter(F.col("dataset_id") == "load")
    .first()
)
_keys = list(_m["grain_key"])
_base = read_ml("assembled_load", ecosystem=ECO).select(*_keys, "local_date")
_keyed = _base.withColumn(
    "partition", calendar_partition("local_date", "energy_daily")
).withColumn(
    "fold_id",
    F.when(F.col("partition") == "train", rolling_fold("local_date", "energy_daily")),
)
write_partition_manifest(
    _keyed,
    "load",
    ECO,
    key_cols=_keys,
    fold_col="fold_id",
    rule_id="calendar:energy_daily",
)

# COMMAND ----------

# DBTITLE 1,Manifest -- capacity_additions
_m = (
    read_ml("dataset_manifest", ecosystem=ECO)
    .filter(F.col("dataset_id") == "capacity_additions")
    .first()
)
_keys = list(_m["grain_key"])
_base = read_ml("assembled_capacity_additions", ecosystem=ECO).select(
    *_keys, "commissioning_month"
)
_keyed = _base.withColumn(
    "partition", calendar_partition("commissioning_month", "energy_daily")
).withColumn(
    "fold_id",
    F.when(
        F.col("partition") == "train",
        rolling_fold("commissioning_month", "energy_daily"),
    ),
)
write_partition_manifest(
    _keyed,
    "capacity_additions",
    ECO,
    key_cols=_keys,
    fold_col="fold_id",
    rule_id="calendar:energy_daily",
)

# COMMAND ----------

# DBTITLE 1,Manifest -- zone_generation
_m = (
    read_ml("dataset_manifest", ecosystem=ECO)
    .filter(F.col("dataset_id") == "zone_generation")
    .first()
)
_keys = list(_m["grain_key"])
_base = read_ml("assembled_zone_generation", ecosystem=ECO).select(*_keys, "local_date")
_keyed = _base.withColumn(
    "partition", calendar_partition("local_date", "energy_daily")
).withColumn(
    "fold_id",
    F.when(F.col("partition") == "train", rolling_fold("local_date", "energy_daily")),
)
write_partition_manifest(
    _keyed,
    "zone_generation",
    ECO,
    key_cols=_keys,
    fold_col="fold_id",
    rule_id="calendar:energy_daily",
)

# COMMAND ----------

# DBTITLE 1,Partition counts
for r in (
    read_ml(TABLE, ecosystem=ECO)
    .filter(F.col("rule_id") == "calendar:energy_daily")
    .groupBy("dataset_id", "partition")
    .count()
    .orderBy("dataset_id", "partition")
    .collect()
):
    print(r["dataset_id"], r["partition"], r["count"])
