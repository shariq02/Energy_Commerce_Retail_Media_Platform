# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # ZONE HELD-OUT DIAGNOSTIC
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** manifest of the rotating zone held-out diagnostic for the zone datasets:
# MAGIC the standard date partition with the zone as group. A run trains on the
# MAGIC other zones and scores the held-out zone. Reported, not the main
# MAGIC evaluation.

# COMMAND ----------

# DBTITLE 1,ML shared library
# MAGIC %run ../../_ml_common

# COMMAND ----------

# DBTITLE 1,Configuration
ECO = "energy"
SOURCE = "ml"
COMPONENT = "ml/energy/split/zone_held_out_diagnostic"
TABLE = "partition_manifest"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = ml_run_id()

# COMMAND ----------

# DBTITLE 1,Manifest -- zone_generation__zone_held_out
_keys = ["market_area_code", "local_date", "carrier"]
_keyed = (
    read_ml("assembled_zone_generation", ecosystem=ECO)
    .select(*_keys)
    .withColumn("partition", calendar_partition("local_date", "energy_daily"))
    .withColumn(
        "fold_id",
        F.when(
            F.col("partition") == "train", rolling_fold("local_date", "energy_daily")
        ),
    )
    .withColumn("group_key", F.col("market_area_code"))
    .withColumn("diagnostic_group", F.col("market_area_code"))
)
write_partition_manifest(
    _keyed,
    "zone_generation__zone_held_out",
    ECO,
    key_cols=_keys,
    fold_col="fold_id",
    group_col="group_key",
    rule_id="diagnostic:zone_held_out",
    diagnostic_col="diagnostic_group",
)

# COMMAND ----------

# DBTITLE 1,Manifest -- load__zone_held_out
_keys = ["market_area_code", "local_date", "load_kind"]
_keyed = (
    read_ml("assembled_load", ecosystem=ECO)
    .select(*_keys)
    .filter(F.col("market_area_code") != "de_lu")
    .withColumn("partition", calendar_partition("local_date", "energy_daily"))
    .withColumn(
        "fold_id",
        F.when(
            F.col("partition") == "train", rolling_fold("local_date", "energy_daily")
        ),
    )
    .withColumn("group_key", F.col("market_area_code"))
    .withColumn("diagnostic_group", F.col("market_area_code"))
)
write_partition_manifest(
    _keyed,
    "load__zone_held_out",
    ECO,
    key_cols=_keys,
    fold_col="fold_id",
    group_col="group_key",
    rule_id="diagnostic:zone_held_out",
    diagnostic_col="diagnostic_group",
)
