# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # MANIFEST SURVIVAL
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** unit-grouped split stratified by event and offshore flag, plus the
# MAGIC calendar follow-up check recorded as a diagnostic group.

# COMMAND ----------

# DBTITLE 1,ML shared library
# MAGIC %run ../../_ml_common

# COMMAND ----------

# DBTITLE 1,Configuration
ECO = "energy"
SOURCE = "ml"
COMPONENT = "ml/energy/split/manifest_survival"
TABLE = "partition_manifest"
CHECK_FIT_THROUGH = SURVIVAL_CHECK_FIT_THROUGH

# COMMAND ----------

# DBTITLE 1,Run identity
rid = ml_run_id()

# COMMAND ----------

# DBTITLE 1,Units with their event and stratum
_units = read_ml("assembled_survival", ecosystem=ECO).select(
    "unit_id", "target_event", "is_offshore"
)
_target = read_ml("target_unit_survival", ecosystem=ECO).select(
    "unit_id", "entry_date", "stop_date"
)
_base = _units.join(_target, "unit_id")

# COMMAND ----------

# DBTITLE 1,Stratified hash partition
_stratum = F.concat_ws(
    "|", F.col("target_event").cast("string"), F.col("is_offshore").cast("string")
)
_w = Window.partitionBy(_stratum).orderBy(user_hash_bucket(["unit_id"]), "unit_id")
_rank = F.percent_rank().over(_w)
_tr, _va, _ = USER_SPLIT_PERCENT
_keyed = (
    _base.withColumn("_r", _rank)
    .withColumn(
        "partition",
        F.when(F.col("_r") < _tr / 100, "train")
        .when(F.col("_r") < (_tr + _va) / 100, "validation")
        .otherwise("test"),
    )
    .withColumn(
        "fold_id", F.when(F.col("partition") == "train", grouped_fold(["unit_id"]))
    )
    .withColumn("group_key", F.col("unit_id"))
    .withColumn(
        "diagnostic_group",
        F.when(
            (F.col("entry_date") <= F.lit("2025-01-01").cast("date"))
            & (F.col("stop_date") >= F.lit("2025-01-01").cast("date")),
            "calendar_score",
        ).otherwise("calendar_fit_only"),
    )
)

# COMMAND ----------

# DBTITLE 1,Write the manifest
write_partition_manifest(
    _keyed,
    "survival",
    ECO,
    key_cols=["unit_id"],
    fold_col="fold_id",
    group_col="group_key",
    rule_id="hash:unit_stratified",
    diagnostic_col="diagnostic_group",
)
