# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # MASK SPECIFICATION
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** store the masking specification of the self-supervised and anomaly
# MAGIC datasets: seed, mask lengths weighted by the declared gap distribution,
# MAGIC held-out blocks and stations. No masked copy is materialised.

# COMMAND ----------

# DBTITLE 1,ML shared library
# MAGIC %run ../../_ml_common

# COMMAND ----------

# DBTITLE 1,Configuration
ECO = "energy"
SOURCE = "ml"
COMPONENT = "ml/energy/split/mask_specification"
TABLE = "mask_specification"
SEED = 42
HELD_OUT_STATION_PERCENT = 15
LENGTH_BUCKETS_HOURS = [1, 2, 3, 6, 12, 24, 48, 96]

# COMMAND ----------

# DBTITLE 1,Run identity
rid = ml_run_id()

# COMMAND ----------

# DBTITLE 1,Declared gap lengths in hours
_gaps = (
    read_shared_conformed("data_gap_period")
    .withColumn(
        "hours",
        (
            F.col("gap_end_timestamp").cast("long")
            - F.col("gap_start_timestamp").cast("long")
        )
        / 3600.0,
    )
    .filter(F.col("hours") > 0)
)
_bucket = F.lit(None).cast("int")
for _b in reversed(LENGTH_BUCKETS_HOURS):
    _bucket = F.when(F.col("hours") <= _b, F.lit(_b)).otherwise(_bucket)
_dist = (
    _gaps.withColumn("bucket", F.coalesce(_bucket, F.lit(LENGTH_BUCKETS_HOURS[-1])))
    .groupBy("bucket")
    .count()
    .collect()
)
_total = sum(r["count"] for r in _dist) or 1
weights = {int(r["bucket"]): r["count"] / _total for r in _dist}
print("declared gap length weights:", weights)

# COMMAND ----------

# DBTITLE 1,Held-out stations by stable hash
import json

_stations = [
    r[0]
    for r in read_ml("assembled_weather_imputation", ecosystem=ECO)
    .select("location_key")
    .distinct()
    .collect()
]
_bucket_of = {
    r["location_key"]: r["b"]
    for r in spark.createDataFrame([(s,) for s in _stations], "location_key string")
    .select("location_key", user_hash_bucket(["location_key"]).alias("b"))
    .collect()
}
held_out = sorted(s for s, b in _bucket_of.items() if b < HELD_OUT_STATION_PERCENT)
print(f"{len(held_out)} of {len(_stations)} stations held out")

# COMMAND ----------

# DBTITLE 1,Specification rows
_cal = SPLIT_CALENDARS["energy_daily"]
rows = []
for length, weight in sorted(weights.items()):
    rows.append(
        (
            "weather_imputation",
            SPLIT_VERSION,
            SEED,
            length,
            float(weight),
            _cal["validation"][0],
            _cal["validation"][1],
            json.dumps(held_out),
        )
    )
for length in (24, 72, 168):
    rows.append(
        (
            "self_supervised_other",
            SPLIT_VERSION,
            SEED,
            length,
            1.0 / 3,
            _cal["validation"][0],
            _cal["validation"][1],
            None,
        )
    )
for length in (1, 3, 24):
    rows.append(
        (
            "honda_anomaly",
            SPLIT_VERSION,
            SEED,
            length,
            1.0 / 3,
            SPLIT_CALENDARS["honda"]["validation"][0],
            SPLIT_CALENDARS["honda"]["validation"][1],
            json.dumps(
                {"kinds": ["spike", "drop", "flatline"], "magnitude_sd": [3, 5]}
            ),
        )
    )
out = spark.createDataFrame(rows, REGISTRY_DDL["mask_specification"])

# COMMAND ----------

# DBTITLE 1,Write the specification
replace_rows(
    out,
    TABLE,
    ecosystem=ECO,
    predicate=f"split_version = '{SPLIT_VERSION}' AND dataset_id IN ('weather_imputation', 'self_supervised_other', 'honda_anomaly')",
)
