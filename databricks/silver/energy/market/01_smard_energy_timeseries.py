# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER -- SMARD ENERGY TIME SERIES
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** `smard_energy_timeseries` into source-scoped Silver at its
# MAGIC (metric, series, market zone, resolution, timestamp) grain -- long format
# MAGIC preserved, unit attached per metric x resolution. The PV-forecast
# MAGIC sign-mirror is flagged `metric_semantic_status = 'disputed'` and left
# MAGIC unaltered.

# COMMAND ----------

# DBTITLE 1,Shared library
# MAGIC %run ../../_silver_common

# COMMAND ----------

# DBTITLE 1,Imports + config
from pyspark.sql import functions as F

SOURCE = "smard"
COMPONENT = "silver/energy/market/01_smard_energy_timeseries"
RID = run_id()
BT = "smard_energy_timeseries"

MAPPING = load_mapping(SOURCE)
METRICS = MAPPING["business_names"]["metrics"]
COLUMN_RENAMES = MAPPING["business_names"]["columns"]

BN_MAP = {k: v["business_name"] for k, v in METRICS.items()}
UNIT_DAY_MAP = {k: v["unit_day"] for k, v in METRICS.items()}
UNIT_QH_MAP = {k: v["unit_quarterhour"] for k, v in METRICS.items()}
DISPUTED_METRIC = "forecast_generation_photovoltaic"
SEMANTIC_ISSUE_REF = "smard.quality_rules.forecast_pv_sign_mirror"


def _map(d: dict):
    return F.create_map([F.lit(x) for kv in d.items() for x in kv])


# COMMAND ----------

# DBTITLE 1,smard_energy_timeseries -> Silver
src = read_bronze(BT).dropDuplicates()
src = src.withColumn(
    "_srid",
    sha_key("metric", "filter_id", "region", "resolution", "timestamp_utc"),
)

df = src
for s, t in COLUMN_RENAMES.items():
    if s != t:
        df = df.withColumnRenamed(s, t)

df = (
    df.withColumn("value", F.col("value").cast("double"))
    .withColumn(
        "observation_timestamp", F.col("observation_timestamp").cast("timestamp")
    )
    .withColumn("observation_ts", F.col("observation_timestamp"))
    .withColumn("metric_business_name", _map(BN_MAP)[F.col("metric")])
    .withColumn(
        "unit",
        F.when(
            F.col("aggregation_resolution") == "day",
            _map(UNIT_DAY_MAP)[F.col("metric")],
        ).otherwise(_map(UNIT_QH_MAP)[F.col("metric")]),
    )
    .withColumn(
        "metric_semantic_status",
        F.when(F.col("metric") == DISPUTED_METRIC, F.lit("disputed")).otherwise(
            F.lit("confirmed")
        ),
    )
    .withColumn(
        "semantic_issue_ref",
        F.when(F.col("metric") == DISPUTED_METRIC, F.lit(SEMANTIC_ISSUE_REF)).otherwise(
            F.lit(None).cast("string")
        ),
    )
)

df = add_provenance(df, SOURCE, "_srid", RID)
write_silver(df, BT, source=SOURCE, component=COMPONENT, rid=RID)

# COMMAND ----------

# DBTITLE 1,Summary
print("=" * 70)
print(f"SMARD ENERGY TIME SERIES -- COMPLETE  (run_id {RID})")
print("=" * 70)
