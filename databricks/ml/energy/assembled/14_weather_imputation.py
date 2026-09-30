# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # ASSEMBLED WEATHER IMPUTATION
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** assemble the weather quality and imputation (self-supervised) dataset as a
# MAGIC view: target plus as-of features at the declared grain, with the feature
# MAGIC contract and null classes registered.

# COMMAND ----------

# DBTITLE 1,ML shared library
# MAGIC %run ../../_ml_common

# COMMAND ----------

# DBTITLE 1,ML inspection library
# MAGIC %run ../../_ml_inspect

# COMMAND ----------

# DBTITLE 1,Configuration
ECO = "energy"
SOURCE = "ml"
COMPONENT = "ml/energy/assembled/weather_imputation"
TABLE = "assembled_weather_imputation"
DATASET_ID = "weather_imputation"
GRAIN = ["location_key", "observation_timestamp_utc"]

# COMMAND ----------

# DBTITLE 1,Run identity
rid = ml_run_id()

# COMMAND ----------

# DBTITLE 1,Preflight -- every input structure exists
_needed = []
_missing = [t for t in _needed if not spark.catalog.tableExists(ml_fqn(t, ECO))]
if _missing:
    raise RuntimeError(f"build these first: {_missing}")

# COMMAND ----------

# DBTITLE 1,Create the assembled view
write_ml_view(
    f"""
SELECT 'weather_imputation' AS dataset_id,
       location_key, observation_timestamp_utc, local_date,
       temperature__air_temperature_degc AS air_temperature,
       temperature__dew_point_temperature_degc AS dew_point_temperature,
       humidity__relative_humidity_percent AS relative_humidity,
       pressure__pressure_station_hpa AS pressure_station,
       pressure__pressure_sea_level_hpa AS pressure_sea_level,
       coalesce(filter(wind__readings, r -> r.statistic = 'mean')[0], wind__readings[0]).wind_speed_m_per_s AS wind_speed,
       coalesce(filter(wind__readings, r -> r.statistic = 'mean')[0], wind__readings[0]).wind_direction_degrees AS wind_direction,
       cloud__cloud_cover_total_percent AS cloud_cover,
       visibility__visibility_m AS visibility,
       soil_temperature__soil_temperature_5cm_degc AS soil_temperature
FROM {CATALOG}.energy_gold.weather_observation
WHERE origin_source_system = 'dwd' AND interval_seconds = 3600 AND local_date >= DATE'2018-10-01'
""",
    TABLE,
    ecosystem=ECO,
)

# COMMAND ----------

# DBTITLE 1,Grain gate
assert_unique_grain(
    read_ml(TABLE, ecosystem=ECO), GRAIN, component=COMPONENT, source=SOURCE, rid=rid
)
assert_no_forbidden_columns(
    read_ml(TABLE, ecosystem=ECO), component=COMPONENT, source=SOURCE, rid=rid
)

# COMMAND ----------

# DBTITLE 1,Feature contract and null classes
_contract, _classes, _defaulted = derive_contract(
    read_ml(TABLE, ecosystem=ECO).columns,
    keys=GRAIN,
    time_col="local_date",
    static=(),
    registry=(),
    perfect_prognosis=(),
    context=(
        "air_temperature",
        "dew_point_temperature",
        "relative_humidity",
        "pressure_station",
        "pressure_sea_level",
        "wind_speed",
        "wind_direction",
        "cloud_cover",
        "visibility",
        "soil_temperature",
    ),
    overrides={},
)
_contract += [
    (
        c,
        "label_input",
        "observation_realised",
        0,
        None,
        "defines the target; excluded from the dataset",
    )
    for c in []
]
register_feature_contract(DATASET_ID, ECO, _contract)
register_null_classes(DATASET_ID, ECO, _classes)
if _defaulted:
    print("WARN columns given the default clock (derived_from_earlier):", _defaulted)

# COMMAND ----------

# DBTITLE 1,Register the dataset
register_dataset(
    DATASET_ID,
    ECO,
    capability="weather quality and imputation (self-supervised)",
    grain=GRAIN,
    dependency_group=1,
    target_name="masked_value",
    provenance_tier="constructed",
    status="assembled",
    notes="masks come from the stored mask specification; evaluation is on held-out real values",
)

# COMMAND ----------

# DBTITLE 1,Inspect and export findings
_blocks = inspect_ml_table(
    read_ml(TABLE, ecosystem=ECO), TABLE, ecosystem=ECO, key_cols=GRAIN
)
write_ml_findings(ECO, "assembled__" + TABLE, TABLE, _blocks)
