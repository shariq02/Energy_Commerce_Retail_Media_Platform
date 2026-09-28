# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # GOLD -- ENERGY VALIDATION
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** cross-table join gates that no single build notebook can
# MAGIC assert on its own: site place keys resolve against `dim_place`, the
# MAGIC radiation and boundary-aligned weather structures don't share instants,
# MAGIC `iot_device`'s distinct-device count matches its row count, and the
# MAGIC carrier map hasn't silently dropped a candidate value. Run last, after
# MAGIC every other `energy_gold`/`shared_conformed` notebook.

# COMMAND ----------

# DBTITLE 1,Shared Silver library
# MAGIC %run ../../silver/_silver_common

# COMMAND ----------

# DBTITLE 1,Gold shared library
# MAGIC %run ../_gold_common

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "dwd"
COMPONENT = "gold/energy/validation"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = gold_run_id()

# COMMAND ----------

# DBTITLE 1,Gate 1 -- every site place key used by channel_reading resolves in dim_place
_channel_places = (
    read_gold("channel_reading", source=SOURCE).select("location_key").distinct()
)
_dim_places = read_gold(
    "dim_place", source=SOURCE, schema=SHARED_CONFORMED_SCHEMA
).select(F.col("place_key").alias("location_key"))
_unresolved = _channel_places.join(_dim_places, "location_key", "left_anti").count()
check(
    COMPONENT,
    SOURCE,
    "site_place_keys_resolve",
    _unresolved == 0,
    detail=f"unresolved_place_count={_unresolved}",
    metric_value=float(_unresolved),
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,Gate 2 -- DWD's radiation instants (true solar time) don't share
# clock instants with DWD's boundary-aligned families. Honda/AccuWeather
# solar rows are clock-referenced like their other families and legitimately
# share instants with weather_observation -- scoped to DWD only.
_obs_keys = (
    read_gold("weather_observation", source=SOURCE)
    .filter(F.col("origin_source_system") == "dwd")
    .select("location_key", "observation_timestamp_utc")
)
_rad_keys = (
    read_gold("weather_observation_radiation", source=SOURCE)
    .filter(F.col("origin_source_system") == "dwd")
    .select("location_key", "observation_timestamp_utc")
)
_shared_instants = _obs_keys.intersect(_rad_keys).count()
check(
    COMPONENT,
    SOURCE,
    "observation_radiation_instants_distinct",
    _shared_instants == 0,
    detail=f"shared_instant_count={_shared_instants}",
    metric_value=float(_shared_instants),
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,Gate 3 -- iot_device distinct-device count matches its row count
_iot_device_rows = read_gold("iot_device", source="iot").count()
_distinct_devices = (
    read_silver("device_telemetry_snapshot").select("device_key").distinct().count()
)
check(
    COMPONENT,
    "iot",
    "iot_device_matches_distinct_devices",
    _iot_device_rows == _distinct_devices,
    detail=f"iot_device_rows={_iot_device_rows} distinct_devices={_distinct_devices}",
    metric_value=float(_iot_device_rows - _distinct_devices),
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,Gate 4 -- ref_energy_carrier_map has not silently dropped a candidate
_map_rows = read_gold(
    "ref_energy_carrier_map", source="mastr", schema=SHARED_CONFORMED_SCHEMA
).count()
_generation_unit = read_silver("generation_unit")
_candidate_count = (
    _generation_unit.select("main_fuel")
    .filter(F.col("main_fuel").isNotNull())
    .select(
        F.lit("generation_unit.main_fuel").alias("v"), F.col("main_fuel").alias("val")
    )
    .unionByName(
        _generation_unit.select("biomass_type")
        .filter(F.col("biomass_type").isNotNull())
        .select(
            F.lit("generation_unit.biomass_type").alias("v"),
            F.col("biomass_type").alias("val"),
        )
    )
    .unionByName(
        _generation_unit.select("unit_type")
        .filter(F.col("unit_type").isNotNull())
        .select(
            F.lit("generation_unit.unit_type").alias("v"),
            F.col("unit_type").alias("val"),
        )
    )
    .distinct()
    .count()
)
check(
    COMPONENT,
    "mastr",
    "carrier_map_no_silent_drop_generation_unit",
    _map_rows >= _candidate_count,
    detail=f"map_rows={_map_rows} generation_unit_candidates={_candidate_count}",
    metric_value=float(_map_rows - _candidate_count),
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,Summary
print("=" * 70)
print("ENERGY GOLD VALIDATION -- all gates PASSED")
print("=" * 70)
