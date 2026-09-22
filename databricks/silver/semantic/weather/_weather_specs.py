# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # WEATHER SOURCE CONSTANTS
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** shared constants for the DWD/Honda/AccuWeather notebooks --
# MAGIC per-table read metadata, code/label maps, and the unit-conversion factors
# MAGIC used by more than one family builder. The family record shape and the
# MAGIC source-column -> family-column mapping live in each notebook, next to the
# MAGIC read of that source, not here. Pulled in with `%run ./_weather_specs`
# MAGIC after `_semantic_common`. Definitions only.

# COMMAND ----------

# DBTITLE 1,DWD -- per-table read metadata (quality column, native time grid)
DWD_TABLES = {
    "dwd_air_temperature": {"qn": "QN_9", "time": "hourly"},
    "dwd_moisture": {"qn": "QN_8", "time": "hourly"},
    "dwd_dew_point": {"qn": "QN_8", "time": "hourly"},
    "dwd_pressure": {"qn": "QN_8", "time": "hourly"},
    "dwd_precipitation": {"qn": "QN_8", "time": "hourly"},
    "dwd_sun": {"qn": "QN_7", "time": "hourly"},
    "dwd_wind": {"qn": "QN_3", "time": "hourly"},
    "dwd_wind_synop": {"qn": "QN_8", "time": "hourly"},
    "dwd_extreme_wind": {"qn": "QN_8", "time": "hourly"},
    "dwd_visibility": {"qn": "QN_8", "time": "hourly"},
    "dwd_cloudiness": {"qn": "QN_8", "time": "hourly"},
    "dwd_cloud_type": {"qn": "QN_8", "time": "hourly"},
    "dwd_weather_phenomena": {"qn": "QN_8", "time": "hourly"},
    "dwd_soil_temperature": {"qn": "QN_2", "time": "hourly"},
    "dwd_solar": {"qn": "QN_592", "time": "10min"},
}

# COMMAND ----------

# DBTITLE 1,Shared labels
METHOD_LABELS = {"P": "human_observation", "I": "instrument"}

# COMMAND ----------

# DBTITLE 1,Honda -- sampling frequency to interval seconds
HONDA_INTERVAL_SECONDS = {"1min": 60, "15min": 900, "1h": 3600}

# COMMAND ----------

# DBTITLE 1,Unit-conversion factors used by more than one family builder
# eighths (okta) -> percent; used by weather_cloud for both total cover and
# per-layer cover (DWD is the only eighths-native source).
EIGHTHS_TO_PERCENT = 12.5
# J/cm2 summed over an interval -> mean W/m2: 10000 J/m2 per J/cm2, divided by
# the interval in seconds; used by weather_solar_radiation for DWD's three
# radiation sums.
J_CM2_TO_W_M2 = 10000.0
