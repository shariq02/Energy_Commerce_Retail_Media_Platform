# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # GOLD -- FACT WEATHER -- DWD_DEW_POINT
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Grain:** one row per (`STATIONS_ID`, `observation_ts`).
# MAGIC
# MAGIC **Sources:** `dwd_dew_point` (Silver, energy_silver); `dim_weather_station`
# MAGIC (Gold, `energy/weather/01_dim_weather_station.py`).
# MAGIC
# MAGIC **Serves use case:** any Energy use case needing `dwd_dew_point` observations
# MAGIC attributed to the station's own attributes AS OF the observation time.
# MAGIC
# MAGIC **Purpose:** one of 15 DWD parameter facts, split -- see the sibling
# MAGIC notebooks in this folder for the other 14 (14 hourly parameters + solar).
# MAGIC Never conformed into a single long `fact_weather`. Resolves
# MAGIC `weather_station_key` via `pit_join()` against `dim_weather_station`'s SCD
# MAGIC validity window, never a plain equi-join on `station_id` alone.

# COMMAND ----------

# DBTITLE 1,Shared Silver library
# MAGIC %run ../../../../silver/_silver_common

# COMMAND ----------

# DBTITLE 1,Gold shared library
# MAGIC %run ../../../_gold_common

# COMMAND ----------

# DBTITLE 1,Inspection library
# MAGIC %run ../../../_gold_inspect

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "dwd"
COMPONENT = "gold/energy/weather/fact_weather/08_fact_weather_dew_point"
RID = gold_run_id()
GOLD_TABLE = "fact_weather_dew_point"

# COMMAND ----------

# DBTITLE 1,Read Silver -- dwd_dew_point
_dew_point_silver = read_silver("dwd_dew_point")

# COMMAND ----------

# DBTITLE 1,Read Gold -- dim_weather_station
_weather_station_dim = read_gold("dim_weather_station", source="dwd")

# COMMAND ----------

# DBTITLE 1,Transform -- fact_weather_dew_point (point-in-time station key)
dew_point = pit_join(
    _dew_point_silver,
    _weather_station_dim,
    fact_key_col="STATIONS_ID",
    fact_ts_col="observation_ts",
    dim_key_col="station_id",
    dim_select=["station_id", "valid_from", "valid_to", "weather_station_key"],
)

# COMMAND ----------

# DBTITLE 1,Transform -- fact_weather_dew_point (surrogate key + Gold provenance)
dew_point = dew_point.withColumn(
    "dew_point_key", surrogate_key("STATIONS_ID", "observation_ts")
)
dew_point = add_gold_provenance(dew_point, SOURCE, RID)

# COMMAND ----------

# DBTITLE 1,Grain assertion -- fact_weather_dew_point
assert_unique_grain(
    dew_point,
    ["STATIONS_ID", "observation_ts"],
    component=COMPONENT,
    source=SOURCE,
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Write Gold -- fact_weather_dew_point
write_gold(dew_point, GOLD_TABLE, source=SOURCE, component=COMPONENT, rid=RID)

# COMMAND ----------

# DBTITLE 1,Inspect fact_weather_dew_point + export findings
_findings_blocks = inspect_gold_table(
    dew_point,
    GOLD_TABLE,
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["STATIONS_ID", "observation_ts"],
    df_before=_dew_point_silver,
    extra_checks={
        "weather_station_key_unmatched_count": dew_point.filter(
            F.col("weather_station_key").isNull()
        ).count(),
    },
)
write_gold_findings(
    SOURCE,
    "fact_weather__fact_weather_dew_point",
    GOLD_TABLE,
    _findings_blocks,
)
