# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # GOLD -- FACT WEATHER -- DWD_WIND_SYNOP
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Grain:** one row per (`STATIONS_ID`, `observation_ts`).
# MAGIC
# MAGIC **Sources:** `dwd_wind_synop` (Silver, energy_silver); `dim_weather_station`
# MAGIC (Gold, `energy/weather/01_dim_weather_station.py`).
# MAGIC
# MAGIC **Serves use case:** any Energy use case needing `dwd_wind_synop` observations
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
COMPONENT = "gold/energy/weather/fact_weather/12_fact_weather_wind_synop"
RID = gold_run_id()
GOLD_TABLE = "fact_weather_wind_synop"

# COMMAND ----------

# DBTITLE 1,Read Silver -- dwd_wind_synop
_wind_synop_silver = read_silver("dwd_wind_synop")

# COMMAND ----------

# DBTITLE 1,Read Gold -- dim_weather_station
_weather_station_dim = read_gold("dim_weather_station", source="dwd")

# COMMAND ----------

# DBTITLE 1,Transform -- fact_weather_wind_synop (point-in-time station key)
wind_synop = pit_join(
    _wind_synop_silver,
    _weather_station_dim,
    fact_key_col="STATIONS_ID",
    fact_ts_col="observation_ts",
    dim_key_col="station_id",
    dim_select=["station_id", "valid_from", "valid_to", "weather_station_key"],
)

# COMMAND ----------

# DBTITLE 1,Transform -- fact_weather_wind_synop (surrogate key + Gold provenance)
wind_synop = wind_synop.withColumn(
    "wind_synop_key", surrogate_key("STATIONS_ID", "observation_ts")
)
wind_synop = add_gold_provenance(wind_synop, SOURCE, RID)

# COMMAND ----------

# DBTITLE 1,Grain assertion -- fact_weather_wind_synop
assert_unique_grain(
    wind_synop,
    ["STATIONS_ID", "observation_ts"],
    component=COMPONENT,
    source=SOURCE,
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Write Gold -- fact_weather_wind_synop
write_gold(wind_synop, GOLD_TABLE, source=SOURCE, component=COMPONENT, rid=RID)

# COMMAND ----------

# DBTITLE 1,Inspect fact_weather_wind_synop + export findings
_findings_blocks = inspect_gold_table(
    wind_synop,
    GOLD_TABLE,
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["STATIONS_ID", "observation_ts"],
    df_before=_wind_synop_silver,
    extra_checks={
        "weather_station_key_unmatched_count": wind_synop.filter(
            F.col("weather_station_key").isNull()
        ).count(),
    },
)
write_gold_findings(
    SOURCE,
    "fact_weather__fact_weather_wind_synop",
    GOLD_TABLE,
    _findings_blocks,
)
