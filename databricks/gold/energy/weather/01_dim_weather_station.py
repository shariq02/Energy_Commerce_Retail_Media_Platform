# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # GOLD -- DIM WEATHER STATION
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Grain:** one row per (`station_id`, `valid_from`) -- time-varying (SCD).
# MAGIC `dim_weather_station_name_history` is a separate small SCD at the same
# MAGIC grain, kept apart because its own `valid_from`/`valid_to` windows are not
# MAGIC guaranteed to align with `dim_weather_station`'s.
# MAGIC
# MAGIC **Sources:** `dwd_station_geography`, `dwd_station_name_history` (Silver,
# MAGIC energy_silver_reference).
# MAGIC
# MAGIC **Serves use case:** any Energy use case needing DWD station attributes
# MAGIC resolved point-in-time against a weather observation's own timestamp
# MAGIC (see `pit_join()` in `_gold_common.py`).
# MAGIC
# MAGIC **Purpose:** promote `dwd_station_geography` to Gold as the primary
# MAGIC time-varying station dimension `fact_weather` joins against.

# COMMAND ----------

# DBTITLE 1,Shared Silver library
# MAGIC %run ../../../silver/_silver_common

# COMMAND ----------

# DBTITLE 1,Gold shared library
# MAGIC %run ../../_gold_common

# COMMAND ----------

# DBTITLE 1,Inspection library
# MAGIC %run ../../_gold_inspect

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "dwd"
COMPONENT = "gold/energy/weather/01_dim_weather_station"
RID = gold_run_id()

# COMMAND ----------

# DBTITLE 1,Read Silver -- dwd_station_geography
_station_geography_silver = read_silver("dwd_station_geography")

# COMMAND ----------

# DBTITLE 1,Read Silver -- dwd_station_name_history
_station_name_history_silver = read_silver("dwd_station_name_history")

# COMMAND ----------

# DBTITLE 1,Transform -- dim_weather_station
dim_weather_station = _station_geography_silver.withColumn(
    "weather_station_key", surrogate_key("station_id", "valid_from")
)
dim_weather_station = add_gold_provenance(dim_weather_station, SOURCE, RID)

# COMMAND ----------

# DBTITLE 1,Grain assertion -- dim_weather_station, one row per (station_id, valid_from)
assert_unique_grain(
    dim_weather_station,
    ["station_id", "valid_from"],
    component=COMPONENT,
    source=SOURCE,
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Write Gold -- dim_weather_station
write_gold(
    dim_weather_station,
    "dim_weather_station",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Inspect dim_weather_station + export findings
_findings_blocks = inspect_gold_table(
    dim_weather_station,
    "dim_weather_station",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["station_id", "valid_from"],
    df_before=_station_geography_silver,
)
write_gold_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__dim_weather_station",
    "dim_weather_station",
    _findings_blocks,
)

# COMMAND ----------

# DBTITLE 1,Transform -- dim_weather_station_name_history
dim_weather_station_name_history = _station_name_history_silver.withColumn(
    "weather_station_name_history_key", surrogate_key("station_id", "valid_from")
)
dim_weather_station_name_history = add_gold_provenance(
    dim_weather_station_name_history, SOURCE, RID
)

# COMMAND ----------

# DBTITLE 1,Grain assertion -- dim_weather_station_name_history
assert_unique_grain(
    dim_weather_station_name_history,
    ["station_id", "valid_from"],
    component=COMPONENT,
    source=SOURCE,
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Write Gold -- dim_weather_station_name_history
write_gold(
    dim_weather_station_name_history,
    "dim_weather_station_name_history",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Inspect dim_weather_station_name_history + export findings
_findings_blocks = inspect_gold_table(
    dim_weather_station_name_history,
    "dim_weather_station_name_history",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["station_id", "valid_from"],
    df_before=_station_name_history_silver,
)
write_gold_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__dim_weather_station_name_history",
    "dim_weather_station_name_history",
    _findings_blocks,
)
