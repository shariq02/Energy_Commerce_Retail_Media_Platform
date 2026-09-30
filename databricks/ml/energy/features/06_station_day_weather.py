# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # STATION DAY WEATHER
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** per weather station and local date: mean wind speed and mean speed cubed
# MAGIC with the sensor height valid that day, and mean global radiation. Shared
# MAGIC by the zone-weather fit and build.

# COMMAND ----------

# DBTITLE 1,ML shared library
# MAGIC %run ../../_ml_common

# COMMAND ----------

# DBTITLE 1,ML inspection library
# MAGIC %run ../../_ml_inspect

# COMMAND ----------

# DBTITLE 1,Configuration
ECO = "energy"
SOURCE = "dwd"
COMPONENT = "ml/energy/features/station_day_weather"
TABLE = "features_station_day_wind"
DATE_FROM = SPLIT_CALENDARS["energy_daily"]["train"][0]
RADIATION_TABLE = "features_station_day_radiation"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = ml_run_id()

# COMMAND ----------

# DBTITLE 1,Read Gold
weather = read_gold("weather_observation", source=SOURCE)
radiation = read_gold("weather_observation_radiation", source=SOURCE)
periods = read_gold("place_instrument_period", source=SOURCE)
_date_to = read_gold("market_price", source="smard").agg(F.max("local_date")).first()[0]

# COMMAND ----------

# DBTITLE 1,Wind station days
out = station_day_wind(weather, wind_height_periods(periods), DATE_FROM, str(_date_to))
out = add_ml_provenance(out, TABLE, ECO, rid)

# COMMAND ----------

# DBTITLE 1,Grain and forbidden-column gate
_GRAIN = ["place_key", "local_date"]
assert_unique_grain(out, _GRAIN, component=COMPONENT, source=SOURCE, rid=rid)
assert_no_forbidden_columns(out, component=COMPONENT, source=SOURCE, rid=rid)
check(
    COMPONENT,
    SOURCE,
    "non_empty",
    out.limit(1).count() > 0,
    detail="no rows produced; check the input filters",
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,Write
write_ml(out, TABLE, ecosystem=ECO, source=SOURCE, component=COMPONENT, rid=rid)

# COMMAND ----------

# DBTITLE 1,Inspect and export findings
_blocks = inspect_ml_table(
    read_ml(TABLE, ecosystem=ECO), TABLE, ecosystem=ECO, key_cols=_GRAIN
)
write_ml_findings(ECO, "features__" + TABLE, TABLE, _blocks)

# COMMAND ----------

# DBTITLE 1,Radiation station days
out_rad = station_day_radiation(radiation, DATE_FROM, str(_date_to))
out_rad = add_ml_provenance(out_rad, RADIATION_TABLE, ECO, rid)

# COMMAND ----------

# DBTITLE 1,Grain gate for radiation
assert_unique_grain(
    out_rad,
    ["place_key", "local_date"],
    component=COMPONENT + "/radiation",
    source=SOURCE,
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,Write radiation
write_ml(
    out_rad,
    RADIATION_TABLE,
    ecosystem=ECO,
    source=SOURCE,
    component=COMPONENT + "/radiation",
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,Inspect and export radiation findings
_blocks = inspect_ml_table(
    read_ml(RADIATION_TABLE, ecosystem=ECO),
    RADIATION_TABLE,
    ecosystem=ECO,
    key_cols=["place_key", "local_date"],
)
write_ml_findings(ECO, "features__" + RADIATION_TABLE, RADIATION_TABLE, _blocks)
