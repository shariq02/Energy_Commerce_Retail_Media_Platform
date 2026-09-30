# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # HONDA SITE FEATURES
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** channel by interval inputs for the site energy forecast: own lagged
# MAGIC increments and same-place weather at least two days earlier, plus
# MAGIC calendar. Each resolution is kept separate.

# COMMAND ----------

# DBTITLE 1,ML shared library
# MAGIC %run ../../_ml_common

# COMMAND ----------

# DBTITLE 1,ML inspection library
# MAGIC %run ../../_ml_inspect

# COMMAND ----------

# DBTITLE 1,Configuration
ECO = "energy"
SOURCE = "honda_iot"
COMPONENT = "ml/energy/features/honda_site_features"
TABLE = "features_honda_site"
RESOLUTIONS = (3600, 900)
LAG_DAYS = [2, 3, 7]
WEATHER_LAG_DAYS = [2]

# COMMAND ----------

# DBTITLE 1,Run identity
rid = ml_run_id()

# COMMAND ----------

# DBTITLE 1,Session time zone
set_utc_session()

# COMMAND ----------

# DBTITLE 1,Electricity increments at both resolutions
readings = (
    read_gold("channel_reading", source=SOURCE)
    .filter(
        (F.col("subsystem") == "electricity")
        & (F.col("value_kind") == "increment")
        & F.col("interval_seconds").isin(*RESOLUTIONS)
    )
    .select(
        "location_key",
        "channel",
        "interval_seconds",
        "interval_start_utc",
        "local_date",
        F.col("value").alias("increment"),
    )
)

# COMMAND ----------

# DBTITLE 1,Same-place hourly weather
weather = (
    read_gold("weather_observation", source="dwd")
    .filter(F.col("origin_source_system") == "honda_iot")
    .select(
        "location_key",
        F.date_trunc("hour", "observation_timestamp_utc").alias("hour_start"),
        F.col("temperature__air_temperature_degc").alias("air_temperature_degc"),
        F.col("humidity__relative_humidity_percent").alias("relative_humidity_percent"),
    )
    .groupBy("location_key", "hour_start")
    .agg(
        F.avg("air_temperature_degc").alias("air_temperature_degc"),
        F.avg("relative_humidity_percent").alias("relative_humidity_percent"),
    )
)

# COMMAND ----------

# DBTITLE 1,Lag the increments per channel and resolution
lagged = add_timestamp_lags(
    readings,
    keys=["location_key", "channel", "interval_seconds"],
    ts_col="interval_start_utc",
    cols=["increment"],
    lag_days=LAG_DAYS,
).drop("increment")

# COMMAND ----------

# DBTITLE 1,Attach weather from the same hour one day earlier and the calendar
_hour = F.date_trunc("hour", "interval_start_utc")
_wl = add_timestamp_lags(
    weather,
    keys=["location_key"],
    ts_col="hour_start",
    cols=["air_temperature_degc", "relative_humidity_percent"],
    lag_days=WEATHER_LAG_DAYS,
)
_wl = _wl.select("location_key", "hour_start", *[c for c in _wl.columns if "_lag" in c])
_local = F.from_utc_timestamp("interval_start_utc", PROJECT_TIMEZONE)
_cal = (
    read_ml("features_calendar_market_area", ecosystem=ECO)
    .filter(F.col("market_area_code") == "de_lu")
    .select("local_date", "is_holiday", "is_bridge_day")
)
out = (
    lagged.withColumn("hour_start", _hour)
    .join(_wl, ["location_key", "hour_start"], "left")
    .drop("hour_start")
    .withColumn("hour_of_day", F.hour(_local))
    .withColumn("day_of_week", F.dayofweek(_local))
    .withColumn("month", F.month(_local))
    .withColumn("as_of_ts", F.col("interval_start_utc"))
    .join(_cal, "local_date", "left")
)
out = add_ml_provenance(out, TABLE, ECO, rid)

# COMMAND ----------

# DBTITLE 1,Grain and forbidden-column gate
_GRAIN = ["location_key", "channel", "interval_seconds", "interval_start_utc"]
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
