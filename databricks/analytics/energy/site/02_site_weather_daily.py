# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # ANALYTICS -- SITE_WEATHER_DAILY
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** `energy_analytics.site_weather_daily` -- the Honda-site rows
# MAGIC of Gold `weather_observation` (`origin_source_system = 'honda_iot'`)
# MAGIC rolled up from hour to day. Not Gold's own `weather_daily`, which only
# MAGIC ever covers DWD/Seattle stations (Silver's derived-daily notebook is
# MAGIC `SOURCE = 'dwd'`-scoped) -- a Honda site's daily weather has to be built
# MAGIC from its hourly rows directly. Grain: site x local date.

# COMMAND ----------

# DBTITLE 1,Analytics shared library
# MAGIC %run ../../_analytics_common

# COMMAND ----------

# DBTITLE 1,Analytics inspection library
# MAGIC %run ../../_analytics_inspect

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "honda_iot"
COMPONENT = "analytics/energy/site/site_weather_daily"
TABLE = "site_weather_daily"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = analytics_run_id()

# COMMAND ----------

# DBTITLE 1,Read Gold -- Honda-origin rows only
weather_observation = read_gold("weather_observation", source="dwd").filter(
    (F.col("origin_source_system") == SOURCE)
    & F.col("temperature__air_temperature_degc").isNotNull()
)

# COMMAND ----------

# DBTITLE 1,Aggregate
site_weather_daily = weather_observation.groupBy("location_key", "local_date").agg(
    F.avg("temperature__air_temperature_degc").alias("mean_air_temperature_degc"),
    F.min("temperature__air_temperature_degc").alias("min_air_temperature_degc"),
    F.max("temperature__air_temperature_degc").alias("max_air_temperature_degc"),
    F.count(F.lit(1)).alias("observation_count"),
)
site_weather_daily = add_analytics_provenance(site_weather_daily, SOURCE, rid)

# COMMAND ----------

# DBTITLE 1,Grain gate
_GRAIN = ["location_key", "local_date"]
assert_unique_grain(
    site_weather_daily, _GRAIN, component=COMPONENT, source=SOURCE, rid=rid
)

# COMMAND ----------

# DBTITLE 1,Write
write_analytics(site_weather_daily, TABLE, source=SOURCE, component=COMPONENT, rid=rid)

# COMMAND ----------

# DBTITLE 1,Inspect + export findings
_blocks = inspect_analytics_table(
    site_weather_daily,
    TABLE,
    source=SOURCE,
    component=COMPONENT,
    rid=rid,
    key_cols=_GRAIN,
    df_before=weather_observation,
)
write_analytics_findings(SOURCE, f"site__{TABLE}", TABLE, _blocks)
