# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # ANALYTICS -- SITE_ENERGY_WEATHER_DAILY
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** `energy_analytics.site_energy_weather_daily` -- joins
# MAGIC `site_energy_daily` to `site_weather_daily` on (`location_key`,
# MAGIC `local_date`) -- the one cross-source join Gold's own design record
# MAGIC states is defensible without fabricating an identity: a Honda site's
# MAGIC energy and its weather share the same `location_key` because both derive
# MAGIC it from the same site definition. No other place is invented or assumed
# MAGIC to match. Grain: site x local date x subsystem x channel (weather columns
# MAGIC repeat per channel row at that site/date, since weather has no channel
# MAGIC dimension of its own).

# COMMAND ----------

# DBTITLE 1,Analytics shared library
# MAGIC %run ../../_analytics_common

# COMMAND ----------

# DBTITLE 1,Analytics inspection library
# MAGIC %run ../../_analytics_inspect

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "honda_iot"
COMPONENT = "analytics/energy/site/site_energy_weather_daily"
TABLE = "site_energy_weather_daily"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = analytics_run_id()

# COMMAND ----------

# DBTITLE 1,Read Analytics
site_energy_daily = read_analytics("site_energy_daily", source=SOURCE)
site_weather_daily = read_analytics("site_weather_daily", source=SOURCE)

# COMMAND ----------

# DBTITLE 1,Join gate -- confirm the site place keys actually match before joining
_energy_places = site_energy_daily.select("location_key").distinct()
_weather_places = site_weather_daily.select("location_key").distinct()
_unmatched = _energy_places.join(_weather_places, "location_key", "left_anti").count()
check(
    COMPONENT,
    SOURCE,
    "site_place_keys_match_weather",
    _unmatched == 0,
    detail=f"unmatched_site_count={_unmatched}",
    metric_value=float(_unmatched),
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,Join
site_energy_weather_daily = site_energy_daily.join(
    site_weather_daily.select(
        "location_key",
        "local_date",
        "mean_air_temperature_degc",
        "min_air_temperature_degc",
        "max_air_temperature_degc",
    ),
    ["location_key", "local_date"],
    "left",
)
site_energy_weather_daily = add_analytics_provenance(
    site_energy_weather_daily, SOURCE, rid
)

# COMMAND ----------

# DBTITLE 1,Grain gate
_GRAIN = ["location_key", "local_date", "subsystem", "channel"]
assert_unique_grain(
    site_energy_weather_daily, _GRAIN, component=COMPONENT, source=SOURCE, rid=rid
)

# COMMAND ----------

# DBTITLE 1,Write
write_analytics(
    site_energy_weather_daily, TABLE, source=SOURCE, component=COMPONENT, rid=rid
)

# COMMAND ----------

# DBTITLE 1,Inspect + export findings
_blocks = inspect_analytics_table(
    site_energy_weather_daily,
    TABLE,
    source=SOURCE,
    component=COMPONENT,
    rid=rid,
    key_cols=_GRAIN,
    df_before=site_energy_daily,
)
write_analytics_findings(SOURCE, f"site__{TABLE}", TABLE, _blocks)
