# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # ANALYTICS -- ENERGY METRIC DEFINITIONS
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** `energy_semantic.metric_definition` -- scoped to metric
# MAGIC definitions, not computed values: one row per metric this build's Energy
# MAGIC marts actually support. No metric is listed without a mart backing it.

# COMMAND ----------

# DBTITLE 1,Analytics shared library
# MAGIC %run ../../_analytics_common

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "smard"
COMPONENT = "analytics/energy/semantic/metric_definition"
TABLE = "metric_definition"

METRICS = [
    (
        "generation_by_carrier",
        "Electricity generation, by resolved energy carrier, per market area and day.",
        "sum(generation_mwh) grouped by market_area_code, local_date, carrier_key",
        "generation_by_carrier_daily",
        "market_area_code x local_date x carrier_key",
    ),
    (
        "average_market_price",
        "Mean day-ahead electricity price per market area and day.",
        "avg(price_eur_per_mwh) grouped by market_area_code, local_date",
        "market_price_daily",
        "market_area_code x local_date",
    ),
    (
        "price_volatility",
        "Within-day dispersion of the day-ahead price.",
        "stddev_pop(price_eur_per_mwh) grouped by market_area_code, local_date",
        "market_price_daily",
        "market_area_code x local_date",
    ),
    (
        "site_energy_consumption",
        "Daily site energy flow per subsystem/channel, additive readings only.",
        "sum(value) where value_kind = 'increment', grouped by location_key, local_date, subsystem, channel",
        "site_energy_daily",
        "location_key x local_date x subsystem x channel",
    ),
    (
        "site_energy_weather_context",
        "Site daily energy paired with that site's own mean air temperature.",
        "site_energy_daily joined to site_weather_daily on (location_key, local_date)",
        "site_energy_weather_daily",
        "location_key x local_date x subsystem x channel",
    ),
]

# COMMAND ----------

# DBTITLE 1,Run identity
rid = analytics_run_id()

# COMMAND ----------

# DBTITLE 1,Build metric_definition
metric_definition = spark.createDataFrame(
    METRICS,
    "metric_key string, definition string, formula string, source_mart string, grain string",
)
metric_definition = add_analytics_provenance(metric_definition, SOURCE, rid)

# COMMAND ----------

# DBTITLE 1,Grain gate
assert_unique_grain(
    metric_definition, ["metric_key"], component=COMPONENT, source=SOURCE, rid=rid
)

# COMMAND ----------

# DBTITLE 1,Write
write_analytics(
    metric_definition,
    TABLE,
    schema=semantic_schema_for(SOURCE),
    source=SOURCE,
    component=COMPONENT,
    rid=rid,
)
