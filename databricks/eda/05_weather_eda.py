# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # EDA -- WEATHER (GOLD)
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** exploratory analysis of the weather data on the Gold tables, run
# MAGIC after Gold and before ML. Each section profiles one group of tables and writes
# MAGIC its findings to `src/schemas/profiling/<source>.md` (one section key per group).

# COMMAND ----------

# MAGIC %run ./_eda_common

# COMMAND ----------

# MAGIC %md
# MAGIC ## EDA -- WEATHER OBSERVATION AND DAILY TABLES
# MAGIC
# MAGIC **Purpose:** Profile the weather tables of energy Gold (`weather_observation`,
# MAGIC `weather_daily`): nulls, constant columns, key, rates per column kind,
# MAGIC coverage by source, the fill of the link columns and the date-like columns --
# MAGIC as evidence.

# COMMAND ----------

# DBTITLE 1,Standard EDA of the weather tables
run_groups(
    "energy_commerce_retail_media",
    "weather",
    [
        {
            "schema": "energy_gold",
            "nb_key": "01_weather_tables",
            "title": "Weather observation and daily tables",
            "tables": ["weather_observation", "weather_daily"],
        }
    ],
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## EDA -- WEATHER CONTINUITY, LINKS AND COVERAGE BIAS
# MAGIC
# MAGIC **Purpose:** Measure the weather series against an expected time grid (per source
# MAGIC and step, per location), check that the location key resolves to a place row, and
# MAGIC measure how the rows spread over the locations.

# COMMAND ----------

# DBTITLE 1,Continuity, link checks, rows per location and export
CATALOG = "energy_commerce_retail_media"
GOLD = "energy_gold"
OBSERVATION = spark.table(f"{CATALOG}.{GOLD}.weather_observation")
DAILY = spark.table(f"{CATALOG}.{GOLD}.weather_daily")
CONTINUITY = series_continuity(
    OBSERVATION, "weather_observation", "observation_timestamp_utc"
)
for source in [
    r[0] for r in DAILY.select("source_system").distinct().collect() if r[0]
]:
    CONTINUITY[f"weather_daily / {source} / 86400 s"] = continuity_grid(
        DAILY.where(F.col("source_system") == source),
        "local_date",
        86400,
        entity_col="location_key",
    )
print(continuity_block(CONTINUITY))
PLACE = ("shared_conformed", "dim_place", "location_key")
BLOCKS = link_bias_blocks(
    CATALOG,
    [
        (GOLD, table, "location_key", *PLACE)
        for table in ("weather_observation", "weather_daily")
    ],
    [
        (f"{t} per location", GOLD, t, "location_key")
        for t in ("weather_observation", "weather_daily")
    ],
    "Links to the place table",
    "Rows per location",
)
write_profiling(
    "weather",
    "02_continuity_links_and_bias",
    "Weather continuity, links and coverage bias",
    [("Continuity against the expected grid", continuity_block(CONTINUITY)), *BLOCKS],
)
