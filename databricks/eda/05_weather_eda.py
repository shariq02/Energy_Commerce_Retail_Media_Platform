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

# DBTITLE 1,Configuration
CATALOG = "energy_commerce_retail_media"
SCHEMA = "energy_gold"
SOURCE = "weather"
NB_KEY = "01_weather_tables"
SECTION_TITLE = "Weather observation and daily tables"
TABLES = ["weather_observation", "weather_daily"]

# COMMAND ----------

# DBTITLE 1,Read tables
FRAMES = read_tables(CATALOG, SCHEMA, TABLES)

# COMMAND ----------

# DBTITLE 1,Standard EDA -- profile, keys, rates, coverage, links, dates and export
standard_group_eda(SOURCE, NB_KEY, SECTION_TITLE, FRAMES, TABLES, "weather_01_rows.png")

# COMMAND ----------

# MAGIC %md
# MAGIC ## EDA -- WEATHER CONTINUITY, LINKS AND COVERAGE BIAS
# MAGIC
# MAGIC **Purpose:** Measure the weather series against an expected time grid (per source
# MAGIC and step, per location), check that the location key resolves to a place row, and
# MAGIC measure how the rows spread over the locations.

# COMMAND ----------

# DBTITLE 1,Configuration
CATALOG = "energy_commerce_retail_media"
SCHEMA = "energy_gold"
SOURCE = "weather"
NB_KEY = "02_continuity_links_and_bias"
SECTION_TITLE = "Weather continuity, links and coverage bias"
LINKS = [
    (
        "energy_gold",
        "weather_observation",
        "location_key",
        "shared_conformed",
        "dim_place",
        "location_key",
    ),
    (
        "energy_gold",
        "weather_daily",
        "location_key",
        "shared_conformed",
        "dim_place",
        "location_key",
    ),
]

# COMMAND ----------

# DBTITLE 1,Continuity against the expected grid
OBSERVATION = spark.table(f"{CATALOG}.{SCHEMA}.weather_observation")
CONTINUITY = series_continuity(
    OBSERVATION, "weather_observation", "observation_timestamp_utc"
)
DAILY = spark.table(f"{CATALOG}.{SCHEMA}.weather_daily")
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

# COMMAND ----------

# DBTITLE 1,Link checks and rows per location
LINK_RESULTS = link_checks(CATALOG, LINKS)
print(links_block(LINK_RESULTS))
BIAS = {
    "weather_observation per location": coverage_bias(
        rows_per_key(OBSERVATION, "location_key")
    ),
    "weather_daily per location": coverage_bias(rows_per_key(DAILY, "location_key")),
}
print(bias_block(BIAS))

# COMMAND ----------

# DBTITLE 1,Export findings
write_profiling(
    SOURCE,
    NB_KEY,
    SECTION_TITLE,
    [
        ("Continuity against the expected grid", continuity_block(CONTINUITY)),
        ("Links to the place table", links_block(LINK_RESULTS)),
        ("Rows per location", bias_block(BIAS)),
    ],
)
