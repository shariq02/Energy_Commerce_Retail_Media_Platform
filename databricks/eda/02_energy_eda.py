# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # EDA -- ENERGY (GOLD)
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** exploratory analysis of the energy data on the Gold tables, run
# MAGIC after Gold and before ML. Each section profiles one group of tables and writes
# MAGIC its findings to `src/schemas/profiling/<source>.md` (one section key per group).

# COMMAND ----------

# MAGIC %run ./_eda_common

# COMMAND ----------

# MAGIC %md
# MAGIC ## EDA -- ENERGY SERIES TABLES
# MAGIC
# MAGIC **Purpose:** Profile the series tables of energy Gold (`channel_reading`,
# MAGIC `energy_component`, `market_price`, `forecast_actual`): nulls, constant
# MAGIC columns, key, rates per column kind, coverage by source, the fill of the link
# MAGIC columns and the date-like columns -- as evidence.

# COMMAND ----------

# DBTITLE 1,Configuration
CATALOG = "energy_commerce_retail_media"
SCHEMA = "energy_gold"
SOURCE = "energy"
NB_KEY = "01_series"
SECTION_TITLE = "Energy series tables"
TABLES = ["channel_reading", "energy_component", "market_price", "forecast_actual"]

# COMMAND ----------

# DBTITLE 1,Read tables
FRAMES = read_tables(CATALOG, SCHEMA, TABLES)

# COMMAND ----------

# DBTITLE 1,Standard EDA -- profile, keys, rates, coverage, links, dates and export
standard_group_eda(SOURCE, NB_KEY, SECTION_TITLE, FRAMES, TABLES, "energy_01_rows.png")

# COMMAND ----------

# MAGIC %md
# MAGIC ## EDA -- DEVICE, PLANT SENSOR AND GRID INTERVENTION TABLES
# MAGIC
# MAGIC **Purpose:** Profile `iot_device`, `plant_sensor_observation` and
# MAGIC `grid_intervention_event`: nulls, constant columns, key, rates per column
# MAGIC kind, coverage by source, the fill of the link columns and the date-like
# MAGIC columns -- as evidence.

# COMMAND ----------

# DBTITLE 1,Configuration
CATALOG = "energy_commerce_retail_media"
SCHEMA = "energy_gold"
SOURCE = "energy"
NB_KEY = "02_devices_and_grid_events"
SECTION_TITLE = "Device, plant sensor and grid intervention tables"
TABLES = ["iot_device", "plant_sensor_observation", "grid_intervention_event"]

# COMMAND ----------

# DBTITLE 1,Read tables
FRAMES = read_tables(CATALOG, SCHEMA, TABLES)

# COMMAND ----------

# DBTITLE 1,Standard EDA -- profile, keys, rates, coverage, links, dates and export
standard_group_eda(SOURCE, NB_KEY, SECTION_TITLE, FRAMES, TABLES, "energy_02_rows.png")

# COMMAND ----------

# MAGIC %md
# MAGIC ## EDA -- SERIES COVERAGE AND REFERENCE TABLES
# MAGIC
# MAGIC **Purpose:** Profile the shared tables that describe the energy series
# MAGIC (`series_coverage`, `ref_measure`, `ref_energy_carrier`,
# MAGIC `ref_energy_carrier_map`, `dim_place`, `metric_definition`): nulls, constant
# MAGIC columns, key, rates per column kind and coverage by source -- as evidence.

# COMMAND ----------

# DBTITLE 1,Configuration
CATALOG = "energy_commerce_retail_media"
SCHEMA = "shared_conformed"
SOURCE = "energy"
NB_KEY = "03_shared_series_coverage"
SECTION_TITLE = "Series coverage and reference tables"
TABLES = [
    "series_coverage",
    "ref_measure",
    "ref_energy_carrier",
    "ref_energy_carrier_map",
    "dim_place",
    "metric_definition",
]

# COMMAND ----------

# DBTITLE 1,Read tables
FRAMES = read_tables(CATALOG, SCHEMA, TABLES)

# COMMAND ----------

# DBTITLE 1,Standard EDA -- profile, keys, rates, coverage, links, dates and export
standard_group_eda(SOURCE, NB_KEY, SECTION_TITLE, FRAMES, TABLES, "energy_03_rows.png")

# COMMAND ----------

# MAGIC %md
# MAGIC ## EDA -- SERIES CONTINUITY AND COMMON PERIOD
# MAGIC
# MAGIC **Purpose:** Measure each Gold series against an expected time grid (per source
# MAGIC and step, per location), and measure the period that the series tables share --
# MAGIC the limit of any study that joins them on time.

# COMMAND ----------

# DBTITLE 1,Configuration
CATALOG = "energy_commerce_retail_media"
SCHEMA = "energy_gold"
SOURCE = "energy"
NB_KEY = "04_series_continuity"
SECTION_TITLE = "Series continuity and common period"
SERIES = ["channel_reading", "energy_component", "market_price", "forecast_actual"]
TS_COL = "observation_timestamp_utc"

# COMMAND ----------

# DBTITLE 1,Continuity against the expected grid
CONTINUITY = {}
for table in SERIES:
    frame = spark.table(f"{CATALOG}.{SCHEMA}.{table}")
    CONTINUITY.update(series_continuity(frame, table, TS_COL))
print(continuity_block(CONTINUITY))

# COMMAND ----------

# DBTITLE 1,Common period of the series tables and weather
SPANS = {}
for table in [*SERIES, "weather_observation"]:
    row = (
        spark.table(f"{CATALOG}.{SCHEMA}.{table}")
        .agg(F.min(TS_COL), F.max(TS_COL))
        .first()
    )
    SPANS[table] = (str(row[0]), str(row[1]))
OVERLAP = {"series and weather": cross_source_overlap(SPANS)}
print(overlap_block(OVERLAP))

# COMMAND ----------

# DBTITLE 1,Export findings
write_profiling(
    SOURCE,
    NB_KEY,
    SECTION_TITLE,
    [
        ("Continuity against the expected grid", continuity_block(CONTINUITY)),
        ("Common period of the series tables and weather", overlap_block(OVERLAP)),
    ],
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## EDA -- SERIES LINKS AND COVERAGE BIAS
# MAGIC
# MAGIC **Purpose:** Check that the location key of each series table resolves to a place
# MAGIC row, and measure how the rows spread over the locations.

# COMMAND ----------

# DBTITLE 1,Configuration
CATALOG = "energy_commerce_retail_media"
SOURCE = "energy"
NB_KEY = "05_links_and_bias"
SECTION_TITLE = "Series links and coverage bias"
LINKS = [
    (
        "energy_gold",
        "channel_reading",
        "location_key",
        "shared_conformed",
        "dim_place",
        "location_key",
    ),
    (
        "energy_gold",
        "energy_component",
        "location_key",
        "shared_conformed",
        "dim_place",
        "location_key",
    ),
    (
        "energy_gold",
        "market_price",
        "location_key",
        "shared_conformed",
        "dim_place",
        "location_key",
    ),
    (
        "energy_gold",
        "forecast_actual",
        "location_key",
        "shared_conformed",
        "dim_place",
        "location_key",
    ),
]

# COMMAND ----------

# DBTITLE 1,Link checks and rows per location
LINK_RESULTS = link_checks(CATALOG, LINKS)
print(links_block(LINK_RESULTS))
BIAS = {
    f"{c_table} per location": coverage_bias(
        rows_per_key(spark.table(f"{CATALOG}.{c_schema}.{c_table}"), "location_key")
    )
    for c_schema, c_table, *_ in LINKS
}
print(bias_block(BIAS))

# COMMAND ----------

# DBTITLE 1,Export findings
write_profiling(
    SOURCE,
    NB_KEY,
    SECTION_TITLE,
    [
        ("Links to the place table", links_block(LINK_RESULTS)),
        ("Rows per location", bias_block(BIAS)),
    ],
)
