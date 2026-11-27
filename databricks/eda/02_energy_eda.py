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
# MAGIC ## EDA -- ENERGY TABLE GROUPS
# MAGIC
# MAGIC **Purpose:** Profile the energy Gold tables in three groups: the series tables
# MAGIC (`channel_reading`, `energy_component`, `market_price`, `forecast_actual`), the
# MAGIC device, plant sensor and grid intervention tables, and the shared series
# MAGIC coverage and reference tables: nulls, constant columns, key, rates per column
# MAGIC kind, coverage by source, the fill of the link columns and the date-like
# MAGIC columns -- as evidence.

# COMMAND ----------

# DBTITLE 1,Standard EDA of the table groups
run_groups(
    "energy_commerce_retail_media",
    "energy",
    [
        {
            "schema": "energy_gold",
            "nb_key": "01_series",
            "title": "Energy series tables",
            "tables": [
                "channel_reading",
                "energy_component",
                "market_price",
                "market_zone_price_daily",
                "forecast_actual",
            ],
        },
        {
            "schema": "energy_gold",
            "nb_key": "02_devices_and_grid_events",
            "title": "Device, plant sensor and grid intervention tables",
            "tables": [
                "iot_device",
                "plant_sensor_observation",
                "grid_intervention_event",
            ],
        },
        {
            "schema": "shared_conformed",
            "nb_key": "03_shared_series_coverage",
            "title": "Series coverage and reference tables",
            "tables": [
                "series_coverage",
                "ref_measure",
                "ref_energy_carrier",
                "ref_energy_carrier_map",
                "dim_place",
                "metric_definition",
            ],
        },
    ],
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## EDA -- SERIES CONTINUITY AND COMMON PERIOD
# MAGIC
# MAGIC **Purpose:** Measure each Gold series against an expected time grid (per source
# MAGIC and step, per location), and measure the period that the series tables share --
# MAGIC the limit of any study that joins them on time.

# COMMAND ----------

# DBTITLE 1,Continuity, common period and export
CATALOG = "energy_commerce_retail_media"
SCHEMA = "energy_gold"
SERIES = ["channel_reading", "energy_component", "market_price", "forecast_actual"]
TS_COL = "observation_timestamp_utc"
CONTINUITY = {}
for table in SERIES:
    frame = spark.table(f"{CATALOG}.{SCHEMA}.{table}")
    CONTINUITY.update(series_continuity(frame, table, TS_COL))
# the zone prices are daily rows of a market area, with no timestamp column
CONTINUITY["market_zone_price_daily / 86400 s"] = continuity_grid(
    spark.table(f"{CATALOG}.{SCHEMA}.market_zone_price_daily"),
    "local_date",
    86400,
    entity_col="market_area_code",
)
print(continuity_block(CONTINUITY))
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
write_profiling(
    "energy",
    "04_series_continuity",
    "Series continuity and common period",
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

# DBTITLE 1,Link checks, rows per location and export
PLACE = ("shared_conformed", "dim_place", "location_key")
LINKS = [("energy_gold", table, "location_key", *PLACE) for table in SERIES]
BLOCKS = link_bias_blocks(
    CATALOG,
    LINKS,
    [(f"{t} per location", "energy_gold", t, "location_key") for t in SERIES],
    "Links to the place table",
    "Rows per location",
)
write_profiling("energy", "05_links_and_bias", "Series links and coverage bias", BLOCKS)
