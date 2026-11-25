# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # EDA -- POWER SYSTEM (GOLD)
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** exploratory analysis of the power system data on the Gold tables, run
# MAGIC after Gold and before ML. Each section profiles one group of tables and writes
# MAGIC its findings to `src/schemas/profiling/<source>.md` (one section key per group).

# COMMAND ----------

# MAGIC %run ./_eda_common

# COMMAND ----------

# MAGIC %md
# MAGIC ## EDA -- GENERATION UNIT, PLANT AND UNIT EVENT TABLES
# MAGIC
# MAGIC **Purpose:** Profile the register tables of energy Gold (`generation_unit`,
# MAGIC `power_plant`, `storage_plant`, `unit_event`, `capacity_plan`): nulls,
# MAGIC constant columns, key, rates per column kind, coverage by source, the fill of
# MAGIC the link columns and the date-like columns -- as evidence.

# COMMAND ----------

# DBTITLE 1,Configuration
CATALOG = "energy_commerce_retail_media"
SCHEMA = "energy_gold"
SOURCE = "power_system"
NB_KEY = "01_units_and_plants"
SECTION_TITLE = "Generation unit, plant and unit event tables"
TABLES = [
    "generation_unit",
    "power_plant",
    "storage_plant",
    "unit_event",
    "capacity_plan",
]

# COMMAND ----------

# DBTITLE 1,Read tables
FRAMES = read_tables(CATALOG, SCHEMA, TABLES)

# COMMAND ----------

# DBTITLE 1,Standard EDA -- profile, keys, rates, coverage, links, dates and export
standard_group_eda(
    SOURCE, NB_KEY, SECTION_TITLE, FRAMES, TABLES, "power_system_01_rows.png"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## EDA -- GRID AND MARKET ACTOR TABLES
# MAGIC
# MAGIC **Purpose:** Profile the grid and actor tables of energy Gold (`grid_location`,
# MAGIC `grid_connection_point`, `grid_network`, `balancing_area`, `market_actor`,
# MAGIC `market_actor_role`): nulls, constant columns, key, rates per column kind,
# MAGIC coverage by source, the fill of the link columns and the date-like columns --
# MAGIC as evidence.

# COMMAND ----------

# DBTITLE 1,Configuration
CATALOG = "energy_commerce_retail_media"
SCHEMA = "energy_gold"
SOURCE = "power_system"
NB_KEY = "02_grid_and_actors"
SECTION_TITLE = "Grid and market actor tables"
TABLES = [
    "grid_location",
    "grid_connection_point",
    "grid_network",
    "balancing_area",
    "market_actor",
    "market_actor_role",
]

# COMMAND ----------

# DBTITLE 1,Read tables
FRAMES = read_tables(CATALOG, SCHEMA, TABLES)

# COMMAND ----------

# DBTITLE 1,Standard EDA -- profile, keys, rates, coverage, links, dates and export
standard_group_eda(
    SOURCE, NB_KEY, SECTION_TITLE, FRAMES, TABLES, "power_system_02_rows.png"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## EDA -- REGISTER LINK TABLE
# MAGIC
# MAGIC **Purpose:** Profile `entity_link` (the links between register records): nulls,
# MAGIC constant columns, key, rates per column kind, coverage by source, the fill of
# MAGIC the link columns and the date-like columns -- as evidence.

# COMMAND ----------

# DBTITLE 1,Configuration
CATALOG = "energy_commerce_retail_media"
SCHEMA = "energy_gold"
SOURCE = "power_system"
NB_KEY = "03_entity_links"
SECTION_TITLE = "Register link table"
TABLES = ["entity_link"]

# COMMAND ----------

# DBTITLE 1,Read tables
FRAMES = read_tables(CATALOG, SCHEMA, TABLES)

# COMMAND ----------

# DBTITLE 1,Standard EDA -- profile, keys, rates, coverage, links, dates and export
standard_group_eda(
    SOURCE, NB_KEY, SECTION_TITLE, FRAMES, TABLES, "power_system_03_rows.png"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## EDA -- REGISTER CHECKS, LINKS AND COVERAGE BIAS
# MAGIC
# MAGIC **Purpose:** Check the coordinates and the capacity order of the unit and plant
# MAGIC tables, check that the keys between the register tables resolve, and measure how
# MAGIC the units spread over the municipalities.

# COMMAND ----------

# DBTITLE 1,Configuration
CATALOG = "energy_commerce_retail_media"
SCHEMA = "energy_gold"
SOURCE = "power_system"
NB_KEY = "04_register_checks"
SECTION_TITLE = "Register checks, links and coverage bias"
LINKS = [
    (
        "energy_gold",
        "generation_unit",
        "location_id",
        "energy_gold",
        "grid_location",
        "location_id",
    ),
    (
        "energy_gold",
        "generation_unit",
        "operator_id",
        "energy_gold",
        "market_actor",
        "market_actor_id",
    ),
    (
        "energy_gold",
        "grid_connection_point",
        "location_id",
        "energy_gold",
        "grid_location",
        "location_id",
    ),
    (
        "energy_gold",
        "grid_connection_point",
        "grid_id",
        "energy_gold",
        "grid_network",
        "grid_id",
    ),
    (
        "energy_gold",
        "unit_event",
        "unit_id",
        "energy_gold",
        "generation_unit",
        "unit_id",
    ),
]

# COMMAND ----------

# DBTITLE 1,Coordinates and capacity order
UNITS = spark.table(f"{CATALOG}.{SCHEMA}.generation_unit")
PLANTS = spark.table(f"{CATALOG}.{SCHEMA}.power_plant")
SPATIAL = {"generation_unit": spatial_validity(UNITS, "latitude", "longitude")}
print(spatial_block(SPATIAL))
ORDER = [
    {
        **numeric_order_check(UNITS, "capacity_net_kw", "capacity_gross_kw"),
        "table": "generation_unit",
    },
    {
        **numeric_order_check(PLANTS, "capacity_net_mw", "capacity_gross_mw"),
        "table": "power_plant",
    },
]
ORDER = [{**r, "label": f"{r['table']}: {r['label']}"} for r in ORDER]
print(order_block(ORDER))

# COMMAND ----------

# DBTITLE 1,Link checks and units per municipality
LINK_RESULTS = link_checks(CATALOG, LINKS)
print(links_block(LINK_RESULTS))
BIAS = {
    "generation_unit per municipality": coverage_bias(
        rows_per_key(UNITS, "source_municipality_key")
    )
}
print(bias_block(BIAS))

# COMMAND ----------

# DBTITLE 1,Export findings
write_profiling(
    SOURCE,
    NB_KEY,
    SECTION_TITLE,
    [
        ("Coordinates", spatial_block(SPATIAL)),
        ("Capacity order", order_block(ORDER)),
        ("Links between register tables", links_block(LINK_RESULTS)),
        ("Units per municipality", bias_block(BIAS)),
    ],
)
