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
# MAGIC ## EDA -- REGISTER, GRID, ACTOR AND LINK TABLES
# MAGIC
# MAGIC **Purpose:** Profile the register tables of energy Gold in three groups: the
# MAGIC generation unit, plant and unit event tables (`generation_unit`, `power_plant`,
# MAGIC `storage_plant`, `unit_event`, `capacity_plan`), the grid and market actor
# MAGIC tables, and `entity_link` (the links between register records): nulls, constant
# MAGIC columns, key, rates per column kind, coverage by source, the fill of the link
# MAGIC columns and the date-like columns -- as evidence.

# COMMAND ----------

# DBTITLE 1,Standard EDA of the table groups
run_groups(
    "energy_commerce_retail_media",
    "power_system",
    [
        {
            "schema": "energy_gold",
            "nb_key": "01_units_and_plants",
            "title": "Generation unit, plant and unit event tables",
            "tables": [
                "generation_unit",
                "power_plant",
                "storage_plant",
                "unit_event",
                "capacity_plan",
            ],
        },
        {
            "schema": "energy_gold",
            "nb_key": "02_grid_and_actors",
            "title": "Grid and market actor tables",
            "tables": [
                "grid_location",
                "grid_connection_point",
                "grid_network",
                "balancing_area",
                "market_actor",
                "market_actor_role",
            ],
        },
        {
            "schema": "energy_gold",
            "nb_key": "03_entity_links",
            "title": "Register link table",
            "tables": ["entity_link"],
        },
    ],
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## EDA -- REGISTER CHECKS, LINKS AND COVERAGE BIAS
# MAGIC
# MAGIC **Purpose:** Check the coordinates and the capacity order of the unit and plant
# MAGIC tables, check that the keys between the register tables resolve, and measure how
# MAGIC the units spread over the municipalities.

# COMMAND ----------

# DBTITLE 1,Coordinates, capacity order, link checks and export
CATALOG = "energy_commerce_retail_media"
SCHEMA = "energy_gold"
GOLD = "energy_gold"
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
LINKS = [
    (GOLD, "generation_unit", "location_id", GOLD, "grid_location", "location_id"),
    (GOLD, "generation_unit", "operator_id", GOLD, "market_actor", "market_actor_id"),
    (
        GOLD,
        "grid_connection_point",
        "location_id",
        GOLD,
        "grid_location",
        "location_id",
    ),
    (GOLD, "grid_connection_point", "grid_id", GOLD, "grid_network", "grid_id"),
    (GOLD, "unit_event", "unit_id", GOLD, "generation_unit", "unit_id"),
]
BLOCKS = link_bias_blocks(
    CATALOG,
    LINKS,
    [
        (
            "generation_unit per municipality",
            GOLD,
            "generation_unit",
            "source_municipality_key",
        )
    ],
    "Links between register tables",
    "Units per municipality",
)
write_profiling(
    "power_system",
    "04_register_checks",
    "Register checks, links and coverage bias",
    [
        ("Coordinates", spatial_block(SPATIAL)),
        ("Capacity order", order_block(ORDER)),
        *BLOCKS,
    ],
)
