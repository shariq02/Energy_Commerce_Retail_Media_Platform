# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # GOLD ENERGY -- REGISTERS AND GRID
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** November 2026
# MAGIC
# MAGIC **Purpose:** the 11 register tables of `energy_gold`: units (with the columns of
# MAGIC their support registrations), unit events, plants (with their unit), the
# MAGIC capacity plan (year columns as rows), market actors and roles, grid locations,
# MAGIC networks, connection points (with their parent attributes) and balancing areas.

# COMMAND ----------

# DBTITLE 1,Gold library
# MAGIC %run ../_gold_common

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "mastr"
COMPONENT = "gold/energy/01_registers_and_grid"
RID = gold_run_id()
GATE = {"source": SOURCE, "component": COMPONENT, "rid": RID}
COMMON = {**GATE, "schema": gold_schema_for(SOURCE)}
RECORD = ["source_record_id"]
# Silver table -> Gold table and grain, copied as they are
UNIT_GRAIN = ["unit_type", "unit_id"]
PLANT_GRAIN = [
    "mastr_unit_id",
    "plant_name",
    "commissioning_year",
    "_source_id_ordinal",
]
# unit column -> prefix of the support registration columns joined to the unit
# (each support id is unique in `support_registration`, and no row has both ids)
SUPPORT_IDS = {
    "renewable_energy_act_support_id": "renewable_support_",
    "combined_heat_and_power_support_id": "chp_support_",
}
COPIES = {
    "storage_plant": ("storage_plant", RECORD),
    "market_actor": ("market_actor", RECORD),
    "grid_location": ("grid_location", RECORD),
    "grid_network": ("grid_network", RECORD),
    "balancing_area": ("balancing_area", RECORD),
}
TABLES = {}

# COMMAND ----------

# DBTITLE 1,Read Silver
silver = {
    name: from_silver(read_silver(name))
    for name in (
        *COPIES,
        "generation_unit",
        "support_registration",
        "power_plant_register",
        "unit_authorisation",
        "unit_repowering",
        "register_change_event",
        "power_plant_capacity_plan",
        "market_actor_role",
        "grid_connection_point",
        "unit_weather_station_link",
    )
}

# COMMAND ----------

# DBTITLE 1,Transform -- the tables copied from Silver
for name, (table, grain) in COPIES.items():
    TABLES[table] = (add_provenance(silver[name], SOURCE, RID), grain)

# COMMAND ----------

# DBTITLE 1,Transform -- generation_unit (with the support registrations of its units)
units = silver["generation_unit"]
for column, prefix in SUPPORT_IDS.items():
    units = with_parent(units, silver["support_registration"], on=column, prefix=prefix)
units = units.join(
    silver["unit_weather_station_link"].select(
        "unit_type",
        "unit_id",
        F.col("weather_station_key").alias("nearest_station_key"),
        F.col("distance_km").alias("nearest_station_distance_km"),
    ),
    ["unit_type", "unit_id"],
    "left",
)
TABLES["generation_unit"] = (add_provenance(units, SOURCE, RID), UNIT_GRAIN)

# COMMAND ----------

# DBTITLE 1,Transform -- power_plant (with the unit that its MaStR unit id names)
# A plant matches no unit or one unit; the 56 repeated unit ids match no plant.
plant_units = silver["generation_unit"].join(
    silver["power_plant_register"].select(F.col("mastr_unit_id").alias("unit_id")),
    "unit_id",
    "left_semi",
)
plant = with_parent(
    silver["power_plant_register"],
    plant_units,
    on="mastr_unit_id",
    parent_key="unit_id",
    prefix="unit_",
)
TABLES["power_plant"] = (add_provenance(plant, SOURCE, RID), PLANT_GRAIN)

# COMMAND ----------

# DBTITLE 1,Transform -- unit_event (register changes, authorisations, repowerings)
unit_event = (
    silver["register_change_event"]
    .unionByName(
        silver["unit_authorisation"].withColumn("event_kind", F.lit("authorisation")),
        allowMissingColumns=True,
    )
    .unionByName(
        silver["unit_repowering"].withColumn("event_kind", F.lit("repowering")),
        allowMissingColumns=True,
    )
)
TABLES["unit_event"] = (
    add_provenance(unit_event, SOURCE, RID),
    ["event_kind", "source_record_id"],
)

# COMMAND ----------

# DBTITLE 1,Transform -- capacity_plan (the year columns become rows)
plan = silver["power_plant_capacity_plan"]
years = [c for c in plan.columns if c.isdigit() and len(c) == 4]
TABLES["capacity_plan"] = (
    add_provenance(
        columns_to_rows(
            plan,
            [c for c in plan.columns if c not in years],
            [
                F.struct(
                    F.lit(int(year)).alias("year"),
                    F.col(f"`{year}`").alias("capacity_mw"),
                )
                for year in years
            ],
        ),
        SOURCE,
        RID,
    ),
    ["capacity_section", "energy_carrier", "year"],
)

# COMMAND ----------

# DBTITLE 1,Transform -- market_actor_role (actor attributes joined in)
TABLES["market_actor_role"] = (
    add_provenance(
        with_parent(
            silver["market_actor_role"],
            silver["market_actor"],
            on="market_actor_id",
            prefix="actor_",
        ),
        SOURCE,
        RID,
    ),
    RECORD,
)

# COMMAND ----------

# DBTITLE 1,Transform -- grid_connection_point (network, location, area, operator)
point = silver["grid_connection_point"]
for parent, on, parent_key, prefix in (
    ("grid_network", "grid_id", None, "network_"),
    ("grid_location", "location_id", None, "location_"),
    (
        "balancing_area",
        "balancing_area_connection_point_id",
        "balancing_area_id",
        "balancing_area_",
    ),
    ("market_actor", "grid_operator_id", "market_actor_id", "operator_"),
):
    point = with_parent(
        point, silver[parent], on=on, parent_key=parent_key, prefix=prefix
    )
TABLES["grid_connection_point"] = (add_provenance(point, SOURCE, RID), RECORD)

# COMMAND ----------

# DBTITLE 1,Write -- every table
write_tables(TABLES, **COMMON)

# COMMAND ----------

# DBTITLE 1,Grain gate -- every written table
gate_tables(TABLES, **COMMON)

# COMMAND ----------

# DBTITLE 1,Inspect and export -- every table
inspect_tables(TABLES, findings="energy", **COMMON)

# COMMAND ----------

# DBTITLE 1,Validation -- every table exists and has rows
validate_tables(TABLES, **COMMON)
