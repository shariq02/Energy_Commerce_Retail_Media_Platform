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
# MAGIC **Purpose:** the 11 register tables of `energy_gold`: units, unit events,
# MAGIC support registrations, plants, the capacity plan (year columns as rows), market
# MAGIC actors and roles, grid locations, networks, connection points (with their
# MAGIC parent attributes) and balancing areas.

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
COPIES = {
    "generation_unit": ("generation_unit", ["unit_id"]),
    "support_registration": ("support_registration", RECORD),
    "storage_plant": ("storage_plant", RECORD),
    "power_plant_register": (
        "power_plant",
        ["mastr_unit_id", "plant_name", "commissioning_year", "_source_id_ordinal"],
    ),
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
        "unit_authorisation",
        "unit_repowering",
        "register_change_event",
        "power_plant_capacity_plan",
        "market_actor_role",
        "grid_connection_point",
    )
}

# COMMAND ----------

# DBTITLE 1,Transform -- the tables copied from Silver
for name, (table, grain) in COPIES.items():
    TABLES[table] = (add_provenance(silver[name], SOURCE, RID), grain)

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

# DBTITLE 1,Grain gate -- every table
gate_tables(TABLES, **GATE)

# COMMAND ----------

# DBTITLE 1,Write -- every table
write_tables(TABLES, **COMMON)

# COMMAND ----------

# DBTITLE 1,Inspect and export -- every table
inspect_tables(TABLES, findings="energy", **COMMON)

# COMMAND ----------

# DBTITLE 1,Validation -- every table exists and has rows
validate_tables(TABLES, **COMMON)
