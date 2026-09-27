# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # GOLD -- GENERATION_UNIT
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** `energy_gold.generation_unit` -- the asset entity, every
# MAGIC Silver `generation_unit` column carried through, plus `carrier_key`
# MAGIC (resolved via `ref_energy_carrier_map`), `lifecycle_state` (from the
# MAGIC unit's own dates) and `n_support_registrations`. Grain: unit id.

# COMMAND ----------

# DBTITLE 1,Shared Silver library
# MAGIC %run ../../../silver/_silver_common

# COMMAND ----------

# DBTITLE 1,Gold shared library
# MAGIC %run ../../_gold_common

# COMMAND ----------

# DBTITLE 1,Gold inspection library
# MAGIC %run ../../_gold_inspect

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "mastr"
COMPONENT = "gold/energy/generation/generation_unit"
TABLE = "generation_unit"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = gold_run_id()

# COMMAND ----------

# DBTITLE 1,Read Silver
generation_unit = read_silver("generation_unit")
carrier_map = read_gold(
    "ref_energy_carrier_map", source=SOURCE, schema=SHARED_CONFORMED_SCHEMA
)

# COMMAND ----------

# DBTITLE 1,Resolve carrier_key -- prefer main_fuel, then biomass_type, then unit_type
_fuel_map = carrier_map.filter(
    F.col("source_vocabulary") == "generation_unit.main_fuel"
).select(
    F.col("source_value").alias("main_fuel"), F.col("carrier_key").alias("_ck_fuel")
)
_biomass_map = carrier_map.filter(
    F.col("source_vocabulary") == "generation_unit.biomass_type"
).select(
    F.col("source_value").alias("biomass_type"),
    F.col("carrier_key").alias("_ck_biomass"),
)
_type_map = carrier_map.filter(
    F.col("source_vocabulary") == "generation_unit.unit_type"
).select(
    F.col("source_value").alias("unit_type"), F.col("carrier_key").alias("_ck_type")
)

generation_unit = (
    generation_unit.join(_fuel_map, "main_fuel", "left")
    .join(_biomass_map, "biomass_type", "left")
    .join(_type_map, "unit_type", "left")
    .withColumn("carrier_key", F.coalesce("_ck_fuel", "_ck_biomass", "_ck_type"))
    .drop("_ck_fuel", "_ck_biomass", "_ck_type")
)

# COMMAND ----------

# DBTITLE 1,Derive lifecycle_state from the unit's own dates
generation_unit = generation_unit.withColumn(
    "lifecycle_state",
    F.when(F.col("final_decommissioning_date").isNotNull(), F.lit("decommissioned"))
    .when(
        F.col("provisional_shutdown_start_date").isNotNull(),
        F.lit("provisionally_shutdown"),
    )
    .when(F.col("commissioning_date").isNotNull(), F.lit("operating"))
    .when(F.col("planned_commissioning_date").isNotNull(), F.lit("planned"))
    .otherwise(F.lit("unknown")),
)

# COMMAND ----------

# DBTITLE 1,Derive n_support_registrations -- populated support-id slots
generation_unit = generation_unit.withColumn(
    "n_support_registrations",
    F.col("renewable_energy_act_support_id").isNotNull().cast("int")
    + F.col("combined_heat_and_power_support_id").isNotNull().cast("int"),
)

# COMMAND ----------

# DBTITLE 1,Build the Gold row -- every Silver column, plus the derived ones above
generation_unit = generation_unit.drop("_silver_loaded_at", "_silver_run_id")
generation_unit = add_gold_provenance(generation_unit, SOURCE, rid)

# COMMAND ----------

# DBTITLE 1,Grain gate
assert_unique_grain(
    generation_unit, ["unit_id"], component=COMPONENT, source=SOURCE, rid=rid
)

# COMMAND ----------

# DBTITLE 1,Write
write_gold(generation_unit, TABLE, source=SOURCE, component=COMPONENT, rid=rid)

# COMMAND ----------

# DBTITLE 1,Inspect + export findings
_blocks = inspect_gold_table(
    generation_unit,
    TABLE,
    source=SOURCE,
    component=COMPONENT,
    rid=rid,
    key_cols=["unit_id"],
)
write_gold_findings(SOURCE, f"generation__{TABLE}", TABLE, _blocks)
