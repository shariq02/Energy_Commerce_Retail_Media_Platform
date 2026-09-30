# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # GOLD -- POWER_PLANT
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** `energy_gold.power_plant` -- every Silver
# MAGIC `power_plant_register` column carried through, plus `carrier_key`
# MAGIC (resolved via `ref_energy_carrier_map`) and `unit_resolved` (whether the
# MAGIC register's own `mastr_unit_id` matches a real `generation_unit`). Grain:
# MAGIC plant record.

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
SOURCE = "power_plant_list"
COMPONENT = "gold/energy/generation/power_plant"
TABLE = "power_plant"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = gold_run_id()

# COMMAND ----------

# DBTITLE 1,Read Silver
power_plant_register = read_silver("power_plant_register")
carrier_map = read_gold(
    "ref_energy_carrier_map", source=SOURCE, schema=SHARED_CONFORMED_SCHEMA
)
unit_ids = read_silver("generation_unit").select("unit_id").distinct()

# COMMAND ----------

# DBTITLE 1,Resolve carrier_key
_carrier = carrier_map.filter(
    F.col("source_vocabulary") == "power_plant_register.energy_carrier"
).select(F.col("source_value").alias("energy_carrier"), "carrier_key")
power_plant_register = power_plant_register.join(_carrier, "energy_carrier", "left")

# COMMAND ----------

# DBTITLE 1,Derive unit_resolved
power_plant_register = power_plant_register.join(
    unit_ids.withColumnRenamed("unit_id", "mastr_unit_id").withColumn(
        "unit_resolved", F.lit(True)
    ),
    "mastr_unit_id",
    "left",
).fillna({"unit_resolved": False})

# COMMAND ----------

# DBTITLE 1,Build the Gold row
power_plant = power_plant_register.drop("_silver_loaded_at", "_silver_run_id")
power_plant = add_gold_provenance(power_plant, SOURCE, rid)

# COMMAND ----------

# DBTITLE 1,Grain gate
assert_unique_grain(
    power_plant, ["source_record_id"], component=COMPONENT, source=SOURCE, rid=rid
)

# COMMAND ----------

# DBTITLE 1,Write
write_gold(power_plant, TABLE, source=SOURCE, component=COMPONENT, rid=rid)

# COMMAND ----------

# DBTITLE 1,Inspect + export findings
_blocks = inspect_gold_table(
    power_plant,
    TABLE,
    source=SOURCE,
    component=COMPONENT,
    rid=rid,
    key_cols=["source_record_id"],
)
write_gold_findings(SOURCE, f"generation__{TABLE}", TABLE, _blocks)
