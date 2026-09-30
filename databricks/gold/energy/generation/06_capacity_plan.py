# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # GOLD -- CAPACITY_PLAN
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** `energy_gold.capacity_plan` -- every Silver
# MAGIC `power_plant_capacity_plan` column carried through, plus `carrier_key`
# MAGIC (resolved via `ref_energy_carrier_map`). Grain: capacity section x
# MAGIC carrier.

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
COMPONENT = "gold/energy/generation/capacity_plan"
TABLE = "capacity_plan"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = gold_run_id()

# COMMAND ----------

# DBTITLE 1,Read Silver
power_plant_capacity_plan = read_silver("power_plant_capacity_plan")
carrier_map = read_gold(
    "ref_energy_carrier_map", source=SOURCE, schema=SHARED_CONFORMED_SCHEMA
)

# COMMAND ----------

# DBTITLE 1,Resolve carrier_key
_carrier = carrier_map.filter(
    F.col("source_vocabulary") == "power_plant_capacity_plan.energy_carrier"
).select(F.col("source_value").alias("energy_carrier"), "carrier_key")
power_plant_capacity_plan = power_plant_capacity_plan.join(
    _carrier, "energy_carrier", "left"
)

# COMMAND ----------

# DBTITLE 1,Build the Gold row
capacity_plan = power_plant_capacity_plan.drop("_silver_loaded_at", "_silver_run_id")
capacity_plan = add_gold_provenance(capacity_plan, SOURCE, rid)

# COMMAND ----------

# DBTITLE 1,Grain gate
assert_unique_grain(
    capacity_plan, ["source_record_id"], component=COMPONENT, source=SOURCE, rid=rid
)

# COMMAND ----------

# DBTITLE 1,Write
write_gold(capacity_plan, TABLE, source=SOURCE, component=COMPONENT, rid=rid)

# COMMAND ----------

# DBTITLE 1,Inspect + export findings
_blocks = inspect_gold_table(
    capacity_plan,
    TABLE,
    source=SOURCE,
    component=COMPONENT,
    rid=rid,
    key_cols=["source_record_id"],
)
write_gold_findings(SOURCE, f"generation__{TABLE}", TABLE, _blocks)
