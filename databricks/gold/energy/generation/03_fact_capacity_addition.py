# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # GOLD -- FACT CAPACITY ADDITION
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Grain:** one row per (`energy_carrier`, `_src_id_ord`) -- the wide
# MAGIC 2026-2029 retiring-capacity forecast, one row per carrier.
# MAGIC
# MAGIC **Sources:** `power_plant_capacity_additions` (Silver, energy_silver).
# MAGIC
# MAGIC **Serves use case:** any Energy use case needing the BNetzA forward
# MAGIC retiring-capacity forecast by carrier.
# MAGIC
# MAGIC **Purpose:** promote `power_plant_capacity_additions` to Gold. Near
# MAGIC pass-through -- no dimension to resolve at this grain, `energy_carrier`
# MAGIC stays a free-text attribute (BNetzA's own carrier grouping, not a
# MAGIC generation-unit-level link).

# COMMAND ----------

# DBTITLE 1,Shared Silver library
# MAGIC %run ../../../silver/_silver_common

# COMMAND ----------

# DBTITLE 1,Gold shared library
# MAGIC %run ../../_gold_common

# COMMAND ----------

# DBTITLE 1,Inspection library
# MAGIC %run ../../_gold_inspect

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "power_plant_list"
COMPONENT = "gold/energy/generation/03_fact_capacity_addition"
RID = gold_run_id()
GOLD_TABLE = "fact_capacity_addition"

# COMMAND ----------

# DBTITLE 1,Read Silver -- power_plant_capacity_additions
_additions_silver = read_silver("power_plant_capacity_additions")

# COMMAND ----------

# DBTITLE 1,Transform -- surrogate key + Gold provenance
fact = _additions_silver.withColumn(
    "capacity_addition_key", surrogate_key("energy_carrier", "_src_id_ord")
)
fact = add_gold_provenance(fact, SOURCE, RID)

# COMMAND ----------

# DBTITLE 1,Grain assertion -- one row per (energy_carrier, _src_id_ord)
assert_unique_grain(
    fact,
    ["energy_carrier", "_src_id_ord"],
    component=COMPONENT,
    source=SOURCE,
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Write Gold -- fact_capacity_addition
write_gold(fact, GOLD_TABLE, source=SOURCE, component=COMPONENT, rid=RID)

# COMMAND ----------

# DBTITLE 1,Inspect fact_capacity_addition + export findings
_findings_blocks = inspect_gold_table(
    fact,
    GOLD_TABLE,
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["energy_carrier", "_src_id_ord"],
    df_before=_additions_silver,
)
write_gold_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__{GOLD_TABLE}",
    GOLD_TABLE,
    _findings_blocks,
)
