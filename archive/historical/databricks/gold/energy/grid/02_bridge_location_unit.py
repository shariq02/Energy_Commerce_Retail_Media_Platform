# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # GOLD -- LOCATION LINK BRIDGES
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Grain:** one row per (`parent_id`, `linked_id`) per bridge.
# MAGIC
# MAGIC **Sources:** `mastr_location_unit_bridge`, `mastr_location_connection_bridge`
# MAGIC (Silver, energy_silver_reference); `dim_location`, `dim_generation_unit`,
# MAGIC `dim_grid_connection_point` (Gold, this run).
# MAGIC
# MAGIC **Serves use case:** any Energy use case needing per-location unit or
# MAGIC connection-point counts -- aggregate through these bridges before joining
# MAGIC to a fact, never join raw and let it fan out.
# MAGIC
# MAGIC **Purpose:** re-key the two additive Silver location-link bridges against
# MAGIC `dim_location` (`parent_id`) and, respectively, `dim_generation_unit` /
# MAGIC `dim_grid_connection_point` (`linked_id`).

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
SOURCE = "mastr"
COMPONENT = "gold/energy/grid/02_bridge_location_unit"
RID = gold_run_id()

# COMMAND ----------

# DBTITLE 1,Read Silver -- mastr_location_unit_bridge
_location_unit_bridge_silver = read_silver("mastr_location_unit_bridge")

# COMMAND ----------

# DBTITLE 1,Read Silver -- mastr_location_connection_bridge
_location_connection_bridge_silver = read_silver("mastr_location_connection_bridge")

# COMMAND ----------

# DBTITLE 1,Read Gold -- dim_location
_location = read_gold("dim_location", source="mastr")

# COMMAND ----------

# DBTITLE 1,Read Gold -- dim_generation_unit
_gen_unit = read_gold("dim_generation_unit", source="mastr")

# COMMAND ----------

# DBTITLE 1,Read Gold -- dim_grid_connection_point
_connection_point = read_gold("dim_grid_connection_point", source="mastr")

# COMMAND ----------

# DBTITLE 1,Transform -- bridge_location_unit
bridge_location_unit = resolve_fk(
    _location_unit_bridge_silver,
    _location,
    fact_key_cols=["parent_id"],
    dim_key_cols=["location_id"],
    dim_surrogate_col="location_key",
    output_col="location_key",
)
bridge_location_unit = resolve_fk(
    bridge_location_unit,
    _gen_unit,
    fact_key_cols=["linked_id"],
    dim_key_cols=["unit_id"],
    dim_surrogate_col="generation_unit_key",
    output_col="generation_unit_key",
)
bridge_location_unit = bridge_location_unit.withColumn(
    "bridge_location_unit_key", surrogate_key("parent_id", "linked_id")
)
bridge_location_unit = add_gold_provenance(bridge_location_unit, SOURCE, RID)

# COMMAND ----------

# DBTITLE 1,Grain assertion -- bridge_location_unit, one row per (parent_id, linked_id)
assert_unique_grain(
    bridge_location_unit,
    ["parent_id", "linked_id"],
    component=COMPONENT,
    source=SOURCE,
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Write Gold -- bridge_location_unit
write_gold(
    bridge_location_unit,
    "bridge_location_unit",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Inspect bridge_location_unit + export findings
_findings_blocks = inspect_gold_table(
    bridge_location_unit,
    "bridge_location_unit",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["parent_id", "linked_id"],
    df_before=_location_unit_bridge_silver,
)
write_gold_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__bridge_location_unit",
    "bridge_location_unit",
    _findings_blocks,
)

# COMMAND ----------

# DBTITLE 1,Transform -- bridge_location_connection
bridge_location_connection = resolve_fk(
    _location_connection_bridge_silver,
    _location,
    fact_key_cols=["parent_id"],
    dim_key_cols=["location_id"],
    dim_surrogate_col="location_key",
    output_col="location_key",
)
bridge_location_connection = resolve_fk(
    bridge_location_connection,
    _connection_point,
    fact_key_cols=["linked_id"],
    dim_key_cols=["connection_point_id"],
    dim_surrogate_col="grid_connection_point_key",
    output_col="grid_connection_point_key",
)
bridge_location_connection = bridge_location_connection.withColumn(
    "bridge_location_connection_key", surrogate_key("parent_id", "linked_id")
)
bridge_location_connection = add_gold_provenance(
    bridge_location_connection, SOURCE, RID
)

# COMMAND ----------

# DBTITLE 1,Grain assertion -- bridge_location_connection, one row per (parent_id, linked_id)
assert_unique_grain(
    bridge_location_connection,
    ["parent_id", "linked_id"],
    component=COMPONENT,
    source=SOURCE,
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Write Gold -- bridge_location_connection
write_gold(
    bridge_location_connection,
    "bridge_location_connection",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Inspect bridge_location_connection + export findings
_findings_blocks = inspect_gold_table(
    bridge_location_connection,
    "bridge_location_connection",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["parent_id", "linked_id"],
    df_before=_location_connection_bridge_silver,
)
write_gold_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__bridge_location_connection",
    "bridge_location_connection",
    _findings_blocks,
)
