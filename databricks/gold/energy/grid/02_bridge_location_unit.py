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

# DBTITLE 1,Imports
from pyspark.sql import functions as F

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
_location_keys = read_gold("dim_location", source="mastr").select(
    F.col("location_id").alias("_loc_id"), F.col("location_key").alias("_loc_key")
)

# COMMAND ----------

# DBTITLE 1,Read Gold -- dim_generation_unit
_gen_unit_keys = read_gold("dim_generation_unit", source="mastr").select(
    F.col("unit_id").alias("_gu_unit_id"),
    F.col("generation_unit_key").alias("_gu_key"),
)

# COMMAND ----------

# DBTITLE 1,Read Gold -- dim_grid_connection_point
_connection_point_keys = read_gold("dim_grid_connection_point", source="mastr").select(
    F.col("connection_point_id").alias("_cp_id"),
    F.col("grid_connection_point_key").alias("_cp_key"),
)

# COMMAND ----------

# DBTITLE 1,Transform -- bridge_location_unit
bridge_location_unit = (
    _location_unit_bridge_silver.join(
        _location_keys,
        _location_unit_bridge_silver["parent_id"] == F.col("_loc_id"),
        "left",
    )
    .withColumn("location_key", F.col("_loc_key"))
    .drop("_loc_id", "_loc_key")
    .join(_gen_unit_keys, F.col("linked_id") == F.col("_gu_unit_id"), "left")
    .withColumn("generation_unit_key", F.col("_gu_key"))
    .drop("_gu_unit_id", "_gu_key")
)
bridge_location_unit = bridge_location_unit.withColumn(
    "bridge_location_unit_key", surrogate_key("parent_id", "linked_id")
)
bridge_location_unit = add_gold_provenance(bridge_location_unit, SOURCE, RID)

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
bridge_location_connection = (
    _location_connection_bridge_silver.join(
        _location_keys,
        _location_connection_bridge_silver["parent_id"] == F.col("_loc_id"),
        "left",
    )
    .withColumn("location_key", F.col("_loc_key"))
    .drop("_loc_id", "_loc_key")
    .join(_connection_point_keys, F.col("linked_id") == F.col("_cp_id"), "left")
    .withColumn("grid_connection_point_key", F.col("_cp_key"))
    .drop("_cp_id", "_cp_key")
)
bridge_location_connection = bridge_location_connection.withColumn(
    "bridge_location_connection_key", surrogate_key("parent_id", "linked_id")
)
bridge_location_connection = add_gold_provenance(
    bridge_location_connection, SOURCE, RID
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
