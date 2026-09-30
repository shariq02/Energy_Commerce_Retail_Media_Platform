# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # GOLD -- GRID_LOCATION
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** `energy_gold.grid_location` -- every Silver `grid_location`
# MAGIC column carried through, plus `n_units`/`n_connection_points` (counted
# MAGIC from the location's own delimited id lists) and
# MAGIC `has_coordinate_conflict` (from `grid_location_coordinate_conflict`).
# MAGIC Grain: location id.

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
COMPONENT = "gold/energy/grid/grid_location"
TABLE = "grid_location"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = gold_run_id()

# COMMAND ----------

# DBTITLE 1,Read Silver
grid_location = read_silver("grid_location")
conflicts = read_silver("grid_location_coordinate_conflict").select(
    F.col("location_id"), F.lit(True).alias("has_coordinate_conflict")
)

# COMMAND ----------


# DBTITLE 1,Derive n_units and n_connection_points -- delimited id-list length
def _list_len(col):
    return F.when(F.col(col).isNull() | (F.trim(F.col(col)) == ""), F.lit(0)).otherwise(
        F.size(F.split(F.col(col), r"[,;\s]+"))
    )


grid_location = grid_location.withColumn(
    "n_units", _list_len("linked_unit_ids")
).withColumn("n_connection_points", _list_len("connection_point_ids"))

# COMMAND ----------

# DBTITLE 1,Join has_coordinate_conflict
grid_location = grid_location.join(conflicts, "location_id", "left").fillna(
    {"has_coordinate_conflict": False}
)

# COMMAND ----------

# DBTITLE 1,Build the Gold row
grid_location = grid_location.drop("_silver_loaded_at", "_silver_run_id")
grid_location = add_gold_provenance(grid_location, SOURCE, rid)

# COMMAND ----------

# DBTITLE 1,Grain gate
assert_unique_grain(
    grid_location, ["location_id"], component=COMPONENT, source=SOURCE, rid=rid
)

# COMMAND ----------

# DBTITLE 1,Write
write_gold(grid_location, TABLE, source=SOURCE, component=COMPONENT, rid=rid)

# COMMAND ----------

# DBTITLE 1,Inspect + export findings
_blocks = inspect_gold_table(
    grid_location,
    TABLE,
    source=SOURCE,
    component=COMPONENT,
    rid=rid,
    key_cols=["location_id"],
)
write_gold_findings(SOURCE, f"grid__{TABLE}", TABLE, _blocks)
