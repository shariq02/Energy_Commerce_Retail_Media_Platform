# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # GOLD -- GRID TOPOLOGY (grid_connection_point, grid_network, balancing_area, grid_location_coordinate_conflict)
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** `grid_connection_point` and `balancing_area` are views (Silver
# MAGIC already carries their FKs resolved). `grid_network` is a table with
# MAGIC `n_connection_points` added. `grid_location_coordinate_conflict` is a
# MAGIC view, listed here beside its subject rather than under `dim_location`.

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
COMPONENT = "gold/energy/grid/grid_topology"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = gold_run_id()

# COMMAND ----------

# DBTITLE 1,grid_connection_point -- view
read_silver("grid_connection_point").createOrReplaceTempView("_gcp")
write_gold_view("SELECT * FROM _gcp", "grid_connection_point", source=SOURCE)

# COMMAND ----------

# DBTITLE 1,balancing_area -- view
read_silver("balancing_area").createOrReplaceTempView("_ba")
write_gold_view("SELECT * FROM _ba", "balancing_area", source=SOURCE)

# COMMAND ----------

# DBTITLE 1,grid_location_coordinate_conflict -- view
read_silver("grid_location_coordinate_conflict").createOrReplaceTempView("_glcc")
write_gold_view(
    "SELECT * FROM _glcc", "grid_location_coordinate_conflict", source=SOURCE
)

# COMMAND ----------

# DBTITLE 1,Build grid_network -- n_connection_points from grid_connection_point
grid_network = read_silver("grid_network")
_cp_counts = (
    read_silver("grid_connection_point")
    .groupBy("grid_id")
    .agg(F.count(F.lit(1)).alias("n_connection_points"))
)
grid_network = grid_network.join(_cp_counts, "grid_id", "left").fillna(
    {"n_connection_points": 0}
)
grid_network = grid_network.drop("_silver_loaded_at", "_silver_run_id")
grid_network = add_gold_provenance(grid_network, SOURCE, rid)

# COMMAND ----------

# DBTITLE 1,Grain gate -- grid_network
assert_unique_grain(
    grid_network, ["grid_id"], component=COMPONENT, source=SOURCE, rid=rid
)

# COMMAND ----------

# DBTITLE 1,Write grid_network
write_gold(grid_network, "grid_network", source=SOURCE, component=COMPONENT, rid=rid)

# COMMAND ----------

# DBTITLE 1,Inspect + export findings
_blocks = inspect_gold_table(
    read_gold("grid_connection_point", source=SOURCE),
    "grid_connection_point",
    source=SOURCE,
    component=COMPONENT,
    rid=rid,
    key_cols=["connection_point_id"],
)
_blocks += inspect_gold_table(
    read_gold("balancing_area", source=SOURCE),
    "balancing_area",
    source=SOURCE,
    component=COMPONENT,
    rid=rid,
    key_cols=["balancing_area_id"],
)
_blocks += inspect_gold_table(
    read_gold("grid_location_coordinate_conflict", source=SOURCE),
    "grid_location_coordinate_conflict",
    source=SOURCE,
    component=COMPONENT,
    rid=rid,
    key_cols=["location_id"],
)
_blocks += inspect_gold_table(
    grid_network,
    "grid_network",
    source=SOURCE,
    component=COMPONENT,
    rid=rid,
    key_cols=["grid_id"],
)
write_gold_findings(SOURCE, "grid__grid_topology", "grid topology", _blocks)
