# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # GOLD -- GRID TOPOLOGY DIMENSIONS
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Grain:** one row per natural key, per dimension (see each table below).
# MAGIC
# MAGIC **Sources:** `mastr_lokationen`, `mastr_netzanschlusspunkte`,
# MAGIC `mastr_netze`, `mastr_bilanzierungsgebiete` (Silver, energy_silver).
# MAGIC
# MAGIC **Serves use case:** any Energy use case needing MaStR grid-topology
# MAGIC entities -- these are four distinct entity types at four distinct grains,
# MAGIC never unioned into one dimension.
# MAGIC
# MAGIC **Purpose:** promote the four MaStR grid-topology Silver tables to Gold,
# MAGIC each its own dimension. `dim_location`'s natural key is renamed
# MAGIC `MastrNummer` -> `location_id` per `mappings/mastr.yml`'s own documented
# MAGIC canonical target; `dim_grid_connection_point` already carries
# MAGIC `connection_point_id` from Silver's identifier rename.

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
COMPONENT = "gold/energy/grid/01_dim_location"
RID = gold_run_id()

# COMMAND ----------

# DBTITLE 1,Read Silver -- mastr_lokationen
_lokationen_silver = read_silver("mastr_lokationen")

# COMMAND ----------

# DBTITLE 1,Read Silver -- mastr_netzanschlusspunkte
_netzanschlusspunkte_silver = read_silver("mastr_netzanschlusspunkte")

# COMMAND ----------

# DBTITLE 1,Read Silver -- mastr_netze
_netze_silver = read_silver("mastr_netze")

# COMMAND ----------

# DBTITLE 1,Read Silver -- mastr_bilanzierungsgebiete
_bilanzierungsgebiete_silver = read_silver("mastr_bilanzierungsgebiete")

# COMMAND ----------

# DBTITLE 1,Transform -- dim_location
dim_location = _lokationen_silver.withColumnRenamed("MastrNummer", "location_id")
dim_location = dim_location.withColumn("location_key", surrogate_key("location_id"))
dim_location = add_gold_provenance(dim_location, SOURCE, RID)

# COMMAND ----------

# DBTITLE 1,Grain assertion -- dim_location, one row per location_id
assert_unique_grain(
    dim_location, ["location_id"], component=COMPONENT, source=SOURCE, rid=RID
)

# COMMAND ----------

# DBTITLE 1,Write Gold -- dim_location
write_gold(dim_location, "dim_location", source=SOURCE, component=COMPONENT, rid=RID)

# COMMAND ----------

# DBTITLE 1,Inspect dim_location + export findings
_findings_blocks = inspect_gold_table(
    dim_location,
    "dim_location",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["location_id"],
    df_before=_lokationen_silver,
)
write_gold_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__dim_location",
    "dim_location",
    _findings_blocks,
)

# COMMAND ----------

# DBTITLE 1,Transform -- dim_grid_connection_point
dim_grid_connection_point = _netzanschlusspunkte_silver.withColumn(
    "grid_connection_point_key", surrogate_key("connection_point_id")
)
dim_grid_connection_point = add_gold_provenance(dim_grid_connection_point, SOURCE, RID)

# COMMAND ----------

# DBTITLE 1,Grain assertion -- dim_grid_connection_point, one row per connection_point_id
assert_unique_grain(
    dim_grid_connection_point,
    ["connection_point_id"],
    component=COMPONENT,
    source=SOURCE,
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Write Gold -- dim_grid_connection_point
write_gold(
    dim_grid_connection_point,
    "dim_grid_connection_point",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Inspect dim_grid_connection_point + export findings
_findings_blocks = inspect_gold_table(
    dim_grid_connection_point,
    "dim_grid_connection_point",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["connection_point_id"],
    df_before=_netzanschlusspunkte_silver,
)
write_gold_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__dim_grid_connection_point",
    "dim_grid_connection_point",
    _findings_blocks,
)

# COMMAND ----------

# DBTITLE 1,Transform -- dim_network
dim_network = _netze_silver.withColumnRenamed("MastrNummer", "network_id")
dim_network = dim_network.withColumn("network_key", surrogate_key("network_id"))
dim_network = add_gold_provenance(dim_network, SOURCE, RID)

# COMMAND ----------

# DBTITLE 1,Grain assertion -- dim_network, one row per network_id
assert_unique_grain(
    dim_network, ["network_id"], component=COMPONENT, source=SOURCE, rid=RID
)

# COMMAND ----------

# DBTITLE 1,Write Gold -- dim_network
write_gold(dim_network, "dim_network", source=SOURCE, component=COMPONENT, rid=RID)

# COMMAND ----------

# DBTITLE 1,Inspect dim_network + export findings
_findings_blocks = inspect_gold_table(
    dim_network,
    "dim_network",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["network_id"],
    df_before=_netze_silver,
)
write_gold_findings(
    SOURCE, f"{COMPONENT.split('/')[-1]}__dim_network", "dim_network", _findings_blocks
)

# COMMAND ----------

# DBTITLE 1,Transform -- dim_balancing_area
dim_balancing_area = _bilanzierungsgebiete_silver.withColumnRenamed(
    "Id", "balancing_area_id"
)
dim_balancing_area = dim_balancing_area.withColumn(
    "balancing_area_key", surrogate_key("balancing_area_id")
)
dim_balancing_area = add_gold_provenance(dim_balancing_area, SOURCE, RID)

# COMMAND ----------

# DBTITLE 1,Grain assertion -- dim_balancing_area, one row per balancing_area_id
assert_unique_grain(
    dim_balancing_area,
    ["balancing_area_id"],
    component=COMPONENT,
    source=SOURCE,
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Write Gold -- dim_balancing_area
write_gold(
    dim_balancing_area,
    "dim_balancing_area",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Inspect dim_balancing_area + export findings
_findings_blocks = inspect_gold_table(
    dim_balancing_area,
    "dim_balancing_area",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["balancing_area_id"],
    df_before=_bilanzierungsgebiete_silver,
)
write_gold_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__dim_balancing_area",
    "dim_balancing_area",
    _findings_blocks,
)
