# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER -- GRID TOPOLOGY (MASTR)
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** MaStR locations, grid connection points, networks and
# MAGIC balancing areas (distinct entities), plus the per-location coordinate
# MAGIC agreement check across linked generation units.

# COMMAND ----------

# DBTITLE 1,Shared library
# MAGIC %run ../../_silver_common

# COMMAND ----------

# DBTITLE 1,Inspection library
# MAGIC %run ../../_silver_inspect

# COMMAND ----------

# DBTITLE 1,Semantic structures library
# MAGIC %run ../../_semantic_common

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "mastr"
COMPONENT = "silver/energy/grid/07_grid_topology"
RID = run_id()
FINDINGS = "mastr"
MAPPING = load_mapping(SOURCE)
NAME_MAP = flatten_business_names(MAPPING, SOURCE)
CODED = coded_columns(MAPPING, SOURCE)
# structure -> (Bronze table, id column)
ENTITIES = {
    "grid_location": ("mastr_lokationen", "MastrNummer"),
    "grid_connection_point": (
        "mastr_netzanschlusspunkte",
        "NetzanschlusspunktMastrNummer",
    ),
    "grid_network": ("mastr_netze", "MastrNummer"),
}
BALANCING_BT = "mastr_bilanzierungsgebiete"
CONFLICT = "grid_location_coordinate_conflict"
KEYS = {"balancing_area": [NAME_MAP.get("Id", "Id")]}

# COMMAND ----------

# DBTITLE 1,Read Bronze -- locations, connection points, networks
bronze = {name: read_bronze(bt) for name, (bt, _) in ENTITIES.items()}

# COMMAND ----------

# DBTITLE 1,Read Bronze -- mastr_bilanzierungsgebiete
balancing_bronze = read_bronze(BALANCING_BT)

# COMMAND ----------

# DBTITLE 1,Transform -- grid_location, grid_connection_point, grid_network
STRUCTURES = {}
for name, (bt, id_col) in ENTITIES.items():
    df = mastr_standardise(bronze[name], NAME_MAP, CODED)
    df = df.withColumn("_srid", F.col(NAME_MAP.get(id_col, id_col)).cast("string"))
    STRUCTURES[name] = add_semantic_provenance(df, SOURCE, bt, RID, "_srid")

# COMMAND ----------

# DBTITLE 1,Transform -- balancing_area (Id not unique: ordinal, never a drop)
_id = NAME_MAP.get("Id", "Id")
areas = mastr_standardise(balancing_bronze.dropDuplicates(), NAME_MAP, CODED)
areas = within_group_ordinal(
    areas, [_id], ["Yeic", "control_zone", "BilanzierungsgebietNetzanschlusspunkt"]
)
areas = areas.withColumn("_srid", sha_key(_id, "_src_id_ord"))
STRUCTURES["balancing_area"] = add_semantic_provenance(
    areas, SOURCE, BALANCING_BT, RID, "_srid"
)

# COMMAND ----------

# DBTITLE 1,Write Silver -- topology structures
for name, frame in STRUCTURES.items():
    write_semantic(frame, name, source=SOURCE, component=COMPONENT, rid=RID)

# COMMAND ----------

# DBTITLE 1,Read Silver -- generation_unit coordinates per location
unit_coords = (
    spark.table(semantic_table("generation_unit"))
    .filter(
        F.col("location_id").isNotNull()
        & F.col("latitude").isNotNull()
        & F.col("longitude").isNotNull()
    )
    .select(
        "location_id",
        F.round("latitude", 2).alias("lat2dp"),
        F.round("longitude", 2).alias("lon2dp"),
    )
)

# COMMAND ----------

# DBTITLE 1,Transform -- grid_location_coordinate_conflict (2-dp disagreement)
conflict = (
    unit_coords.dropDuplicates()
    .groupBy("location_id")
    .agg(F.countDistinct("lat2dp", "lon2dp").alias("distinct_coords"))
    .withColumn("_coordinate_conflict", F.col("distinct_coords") > 1)
    .withColumn("_srid", F.col("location_id").cast("string"))
)
conflict = add_semantic_provenance(conflict, SOURCE, "generation_unit", RID, "_srid")

# COMMAND ----------

# DBTITLE 1,Write Silver -- grid_location_coordinate_conflict
write_semantic(conflict, CONFLICT, source=SOURCE, component=COMPONENT, rid=RID)

# COMMAND ----------

# DBTITLE 1,Inspect -- topology structures and the coordinate check
findings_blocks = {
    name: inspect_table(
        spark.table(semantic_table(name)),
        name,
        source=FINDINGS,
        component=COMPONENT,
        rid=RID,
        key_cols=KEYS.get(name, ["source_record_id"]),
    )
    for name in [*STRUCTURES, CONFLICT]
}

# COMMAND ----------

# DBTITLE 1,Export findings -- topology structures and the coordinate check
for name, blocks in findings_blocks.items():
    write_silver_findings(FINDINGS, f"{COMPONENT.split('/')[-1]}__{name}", name, blocks)
