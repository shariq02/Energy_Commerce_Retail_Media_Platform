# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER -- MASTR_LOCATION_COORDINATE_CONFLICT
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** the derived coordinate-conflict check, reading the 6 MaStR generation-unit Silver tables (`../../generation/mastr_generation_units/`, must run first) and flagging locations whose linked units disagree on coordinates. Never picks a "correct" coordinate. One of 7 sibling notebooks in this folder, split
# MAGIC from a single `02_mastr_grid_topology.py` -- see the folder's other
# MAGIC files for the rest. Runs after `../../_reference/mastr_reference_catalogs/`.

# COMMAND ----------

# DBTITLE 1,Shared library
# MAGIC %run ../../../_silver_common

# COMMAND ----------

# DBTITLE 1,Inspection library
# MAGIC %run ../../../_silver_inspect

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "mastr"
COMPONENT = "silver/energy/grid/mastr_grid_topology/07_location_coordinate_conflict"
RID = run_id()

MAPPING = load_mapping(SOURCE)
NAME_MAP = flatten_business_names(MAPPING, SOURCE)
CODED = coded_columns(MAPPING, SOURCE)

# COMMAND ----------

# DBTITLE 1,Configuration (generation-unit table list)
_GENERATION_UNIT_TABLES = [
    "mastr_einheiten_wind",
    "mastr_einheiten_biomasse",
    "mastr_einheiten_wasser",
    "mastr_einheiten_verbrennung",
    "mastr_einheiten_kernkraft",
    "mastr_einheiten_geothermie_gsgk",
]

# COMMAND ----------

# DBTITLE 1,Transform -- gather coordinates from the generation-unit Silver tables
_coords = None
for _t in _GENERATION_UNIT_TABLES:
    try:
        _part = (
            read_silver(_t)
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
    except Exception as exc:
        print(f"SKIP {_t} in coordinate-conflict check: {exc}")
        continue
    _coords = _part if _coords is None else _coords.unionByName(_part)

# COMMAND ----------

# DBTITLE 1,Transform -- mastr_location_coordinate_conflict
if _coords is not None:
    _conflict = (
        _coords.dropDuplicates(["location_id", "lat2dp", "lon2dp"])
        .groupBy("location_id")
        .agg(
            F.countDistinct(F.concat_ws(",", "lat2dp", "lon2dp")).alias(
                "distinct_coords"
            )
        )
        .withColumn("_coordinate_conflict", F.col("distinct_coords") > 1)
    )
    _conflict = _conflict.withColumn("_srid", F.col("location_id").cast("string"))
    _conflict = add_provenance(_conflict, SOURCE, "_srid", RID)

# COMMAND ----------

# DBTITLE 1,Write Silver -- mastr_location_coordinate_conflict
if _coords is not None:
    write_silver(
        _conflict,
        "mastr_location_coordinate_conflict",
        source=SOURCE,
        component=COMPONENT,
        rid=RID,
    )

# COMMAND ----------

# DBTITLE 1,Inspect mastr_location_coordinate_conflict + export findings
if _coords is not None:
    _findings_blocks = inspect_table(
        _conflict,
        "mastr_location_coordinate_conflict",
        source=SOURCE,
        component=COMPONENT,
        rid=RID,
        key_cols=["location_id"],
        extra_checks={
            "conflicting_location_count": _conflict.filter(
                F.col("_coordinate_conflict")
            ).count(),
        },
    )
    write_silver_findings(
        SOURCE,
        f"{COMPONENT.split('/')[-1]}__mastr_location_coordinate_conflict",
        "mastr_location_coordinate_conflict",
        _findings_blocks,
    )
else:
    print("no generation-unit Silver tables available yet -- coordinate check skipped.")
