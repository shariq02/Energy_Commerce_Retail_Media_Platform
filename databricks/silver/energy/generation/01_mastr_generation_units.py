# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER -- MASTR GENERATION UNITS
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** the six MaStR `einheiten_*` carrier Bronze tables into
# MAGIC source-scoped Silver at their unit grain (`EinheitMastrNummer`) --
# MAGIC catalog decode, English business names, capacities in kW, coordinates
# MAGIC unchanged (out-of-Germany points recorded to quarantine), AGS from the
# MAGIC Gemeindeschluessel prefix. Runs after `02_mastr_reference_catalogs`.

# COMMAND ----------

# DBTITLE 1,Shared library
# MAGIC %run ../../_silver_common

# COMMAND ----------

# DBTITLE 1,Imports + config
from pyspark.sql import functions as F

SOURCE = "mastr"
COMPONENT = "silver/energy/generation/01_mastr_generation_units"
RID = run_id()

MAPPING = load_mapping(SOURCE)
NAME_MAP = flatten_business_names(MAPPING, SOURCE)
CODED = coded_columns(MAPPING, SOURCE)

UNIT_TABLES = [
    "mastr_einheiten_wind",
    "mastr_einheiten_biomasse",
    "mastr_einheiten_wasser",
    "mastr_einheiten_verbrennung",
    "mastr_einheiten_kernkraft",
    "mastr_einheiten_geothermie_gsgk",
]

# COMMAND ----------

# DBTITLE 1,One einheiten_* table -> Silver


def process(bt: str) -> None:
    df = mastr_standardise(read_bronze(bt), NAME_MAP, CODED, source=SOURCE)

    bad = df.filter(bbox_outside_de("latitude", "longitude"))
    write_quarantine(
        _q_rows(
            bad,
            "coord_outside_de_bbox",
            "unit coordinate outside the Germany bounding box",
            "latitude,longitude",
            "latitude",
            "unit_id",
            SOURCE,
            bt,
        ),
        RID,
    )

    df = attach_ags_prefix(df, "municipality_key_ags")
    df = df.withColumn("_srid", F.col("unit_id").cast("string"))
    df = add_provenance(df, SOURCE, "_srid", RID)
    write_silver(df, bt, source=SOURCE, component=COMPONENT, rid=RID)


for _bt in UNIT_TABLES:
    process(_bt)

# COMMAND ----------

# DBTITLE 1,Summary
audit(
    COMPONENT,
    SOURCE,
    "unit_tables_written",
    float(len(UNIT_TABLES)),
    status="PASS",
    rid=RID,
)
print("=" * 70)
print(f"MASTR GENERATION UNITS -- COMPLETE  (run_id {RID})")
print("=" * 70)
