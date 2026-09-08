# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER -- MASTR GRID TOPOLOGY
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** `mastr_lokationen`, `mastr_netzanschlusspunkte`,
# MAGIC `mastr_netze` and `mastr_bilanzierungsgebiete` into source-scoped Silver
# MAGIC at their own grain, plus two additive bridges from the location link
# MAGIC arrays (`mastr_location_unit_bridge`,
# MAGIC `mastr_location_connection_bridge`). Runs after
# MAGIC `02_mastr_reference_catalogs`.

# COMMAND ----------

# DBTITLE 1,Shared library
# MAGIC %run ../../_silver_common

# COMMAND ----------

# DBTITLE 1,Imports + config
from pyspark.sql import functions as F

SOURCE = "mastr"
COMPONENT = "silver/energy/grid/02_mastr_grid_topology"
RID = run_id()

MAPPING = load_mapping(SOURCE)
NAME_MAP = flatten_business_names(MAPPING, SOURCE)
CODED = coded_columns(MAPPING, SOURCE)

PRIMARY_TABLES = {
    "mastr_lokationen": "MastrNummer",
    "mastr_netzanschlusspunkte": "NetzanschlusspunktMastrNummer",
    "mastr_netze": "MastrNummer",
    "mastr_bilanzierungsgebiete": "Id",
}

# COMMAND ----------

# DBTITLE 1,Primary topology tables -> Silver


def process(bt: str, pk: str) -> None:
    df = read_bronze(bt)
    if bt == "mastr_bilanzierungsgebiete":
        df = df.dropDuplicates()
    df = mastr_standardise(df, NAME_MAP, CODED, source=SOURCE)
    id_col = NAME_MAP.get(pk, pk)
    df = df.withColumn("_srid", F.col(id_col).cast("string"))
    df = add_provenance(df, SOURCE, "_srid", RID)
    write_silver(df, bt, source=SOURCE, component=COMPONENT, rid=RID)


for _bt, _pk in PRIMARY_TABLES.items():
    process(_bt, _pk)

# COMMAND ----------

# DBTITLE 1,Additive location bridges
_lok = read_bronze("mastr_lokationen")
explode_link_bridge(
    _lok,
    "MastrNummer",
    "VerknuepfteEinheitenMaStRNummern",
    "mastr_location_unit_bridge",
    source=SOURCE,
    component=COMPONENT,
    bronze_table="mastr_lokationen",
    rid=RID,
)
explode_link_bridge(
    _lok,
    "MastrNummer",
    "NetzanschlusspunkteMaStRNummern",
    "mastr_location_connection_bridge",
    source=SOURCE,
    component=COMPONENT,
    bronze_table="mastr_lokationen",
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Summary
print("=" * 70)
print(f"MASTR GRID TOPOLOGY -- COMPLETE  (run_id {RID})")
print("=" * 70)
