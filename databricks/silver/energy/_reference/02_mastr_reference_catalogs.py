# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER -- MASTR REFERENCE CATALOGS
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** the six MaStR reference-catalog Bronze tables into
# MAGIC source-scoped Silver at their source grain. `mastr_katalogwerte` (with
# MAGIC `mastr_katalogkategorien`) is the decode authority every other MaStR
# MAGIC notebook joins to, so this notebook runs first.

# COMMAND ----------

# DBTITLE 1,Shared library
# MAGIC %run ../../_silver_common

# COMMAND ----------

# DBTITLE 1,Imports + config
from pyspark.sql import functions as F

SOURCE = "mastr"
COMPONENT = "silver/energy/_reference/02_mastr_reference_catalogs"
RID = run_id()

CONTRACT = load_contract(SOURCE)
TABLES = contract_tables(CONTRACT)

CATALOG_TABLES = [
    "mastr_katalogkategorien",
    "mastr_katalogwerte",
    "mastr_einheitentypen",
    "mastr_lokationstypen",
    "mastr_marktfunktionen",
    "mastr_marktrollen",
]

# COMMAND ----------

# DBTITLE 1,Reference catalogs -> Silver
for bt in CATALOG_TABLES:
    pk = TABLES[bt]["grain"]["key"][0]
    df = read_bronze(bt).withColumn("_srid", F.col(pk).cast("string"))
    df = add_provenance(df, SOURCE, "_srid", RID)
    write_silver(df, bt, source=SOURCE, component=COMPONENT, rid=RID)

# COMMAND ----------

# DBTITLE 1,Summary
audit(
    COMPONENT,
    SOURCE,
    "catalog_tables_written",
    float(len(CATALOG_TABLES)),
    status="PASS",
    rid=RID,
)
print("=" * 70)
print(f"MASTR REFERENCE CATALOGS -- COMPLETE  (run_id {RID})")
print("=" * 70)
