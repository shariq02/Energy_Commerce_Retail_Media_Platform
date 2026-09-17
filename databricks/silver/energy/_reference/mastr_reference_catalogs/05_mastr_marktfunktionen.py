# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER -- MASTR_MARKTFUNKTIONEN
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** `mastr_marktfunktionen` into source-scoped Silver at its source grain. One of
# MAGIC 6 sibling notebooks in this folder, split from a single
# MAGIC `02_mastr_reference_catalogs.py` -- see the folder's other files for the
# MAGIC rest. `mastr_katalogwerte` (with `mastr_katalogkategorien`) is the decode
# MAGIC authority every other MaStR notebook joins to, so this folder runs first.

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
COMPONENT = "silver/energy/_reference/mastr_reference_catalogs/05_mastr_marktfunktionen"
RID = run_id()

CONTRACT = load_contract(SOURCE)
TABLES = contract_tables(CONTRACT)

# COMMAND ----------

# DBTITLE 1,Read Bronze -- mastr_marktfunktionen
_bronze = read_bronze("mastr_marktfunktionen")

# COMMAND ----------

# DBTITLE 1,Transform -- mastr_marktfunktionen
_pk = TABLES["mastr_marktfunktionen"]["grain"]["key"][0]
df = _bronze.withColumn("_srid", F.col(_pk).cast("string"))
df = add_provenance(df, SOURCE, "_srid", RID)

# COMMAND ----------

# DBTITLE 1,Write Silver -- mastr_marktfunktionen
write_silver(df, "mastr_marktfunktionen", source=SOURCE, component=COMPONENT, rid=RID)

# COMMAND ----------

# DBTITLE 1,Inspect mastr_marktfunktionen + export findings
_findings_blocks = inspect_table(
    df,
    "mastr_marktfunktionen",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=[_pk],
    df_before=_bronze,
)
write_silver_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__mastr_marktfunktionen",
    "mastr_marktfunktionen",
    _findings_blocks,
)
