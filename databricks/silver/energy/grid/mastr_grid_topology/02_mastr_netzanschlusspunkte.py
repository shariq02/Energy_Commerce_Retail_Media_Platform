# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER -- MASTR_NETZANSCHLUSSPUNKTE
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** `mastr_netzanschlusspunkte` into source-scoped Silver at its own grain. One of 7 sibling notebooks in this folder, split
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
COMPONENT = "silver/energy/grid/mastr_grid_topology/02_mastr_netzanschlusspunkte"
RID = run_id()

MAPPING = load_mapping(SOURCE)
NAME_MAP = flatten_business_names(MAPPING, SOURCE)
CODED = coded_columns(MAPPING, SOURCE)

# COMMAND ----------

# DBTITLE 1,Read Bronze -- mastr_netzanschlusspunkte
_bronze = read_bronze("mastr_netzanschlusspunkte")

# COMMAND ----------

# DBTITLE 1,Transform -- mastr_netzanschlusspunkte
nap = mastr_standardise(_bronze, NAME_MAP, CODED, source=SOURCE)
nap = nap.withColumn(
    "_srid",
    F.col(
        NAME_MAP.get("NetzanschlusspunktMastrNummer", "NetzanschlusspunktMastrNummer")
    ).cast("string"),
)
nap = add_provenance(nap, SOURCE, "_srid", RID)

# COMMAND ----------

# DBTITLE 1,Write Silver -- mastr_netzanschlusspunkte
write_silver(
    nap, "mastr_netzanschlusspunkte", source=SOURCE, component=COMPONENT, rid=RID
)

# COMMAND ----------

# DBTITLE 1,Inspect mastr_netzanschlusspunkte + export findings
_findings_blocks = inspect_table(
    nap,
    "mastr_netzanschlusspunkte",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=[
        NAME_MAP.get("NetzanschlusspunktMastrNummer", "NetzanschlusspunktMastrNummer")
    ],
    df_before=_bronze,
)
write_silver_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__mastr_netzanschlusspunkte",
    "mastr_netzanschlusspunkte",
    _findings_blocks,
)
