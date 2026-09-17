# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER -- MASTR_ANLAGEN_EEG_WASSER
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** `mastr_anlagen_eeg_wasser` into source-scoped Silver at its own record grain. One of 11 sibling notebooks in this folder,
# MAGIC split from a single `02_mastr_support_authorisation.py` -- see the
# MAGIC folder's other files for the rest. Runs after `../../_reference/mastr_reference_catalogs/`.

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
COMPONENT = (
    "silver/energy/generation/mastr_support_authorisation/03_mastr_anlagen_eeg_wasser"
)
RID = run_id()

MAPPING = load_mapping(SOURCE)
NAME_MAP = flatten_business_names(MAPPING, SOURCE)
CODED = coded_columns(MAPPING, SOURCE)

# COMMAND ----------

# DBTITLE 1,Read Bronze -- mastr_anlagen_eeg_wasser
_bronze = read_bronze("mastr_anlagen_eeg_wasser")

# COMMAND ----------

# DBTITLE 1,Transform -- mastr_anlagen_eeg_wasser
df = mastr_standardise(_bronze, NAME_MAP, CODED, source=SOURCE)
_id_col = NAME_MAP.get("EegMaStRNummer", "EegMaStRNummer")
df = df.withColumn("_srid", F.col(_id_col).cast("string"))
df = add_provenance(df, SOURCE, "_srid", RID)

# COMMAND ----------

# DBTITLE 1,Write Silver -- mastr_anlagen_eeg_wasser
write_silver(
    df, "mastr_anlagen_eeg_wasser", source=SOURCE, component=COMPONENT, rid=RID
)

# COMMAND ----------

# DBTITLE 1,Inspect -- mastr_anlagen_eeg_wasser
_findings_blocks = inspect_table(
    df,
    "mastr_anlagen_eeg_wasser",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=[_id_col],
    df_before=_bronze,
)
write_silver_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__mastr_anlagen_eeg_wasser",
    "mastr_anlagen_eeg_wasser",
    _findings_blocks,
)
