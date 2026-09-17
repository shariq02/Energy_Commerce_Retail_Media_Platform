# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER -- MASTR_EEG_SUPPORT_UNIT_BRIDGE
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** the additive `(parent_id, linked_id)` bridge from the delimited `VerknuepfteEinheitenMaStRNummern` link array, unioned across the 4 EEG Bronze tables (wind/biomasse/wasser/geothermie_gsgk). Never replaces the 4 owning EEG tables (notebooks 01-04 in this folder). One of 11 sibling notebooks in this folder,
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

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "mastr"
COMPONENT = (
    "silver/energy/generation/mastr_support_authorisation/08_bridge_eeg_support_unit"
)
RID = run_id()

MAPPING = load_mapping(SOURCE)
NAME_MAP = flatten_business_names(MAPPING, SOURCE)
CODED = coded_columns(MAPPING, SOURCE)

# COMMAND ----------

# DBTITLE 1,Configuration (link column)
LINK_COL = "VerknuepfteEinheitenMaStRNummern"

# COMMAND ----------

# DBTITLE 1,Read Bronze -- mastr_anlagen_eeg_wind
_eeg_wind_bronze = read_bronze("mastr_anlagen_eeg_wind").select(
    "EegMaStRNummer", LINK_COL
)

# COMMAND ----------

# DBTITLE 1,Read Bronze -- mastr_anlagen_eeg_biomasse
_eeg_biomasse_bronze = read_bronze("mastr_anlagen_eeg_biomasse").select(
    "EegMaStRNummer", LINK_COL
)

# COMMAND ----------

# DBTITLE 1,Read Bronze -- mastr_anlagen_eeg_wasser
_eeg_wasser_bronze = read_bronze("mastr_anlagen_eeg_wasser").select(
    "EegMaStRNummer", LINK_COL
)

# COMMAND ----------

# DBTITLE 1,Read Bronze -- mastr_anlagen_eeg_geothermie_gsgk
_eeg_geothermie_gsgk_bronze = read_bronze("mastr_anlagen_eeg_geothermie_gsgk").select(
    "EegMaStRNummer", LINK_COL
)

# COMMAND ----------

# DBTITLE 1,Transform -- union the 4 EEG sources
_eeg_union = (
    _eeg_wind_bronze.unionByName(_eeg_biomasse_bronze)
    .unionByName(_eeg_wasser_bronze)
    .unionByName(_eeg_geothermie_gsgk_bronze)
)

# COMMAND ----------

# DBTITLE 1,Write mastr_eeg_support_unit_bridge (additive)
_bridge = explode_link_bridge(
    _eeg_union,
    "EegMaStRNummer",
    LINK_COL,
    "mastr_eeg_support_unit_bridge",
    source=SOURCE,
    component=COMPONENT,
    bronze_table="mastr_anlagen_eeg_wind,mastr_anlagen_eeg_biomasse,mastr_anlagen_eeg_wasser,mastr_anlagen_eeg_geothermie_gsgk",
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Inspect mastr_eeg_support_unit_bridge + export findings
_findings_blocks = inspect_table(
    _bridge,
    "mastr_eeg_support_unit_bridge",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["parent_id", "linked_id"],
)
write_silver_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__mastr_eeg_support_unit_bridge",
    "mastr_eeg_support_unit_bridge",
    _findings_blocks,
)
