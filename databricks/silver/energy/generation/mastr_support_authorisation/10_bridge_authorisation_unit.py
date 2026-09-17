# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER -- MASTR_AUTHORISATION_UNIT_BRIDGE
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** the additive `(parent_id, linked_id)` bridge from `mastr_einheiten_genehmigung`'s `VerknuepfteEinheitenMaStRNummern` link array. Never replaces `mastr_einheiten_genehmigung`. One of 11 sibling notebooks in this folder,
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
    "silver/energy/generation/mastr_support_authorisation/10_bridge_authorisation_unit"
)
RID = run_id()

MAPPING = load_mapping(SOURCE)
NAME_MAP = flatten_business_names(MAPPING, SOURCE)
CODED = coded_columns(MAPPING, SOURCE)

# COMMAND ----------

# DBTITLE 1,Read Bronze -- mastr_einheiten_genehmigung
_bronze = read_bronze("mastr_einheiten_genehmigung")

# COMMAND ----------

# DBTITLE 1,Write mastr_authorisation_unit_bridge (additive)
_bridge = explode_link_bridge(
    _bronze,
    "GenMastrNummer",
    "VerknuepfteEinheitenMaStRNummern",
    "mastr_authorisation_unit_bridge",
    source=SOURCE,
    component=COMPONENT,
    bronze_table="mastr_einheiten_genehmigung",
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Inspect mastr_authorisation_unit_bridge + export findings
_findings_blocks = inspect_table(
    _bridge,
    "mastr_authorisation_unit_bridge",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["parent_id", "linked_id"],
)
write_silver_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__mastr_authorisation_unit_bridge",
    "mastr_authorisation_unit_bridge",
    _findings_blocks,
)
