# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER -- MASTR_LOCATION_UNIT_BRIDGE
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** the additive `(parent_id, linked_id)` bridge from `mastr_lokationen`'s `VerknuepfteEinheitenMaStRNummern` link array. Never replaces `mastr_lokationen` (see `01_mastr_lokationen.py`). One of 7 sibling notebooks in this folder, split
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

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "mastr"
COMPONENT = "silver/energy/grid/mastr_grid_topology/05_bridge_location_unit"
RID = run_id()

MAPPING = load_mapping(SOURCE)
NAME_MAP = flatten_business_names(MAPPING, SOURCE)
CODED = coded_columns(MAPPING, SOURCE)

# COMMAND ----------

# DBTITLE 1,Read Bronze -- mastr_lokationen
_bronze = read_bronze("mastr_lokationen")

# COMMAND ----------

# DBTITLE 1,Write mastr_location_unit_bridge (additive)
_bridge = explode_link_bridge(
    _bronze,
    "MastrNummer",
    "VerknuepfteEinheitenMaStRNummern",
    "mastr_location_unit_bridge",
    source=SOURCE,
    component=COMPONENT,
    bronze_table="mastr_lokationen",
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Inspect mastr_location_unit_bridge + export findings
_findings_blocks = inspect_table(
    _bridge,
    "mastr_location_unit_bridge",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["parent_id", "linked_id"],
)
write_silver_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__mastr_location_unit_bridge",
    "mastr_location_unit_bridge",
    _findings_blocks,
)
