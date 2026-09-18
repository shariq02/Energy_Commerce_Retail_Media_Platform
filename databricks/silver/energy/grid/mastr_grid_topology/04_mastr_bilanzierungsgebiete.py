# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER -- MASTR_BILANZIERUNGSGEBIETE
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** `mastr_bilanzierungsgebiete` into source-scoped Silver at
# MAGIC its own (`Id`, `_src_id_ord`) grain -- the Bronze contract documents
# MAGIC `Id` alone as `unique: false` ("no unique key established by
# MAGIC profiling"). One of 7 sibling notebooks in this folder, split
# MAGIC from a single `02_mastr_grid_topology.py` -- see the folder's other
# MAGIC files for the rest. Runs after `../../_reference/mastr_reference_catalogs/`.

# COMMAND ----------

# DBTITLE 1,Shared library
# MAGIC %run ../../../_silver_common

# COMMAND ----------

# DBTITLE 1,Inspection library
# MAGIC %run ../../../_silver_inspect

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "mastr"
COMPONENT = "silver/energy/grid/mastr_grid_topology/04_mastr_bilanzierungsgebiete"
RID = run_id()

MAPPING = load_mapping(SOURCE)
NAME_MAP = flatten_business_names(MAPPING, SOURCE)
CODED = coded_columns(MAPPING, SOURCE)

# COMMAND ----------

# DBTITLE 1,Read Bronze -- mastr_bilanzierungsgebiete
_bronze = read_bronze("mastr_bilanzierungsgebiete")

# COMMAND ----------

# DBTITLE 1,Transform -- mastr_bilanzierungsgebiete
# Bronze contract documents Id as unique: false ("no unique key established
# by profiling") -- dropDuplicates() only collapses byte-identical rows, not
# distinct rows sharing an Id, so the ordinal still disambiguates.
bil = _bronze.dropDuplicates()
bil = mastr_standardise(bil, NAME_MAP, CODED, source=SOURCE)
_ID_COL = NAME_MAP.get("Id", "Id")
bil = within_group_ordinal(
    bil, [_ID_COL], ["Yeic", "control_zone", "BilanzierungsgebietNetzanschlusspunkt"]
)
bil = bil.withColumn("_srid", sha_key(_ID_COL, "_src_id_ord"))
bil = add_provenance(bil, SOURCE, "_srid", RID)

# COMMAND ----------

# DBTITLE 1,Write Silver -- mastr_bilanzierungsgebiete
write_silver(
    bil, "mastr_bilanzierungsgebiete", source=SOURCE, component=COMPONENT, rid=RID
)

# COMMAND ----------

# DBTITLE 1,Inspect mastr_bilanzierungsgebiete + export findings
_findings_blocks = inspect_table(
    bil,
    "mastr_bilanzierungsgebiete",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=[_ID_COL, "_src_id_ord"],
    df_before=_bronze,
)
write_silver_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__mastr_bilanzierungsgebiete",
    "mastr_bilanzierungsgebiete",
    _findings_blocks,
)