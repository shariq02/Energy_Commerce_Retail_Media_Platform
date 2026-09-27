# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # GOLD -- DIM_BALANCING_AREA
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Grain:** one row per `balancing_area_id`.
# MAGIC
# MAGIC **Sources:** `mastr_bilanzierungsgebiete` (Silver, energy_silver).
# MAGIC
# MAGIC **Serves use case:** any Energy use case needing this MaStR
# MAGIC grid-topology entity. One of 4 sibling notebooks in this folder, split
# MAGIC from a single `01_dim_location.py` -- see the folder's other files for
# MAGIC the rest. These are four distinct entity types at four distinct grains,
# MAGIC never unioned into one dimension.
# MAGIC
# MAGIC **Purpose:** promote `mastr_bilanzierungsgebiete` to Gold -- natural key
# MAGIC renamed `Id` -> `balancing_area_id`. Silver may carry more than one
# MAGIC variant per `Id` (`_src_id_ord`, Bronze contract documents `Id` alone
# MAGIC as not unique); Gold keeps exactly the `_src_id_ord == 1` row.

# COMMAND ----------

# DBTITLE 1,Shared Silver library
# MAGIC %run ../../../../silver/_silver_common

# COMMAND ----------

# DBTITLE 1,Gold shared library
# MAGIC %run ../../../_gold_common

# COMMAND ----------

# DBTITLE 1,Inspection library
# MAGIC %run ../../../_gold_inspect

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "mastr"
COMPONENT = "gold/energy/grid/dim_location/04_dim_balancing_area"
RID = gold_run_id()
GOLD_TABLE = "dim_balancing_area"

# COMMAND ----------

# DBTITLE 1,Read Silver -- mastr_bilanzierungsgebiete
_silver = read_silver("mastr_bilanzierungsgebiete")

# COMMAND ----------

# DBTITLE 1,Transform -- canonical row per Id
_silver_canonical = _silver.filter(F.col("_src_id_ord") == 1)
_silver_alt_count = _silver.filter(F.col("_src_id_ord") > 1).count()

# COMMAND ----------

# DBTITLE 1,Transform -- surrogate key + Gold provenance
dim = _silver_canonical.withColumnRenamed("Id", "balancing_area_id")
dim = dim.withColumn("balancing_area_key", surrogate_key("balancing_area_id"))
dim = add_gold_provenance(dim, SOURCE, RID)

# COMMAND ----------

# DBTITLE 1,Grain assertion -- one row per balancing_area_id
assert_unique_grain(
    dim, ["balancing_area_id"], component=COMPONENT, source=SOURCE, rid=RID
)

# COMMAND ----------

# DBTITLE 1,Write Gold -- dim_balancing_area
write_gold(dim, GOLD_TABLE, source=SOURCE, component=COMPONENT, rid=RID)

# COMMAND ----------

# DBTITLE 1,Inspect dim_balancing_area + export findings
_findings_blocks = inspect_gold_table(
    dim,
    GOLD_TABLE,
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["balancing_area_id"],
    df_before=_silver,
    extra_checks={"non_canonical_rows_excluded": _silver_alt_count},
)
write_gold_findings(
    SOURCE,
    "dim_location__dim_balancing_area",
    GOLD_TABLE,
    _findings_blocks,
)
