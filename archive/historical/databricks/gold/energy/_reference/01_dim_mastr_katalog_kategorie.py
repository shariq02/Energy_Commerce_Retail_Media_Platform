# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # GOLD -- DIM MASTR KATALOG KATEGORIE
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Grain:** one row per `Id`. Static reference, no SCD.
# MAGIC
# MAGIC **Sources:** `mastr_katalogkategorien` (Silver, energy_silver_reference).
# MAGIC
# MAGIC **Serves use case:** the MaStR classification category vocabulary
# MAGIC `dim_mastr_katalog_wert` values belong to, governed once.
# MAGIC
# MAGIC **Purpose:** promote `mastr_katalogkategorien` to Gold.

# COMMAND ----------

# DBTITLE 1,Shared Silver library
# MAGIC %run ../../../silver/_silver_common

# COMMAND ----------

# DBTITLE 1,Gold shared library
# MAGIC %run ../../_gold_common

# COMMAND ----------

# DBTITLE 1,Inspection library
# MAGIC %run ../../_gold_inspect

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "mastr"
COMPONENT = "gold/energy/_reference/01_dim_mastr_katalog_kategorie"
RID = gold_run_id()
GOLD_TABLE = "dim_mastr_katalog_kategorie"

# COMMAND ----------

# DBTITLE 1,Read Silver -- mastr_katalogkategorien
_kategorien_silver = read_silver("mastr_katalogkategorien")

# COMMAND ----------

# DBTITLE 1,Transform -- surrogate key + Gold provenance
dim = _kategorien_silver.withColumn("mastr_katalog_kategorie_key", surrogate_key("Id"))
dim = add_gold_provenance(dim, SOURCE, RID)

# COMMAND ----------

# DBTITLE 1,Grain assertion -- one row per Id
assert_unique_grain(dim, ["Id"], component=COMPONENT, source=SOURCE, rid=RID)

# COMMAND ----------

# DBTITLE 1,Write Gold -- dim_mastr_katalog_kategorie
write_gold(dim, GOLD_TABLE, source=SOURCE, component=COMPONENT, rid=RID)

# COMMAND ----------

# DBTITLE 1,Inspect dim_mastr_katalog_kategorie + export findings
_findings_blocks = inspect_gold_table(
    dim,
    GOLD_TABLE,
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["Id"],
    df_before=_kategorien_silver,
)
write_gold_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__{GOLD_TABLE}",
    GOLD_TABLE,
    _findings_blocks,
)
