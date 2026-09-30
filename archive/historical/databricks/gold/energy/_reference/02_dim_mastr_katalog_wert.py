# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # GOLD -- DIM MASTR KATALOG WERT
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Grain:** one row per `Id`. Static reference, no SCD.
# MAGIC
# MAGIC **Sources:** `mastr_katalogwerte` (Silver, energy_silver_reference), FK to
# MAGIC `dim_mastr_katalog_kategorie` via `KatalogKategorieId` (contract-verified
# MAGIC match rate 1.0, 0 orphans).
# MAGIC
# MAGIC **Serves use case:** value-level MaStR classification codes governed once.
# MAGIC
# MAGIC **Purpose:** promote `mastr_katalogwerte` to Gold. Kept as its own
# MAGIC dimension, not merged with `dim_mastr_katalog_kategorie` -- a genuine
# MAGIC parent/child grain.

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

# DBTITLE 1,Imports
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "mastr"
COMPONENT = "gold/energy/_reference/02_dim_mastr_katalog_wert"
RID = gold_run_id()
GOLD_TABLE = "dim_mastr_katalog_wert"

# COMMAND ----------

# DBTITLE 1,Read Silver -- mastr_katalogwerte
_werte_silver = read_silver("mastr_katalogwerte")

# COMMAND ----------

# DBTITLE 1,Read Gold -- dim_mastr_katalog_kategorie
_kategorie_gold = read_gold("dim_mastr_katalog_kategorie", source=SOURCE)

# COMMAND ----------

# DBTITLE 1,Transform -- resolve category FK
dim = resolve_fk(
    _werte_silver,
    _kategorie_gold,
    fact_key_cols=["KatalogKategorieId"],
    dim_key_cols=["Id"],
    dim_surrogate_col="mastr_katalog_kategorie_key",
    output_col="mastr_katalog_kategorie_key",
)

# COMMAND ----------

# DBTITLE 1,Transform -- surrogate key + Gold provenance
dim = dim.withColumn("mastr_katalog_wert_key", surrogate_key("Id"))
dim = add_gold_provenance(dim, SOURCE, RID)

# COMMAND ----------

# DBTITLE 1,Grain assertion -- one row per Id
assert_unique_grain(dim, ["Id"], component=COMPONENT, source=SOURCE, rid=RID)

# COMMAND ----------

# DBTITLE 1,Write Gold -- dim_mastr_katalog_wert
write_gold(dim, GOLD_TABLE, source=SOURCE, component=COMPONENT, rid=RID)

# COMMAND ----------

# DBTITLE 1,Inspect dim_mastr_katalog_wert + export findings
_findings_blocks = inspect_gold_table(
    dim,
    GOLD_TABLE,
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["Id"],
    df_before=_werte_silver,
    extra_checks={
        "unmatched_category_fk": dim.filter(
            F.col("mastr_katalog_kategorie_key").isNull()
        ).count(),
    },
)
write_gold_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__{GOLD_TABLE}",
    GOLD_TABLE,
    _findings_blocks,
)
