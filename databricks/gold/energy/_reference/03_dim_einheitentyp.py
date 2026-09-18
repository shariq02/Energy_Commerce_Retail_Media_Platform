# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # GOLD -- DIM EINHEITENTYP
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Grain:** one row per `Id`. Static reference, no SCD.
# MAGIC
# MAGIC **Sources:** `mastr_einheitentypen` (Silver, energy_silver_reference).
# MAGIC
# MAGIC **Serves use case:** the MaStR unit-type vocabulary, governed once.
# MAGIC
# MAGIC **Purpose:** promote `mastr_einheitentypen` to Gold.

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
COMPONENT = "gold/energy/_reference/03_dim_einheitentyp"
RID = gold_run_id()
GOLD_TABLE = "dim_einheitentyp"

# COMMAND ----------

# DBTITLE 1,Read Silver -- mastr_einheitentypen
_einheitentypen_silver = read_silver("mastr_einheitentypen")

# COMMAND ----------

# DBTITLE 1,Transform -- surrogate key + Gold provenance
dim = _einheitentypen_silver.withColumn("einheitentyp_key", surrogate_key("Id"))
dim = add_gold_provenance(dim, SOURCE, RID)

# COMMAND ----------

# DBTITLE 1,Grain assertion -- one row per Id
assert_unique_grain(dim, ["Id"], component=COMPONENT, source=SOURCE, rid=RID)

# COMMAND ----------

# DBTITLE 1,Write Gold -- dim_einheitentyp
write_gold(dim, GOLD_TABLE, source=SOURCE, component=COMPONENT, rid=RID)

# COMMAND ----------

# DBTITLE 1,Inspect dim_einheitentyp + export findings
_findings_blocks = inspect_gold_table(
    dim,
    GOLD_TABLE,
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["Id"],
    df_before=_einheitentypen_silver,
)
write_gold_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__{GOLD_TABLE}",
    GOLD_TABLE,
    _findings_blocks,
)
