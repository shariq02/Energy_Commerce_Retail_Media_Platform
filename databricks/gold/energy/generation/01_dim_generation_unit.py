# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # GOLD -- DIM GENERATION UNIT
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Grain:** one row per `unit_id` (`EinheitMastrNummer`).
# MAGIC
# MAGIC **Sources:** `mastr_einheiten_wind`, `mastr_einheiten_biomasse`,
# MAGIC `mastr_einheiten_wasser`, `mastr_einheiten_verbrennung`,
# MAGIC `mastr_einheiten_kernkraft`, `mastr_einheiten_geothermie_gsgk` (Silver,
# MAGIC energy_silver).
# MAGIC
# MAGIC **Serves use case:** any Energy use case needing a single generation-unit
# MAGIC entity regardless of fuel/carrier type.
# MAGIC
# MAGIC **Purpose:** conform the six MaStR carrier-specific Silver tables into
# MAGIC one dimension -- fuel type becomes the `generation_unit_type` attribute,
# MAGIC not a table split. `EinheitMastrNummer` is a single global identifier
# MAGIC across every carrier table in MaStR's own numbering, so this is a union,
# MAGIC never a reconciliation join. Carrier-specific columns that only one
# MAGIC source table carries stay nullable on the conformed dimension.

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
COMPONENT = "gold/energy/generation/01_dim_generation_unit"
RID = gold_run_id()
GOLD_TABLE = "dim_generation_unit"

# COMMAND ----------

# DBTITLE 1,Read Silver -- mastr_einheiten_wind
_wind_silver = read_silver("mastr_einheiten_wind")

# COMMAND ----------

# DBTITLE 1,Read Silver -- mastr_einheiten_biomasse
_biomasse_silver = read_silver("mastr_einheiten_biomasse")

# COMMAND ----------

# DBTITLE 1,Read Silver -- mastr_einheiten_wasser
_wasser_silver = read_silver("mastr_einheiten_wasser")

# COMMAND ----------

# DBTITLE 1,Read Silver -- mastr_einheiten_verbrennung
_verbrennung_silver = read_silver("mastr_einheiten_verbrennung")

# COMMAND ----------

# DBTITLE 1,Read Silver -- mastr_einheiten_kernkraft
_kernkraft_silver = read_silver("mastr_einheiten_kernkraft")

# COMMAND ----------

# DBTITLE 1,Read Silver -- mastr_einheiten_geothermie_gsgk
_geothermie_gsgk_silver = read_silver("mastr_einheiten_geothermie_gsgk")

# COMMAND ----------

# DBTITLE 1,Transform -- tag each carrier table with generation_unit_type
_wind = _wind_silver.withColumn("generation_unit_type", F.lit("wind"))
_biomasse = _biomasse_silver.withColumn("generation_unit_type", F.lit("biomasse"))
_wasser = _wasser_silver.withColumn("generation_unit_type", F.lit("wasser"))
_verbrennung = _verbrennung_silver.withColumn(
    "generation_unit_type", F.lit("verbrennung")
)
_kernkraft = _kernkraft_silver.withColumn("generation_unit_type", F.lit("kernkraft"))
_geothermie_gsgk = _geothermie_gsgk_silver.withColumn(
    "generation_unit_type", F.lit("geothermie_gsgk")
)

# COMMAND ----------

# DBTITLE 1,Transform -- union the six carrier tables (conformed by name)
dim = (
    _wind.unionByName(_biomasse, allowMissingColumns=True)
    .unionByName(_wasser, allowMissingColumns=True)
    .unionByName(_verbrennung, allowMissingColumns=True)
    .unionByName(_kernkraft, allowMissingColumns=True)
    .unionByName(_geothermie_gsgk, allowMissingColumns=True)
)

# COMMAND ----------

# DBTITLE 1,Transform -- surrogate key + Gold provenance
dim = dim.withColumn("generation_unit_key", surrogate_key("unit_id"))
dim = add_gold_provenance(dim, SOURCE, RID)

# COMMAND ----------

# DBTITLE 1,Grain assertion -- one row per unit_id
assert_unique_grain(dim, ["unit_id"], component=COMPONENT, source=SOURCE, rid=RID)

# COMMAND ----------

# DBTITLE 1,Write Gold -- dim_generation_unit
write_gold(dim, GOLD_TABLE, source=SOURCE, component=COMPONENT, rid=RID)

# COMMAND ----------

# DBTITLE 1,Inspect dim_generation_unit + export findings
_findings_blocks = inspect_gold_table(
    dim,
    GOLD_TABLE,
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["unit_id"],
    extra_checks={
        "unit_count_by_type": dim.groupBy("generation_unit_type").count().collect(),
    },
)
write_gold_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__{GOLD_TABLE}",
    GOLD_TABLE,
    _findings_blocks,
)
