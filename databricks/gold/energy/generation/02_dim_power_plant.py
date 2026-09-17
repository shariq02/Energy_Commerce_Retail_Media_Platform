# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # GOLD -- DIM POWER PLANT
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Grain:** one row per `source_record_id` (power_plant_list's own
# MAGIC deterministic key).
# MAGIC
# MAGIC **Sources:** `power_plant_list` (Silver, energy_silver);
# MAGIC `dim_generation_unit` (Gold, this run).
# MAGIC
# MAGIC **Serves use case:** any Energy use case needing BNetzA power-plant-list
# MAGIC entities, with an explicit (non-identity) link to MaStR generation units.
# MAGIC
# MAGIC **Purpose:** promote `power_plant_list` to Gold. The `mastr_unit_id`
# MAGIC reconciliation against `dim_generation_unit` stays a soft match --
# MAGIC `matched_generation_unit_key` is nullable and `generation_unit_match_type`
# MAGIC records how (or whether) it resolved. Never an identity merge -- Silver
# MAGIC was explicit this link is not a trustworthy join key.

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
SOURCE = "power_plant_list"
COMPONENT = "gold/energy/generation/02_dim_power_plant"
RID = gold_run_id()
GOLD_TABLE = "dim_power_plant"

# COMMAND ----------

# DBTITLE 1,Read Silver -- power_plant_list
_plant_silver = read_silver("power_plant_list")

# COMMAND ----------

# DBTITLE 1,Read Gold -- dim_generation_unit
_gen_unit = read_gold("dim_generation_unit", source="mastr")

# COMMAND ----------

# DBTITLE 1,Transform -- resolve the MaStR reconciliation link (soft match)
dim = resolve_fk(
    _plant_silver,
    _gen_unit,
    fact_key_cols=["mastr_unit_id"],
    dim_key_cols=["unit_id"],
    dim_surrogate_col="generation_unit_key",
    output_col="matched_generation_unit_key",
)
dim = dim.withColumn(
    "generation_unit_match_type",
    F.when(F.col("mastr_unit_id").isNull(), F.lit("no_mastr_unit_id"))
    .when(
        F.col("matched_generation_unit_key").isNotNull(), F.lit("mastr_unit_id_matched")
    )
    .otherwise(F.lit("mastr_unit_id_unmatched")),
)

# COMMAND ----------

# DBTITLE 1,Transform -- surrogate key + Gold provenance
dim = dim.withColumn("power_plant_key", surrogate_key("source_record_id"))
dim = add_gold_provenance(dim, SOURCE, RID)

# COMMAND ----------

# DBTITLE 1,Grain assertion -- one row per source_record_id
assert_unique_grain(
    dim, ["source_record_id"], component=COMPONENT, source=SOURCE, rid=RID
)

# COMMAND ----------

# DBTITLE 1,Write Gold -- dim_power_plant
write_gold(dim, GOLD_TABLE, source=SOURCE, component=COMPONENT, rid=RID)

# COMMAND ----------

# DBTITLE 1,Inspect dim_power_plant + export findings
_findings_blocks = inspect_gold_table(
    dim,
    GOLD_TABLE,
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["source_record_id"],
    df_before=_plant_silver,
    extra_checks={
        "generation_unit_matched_count": dim.filter(
            F.col("generation_unit_match_type") == "mastr_unit_id_matched"
        ).count(),
    },
)
write_gold_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__{GOLD_TABLE}",
    GOLD_TABLE,
    _findings_blocks,
)
