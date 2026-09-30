# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # GOLD -- FACT REDISPATCH MEASURE
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Grain:** one row per `source_record_id`.
# MAGIC
# MAGIC **Sources:** `redispatch_measures` (Silver, energy_silver);
# MAGIC `dim_power_plant` (Gold, this run).
# MAGIC
# MAGIC **Serves use case:** any Energy use case needing redispatch measures,
# MAGIC optionally attributed to a matched power plant.
# MAGIC
# MAGIC **Purpose:** promote `redispatch_measures` to Gold. Silver already carries
# MAGIC `affected_unit_match_name` / `affected_unit_match_confidence` (an
# MAGIC exact-normalised-name match against `power_plant_list`, never an identity
# MAGIC join); this notebook resolves that match to `matched_power_plant_key`
# MAGIC (nullable) without upgrading its confidence. `plant_name` is not unique
# MAGIC in `dim_power_plant` (its own grain is `source_record_id`) -- an
# MAGIC ambiguous name is left unmatched (NULL) rather than fanning the join out
# MAGIC across every plant sharing that name.

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
SOURCE = "redispatch"
COMPONENT = "gold/energy/grid/04_fact_redispatch_measure"
RID = gold_run_id()
GOLD_TABLE = "fact_redispatch_measure"

# COMMAND ----------

# DBTITLE 1,Read Silver -- redispatch_measures
_redispatch_silver = read_silver("redispatch_measures")

# COMMAND ----------

# DBTITLE 1,Read Gold -- dim_power_plant
_power_plant = read_gold("dim_power_plant", source="power_plant_list")

# COMMAND ----------

# DBTITLE 1,Transform -- unique plant names only (never fan out on a shared name)
_name_counts = _power_plant.groupBy("plant_name").agg(
    F.count(F.lit(1)).alias("_plant_name_count")
)
_power_plant_unique_name = _power_plant.join(
    _name_counts.filter(F.col("_plant_name_count") == 1).select("plant_name"),
    "plant_name",
    "inner",
)

# COMMAND ----------

# DBTITLE 1,Transform -- resolve the matched power plant (soft match, unchanged confidence)
fact = resolve_fk(
    _redispatch_silver,
    _power_plant_unique_name,
    fact_key_cols=["affected_unit_match_name"],
    dim_key_cols=["plant_name"],
    dim_surrogate_col="power_plant_key",
    output_col="matched_power_plant_key",
)

# COMMAND ----------

# DBTITLE 1,Transform -- surrogate key + Gold provenance
fact = fact.withColumn("redispatch_measure_key", surrogate_key("source_record_id"))
fact = add_gold_provenance(fact, SOURCE, RID)

# COMMAND ----------

# DBTITLE 1,Grain assertion -- one row per source_record_id
assert_unique_grain(
    fact, ["source_record_id"], component=COMPONENT, source=SOURCE, rid=RID
)

# COMMAND ----------

# DBTITLE 1,Write Gold -- fact_redispatch_measure
write_gold(fact, GOLD_TABLE, source=SOURCE, component=COMPONENT, rid=RID)

# COMMAND ----------

# DBTITLE 1,Inspect fact_redispatch_measure + export findings
_findings_blocks = inspect_gold_table(
    fact,
    GOLD_TABLE,
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["source_record_id"],
    df_before=_redispatch_silver,
    extra_checks={
        "power_plant_matched_count": fact.filter(
            F.col("matched_power_plant_key").isNotNull()
        ).count(),
        "ambiguous_plant_names_excluded": _name_counts.filter(
            F.col("_plant_name_count") > 1
        ).count(),
    },
)
write_gold_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__{GOLD_TABLE}",
    GOLD_TABLE,
    _findings_blocks,
)
