# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # GOLD -- FACT LOCATION COORDINATE CONFLICT
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Grain:** one row per `location_id`.
# MAGIC
# MAGIC **Sources:** `mastr_location_coordinate_conflict` (Silver,
# MAGIC energy_silver -- written outside `_reference/`, so it takes the plain
# MAGIC ecosystem-default schema, not `_reference`); `dim_location` (Gold,
# MAGIC `dim_location/01_dim_location.py`).
# MAGIC
# MAGIC **Serves use case:** any Energy use case needing the coordinate-
# MAGIC agreement signal Silver already computed across a location's linked
# MAGIC generation units -- was previously a DQ table with no Gold consumer at
# MAGIC all.
# MAGIC
# MAGIC **Purpose:** promote `mastr_location_coordinate_conflict` to Gold and
# MAGIC resolve `location_key`, the FK to `dim_location`. Never picks a
# MAGIC "correct" coordinate -- `_coordinate_conflict` / `distinct_coords` carry
# MAGIC through unaltered, same as Silver recorded them.

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
COMPONENT = "gold/energy/grid/05_fact_location_coordinate_conflict"
RID = gold_run_id()
GOLD_TABLE = "fact_location_coordinate_conflict"

# COMMAND ----------

# DBTITLE 1,Read Silver -- mastr_location_coordinate_conflict
_conflict_silver = read_silver("mastr_location_coordinate_conflict")

# COMMAND ----------

# DBTITLE 1,Read Gold -- dim_location
_location = read_gold("dim_location", source="mastr")

# COMMAND ----------

# DBTITLE 1,Transform -- resolve the location key
fact = resolve_fk(
    _conflict_silver,
    _location,
    fact_key_cols=["location_id"],
    dim_key_cols=["location_id"],
    dim_surrogate_col="location_key",
    output_col="location_key",
)

# COMMAND ----------

# DBTITLE 1,Transform -- surrogate key + Gold provenance
fact = fact.withColumn("location_coordinate_conflict_key", surrogate_key("location_id"))
fact = add_gold_provenance(fact, SOURCE, RID)

# COMMAND ----------

# DBTITLE 1,Grain assertion -- one row per location_id
assert_unique_grain(fact, ["location_id"], component=COMPONENT, source=SOURCE, rid=RID)

# COMMAND ----------

# DBTITLE 1,Write Gold -- fact_location_coordinate_conflict
write_gold(fact, GOLD_TABLE, source=SOURCE, component=COMPONENT, rid=RID)

# COMMAND ----------

# DBTITLE 1,Inspect fact_location_coordinate_conflict + export findings
_findings_blocks = inspect_gold_table(
    fact,
    GOLD_TABLE,
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["location_id"],
    df_before=_conflict_silver,
    extra_checks={
        "location_key_unmatched_count": fact.filter(
            F.col("location_key").isNull()
        ).count(),
        "conflicting_location_count": fact.filter(
            F.col("_coordinate_conflict")
        ).count(),
    },
)
write_gold_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__{GOLD_TABLE}",
    GOLD_TABLE,
    _findings_blocks,
)
