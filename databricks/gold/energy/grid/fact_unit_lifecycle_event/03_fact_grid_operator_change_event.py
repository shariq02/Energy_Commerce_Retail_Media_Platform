# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # GOLD -- FACT_GRID_OPERATOR_CHANGE_EVENT
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Sources:** `mastr_grid_operator_change_events` (Silver, energy_silver); `dim_generation_unit` (Gold).
# MAGIC
# MAGIC **Serves use case:** any Energy use case needing the survivorship record
# MAGIC the live MaStR tables omit. One of 3 sibling notebooks in this folder,
# MAGIC split from a single `05_fact_unit_lifecycle_event.py` -- see the folder's
# MAGIC other files for the rest.
# MAGIC
# MAGIC **Purpose:** promote `mastr_grid_operator_change_events` to Gold. Grain:
# MAGIC one row per `source_record_id` -- Silver's own grain, since
# MAGIC `(unit_id, grid_operator_change_effective_date)` is documented `unique:
# MAGIC false` there (`03_mastr_change_logs.py`'s content-hash ordinal), so a
# MAGIC unit can genuinely log more than one change on the same effective date.
# MAGIC Per `mappings/mastr.yml`, deletion/change events
# MAGIC are DISJOINT from the live dimensions by design -- `matched_*_key` is
# MAGIC expected to be mostly or entirely NULL, resolved only to confirm a
# MAGIC departure, never to backfill the dimension. Append-only -- NEVER merged
# MAGIC back into the live dimension.

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
COMPONENT = (
    "gold/energy/grid/fact_unit_lifecycle_event/03_fact_grid_operator_change_event"
)
RID = gold_run_id()
GOLD_TABLE = "fact_grid_operator_change_event"

# COMMAND ----------

# DBTITLE 1,Read Silver -- mastr_grid_operator_change_events
_silver = read_silver("mastr_grid_operator_change_events")

# COMMAND ----------

# DBTITLE 1,Read Gold -- dim_generation_unit
_gen_unit = read_gold("dim_generation_unit", source="mastr")

# COMMAND ----------

# DBTITLE 1,Transform -- fact_grid_operator_change_event
fact = resolve_fk(
    _silver,
    _gen_unit,
    fact_key_cols=["unit_id"],
    dim_key_cols=["unit_id"],
    dim_surrogate_col="generation_unit_key",
    output_col="matched_generation_unit_key",
)
fact = fact.withColumn(
    "grid_operator_change_event_key", surrogate_key("source_record_id")
)
fact = add_gold_provenance(fact, SOURCE, RID)

# COMMAND ----------

# DBTITLE 1,Grain assertion -- one row per source_record_id
assert_unique_grain(
    fact, ["source_record_id"], component=COMPONENT, source=SOURCE, rid=RID
)

# COMMAND ----------

# DBTITLE 1,Write Gold -- fact_grid_operator_change_event
write_gold(fact, GOLD_TABLE, source=SOURCE, component=COMPONENT, rid=RID)

# COMMAND ----------

# DBTITLE 1,Inspect fact_grid_operator_change_event + export findings
_findings_blocks = inspect_gold_table(
    fact,
    GOLD_TABLE,
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["source_record_id"],
    df_before=_silver,
)
write_gold_findings(
    SOURCE,
    "fact_unit_lifecycle_event__fact_grid_operator_change_event",
    GOLD_TABLE,
    _findings_blocks,
)
