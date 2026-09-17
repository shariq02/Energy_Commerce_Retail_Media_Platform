# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # GOLD -- UNIT/ACTOR LIFECYCLE EVENT FACTS
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Grain:** `fact_unit_deletion_event` -- one row per `unit_id`.
# MAGIC `fact_actor_deletion_event` -- one row per `market_actor_id`.
# MAGIC `fact_grid_operator_change_event` -- one row per (`unit_id`,
# MAGIC `grid_operator_change_effective_date`).
# MAGIC
# MAGIC **Sources:** `mastr_unit_deletion_events`, `mastr_actor_deletion_events`,
# MAGIC `mastr_grid_operator_change_events` (Silver, energy_silver);
# MAGIC `dim_generation_unit`, `dim_market_actor` (Gold, this run).
# MAGIC
# MAGIC **Serves use case:** any Energy use case needing the survivorship record
# MAGIC the live MaStR tables omit -- unit/actor departures, grid-operator
# MAGIC reassignments.
# MAGIC
# MAGIC **Purpose:** promote the three MaStR change-log Silver tables to Gold,
# MAGIC append-only. Per `mappings/mastr.yml`, deletion events are DISJOINT from
# MAGIC the live dimensions by design -- `matched_*_key` is expected to be mostly
# MAGIC or entirely NULL, resolved only to confirm a departure, never to backfill
# MAGIC the dimension. These facts are NEVER merged back into `dim_generation_unit`
# MAGIC / `dim_market_actor`.

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
COMPONENT = "gold/energy/grid/05_fact_unit_lifecycle_event"
RID = gold_run_id()

# COMMAND ----------

# DBTITLE 1,Read Silver -- mastr_unit_deletion_events
_unit_deletion_silver = read_silver("mastr_unit_deletion_events")

# COMMAND ----------

# DBTITLE 1,Read Silver -- mastr_actor_deletion_events
_actor_deletion_silver = read_silver("mastr_actor_deletion_events")

# COMMAND ----------

# DBTITLE 1,Read Silver -- mastr_grid_operator_change_events
_grid_operator_change_silver = read_silver("mastr_grid_operator_change_events")

# COMMAND ----------

# DBTITLE 1,Read Gold -- dim_generation_unit
_gen_unit_keys = read_gold("dim_generation_unit", source="mastr").select(
    F.col("unit_id").alias("_gu_unit_id"),
    F.col("generation_unit_key").alias("_gu_key"),
)

# COMMAND ----------

# DBTITLE 1,Read Gold -- dim_market_actor
_market_actor_keys = read_gold("dim_market_actor", source="mastr").select(
    F.col("market_actor_id").alias("_ma_id"),
    F.col("market_actor_key").alias("_ma_key"),
)

# COMMAND ----------

# DBTITLE 1,Transform -- fact_unit_deletion_event
fact_unit_deletion_event = (
    _unit_deletion_silver.join(
        _gen_unit_keys, _unit_deletion_silver["unit_id"] == F.col("_gu_unit_id"), "left"
    )
    .withColumn("matched_generation_unit_key", F.col("_gu_key"))
    .drop("_gu_unit_id", "_gu_key")
)
fact_unit_deletion_event = fact_unit_deletion_event.withColumn(
    "unit_deletion_event_key", surrogate_key("unit_id")
)
fact_unit_deletion_event = add_gold_provenance(fact_unit_deletion_event, SOURCE, RID)

# COMMAND ----------

# DBTITLE 1,Grain assertion -- fact_unit_deletion_event, one row per unit_id
assert_unique_grain(
    fact_unit_deletion_event,
    ["unit_id"],
    component=COMPONENT,
    source=SOURCE,
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Write Gold -- fact_unit_deletion_event
write_gold(
    fact_unit_deletion_event,
    "fact_unit_deletion_event",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Inspect fact_unit_deletion_event + export findings
_findings_blocks = inspect_gold_table(
    fact_unit_deletion_event,
    "fact_unit_deletion_event",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["unit_id"],
    df_before=_unit_deletion_silver,
    extra_checks={
        "matched_live_unit_count": fact_unit_deletion_event.filter(
            F.col("matched_generation_unit_key").isNotNull()
        ).count(),
    },
)
write_gold_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__fact_unit_deletion_event",
    "fact_unit_deletion_event",
    _findings_blocks,
)

# COMMAND ----------

# DBTITLE 1,Transform -- fact_actor_deletion_event
fact_actor_deletion_event = (
    _actor_deletion_silver.join(
        _market_actor_keys,
        _actor_deletion_silver["market_actor_id"] == F.col("_ma_id"),
        "left",
    )
    .withColumn("matched_market_actor_key", F.col("_ma_key"))
    .drop("_ma_id", "_ma_key")
)
fact_actor_deletion_event = fact_actor_deletion_event.withColumn(
    "actor_deletion_event_key", surrogate_key("market_actor_id")
)
fact_actor_deletion_event = add_gold_provenance(fact_actor_deletion_event, SOURCE, RID)

# COMMAND ----------

# DBTITLE 1,Grain assertion -- fact_actor_deletion_event, one row per market_actor_id
assert_unique_grain(
    fact_actor_deletion_event,
    ["market_actor_id"],
    component=COMPONENT,
    source=SOURCE,
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Write Gold -- fact_actor_deletion_event
write_gold(
    fact_actor_deletion_event,
    "fact_actor_deletion_event",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Inspect fact_actor_deletion_event + export findings
_findings_blocks = inspect_gold_table(
    fact_actor_deletion_event,
    "fact_actor_deletion_event",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["market_actor_id"],
    df_before=_actor_deletion_silver,
    extra_checks={
        "matched_live_actor_count": fact_actor_deletion_event.filter(
            F.col("matched_market_actor_key").isNotNull()
        ).count(),
    },
)
write_gold_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__fact_actor_deletion_event",
    "fact_actor_deletion_event",
    _findings_blocks,
)

# COMMAND ----------

# DBTITLE 1,Transform -- fact_grid_operator_change_event
fact_grid_operator_change_event = (
    _grid_operator_change_silver.join(
        _gen_unit_keys,
        _grid_operator_change_silver["unit_id"] == F.col("_gu_unit_id"),
        "left",
    )
    .withColumn("matched_generation_unit_key", F.col("_gu_key"))
    .drop("_gu_unit_id", "_gu_key")
)
fact_grid_operator_change_event = fact_grid_operator_change_event.withColumn(
    "grid_operator_change_event_key",
    surrogate_key("unit_id", "grid_operator_change_effective_date"),
)
fact_grid_operator_change_event = add_gold_provenance(
    fact_grid_operator_change_event, SOURCE, RID
)

# COMMAND ----------

# DBTITLE 1,Grain assertion -- fact_grid_operator_change_event
assert_unique_grain(
    fact_grid_operator_change_event,
    ["unit_id", "grid_operator_change_effective_date"],
    component=COMPONENT,
    source=SOURCE,
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Write Gold -- fact_grid_operator_change_event
write_gold(
    fact_grid_operator_change_event,
    "fact_grid_operator_change_event",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Inspect fact_grid_operator_change_event + export findings
_findings_blocks = inspect_gold_table(
    fact_grid_operator_change_event,
    "fact_grid_operator_change_event",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["unit_id", "grid_operator_change_effective_date"],
    df_before=_grid_operator_change_silver,
)
write_gold_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__fact_grid_operator_change_event",
    "fact_grid_operator_change_event",
    _findings_blocks,
)
