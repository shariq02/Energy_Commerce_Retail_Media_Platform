# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # GOLD -- FACT_ACTOR_DELETION_EVENT
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Sources:** `mastr_actor_deletion_events` (Silver, energy_silver); `dim_market_actor` (Gold).
# MAGIC
# MAGIC **Serves use case:** any Energy use case needing the survivorship record
# MAGIC the live MaStR tables omit. One of 3 sibling notebooks in this folder,
# MAGIC split from a single `05_fact_unit_lifecycle_event.py` -- see the folder's
# MAGIC other files for the rest.
# MAGIC
# MAGIC **Purpose:** promote `mastr_actor_deletion_events` to Gold. Grain: one row per `market_actor_id`. Per `mappings/mastr.yml`, deletion/change events
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

# DBTITLE 1,Imports
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "mastr"
COMPONENT = "gold/energy/grid/fact_unit_lifecycle_event/02_fact_actor_deletion_event"
RID = gold_run_id()
GOLD_TABLE = "fact_actor_deletion_event"

# COMMAND ----------

# DBTITLE 1,Read Silver -- mastr_actor_deletion_events
_silver = read_silver("mastr_actor_deletion_events")

# COMMAND ----------

# DBTITLE 1,Read Gold -- dim_market_actor
_market_actor = read_gold("dim_market_actor", source="mastr")

# COMMAND ----------

# DBTITLE 1,Transform -- fact_actor_deletion_event
fact = resolve_fk(
    _silver,
    _market_actor,
    fact_key_cols=["market_actor_id"],
    dim_key_cols=["market_actor_id"],
    dim_surrogate_col="market_actor_key",
    output_col="matched_market_actor_key",
)
fact = fact.withColumn("actor_deletion_event_key", surrogate_key("market_actor_id"))
fact = add_gold_provenance(fact, SOURCE, RID)

# COMMAND ----------

# DBTITLE 1,Grain assertion -- one row per market_actor_id
assert_unique_grain(
    fact, ["market_actor_id"], component=COMPONENT, source=SOURCE, rid=RID
)

# COMMAND ----------

# DBTITLE 1,Write Gold -- fact_actor_deletion_event
write_gold(fact, GOLD_TABLE, source=SOURCE, component=COMPONENT, rid=RID)

# COMMAND ----------

# DBTITLE 1,Inspect fact_actor_deletion_event + export findings
_findings_blocks = inspect_gold_table(
    fact,
    GOLD_TABLE,
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["market_actor_id"],
    df_before=_silver,
    extra_checks={
        "matched_live_actor_count": fact.filter(
            F.col("matched_market_actor_key").isNotNull()
        ).count(),
    },
)
write_gold_findings(
    SOURCE,
    "fact_unit_lifecycle_event__fact_actor_deletion_event",
    GOLD_TABLE,
    _findings_blocks,
)
