# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # GOLD -- FACT WEB EVENT
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Grain:** one row per `source_record_id`.
# MAGIC
# MAGIC **Sources:** `ga4_events` (Silver, commerce_silver).
# MAGIC
# MAGIC **Serves use case:** any Commerce use case needing GA4 event-grain data.
# MAGIC
# MAGIC **Purpose:** promote `ga4_events` to Gold, near pass-through -- `items` /
# MAGIC `ecommerce` stay nested (Silver's own decision, unchanged here);
# MAGIC decomposing `items` into a transaction fact is `fact_ecommerce_item`'s job
# MAGIC (`02_fact_ecommerce_item.py`), not this table's.

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
SOURCE = "ga4"
COMPONENT = "gold/commerce/journey/01_fact_web_event"
RID = gold_run_id()
GOLD_TABLE = "fact_web_event"

# COMMAND ----------

# DBTITLE 1,Read Silver -- ga4_events
_events_silver = read_silver("ga4_events")

# COMMAND ----------

# DBTITLE 1,Transform -- surrogate key + Gold provenance
fact = _events_silver.withColumn("web_event_key", surrogate_key("source_record_id"))
fact = add_gold_provenance(fact, SOURCE, RID)

# COMMAND ----------

# DBTITLE 1,Grain assertion -- one row per source_record_id
assert_unique_grain(
    fact, ["source_record_id"], component=COMPONENT, source=SOURCE, rid=RID
)

# COMMAND ----------

# DBTITLE 1,Write Gold -- fact_web_event
write_gold(fact, GOLD_TABLE, source=SOURCE, component=COMPONENT, rid=RID)

# COMMAND ----------

# DBTITLE 1,Inspect fact_web_event + export findings
_findings_blocks = inspect_gold_table(
    fact,
    GOLD_TABLE,
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["source_record_id"],
    df_before=_events_silver,
)
write_gold_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__{GOLD_TABLE}",
    GOLD_TABLE,
    _findings_blocks,
)
