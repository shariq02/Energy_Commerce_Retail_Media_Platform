# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # GOLD -- FACT CUSTOMER ACTIVITY EVENT
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Grain:** one row per `source_record_id`.
# MAGIC
# MAGIC **Sources:** `rees46_events` (Silver, commerce_silver).
# MAGIC
# MAGIC **Serves use case:** any Commerce use case needing REES46 event-grain
# MAGIC customer-activity data.
# MAGIC
# MAGIC **Purpose:** promote `rees46_events` to Gold, near pass-through --
# MAGIC `currency_unknown`, the category-hierarchy split, and the bot-burst /
# MAGIC category-ambiguity flags Silver already computed carry through unaltered.

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
SOURCE = "rees46"
COMPONENT = "gold/commerce/journey/03_fact_customer_activity_event"
RID = gold_run_id()
GOLD_TABLE = "fact_customer_activity_event"

# COMMAND ----------

# DBTITLE 1,Read Silver -- rees46_events
_events_silver = read_silver("rees46_events")

# COMMAND ----------

# DBTITLE 1,Transform -- surrogate key + Gold provenance
fact = _events_silver.withColumn(
    "customer_activity_event_key", surrogate_key("source_record_id")
)
fact = add_gold_provenance(fact, SOURCE, RID)

# COMMAND ----------

# DBTITLE 1,Grain assertion -- one row per source_record_id
assert_unique_grain(
    fact, ["source_record_id"], component=COMPONENT, source=SOURCE, rid=RID
)

# COMMAND ----------

# DBTITLE 1,Write Gold -- fact_customer_activity_event
write_gold(fact, GOLD_TABLE, source=SOURCE, component=COMPONENT, rid=RID)

# COMMAND ----------

# DBTITLE 1,Inspect fact_customer_activity_event + export findings
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
