# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # GOLD -- FACT ECOMMERCE ITEM
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Grain:** one row per `source_record_id`.
# MAGIC
# MAGIC **Sources:** `ga4_items` (Silver, commerce_silver);
# MAGIC `fact_web_event` (Gold, this run, for the parent-event FK).
# MAGIC
# MAGIC **Serves use case:** any Commerce use case needing item-grain GA4 data
# MAGIC (promotion or product), event x item.
# MAGIC
# MAGIC **Purpose:** promote `ga4_items` to Gold and resolve `web_event_key`, the
# MAGIC FK to its parent `fact_web_event` row (via the shared event natural key).
# MAGIC `item_context` ('promotion' | 'product') stays a row attribute -- a
# MAGIC conformed `dim_product` is future Gold work, not built here.

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
SOURCE = "ga4"
COMPONENT = "gold/commerce/journey/02_fact_ecommerce_item"
RID = gold_run_id()
GOLD_TABLE = "fact_ecommerce_item"
EVENT_NATURAL_KEY = ["event_date", "event_timestamp", "user_pseudo_id", "event_name"]

# COMMAND ----------

# DBTITLE 1,Read Silver -- ga4_items
_items_silver = read_silver("ga4_items")

# COMMAND ----------

# DBTITLE 1,Read Gold -- fact_web_event
_web_event = read_gold("fact_web_event", source="ga4")

# COMMAND ----------

# DBTITLE 1,Transform -- resolve the parent web-event key
fact = resolve_fk(
    _items_silver,
    _web_event,
    fact_key_cols=EVENT_NATURAL_KEY,
    dim_key_cols=EVENT_NATURAL_KEY,
    dim_surrogate_col="web_event_key",
    output_col="web_event_key",
)

# COMMAND ----------

# DBTITLE 1,Transform -- surrogate key + Gold provenance
fact = fact.withColumn("ecommerce_item_key", surrogate_key("source_record_id"))
fact = add_gold_provenance(fact, SOURCE, RID)

# COMMAND ----------

# DBTITLE 1,Grain assertion -- one row per source_record_id
assert_unique_grain(
    fact, ["source_record_id"], component=COMPONENT, source=SOURCE, rid=RID
)

# COMMAND ----------

# DBTITLE 1,Write Gold -- fact_ecommerce_item
write_gold(fact, GOLD_TABLE, source=SOURCE, component=COMPONENT, rid=RID)

# COMMAND ----------

# DBTITLE 1,Inspect fact_ecommerce_item + export findings
_findings_blocks = inspect_gold_table(
    fact,
    GOLD_TABLE,
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["source_record_id"],
    df_before=_items_silver,
    extra_checks={
        "web_event_key_unmatched_count": fact.filter(
            F.col("web_event_key").isNull()
        ).count(),
    },
)
write_gold_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__{GOLD_TABLE}",
    GOLD_TABLE,
    _findings_blocks,
)
