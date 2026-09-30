# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # GOLD -- GA4_EVENT + GA4_EVENT_ITEM + REES46_EVENT
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** `commerce_gold.ga4_event`, `ga4_event_item`, `rees46_event`
# MAGIC -- views over their Silver counterparts, already the right shape. GA4
# MAGIC and REES46 stay source-distinct (no shared identity, no merged event
# MAGIC model). Grain: event (`ga4_event_item`: event x item ordinal).

# COMMAND ----------

# DBTITLE 1,Shared Silver library
# MAGIC %run ../../silver/_silver_common

# COMMAND ----------

# DBTITLE 1,Gold shared library
# MAGIC %run ../_gold_common

# COMMAND ----------

# DBTITLE 1,Gold inspection library
# MAGIC %run ../_gold_inspect

# COMMAND ----------

# DBTITLE 1,Configuration
COMPONENT = "gold/commerce/event_views"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = gold_run_id()

# COMMAND ----------

# DBTITLE 1,ga4_event -- view
write_gold_view(f"SELECT * FROM {silver_fqn('ga4_event')}", "ga4_event", source="ga4")

# COMMAND ----------

# DBTITLE 1,ga4_event_item -- view
write_gold_view(
    f"SELECT * FROM {silver_fqn('ga4_event_item')}", "ga4_event_item", source="ga4"
)

# COMMAND ----------

# DBTITLE 1,rees46_event -- view
write_gold_view(
    f"SELECT * FROM {silver_fqn('rees46_event')}", "rees46_event", source="rees46"
)

# COMMAND ----------

# DBTITLE 1,Inspect + export findings
_blocks = inspect_gold_table(
    read_gold("ga4_event", source="ga4"),
    "ga4_event",
    source="ga4",
    component=COMPONENT,
    rid=rid,
    key_cols=["event_key"],
)
_blocks += inspect_gold_table(
    read_gold("ga4_event_item", source="ga4"),
    "ga4_event_item",
    source="ga4",
    component=COMPONENT,
    rid=rid,
    key_cols=["event_key", "item_ordinal"],
)
_blocks += inspect_gold_table(
    read_gold("rees46_event", source="rees46"),
    "rees46_event",
    source="rees46",
    component=COMPONENT,
    rid=rid,
    key_cols=["event_key"],
)
write_gold_findings(
    "commerce",
    "commerce__event_views",
    "ga4_event / ga4_event_item / rees46_event",
    _blocks,
)
