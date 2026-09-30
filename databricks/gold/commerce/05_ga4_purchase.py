# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # GOLD -- GA4_PURCHASE
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** `commerce_gold.ga4_purchase` -- every `ga4_event` row that
# MAGIC carries a `transaction_id`. Repeated transaction ids are kept as
# MAGIC separate events, never deduplicated. Grain: purchase event.

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
SOURCE = "ga4"
COMPONENT = "gold/commerce/ga4_purchase"
TABLE = "ga4_purchase"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = gold_run_id()

# COMMAND ----------

# DBTITLE 1,Read Silver
ga4_event = read_silver("ga4_event")

# COMMAND ----------

# DBTITLE 1,Filter to purchase events
ga4_purchase = ga4_event.filter(F.col("transaction_id").isNotNull()).select(
    "event_key",
    "transaction_id",
    "user_pseudo_id",
    "session_id",
    "event_timestamp_utc",
    "event_timestamp_project",
    "local_date",
    "purchase_revenue",
    "unique_items",
    "total_item_quantity",
    "geo_country",
    "source_system",
    "source_dataset",
    "source_record_id",
)
ga4_purchase = add_gold_provenance(ga4_purchase, SOURCE, rid)

# COMMAND ----------

# DBTITLE 1,Grain gate
assert_unique_grain(
    ga4_purchase, ["event_key"], component=COMPONENT, source=SOURCE, rid=rid
)

# COMMAND ----------

# DBTITLE 1,Write
write_gold(ga4_purchase, TABLE, source=SOURCE, component=COMPONENT, rid=rid)

# COMMAND ----------

# DBTITLE 1,Inspect + export findings
_blocks = inspect_gold_table(
    ga4_purchase,
    TABLE,
    source=SOURCE,
    component=COMPONENT,
    rid=rid,
    key_cols=["event_key"],
)
write_gold_findings(SOURCE, f"commerce__{TABLE}", TABLE, _blocks)
