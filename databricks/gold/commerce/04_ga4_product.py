# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # GOLD -- GA4_PRODUCT
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** `commerce_gold.ga4_product` -- `ga4_event_item` rolled up to
# MAGIC one row per `item_id`, with a price range and last-seen attributes.
# MAGIC Grain: item_id.

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
COMPONENT = "gold/commerce/ga4_product"
TABLE = "ga4_product"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = gold_run_id()

# COMMAND ----------

# DBTITLE 1,Read Silver
ga4_event_item = read_silver("ga4_event_item")

# COMMAND ----------

# DBTITLE 1,Aggregate to one row per product
ga4_product = (
    ga4_event_item.filter(F.col("item_id").isNotNull())
    .groupBy("item_id")
    .agg(
        F.first("item_name", ignorenulls=True).alias("item_name"),
        F.first("item_category", ignorenulls=True).alias("item_category"),
        F.min("price").alias("min_price"),
        F.max("price").alias("max_price"),
        F.avg("price").alias("avg_price"),
        F.sum("quantity").alias("total_quantity"),
        F.sum("item_revenue").alias("total_revenue"),
        F.count(F.lit(1)).alias("appearance_count"),
        F.min("event_timestamp_utc").alias("first_seen"),
        F.max("event_timestamp_utc").alias("last_seen"),
        F.first("source_system", ignorenulls=True).alias("source_system"),
        F.first("source_dataset", ignorenulls=True).alias("source_dataset"),
    )
)
ga4_product = add_gold_provenance(ga4_product, SOURCE, rid)

# COMMAND ----------

# DBTITLE 1,Grain gate
assert_unique_grain(
    ga4_product, ["item_id"], component=COMPONENT, source=SOURCE, rid=rid
)

# COMMAND ----------

# DBTITLE 1,Write
write_gold(ga4_product, TABLE, source=SOURCE, component=COMPONENT, rid=rid)

# COMMAND ----------

# DBTITLE 1,Inspect + export findings
_blocks = inspect_gold_table(
    ga4_product,
    TABLE,
    source=SOURCE,
    component=COMPONENT,
    rid=rid,
    key_cols=["item_id"],
)
write_gold_findings(SOURCE, f"commerce__{TABLE}", TABLE, _blocks)
