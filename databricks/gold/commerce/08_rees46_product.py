# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # GOLD -- REES46_PRODUCT
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** `commerce_gold.rees46_product` -- `rees46_event` rolled up
# MAGIC to one row per `product_id`, with a price range. Grain: product_id.

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
SOURCE = "rees46"
COMPONENT = "gold/commerce/rees46_product"
TABLE = "rees46_product"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = gold_run_id()

# COMMAND ----------

# DBTITLE 1,Read Silver
rees46_event = read_silver("rees46_event")

# COMMAND ----------

# DBTITLE 1,Aggregate to one row per product
rees46_product = (
    rees46_event.filter(F.col("product_id").isNotNull())
    .groupBy("product_id")
    .agg(
        F.first("category_id", ignorenulls=True).alias("category_id"),
        F.first("category_code", ignorenulls=True).alias("category_code"),
        F.first("category_l1", ignorenulls=True).alias("category_l1"),
        F.first("category_l2", ignorenulls=True).alias("category_l2"),
        F.first("category_l3", ignorenulls=True).alias("category_l3"),
        F.first("brand", ignorenulls=True).alias("brand"),
        F.min("price").alias("min_price"),
        F.max("price").alias("max_price"),
        F.avg("price").alias("avg_price"),
        F.count(F.lit(1)).alias("appearance_count"),
        F.countDistinct("user_id").alias("distinct_user_count"),
        F.min("event_timestamp_utc").alias("first_seen"),
        F.max("event_timestamp_utc").alias("last_seen"),
        F.first("source_system", ignorenulls=True).alias("source_system"),
        F.first("source_dataset", ignorenulls=True).alias("source_dataset"),
    )
)
rees46_product = add_gold_provenance(rees46_product, SOURCE, rid)

# COMMAND ----------

# DBTITLE 1,Grain gate
assert_unique_grain(
    rees46_product, ["product_id"], component=COMPONENT, source=SOURCE, rid=rid
)

# COMMAND ----------

# DBTITLE 1,Write
write_gold(rees46_product, TABLE, source=SOURCE, component=COMPONENT, rid=rid)

# COMMAND ----------

# DBTITLE 1,Inspect + export findings
_blocks = inspect_gold_table(
    rees46_product,
    TABLE,
    source=SOURCE,
    component=COMPONENT,
    rid=rid,
    key_cols=["product_id"],
)
write_gold_findings(SOURCE, f"commerce__{TABLE}", TABLE, _blocks)
