# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # GOLD -- DIM PRODUCT GA4
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Grain:** one row per `item_id`, most recently observed attributes.
# MAGIC Current-state, no SCD. Only `item_context = 'product'` rows -- GA4
# MAGIC overloads `item_id` as a campaign id on promotion events.
# MAGIC
# MAGIC **Sources:** `ga4_items` (Silver, commerce_silver).
# MAGIC
# MAGIC **Serves use case:** GA4 product identity resolved once.
# MAGIC
# MAGIC **Purpose:** the GA4-scoped half of source-scoped product identity.
# MAGIC Never merged with `dim_product_rees46` -- disjoint identity spaces.

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
from pyspark.sql.window import Window

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "ga4"
COMPONENT = "gold/commerce/journey/04_dim_product_ga4"
RID = gold_run_id()
GOLD_TABLE = "dim_product_ga4"

# COMMAND ----------

# DBTITLE 1,Read Silver -- ga4_items
_items_silver = read_silver("ga4_items")

# COMMAND ----------

# DBTITLE 1,Transform -- product-context rows only
_product_items = _items_silver.filter(
    (F.col("item_context") == "product") & F.col("item_id").isNotNull()
)

# COMMAND ----------

# DBTITLE 1,Transform -- most recently observed attributes per item_id
_w_latest = Window.partitionBy("item_id").orderBy(
    F.desc("event_date"), F.desc("event_timestamp")
)
dim = (
    _product_items.withColumn("_recency_rank", F.row_number().over(_w_latest))
    .filter(F.col("_recency_rank") == 1)
    .select("item_id", "item_name", "item_category", "price")
)

# COMMAND ----------

# DBTITLE 1,Transform -- surrogate key + Gold provenance
dim = dim.withColumn("product_ga4_key", surrogate_key("item_id"))
dim = add_gold_provenance(dim, SOURCE, RID)

# COMMAND ----------

# DBTITLE 1,Grain assertion -- one row per item_id
assert_unique_grain(dim, ["item_id"], component=COMPONENT, source=SOURCE, rid=RID)

# COMMAND ----------

# DBTITLE 1,Write Gold -- dim_product_ga4
write_gold(dim, GOLD_TABLE, source=SOURCE, component=COMPONENT, rid=RID)

# COMMAND ----------

# DBTITLE 1,Inspect dim_product_ga4 + export findings
_findings_blocks = inspect_gold_table(
    dim,
    GOLD_TABLE,
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["item_id"],
    df_before=_product_items,
)
write_gold_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__{GOLD_TABLE}",
    GOLD_TABLE,
    _findings_blocks,
)
