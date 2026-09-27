# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # GOLD -- DIM PRODUCT REES46
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Grain:** one row per `product_id`, most recently observed attributes.
# MAGIC Current-state, no SCD.
# MAGIC
# MAGIC **Sources:** `rees46_events` (Silver, commerce_silver).
# MAGIC
# MAGIC **Serves use case:** REES46 product identity resolved once.
# MAGIC
# MAGIC **Purpose:** the REES46-scoped half of source-scoped product identity.
# MAGIC Never merged with `dim_product_ga4` -- disjoint identity spaces.

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
SOURCE = "rees46"
COMPONENT = "gold/commerce/journey/05_dim_product_rees46"
RID = gold_run_id()
GOLD_TABLE = "dim_product_rees46"

# COMMAND ----------

# DBTITLE 1,Read Silver -- rees46_events
_events_silver = read_silver("rees46_events")

# COMMAND ----------

# DBTITLE 1,Transform -- most recently observed attributes per product_id
_products = _events_silver.filter(F.col("product_id").isNotNull())
_w_latest = Window.partitionBy("product_id").orderBy(F.desc("event_time"))
dim = (
    _products.withColumn("_recency_rank", F.row_number().over(_w_latest))
    .filter(F.col("_recency_rank") == 1)
    .select("product_id", "category_id", "category_l1", "brand", "price")
)

# COMMAND ----------

# DBTITLE 1,Transform -- surrogate key + Gold provenance
dim = dim.withColumn("product_rees46_key", surrogate_key("product_id"))
dim = add_gold_provenance(dim, SOURCE, RID)

# COMMAND ----------

# DBTITLE 1,Grain assertion -- one row per product_id
assert_unique_grain(dim, ["product_id"], component=COMPONENT, source=SOURCE, rid=RID)

# COMMAND ----------

# DBTITLE 1,Write Gold -- dim_product_rees46
write_gold(dim, GOLD_TABLE, source=SOURCE, component=COMPONENT, rid=RID)

# COMMAND ----------

# DBTITLE 1,Inspect dim_product_rees46 + export findings
_findings_blocks = inspect_gold_table(
    dim,
    GOLD_TABLE,
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["product_id"],
    df_before=_products,
)
write_gold_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__{GOLD_TABLE}",
    GOLD_TABLE,
    _findings_blocks,
)
