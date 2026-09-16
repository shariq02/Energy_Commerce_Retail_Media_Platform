# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER -- GA4 ITEMS (ADDITIVE, EVENT x ITEM GRAIN)
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** decompose `ga4_events.items` into a source-scoped, additive
# MAGIC item-grain table -- `item_id` is overloaded (a campaign id on promotion
# MAGIC events, a product id otherwise), only separable once exploded.
# MAGIC `ga4_events.items` itself is untouched; that struct stays Silver-correct,
# MAGIC decomposing it into a transaction fact is Gold's job.

# COMMAND ----------

# DBTITLE 1,Shared library
# MAGIC %run ../../_silver_common

# COMMAND ----------

# DBTITLE 1,Inspection library
# MAGIC %run ../../_silver_inspect

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "ga4"
COMPONENT = "silver/commerce/journey/03_ga4_items"
RID = run_id()

BT = "ga4_events"
KEY_COLS = [
    "event_date",
    "event_timestamp",
    "user_pseudo_id",
    "event_name",
    "item_ordinal",
]

UNSET = ("(not set)", "(none)", "")
_PROMOTION_EVENTS = ("view_promotion", "select_promotion")

# COMMAND ----------

# DBTITLE 1,Helper -- clean a scalar string sentinel (definition only)


def _clean_str(col: F.Column) -> F.Column:
    return F.when(col.isin(*UNSET), F.lit(None)).otherwise(col)


# COMMAND ----------

# DBTITLE 1,Read Bronze -- ga4_events
bronze_df = read_bronze(BT)

# COMMAND ----------

# DBTITLE 1,Transform -- ga4_items (explode + select)
items_df = (
    bronze_df.select(
        "event_date",
        "event_timestamp",
        "user_pseudo_id",
        "event_name",
        F.posexplode_outer("items").alias("item_ordinal", "item"),
    )
    .filter(F.col("item").isNotNull())
    .select(
        "event_date",
        "event_timestamp",
        "user_pseudo_id",
        "event_name",
        "item_ordinal",
        F.col("item.item_id").alias("item_id"),
        F.col("item.item_name").alias("item_name"),
        F.col("item.item_category").alias("item_category"),
        F.col("item.price").alias("price"),
        F.col("item.quantity").alias("quantity"),
        F.col("item.item_revenue").alias("item_revenue"),
    )
)

# COMMAND ----------

# DBTITLE 1,Transform -- ga4_items (sentinel normalisation)
# GA4's own "(not set)"/"(none)" sentinel normalised to NULL, same rule as
# 02_ga4_events.py -- Bronze intentionally keeps it raw.
items_df = (
    items_df.withColumn("item_id", _clean_str(F.col("item_id")))
    .withColumn("item_name", _clean_str(F.col("item_name")))
    .withColumn("item_category", _clean_str(F.col("item_category")))
)

# COMMAND ----------

# DBTITLE 1,Transform -- ga4_items (promotion/product disambiguation)
# Core disambiguation: item_id means a campaign id on promotion events,
# a product id everywhere else (ga4.md EDA Findings) -- these are different
# entity types sharing one physical field.
items_df = items_df.withColumn(
    "item_context",
    F.when(F.col("event_name").isin(*_PROMOTION_EVENTS), F.lit("promotion")).otherwise(
        F.lit("product")
    ),
)

# COMMAND ----------

# DBTITLE 1,Transform -- ga4_items (provenance)
items_df = items_df.withColumn(
    "_srid",
    sha_key(*KEY_COLS),
)
items_df = add_provenance(items_df, SOURCE, "_srid", RID)

# COMMAND ----------

# DBTITLE 1,Write Silver -- ga4_items
write_silver(items_df, "ga4_items", source=SOURCE, component=COMPONENT, rid=RID)

# COMMAND ----------

# DBTITLE 1,Inspect ga4_items + export findings
_findings_blocks = inspect_table(
    items_df,
    "ga4_items",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=KEY_COLS,
    extra_checks={
        "promotion_item_count": items_df.filter(
            F.col("item_context") == "promotion"
        ).count(),
        "product_item_count": items_df.filter(
            F.col("item_context") == "product"
        ).count(),
    },
)
write_silver_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__ga4_items",
    "ga4_items",
    _findings_blocks,
)
