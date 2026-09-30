# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # PRODUCT AS-OF POPULARITY
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** per product and local date: cumulative views, carts and purchases and the
# MAGIC last price seen, counted up to the end of that date, for both retail
# MAGIC sources. Joined to sessions with a one-day lag.

# COMMAND ----------

# DBTITLE 1,ML shared library
# MAGIC %run ../../_ml_common

# COMMAND ----------

# DBTITLE 1,ML inspection library
# MAGIC %run ../../_ml_inspect

# COMMAND ----------

# DBTITLE 1,Configuration
ECO = "commerce"
SOURCE = "rees46"
COMPONENT = "ml/commerce/features/product_asof"
TABLE = "features_product_asof_rees46"
GA4_TABLE = "features_product_asof_ga4"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = ml_run_id()

# COMMAND ----------

# DBTITLE 1,REES46 daily product counts
events = read_gold("rees46_event", source="rees46")
_daily = events.groupBy("product_id", "local_date").agg(
    F.sum((F.col("event_type") == "view").cast("int")).alias("views"),
    F.sum((F.col("event_type") == "cart").cast("int")).alias("carts"),
    F.sum((F.col("event_type") == "purchase").cast("int")).alias("purchases"),
    F.max_by("price", "event_timestamp_utc").alias("last_price"),
)

# COMMAND ----------

# DBTITLE 1,Cumulative counts
_w = (
    Window.partitionBy("product_id")
    .orderBy("local_date")
    .rowsBetween(Window.unboundedPreceding, 0)
)
out = (
    _daily.withColumn("product_pop_views", F.sum("views").over(_w))
    .withColumn("product_pop_carts", F.sum("carts").over(_w))
    .withColumn("product_pop_purchases", F.sum("purchases").over(_w))
    .select(
        "product_id",
        "local_date",
        "product_pop_views",
        "product_pop_carts",
        "product_pop_purchases",
        F.col("last_price").alias("product_pop_last_price"),
    )
)
out = add_ml_provenance(out, TABLE, ECO, rid)

# COMMAND ----------

# DBTITLE 1,Grain and forbidden-column gate
_GRAIN = ["product_id", "local_date"]
assert_unique_grain(out, _GRAIN, component=COMPONENT, source=SOURCE, rid=rid)
assert_no_forbidden_columns(out, component=COMPONENT, source=SOURCE, rid=rid)
check(
    COMPONENT,
    SOURCE,
    "non_empty",
    out.limit(1).count() > 0,
    detail="no rows produced; check the input filters",
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,Write
write_ml(out, TABLE, ecosystem=ECO, source=SOURCE, component=COMPONENT, rid=rid)

# COMMAND ----------

# DBTITLE 1,Inspect and export findings
_blocks = inspect_ml_table(
    read_ml(TABLE, ecosystem=ECO), TABLE, ecosystem=ECO, key_cols=_GRAIN
)
write_ml_findings(ECO, "features__" + TABLE, TABLE, _blocks)

# COMMAND ----------

# DBTITLE 1,GA4 daily item counts
_items = read_gold("ga4_event_item", source="ga4").filter(F.col("item_id").isNotNull())
_ev = read_gold("ga4_event", source="ga4").select("event_key", "local_date")
_daily_g = (
    _items.join(_ev, "event_key")
    .groupBy("item_id", "local_date")
    .agg(
        F.sum((F.col("event_name") == "view_item").cast("int")).alias("views"),
        F.sum((F.col("event_name") == "add_to_cart").cast("int")).alias("carts"),
        F.sum((F.col("event_name") == "purchase").cast("int")).alias("purchases"),
        F.max_by("price", "event_timestamp_utc").alias("last_price"),
    )
)

# COMMAND ----------

# DBTITLE 1,GA4 cumulative counts
_wg = (
    Window.partitionBy("item_id")
    .orderBy("local_date")
    .rowsBetween(Window.unboundedPreceding, 0)
)
out_ga4 = (
    _daily_g.withColumn("product_pop_views", F.sum("views").over(_wg))
    .withColumn("product_pop_carts", F.sum("carts").over(_wg))
    .withColumn("product_pop_purchases", F.sum("purchases").over(_wg))
    .select(
        F.col("item_id").alias("product_id"),
        "local_date",
        "product_pop_views",
        "product_pop_carts",
        "product_pop_purchases",
        F.col("last_price").alias("product_pop_last_price"),
    )
)
out_ga4 = add_ml_provenance(out_ga4, GA4_TABLE, ECO, rid)

# COMMAND ----------

# DBTITLE 1,Grain gate for GA4
assert_unique_grain(
    out_ga4,
    ["product_id", "local_date"],
    component=COMPONENT + "/ga4",
    source="ga4",
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,Write GA4
write_ml(
    out_ga4,
    GA4_TABLE,
    ecosystem=ECO,
    source="ga4",
    component=COMPONENT + "/ga4",
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,Inspect and export GA4 findings
_blocks = inspect_ml_table(
    read_ml(GA4_TABLE, ecosystem=ECO),
    GA4_TABLE,
    ecosystem=ECO,
    key_cols=["product_id", "local_date"],
)
write_ml_findings(ECO, "features__" + GA4_TABLE, GA4_TABLE, _blocks)
