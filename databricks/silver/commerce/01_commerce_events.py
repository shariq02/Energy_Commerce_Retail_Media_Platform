# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # SILVER -- COMMERCE EVENTS (COMMERCE)
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** November 2026
# MAGIC
# MAGIC **Purpose:** the REES46 shop events and the GA4 web analytics events in one table
# MAGIC (`commerce_event`, one row per event), and the GA4 event items as rows
# MAGIC (`ga4_event_item`, one row per item of an event). The REES46 category path is
# MAGIC three columns. The GA4 event parameters and transaction fields are columns.

# COMMAND ----------

# DBTITLE 1,Semantic structures library
# MAGIC %run ../_semantic_common

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# DBTITLE 1,Configuration
COMPONENT = "silver/commerce/01_commerce_events"
FINDINGS = "commerce"
RID = run_id()

REES46 = "rees46"
REES46_BT = "rees46_events"
REES46_KEY = ["user_session", "product_id", "event_type", "event_time"]
REES46_CONTENT = ["category_id", "category_code", "brand", "price", "user_id"]
# Same (user, session, second) repeated more often than this -> bot burst.
BOT_BURST_EVENTS = 20

GA4 = "ga4"
GA4_BT = "ga4_events"
GA4_KEY = ["event_date", "event_timestamp", "user_pseudo_id", "event_name"]
GA4_CONTENT = ["event_params", "ecommerce", "items", "geo_country"]
UNSET = ("(not set)", "(none)", "")
PROMOTION_EVENTS = ("view_promotion", "select_promotion")
# output column -> (event_params key, value field); closed 6-key vocabulary
PARAMS = {
    "session_id": ("ga_session_id", "int_value"),
    "session_number": ("ga_session_number", "int_value"),
    "page_location": ("page_location", "string_value"),
    "page_title": ("page_title", "string_value"),
    "search_term": ("search_term", "string_value"),
    "unique_search_term": ("unique_search_term", "string_value"),
}

# COMMAND ----------

# DBTITLE 1,Session time zone
ensure_utc_session()

# COMMAND ----------

# DBTITLE 1,Read Bronze -- REES46 events, deduplicated
rees46_bronze = read_bronze(REES46_BT)
_typed = (
    rees46_bronze.withColumn("event_timestamp_native", F.col("event_time"))
    .withColumn("event_time", F.col("event_time").cast("timestamp"))
    .withColumn("price", F.col("price").cast("double"))
)
_kept, _q = resolve_conflicts(
    _typed, REES46_KEY, REES46_CONTENT, bronze_table=REES46_BT
)
write_quarantine(_q.withColumn("source_system", F.lit(REES46)), RID)
rees46_reconciliation = reconciliation_stats(rees46_bronze, _kept, _q)

# COMMAND ----------

# DBTITLE 1,Transform -- REES46 events (time, category path columns, flags)
_path = F.split(F.col("category_code"), r"\.")
_burst = Window.partitionBy("user_id", "user_session", "event_timestamp_utc")
_ids_per_code = _kept.groupBy("category_code").agg(
    F.countDistinct("category_id").alias("_ids_per_code")
)
rees46_events = (
    _kept.withColumn("event_timestamp_utc", F.col("event_time"))
    .withColumn(
        "event_timestamp_project",
        F.from_utc_timestamp("event_timestamp_utc", PROJECT_TZ),
    )
    .withColumn("local_date", F.to_date("event_timestamp_project"))
    .withColumn("currency_unknown", F.lit(True))
    .withColumn("category_l1", F.get(_path, 0))
    .withColumn("category_l2", F.get(_path, 1))
    .withColumn("category_l3", F.get(_path, 2))
    .join(F.broadcast(_ids_per_code), "category_code", "left")
    .withColumn("key_conflict_resolved", F.col("_had_key_conflict"))
    .withColumn(
        "bot_burst_suspected", F.count(F.lit(1)).over(_burst) > BOT_BURST_EVENTS
    )
    .withColumn("category_code_ambiguous", F.col("_ids_per_code") > 1)
    .withColumn("measurement_basis", F.lit("shop_event_log"))
    .withColumn("event_key", sha_key(F.lit(REES46), *REES46_KEY))
    .withColumn("source_record_id", F.col("event_key"))
)
rees46_events = add_semantic_provenance(rees46_events, REES46, REES46_BT, RID)

# COMMAND ----------

# DBTITLE 1,Read Bronze -- GA4 events, deduplicated


def unset_to_null(col):
    return F.when(col.isin(*UNSET), F.lit(None)).otherwise(col)


ga4_bronze = read_bronze(GA4_BT)
_kept_ga4, _q_ga4 = resolve_conflicts(
    ga4_bronze, GA4_KEY, GA4_CONTENT, bronze_table=GA4_BT
)
write_quarantine(_q_ga4.withColumn("source_system", F.lit(GA4)), RID)
ga4_reconciliation = reconciliation_stats(ga4_bronze, _kept_ga4, _q_ga4)

# COMMAND ----------

# DBTITLE 1,Transform -- GA4 events (parameters and transaction fields as columns)
# event_timestamp is UTC microseconds.
base = (
    _kept_ga4.withColumn("event_key", sha_key(F.lit(GA4), *GA4_KEY))
    .withColumn("event_date_native", F.col("event_date").cast("string"))
    .withColumn("event_timestamp_native", F.col("event_timestamp").cast("string"))
    .withColumn(
        "event_timestamp_utc", F.timestamp_micros(F.col("event_timestamp").cast("long"))
    )
    .withColumn(
        "event_timestamp_project",
        F.from_utc_timestamp("event_timestamp_utc", PROJECT_TZ),
    )
    .withColumn("local_date", F.to_date("event_timestamp_project"))
)
_int_params = F.map_from_entries(
    F.transform("event_params", lambda x: F.struct(x["key"], x["value"]["int_value"]))
)
_str_params = F.map_from_entries(
    F.transform(
        "event_params", lambda x: F.struct(x["key"], x["value"]["string_value"])
    )
)
ga4_events = base
for out_col, (key, field) in PARAMS.items():
    source_map = _int_params if field == "int_value" else _str_params
    ga4_events = ga4_events.withColumn(out_col, source_map[F.lit(key)])
ga4_events = (
    ga4_events.withColumn(
        "transaction_id", unset_to_null(F.col("ecommerce.transaction_id"))
    )
    .withColumn("purchase_revenue", F.col("ecommerce.purchase_revenue"))
    .withColumn("unique_items", F.col("ecommerce.unique_items"))
    .withColumn("total_item_quantity", F.col("ecommerce.total_item_quantity"))
    .withColumn("key_conflict_resolved", F.col("_had_key_conflict"))
    .withColumn("measurement_basis", F.lit("web_analytics_event"))
    .withColumn("source_record_id", F.col("event_key"))
)
ga4_events = add_semantic_provenance(ga4_events, GA4, GA4_BT, RID)

# COMMAND ----------

# DBTITLE 1,Transform -- GA4 event items (item_id is a campaign id on promotions)
ga4_items = (
    base.select(
        "event_key",
        "event_name",
        "event_timestamp_utc",
        "user_pseudo_id",
        F.posexplode("items").alias("item_ordinal", "item"),
    )
    .select(
        "event_key",
        "event_name",
        "event_timestamp_utc",
        "user_pseudo_id",
        "item_ordinal",
        unset_to_null(F.col("item.item_id")).alias("item_id"),
        unset_to_null(F.col("item.item_name")).alias("item_name"),
        unset_to_null(F.col("item.item_category")).alias("item_category"),
        F.col("item.price").alias("price"),
        F.col("item.quantity").alias("quantity"),
        F.col("item.item_revenue").alias("item_revenue"),
    )
    .withColumn(
        "item_context",
        F.when(F.col("event_name").isin(*PROMOTION_EVENTS), "promotion").otherwise(
            "product"
        ),
    )
    .withColumn("source_record_id", sha_key("event_key", "item_ordinal"))
)
ga4_items = conform(
    add_semantic_provenance(ga4_items, GA4, GA4_BT, RID), GA4_EVENT_ITEM_COLUMNS
)
commerce_event = conform(rees46_events, COMMERCE_EVENT_COLUMNS).unionByName(
    conform(ga4_events, COMMERCE_EVENT_COLUMNS)
)

# COMMAND ----------

# DBTITLE 1,Write, inspect and export -- commerce_event
write_inspect_export(
    commerce_event,
    "commerce_event",
    source=REES46,
    component=COMPONENT,
    rid=RID,
    findings=FINDINGS,
    key_cols=["event_key"],
    writer=write_semantic,
    extra_checks=structure_extra_checks,
    extra_blocks=[
        (
            f"dedup/conflict reconciliation -- {REES46_BT}",
            dict_to_markdown_row(rees46_reconciliation),
        ),
        (
            f"dedup/conflict reconciliation -- {GA4_BT}",
            dict_to_markdown_row(ga4_reconciliation),
        ),
    ],
)

# COMMAND ----------

# DBTITLE 1,Write, inspect and export -- ga4_event_item
write_inspect_export(
    ga4_items,
    "ga4_event_item",
    source=GA4,
    component=COMPONENT,
    rid=RID,
    findings=FINDINGS,
    key_cols=["source_record_id"],
    writer=write_semantic,
)
