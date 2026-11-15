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
# MAGIC The sessions, users and products (`commerce_session`, `commerce_user`,
# MAGIC `commerce_product`) carry their counts, sums and first and last event.

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
written_events = write_inspect_export(
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
written_items = write_inspect_export(
    ga4_items,
    "ga4_event_item",
    source=GA4,
    component=COMPONENT,
    rid=RID,
    findings=FINDINGS,
    key_cols=["source_record_id"],
    writer=write_semantic,
)

# COMMAND ----------

# DBTITLE 1,Transform -- commerce_session (one row per session, counts per event name)


def name_counts(name_col):
    """One count column per REES46 event type and GA4 event name."""
    return [
        F.sum(F.when(name_col == name, 1).otherwise(0))
        .cast("bigint")
        .alias(f"{name}_event_count")
        for name in COMMERCE_EVENT_NAMES
    ]


def with_row_provenance(df, key):
    """Provenance that keeps the per-row source_system of the events."""
    df = df.withColumnRenamed("source_system", "_origin")
    df = add_semantic_provenance(df, REES46, None, RID, key)
    return df.withColumn("source_system", F.col("_origin"))


events = (
    written_events.withColumn(
        "source_user_id", F.coalesce(F.col("user_id"), F.col("user_pseudo_id"))
    )
    .withColumn(
        "source_session_id",
        F.coalesce(F.col("user_session"), F.col("session_id").cast("string")),
    )
    .withColumn("_name", F.coalesce(F.col("event_type"), F.col("event_name")))
    .withColumn(
        "_revenue",
        F.coalesce(
            F.col("purchase_revenue"),
            F.when(F.col("event_type") == "purchase", F.col("price")),
        ),
    )
)
SHARED_SUMS = [
    F.min("event_timestamp_utc").alias("first_event_utc"),
    F.max("event_timestamp_utc").alias("last_event_utc"),
    F.count(F.lit(1)).alias("event_count"),
    *name_counts(F.col("_name")),
    F.countDistinct("product_id").alias("distinct_product_count"),
    F.countDistinct("transaction_id").alias("transaction_count"),
    F.sum("_revenue").alias("revenue"),
    F.max(F.coalesce(F.col("currency_unknown"), F.lit(False))).alias(
        "currency_unknown"
    ),
    F.first("geo_country", ignorenulls=True).alias("geo_country"),
    F.first("measurement_basis", ignorenulls=True).alias("measurement_basis"),
    F.first("source_dataset", ignorenulls=True).alias("source_dataset"),
]
sessions = (
    events.groupBy("source_system", "source_user_id", "source_session_id")
    .agg(
        F.max("session_number").alias("session_number"),
        F.min("event_timestamp_project").alias("_first_project"),
        F.countDistinct("category_id").alias("distinct_category_count"),
        F.max(F.coalesce(F.col("bot_burst_suspected"), F.lit(False))).alias(
            "bot_burst_suspected"
        ),
        *SHARED_SUMS,
    )
    .withColumn("local_date", F.to_date("_first_project"))
    .withColumn(
        "session_key", sha_key("source_system", "source_user_id", "source_session_id")
    )
    .withColumn("user_key", sha_key("source_system", "source_user_id"))
)
sessions = conform(
    with_row_provenance(sessions.withColumn("_srid", F.col("session_key")), "_srid"),
    COMMERCE_SESSION_COLUMNS,
)

# COMMAND ----------

# DBTITLE 1,Write, inspect and export -- commerce_session
write_inspect_export(
    sessions,
    "commerce_session",
    source=REES46,
    component=COMPONENT,
    rid=RID,
    findings=FINDINGS,
    key_cols=["session_key"],
    writer=write_semantic,
    extra_checks=structure_extra_checks,
)

# COMMAND ----------

# DBTITLE 1,Transform -- commerce_user and commerce_product (REES46 events, GA4 items)
users = (
    events.groupBy("source_system", "source_user_id")
    .agg(F.countDistinct("source_session_id").alias("session_count"), *SHARED_SUMS)
    .withColumn("user_key", sha_key("source_system", "source_user_id"))
)
users = conform(
    with_row_provenance(users.withColumn("_srid", F.col("user_key")), "_srid"),
    COMMERCE_USER_COLUMNS,
)
PRICE_STATS = [
    F.min("price").alias("minimum_price"),
    F.max("price").alias("maximum_price"),
    F.avg("price").alias("mean_price"),
]
rees46_products = (
    events.filter((F.col("source_system") == REES46) & F.col("product_id").isNotNull())
    .groupBy("source_system", F.col("product_id").alias("source_product_id"))
    .agg(
        *[
            F.first(c, ignorenulls=True).alias(c)
            for c in (
                "category_id",
                "category_code",
                "category_l1",
                "category_l2",
                "category_l3",
                "brand",
            )
        ],
        F.min("event_timestamp_utc").alias("first_event_utc"),
        F.max("event_timestamp_utc").alias("last_event_utc"),
        F.count(F.lit(1)).alias("event_count"),
        *name_counts(F.col("_name")),
        F.countDistinct("user_id").alias("distinct_user_count"),
        *PRICE_STATS,
        F.sum("_revenue").alias("revenue"),
        F.lit(True).alias("currency_unknown"),
        F.first("measurement_basis", ignorenulls=True).alias("measurement_basis"),
        F.first("source_dataset", ignorenulls=True).alias("source_dataset"),
    )
)
ga4_products = (
    written_items.filter(
        (F.col("item_context") == "product") & F.col("item_id").isNotNull()
    )
    .groupBy("source_system", F.col("item_id").alias("source_product_id"))
    .agg(
        F.first("item_name", ignorenulls=True).alias("item_name"),
        F.first("item_category", ignorenulls=True).alias("item_category"),
        F.min("event_timestamp_utc").alias("first_event_utc"),
        F.max("event_timestamp_utc").alias("last_event_utc"),
        F.count(F.lit(1)).alias("event_count"),
        *name_counts(F.col("event_name")),
        F.countDistinct("user_pseudo_id").alias("distinct_user_count"),
        *PRICE_STATS,
        F.sum("quantity").alias("total_quantity"),
        F.sum("item_revenue").alias("revenue"),
        F.first("source_dataset", ignorenulls=True).alias("source_dataset"),
    )
    .withColumn("measurement_basis", F.lit("web_analytics_event_item"))
)
products = (
    conform(rees46_products, COMMERCE_PRODUCT_COLUMNS)
    .unionByName(conform(ga4_products, COMMERCE_PRODUCT_COLUMNS))
    .withColumn("product_key", sha_key("source_system", "source_product_id"))
    .drop("source_record_id", "_silver_loaded_at", "_silver_run_id")
)
products = conform(
    with_row_provenance(products.withColumn("_srid", F.col("product_key")), "_srid"),
    COMMERCE_PRODUCT_COLUMNS,
)

# COMMAND ----------

# DBTITLE 1,Write, inspect and export -- commerce_user and commerce_product
for table, frame, key in (
    ("commerce_user", users, "user_key"),
    ("commerce_product", products, "product_key"),
):
    write_inspect_export(
        frame,
        table,
        source=REES46,
        component=COMPONENT,
        rid=RID,
        findings=FINDINGS,
        key_cols=[key],
        writer=write_semantic,
        extra_checks=structure_extra_checks,
    )
