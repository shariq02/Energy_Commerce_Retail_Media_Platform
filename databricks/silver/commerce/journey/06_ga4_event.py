# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER -- GA4 EVENT AND EVENT ITEM
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** GA4 events (one row per event; event parameters and the
# MAGIC transaction fields as columns) and their items (one row per event and
# MAGIC item). GA4's "(not set)" sentinel becomes NULL.

# COMMAND ----------

# DBTITLE 1,Shared library
# MAGIC %run ../../_silver_common

# COMMAND ----------

# DBTITLE 1,Inspection library
# MAGIC %run ../../_silver_inspect

# COMMAND ----------

# DBTITLE 1,Semantic structures library
# MAGIC %run ../../_semantic_common

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "ga4"
COMPONENT = "silver/commerce/journey/06_ga4_event"
RID = run_id()
FINDINGS = "ga4"
BT = "ga4_events"
KEY = ["event_date", "event_timestamp", "user_pseudo_id", "event_name"]
CONTENT = ["event_params", "ecommerce", "items", "geo_country"]
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

# DBTITLE 1,Helper -- GA4 sentinel to NULL


def unset_to_null(col):
    return F.when(col.isin(*UNSET), F.lit(None)).otherwise(col)


# COMMAND ----------

# DBTITLE 1,Read Bronze -- ga4_events
bronze_df = read_bronze(BT)

# COMMAND ----------

# DBTITLE 1,Dedupe -- collapse identical rows, quarantine same-key value conflicts
_kept, _q = resolve_conflicts(bronze_df, KEY, CONTENT, bronze_table=BT)
write_quarantine(_q.withColumn("source_system", F.lit(SOURCE)), RID)
RECONCILIATION = reconciliation_stats(bronze_df, _kept, _q)

# COMMAND ----------

# DBTITLE 1,Transform -- event keys and time (event_timestamp is UTC microseconds)
events = (
    _kept.withColumn("event_key", sha_key(*KEY))
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

# COMMAND ----------

# DBTITLE 1,Transform -- ga4_event (parameters and transaction fields as columns)
_int_params = F.map_from_entries(
    F.transform("event_params", lambda x: F.struct(x["key"], x["value"]["int_value"]))
)
_str_params = F.map_from_entries(
    F.transform(
        "event_params", lambda x: F.struct(x["key"], x["value"]["string_value"])
    )
)
ga4_event = events
for out_col, (key, field) in PARAMS.items():
    source_map = _int_params if field == "int_value" else _str_params
    ga4_event = ga4_event.withColumn(out_col, source_map[F.lit(key)])
ga4_event = (
    ga4_event.withColumn(
        "transaction_id", unset_to_null(F.col("ecommerce.transaction_id"))
    )
    .withColumn("purchase_revenue", F.col("ecommerce.purchase_revenue"))
    .withColumn("unique_items", F.col("ecommerce.unique_items"))
    .withColumn("total_item_quantity", F.col("ecommerce.total_item_quantity"))
    .withColumn(
        "quality_flags",
        flag_array({"key_conflict_resolved": F.col("_had_key_conflict")}),
    )
    .withColumn("measurement_basis", F.lit("web_analytics_event"))
    .withColumn("source_record_id", F.col("event_key"))
)
ga4_event = add_semantic_provenance(ga4_event, SOURCE, BT, RID)

# COMMAND ----------

# DBTITLE 1,Transform -- ga4_event_item (item_id is a campaign id on promotions)
ga4_item = (
    events.select(
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
ga4_item = add_semantic_provenance(ga4_item, SOURCE, BT, RID)

# COMMAND ----------

# DBTITLE 1,Configuration -- structure -> frame
STRUCTURES = {"ga4_event": ga4_event, "ga4_event_item": ga4_item}

# COMMAND ----------

# DBTITLE 1,Write Silver -- ga4_event and ga4_event_item
for name, frame in STRUCTURES.items():
    write_semantic(
        conform(frame, SEMANTIC_STRUCTURES[name]),
        name,
        source=SOURCE,
        component=COMPONENT,
        rid=RID,
    )

# COMMAND ----------

# DBTITLE 1,Inspect -- ga4_event and ga4_event_item
findings_blocks = {
    name: inspect_table(
        spark.table(semantic_table(name)),
        name,
        source=FINDINGS,
        component=COMPONENT,
        rid=RID,
        key_cols=["source_record_id"],
    )
    for name in STRUCTURES
}

# COMMAND ----------

# DBTITLE 1,Export findings -- ga4_event and ga4_event_item
for name, blocks in findings_blocks.items():
    write_silver_findings(FINDINGS, f"{COMPONENT.split('/')[-1]}__{name}", name, blocks)

# COMMAND ----------

# DBTITLE 1,Export findings -- dedup/conflict reconciliation proof
write_silver_findings(
    FINDINGS,
    f"{COMPONENT.split('/')[-1]}__reconciliation",
    f"dedup/conflict reconciliation -- {BT}",
    [
        (
            "Bronze -> exact duplicates collapsed -> conflicts quarantined -> kept",
            dict_to_markdown_row(RECONCILIATION),
        )
    ],
)
