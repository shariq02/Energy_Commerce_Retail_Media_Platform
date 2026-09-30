# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # GA4 SESSION PREFIX FEATURES
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** features of the first ten events of each GA4 session. Only about one
# MAGIC session in ten reaches ten events. Purchase-adjacent events are left out
# MAGIC of every feature.

# COMMAND ----------

# DBTITLE 1,ML shared library
# MAGIC %run ../../_ml_common

# COMMAND ----------

# DBTITLE 1,ML inspection library
# MAGIC %run ../../_ml_inspect

# COMMAND ----------

# DBTITLE 1,Configuration
ECO = "commerce"
SOURCE = "ga4"
COMPONENT = "ml/commerce/features/session_prefix_ga4"
TABLE = "features_session_prefix_ga4"
K = 10
ADJACENT = ("begin_checkout", "add_shipping_info", "add_payment_info", "purchase")

# COMMAND ----------

# DBTITLE 1,Run identity
rid = ml_run_id()

# COMMAND ----------

# DBTITLE 1,Session time zone
set_utc_session()

# COMMAND ----------

# DBTITLE 1,Rank events within each session
events = (
    read_gold("ga4_event", source=SOURCE)
    .select(
        "event_key",
        "event_timestamp_utc",
        "event_name",
        "user_pseudo_id",
        "session_id",
        "session_number",
        "page_location",
        "search_term",
        "geo_country",
    )
    .withColumn(
        "session_key",
        F.concat_ws("|", "user_pseudo_id", F.col("session_id").cast("string")),
    )
)
_w = Window.partitionBy("session_key").orderBy("event_timestamp_utc", "event_key")
ranked = events.withColumn("rn", F.row_number().over(_w))

# COMMAND ----------

# DBTITLE 1,Aggregate the prefix without purchase-adjacent events
_ok = ~F.col("event_name").isin(*ADJACENT)
prefix = (
    ranked.filter(F.col("rn") <= K)
    .groupBy("session_key")
    .agg(
        F.first("user_pseudo_id").alias("user_pseudo_id"),
        F.min("event_timestamp_utc").alias("session_start_utc"),
        F.max("event_timestamp_utc").alias("as_of_ts"),
        F.count("*").alias("prefix_event_count"),
        F.sum((_ok & (F.col("event_name") == "page_view")).cast("int")).alias(
            "prefix_page_view_count"
        ),
        F.sum((_ok & (F.col("event_name") == "view_item")).cast("int")).alias(
            "prefix_view_item_count"
        ),
        F.sum((_ok & (F.col("event_name") == "add_to_cart")).cast("int")).alias(
            "prefix_add_to_cart_count"
        ),
        F.sum((_ok & F.col("search_term").isNotNull()).cast("int")).alias(
            "prefix_search_count"
        ),
        F.countDistinct(F.when(_ok, F.col("page_location"))).alias(
            "prefix_distinct_pages"
        ),
        F.min_by("session_number", "rn").alias("prefix_session_number"),
        F.min_by("geo_country", "rn").alias("prefix_geo_country"),
        (
            F.max("event_timestamp_utc").cast("long")
            - F.min("event_timestamp_utc").cast("long")
        ).alias("prefix_span_seconds"),
    )
)
eligible = prefix.filter(F.col("prefix_event_count") == K)

# COMMAND ----------

# DBTITLE 1,Time of day
_local = F.from_utc_timestamp("session_start_utc", PROJECT_TIMEZONE)
out = (
    eligible.withColumn("session_date", F.to_date(_local))
    .withColumn("prefix_hour_of_day", F.hour(_local))
    .withColumn("prefix_day_of_week", F.dayofweek(_local))
)
out = add_ml_provenance(out, TABLE, ECO, rid)

# COMMAND ----------

# DBTITLE 1,Record the sessions below the prefix length
_below = prefix.filter(F.col("prefix_event_count") < K).count()
_all = prefix.count()
drop_block = (
    "sessions_below_prefix_length",
    markdown_table(["k", "sessions_excluded", "sessions_total"], [(K, _below, _all)]),
)
print(f"sessions with fewer than {K} events: {_below} of {_all}")

# COMMAND ----------

# DBTITLE 1,Grain and forbidden-column gate
_GRAIN = ["session_key"]
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

# DBTITLE 1,Add the exclusion count to the findings
write_ml_findings(
    ECO, "features__" + TABLE + "__exclusions", TABLE + " exclusions", [drop_block]
)
