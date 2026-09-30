# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # REES46 SESSION PREFIX FEATURES
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** features of the first two events of each REES46 session, the prediction
# MAGIC point that keeps at least 80 percent of purchasing sessions. Sessions with
# MAGIC fewer events are excluded and counted.

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
COMPONENT = "ml/commerce/features/session_prefix_rees46"
TABLE = "features_session_prefix_rees46"
K = 2

# COMMAND ----------

# DBTITLE 1,Run identity
rid = ml_run_id()

# COMMAND ----------

# DBTITLE 1,Session time zone
set_utc_session()

# COMMAND ----------

# DBTITLE 1,Rank events within each session
events = (
    read_gold("rees46_event", source=SOURCE)
    .select(
        "event_key",
        "event_timestamp_utc",
        "event_type",
        "user_id",
        "user_session",
        "product_id",
        "category_l1",
        "brand",
        "price",
    )
    .withColumn("session_key", F.concat_ws("|", "user_id", "user_session"))
)
_w = Window.partitionBy("session_key").orderBy("event_timestamp_utc", "event_key")
ranked = events.withColumn("rn", F.row_number().over(_w))

# COMMAND ----------

# DBTITLE 1,Aggregate the prefix
prefix = (
    ranked.filter(F.col("rn") <= K)
    .groupBy("session_key")
    .agg(
        F.first("user_id").alias("user_id"),
        F.min("event_timestamp_utc").alias("session_start_utc"),
        F.max("event_timestamp_utc").alias("as_of_ts"),
        F.count("*").alias("prefix_event_count"),
        F.sum((F.col("event_type") == "view").cast("int")).alias("prefix_view_count"),
        F.sum((F.col("event_type") == "cart").cast("int")).alias("prefix_cart_count"),
        F.countDistinct("product_id").alias("prefix_distinct_products"),
        F.min_by("product_id", "rn").alias("first_product_id"),
        F.min_by("category_l1", "rn").alias("prefix_first_category"),
        F.min_by("brand", "rn").alias("prefix_first_brand"),
        F.min_by("price", "rn").alias("prefix_first_price"),
        F.avg("price").alias("prefix_mean_price"),
        (
            F.max("event_timestamp_utc").cast("long")
            - F.min("event_timestamp_utc").cast("long")
        ).alias("prefix_span_seconds"),
    )
)
eligible = prefix.filter(F.col("prefix_event_count") == K)

# COMMAND ----------

# DBTITLE 1,Time of day and product popularity the day before
_local = F.from_utc_timestamp("session_start_utc", PROJECT_TIMEZONE)
_pop = read_ml("features_product_asof_rees46", ecosystem=ECO).select(
    F.col("product_id").alias("first_product_id"),
    F.date_add("local_date", 1).alias("session_date"),
    "product_pop_views",
    "product_pop_carts",
    "product_pop_purchases",
    "product_pop_last_price",
)
out = (
    eligible.withColumn("session_date", F.to_date(_local))
    .withColumn("prefix_hour_of_day", F.hour(_local))
    .withColumn("prefix_day_of_week", F.dayofweek(_local))
    .join(_pop, ["first_product_id", "session_date"], "left")
    .drop("first_product_id")
)
out = add_ml_provenance(out, TABLE, ECO, rid)

# COMMAND ----------

# DBTITLE 1,Record the sessions below the prefix length
_below = prefix.filter(F.col("prefix_event_count") < K).count()
drop_block = (
    "sessions_below_prefix_length",
    markdown_table(["k", "sessions_excluded"], [(K, _below)]),
)
print(f"sessions with fewer than {K} events: {_below}")

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
