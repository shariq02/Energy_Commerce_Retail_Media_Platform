# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # ITEM SEQUENCE FEATURES
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** per session step: the current item, its category and brand, the previous
# MAGIC item and the time since the session start, for next-item prediction. Ties
# MAGIC in the event timestamp are ordered by the event key and flagged.

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
COMPONENT = "ml/commerce/features/item_sequence_features"
TABLE = "features_item_sequence_rees46"
GA4_TABLE = "features_item_sequence_ga4"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = ml_run_id()

# COMMAND ----------

# DBTITLE 1,Session time zone
set_utc_session()

# COMMAND ----------

# DBTITLE 1,REES46 events in session order
_e = (
    read_gold("rees46_event", source=SOURCE)
    .select(
        "event_key",
        "event_timestamp_utc",
        "local_date",
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

# COMMAND ----------

# DBTITLE 1,Step features
out = _e.select(
    "session_key",
    "user_id",
    "event_key",
    "local_date",
    F.row_number().over(_w).alias("step"),
    F.col("event_type").alias("seq_event_type"),
    F.col("product_id").alias("seq_product_id"),
    F.col("category_l1").alias("seq_category"),
    F.col("brand").alias("seq_brand"),
    F.col("price").alias("seq_price"),
    F.lag("product_id").over(_w).alias("seq_previous_product_id"),
    F.lag("event_type").over(_w).alias("seq_previous_event_type"),
    (
        F.col("event_timestamp_utc").cast("long")
        - F.first("event_timestamp_utc")
        .over(_w.rowsBetween(Window.unboundedPreceding, Window.currentRow))
        .cast("long")
    ).alias("seq_seconds_since_session_start"),
    (F.col("event_timestamp_utc") == F.lag("event_timestamp_utc").over(_w)).alias(
        "seq_timestamp_tie"
    ),
)
out = add_ml_provenance(out, TABLE, ECO, rid)

# COMMAND ----------

# DBTITLE 1,Grain and forbidden-column gate
_GRAIN = ["session_key", "step"]
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

# DBTITLE 1,GA4 item events in session order
_it = read_gold("ga4_event_item", source="ga4").filter(F.col("item_id").isNotNull())
_ge = read_gold("ga4_event", source="ga4").select(
    "event_key", "session_id", "local_date"
)
_gi = _it.join(_ge, "event_key").withColumn(
    "session_key",
    F.concat_ws("|", "user_pseudo_id", F.col("session_id").cast("string")),
)
_wg = Window.partitionBy("session_key").orderBy(
    "event_timestamp_utc", "event_key", "item_ordinal"
)

# COMMAND ----------

# DBTITLE 1,GA4 step features
out_ga4 = _gi.select(
    "session_key",
    F.col("user_pseudo_id").alias("user_id"),
    "event_key",
    "local_date",
    F.row_number().over(_wg).alias("step"),
    F.col("event_name").alias("seq_event_type"),
    F.col("item_id").alias("seq_product_id"),
    F.col("item_category").alias("seq_category"),
    F.col("price").alias("seq_price"),
    F.lag("item_id").over(_wg).alias("seq_previous_product_id"),
    F.lag("event_name").over(_wg).alias("seq_previous_event_type"),
    (
        F.col("event_timestamp_utc").cast("long")
        - F.first("event_timestamp_utc")
        .over(_wg.rowsBetween(Window.unboundedPreceding, Window.currentRow))
        .cast("long")
    ).alias("seq_seconds_since_session_start"),
    (F.col("event_timestamp_utc") == F.lag("event_timestamp_utc").over(_wg)).alias(
        "seq_timestamp_tie"
    ),
)
out_ga4 = add_ml_provenance(out_ga4, GA4_TABLE, ECO, rid)

# COMMAND ----------

# DBTITLE 1,Grain gate for GA4
assert_unique_grain(
    out_ga4,
    ["session_key", "step"],
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
    key_cols=["session_key", "step"],
)
write_ml_findings(ECO, "features__" + GA4_TABLE, GA4_TABLE, _blocks)
