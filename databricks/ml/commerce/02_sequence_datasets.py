# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # ML COMMERCE -- NEXT ITEM AND SESSION SEQUENCE DATASETS
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** November 2026
# MAGIC
# MAGIC **Purpose:** build the three datasets of the family `next_item_sequence`
# MAGIC (REES46 and GA4 next distinct item, REES46 session sequences with a reward):
# MAGIC the step features of the item sequences, the next-item and reward targets, the
# MAGIC two basket tables (kept), and for each dataset the contract, the split, the
# MAGIC fit and the family rows.

# COMMAND ----------

# DBTITLE 1,ML shared library
# MAGIC %run ../_ml_common

# COMMAND ----------

# DBTITLE 1,ML inspection library
# MAGIC %run ../_ml_inspect

# COMMAND ----------

# DBTITLE 1,ML build library
# MAGIC %run ../_ml_fit

# COMMAND ----------

# DBTITLE 1,Configuration
ECO = "commerce"
SOURCE = "rees46"
COMPONENT = "ml/commerce/sequence"
REES46, GA4 = "rees46_events", "ga4_events"
# reward weight of each event type times the item price
EVENT_REWARD_WEIGHT = {"view": 0.0, "cart": 0.1, "purchase": 1.0}
FRAMES, BLOCKS = {}, {}

# COMMAND ----------

# DBTITLE 1,Run identity and session time zone
rid = ml_run_id()
set_utc_session()

# COMMAND ----------

# DBTITLE 1,Read Gold -- events and GA4 items with the session keys
events = read_gold("commerce_event", source=SOURCE)
rees46_events = events.filter(F.col("source_dataset") == REES46).withColumn(
    "session_key", F.concat_ws("|", "user_id", "user_session")
)
ga4_events = events.filter(F.col("source_dataset") == GA4)
ga4_items = (
    read_gold("commerce_event_item", source="ga4")
    .filter(F.col("item_id").isNotNull())
    .join(ga4_events.select("event_key", "session_id", "local_date"), "event_key")
    .withColumn(
        "session_key",
        F.concat_ws("|", "user_pseudo_id", F.col("session_id").cast("string")),
    )
)

# COMMAND ----------

# DBTITLE 1,Step features of the item sequences -- REES46 and GA4
_w = Window.partitionBy("session_key").orderBy("event_timestamp_utc", "event_key")
sequence_rees46 = rees46_events.select(
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
_wg = Window.partitionBy("session_key").orderBy(
    "event_timestamp_utc", "event_key", "item_ordinal"
)
sequence_ga4 = ga4_items.select(
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

# COMMAND ----------


# DBTITLE 1,Helper -- next distinct product per step
def next_distinct_product(seq):
    """Consecutive repeats collapse into runs; the next distinct product of a
    step is the product of the next run."""
    w = Window.partitionBy("session_key").orderBy("step")
    marked = (
        seq.filter(F.col("seq_product_id").isNotNull())
        .withColumn(
            "_change",
            (F.col("seq_product_id") != F.lag("seq_product_id").over(w)).cast("int"),
        )
        .withColumn(
            "_run",
            F.sum(F.coalesce(F.col("_change"), F.lit(0))).over(
                w.rowsBetween(Window.unboundedPreceding, Window.currentRow)
            ),
        )
    )
    runs = marked.groupBy("session_key", "_run").agg(
        F.min_by("seq_product_id", "step").alias("_run_product")
    )
    nxt = runs.select(
        "session_key",
        (F.col("_run") - 1).alias("_run"),
        F.col("_run_product").alias("target_next_product_id"),
    )
    return marked.join(nxt, ["session_key", "_run"], "inner").select(
        "session_key", "user_id", "step", "local_date", "target_next_product_id"
    )


# COMMAND ----------

# DBTITLE 1,Targets -- next distinct item, and the reward per step
_pairs = (("rees46", sequence_rees46), ("ga4", sequence_ga4))
next_item = {}
for _name, _sequence in _pairs:
    _steps = _sequence.select(
        "session_key", "user_id", "step", "seq_product_id", "local_date"
    )
    next_item[_name] = next_distinct_product(_steps).withColumn(
        "provenance_tier", F.lit("constructed")
    )
    BLOCKS[f"next_item_{_name}"] = [
        (
            "steps",
            markdown_table(
                ["steps_total", "steps_with_next_item"],
                [(_steps.count(), next_item[_name].count())],
            ),
        )
    ]
_weight = F.lit(None).cast("double")
for _type, _w_value in EVENT_REWARD_WEIGHT.items():
    _weight = F.when(
        F.col("seq_event_type") == _type, F.lit(float(_w_value))
    ).otherwise(_weight)
target_rl_session = (
    sequence_rees46.withColumn("episode_id", F.col("session_key"))
    .withColumn(
        "reward",
        F.coalesce(_weight, F.lit(0.0)) * F.coalesce(F.col("seq_price"), F.lit(0.0)),
    )
    .withColumn("target_action_product_id", F.col("seq_product_id"))
    .withColumn("target_action_event_type", F.col("seq_event_type"))
    .withColumn("provenance_tier", F.lit("simulated"))
    .select(
        "episode_id",
        "step",
        "session_key",
        "user_id",
        "event_key",
        "local_date",
        "target_action_event_type",
        "target_action_product_id",
        "reward",
        "provenance_tier",
    )
)

# COMMAND ----------

# DBTITLE 1,Basket tables -- two kept tables, no dataset reads them yet
basket_rees46 = (
    rees46_events.filter(F.col("event_type") == "purchase")
    .groupBy("session_key", "user_id")
    .agg(F.collect_set("product_id").alias("purchased_products"))
    .withColumn("basket_size", F.size("purchased_products"))
    .withColumn("provenance_tier", F.lit("sourced"))
)
basket_ga4 = (
    ga4_items.filter((F.col("event_name") == "purchase") & F.col("item_id").isNotNull())
    .groupBy("session_key", F.col("user_pseudo_id").alias("user_id"))
    .agg(F.collect_set("item_id").alias("purchased_products"))
    .withColumn("basket_size", F.size("purchased_products"))
    .withColumn("provenance_tier", F.lit("sourced"))
)
for _frame, _table in (
    (basket_rees46, "target_basket_rees46"),
    (basket_ga4, "target_basket_ga4"),
):
    write_stored_table(
        _frame,
        _table,
        ECO,
        key_cols=["session_key"],
        source=SOURCE,
        component=f"{COMPONENT}/{_table}",
        rid=rid,
        section="targets",
    )

# COMMAND ----------

# DBTITLE 1,Assemble the three datasets (the join is in memory, no view)
FRAMES["next_item_rees46"] = assembled_frame(
    next_item["rees46"],
    [(sequence_rees46, ["session_key", "step"], ["user_id", "local_date"])],
)
FRAMES["next_item_ga4"] = assembled_frame(
    next_item["ga4"],
    [(sequence_ga4, ["session_key", "step"], ["user_id", "local_date"])],
)
FRAMES["rl_session_sequences"] = assembled_frame(
    target_rl_session,
    [
        (
            sequence_rees46,
            ["session_key", "step"],
            ["user_id", "local_date", "event_key"],
        )
    ],
)

# COMMAND ----------

# DBTITLE 1,Build the datasets -- contract, split, fit and the family rows
build_datasets(FRAMES, rid=rid, source=SOURCE, blocks=BLOCKS)

# COMMAND ----------

# DBTITLE 1,Summary
for _dataset in FRAMES:
    print(
        _dataset,
        read_ml("dataset_manifest", ecosystem=ECO)
        .filter(F.col("dataset_id") == _dataset)
        .first()["status"],
    )
