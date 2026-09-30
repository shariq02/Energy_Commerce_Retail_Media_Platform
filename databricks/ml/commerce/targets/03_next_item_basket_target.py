# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # NEXT ITEM AND BASKET TARGETS
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** the next distinct product after each session step, and the set of products
# MAGIC purchased in a session. REES46 is primary; GA4 is thin. Timestamp ties are
# MAGIC ordered by the event key and flagged in the features.

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
COMPONENT = "ml/commerce/targets/next_item_basket"
TABLE = "target_next_item_rees46"
GA4_TABLE = "target_next_item_ga4"
BASKET_TABLE = "target_basket_rees46"
BASKET_GA4_TABLE = "target_basket_ga4"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = ml_run_id()

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

# DBTITLE 1,REES46 steps and their next distinct product
_seq = read_ml("features_item_sequence_rees46", ecosystem=ECO).select(
    "session_key", "user_id", "step", "seq_product_id", "local_date"
)
out = next_distinct_product(_seq).withColumn("provenance_tier", F.lit("constructed"))
out = add_ml_provenance(out, TABLE, ECO, rid)

# COMMAND ----------

# DBTITLE 1,Record the dropped steps
drop_block = (
    "steps",
    markdown_table(
        ["steps_total", "steps_with_next_item"], [(_seq.count(), out.count())]
    ),
)

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
write_ml_findings(ECO, "targets__" + TABLE, TABLE, _blocks)

# COMMAND ----------

# DBTITLE 1,Add the step report to the findings
write_ml_findings(ECO, "targets__" + TABLE + "__steps", TABLE + " steps", [drop_block])

# COMMAND ----------

# DBTITLE 1,GA4 next distinct item
_sg = read_ml("features_item_sequence_ga4", ecosystem=ECO).select(
    "session_key", "user_id", "step", "seq_product_id", "local_date"
)
out_ga4 = next_distinct_product(_sg).withColumn("provenance_tier", F.lit("constructed"))
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

# DBTITLE 1,REES46 baskets
basket = (
    read_gold("rees46_event", source="rees46")
    .filter(F.col("event_type") == "purchase")
    .withColumn("session_key", F.concat_ws("|", "user_id", "user_session"))
    .groupBy("session_key", F.col("user_id"))
    .agg(F.collect_set("product_id").alias("purchased_products"))
    .withColumn("basket_size", F.size("purchased_products"))
    .withColumn("provenance_tier", F.lit("sourced"))
)
basket = add_ml_provenance(basket, BASKET_TABLE, ECO, rid)

# COMMAND ----------

# DBTITLE 1,Write REES46 baskets
write_ml(
    basket,
    BASKET_TABLE,
    ecosystem=ECO,
    source="rees46",
    component=COMPONENT + "/basket",
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,GA4 baskets
_pi = read_gold("ga4_event_item", source="ga4").filter(
    (F.col("event_name") == "purchase") & F.col("item_id").isNotNull()
)
_pe = read_gold("ga4_event", source="ga4").select("event_key", "session_id")
basket_ga4 = (
    _pi.join(_pe, "event_key")
    .withColumn(
        "session_key",
        F.concat_ws("|", "user_pseudo_id", F.col("session_id").cast("string")),
    )
    .groupBy("session_key", F.col("user_pseudo_id").alias("user_id"))
    .agg(F.collect_set("item_id").alias("purchased_products"))
    .withColumn("basket_size", F.size("purchased_products"))
    .withColumn("provenance_tier", F.lit("sourced"))
)
basket_ga4 = add_ml_provenance(basket_ga4, BASKET_GA4_TABLE, ECO, rid)

# COMMAND ----------

# DBTITLE 1,Write GA4 baskets
write_ml(
    basket_ga4,
    BASKET_GA4_TABLE,
    ecosystem=ECO,
    source="ga4",
    component=COMPONENT + "/basket_ga4",
    rid=rid,
)
