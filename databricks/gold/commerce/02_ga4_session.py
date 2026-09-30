# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # GOLD -- GA4_SESSION
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** `commerce_gold.ga4_session` -- `ga4_event` rolled up to one
# MAGIC row per (user_pseudo_id, session_id). No GA4 `event_name` vocabulary is
# MAGIC assumed (e.g. filtering for a literal `"purchase"` string) -- every
# MAGIC aggregate here is name-agnostic. Grain: (user_pseudo_id, session_id).

# COMMAND ----------

# DBTITLE 1,Shared Silver library
# MAGIC %run ../../silver/_silver_common

# COMMAND ----------

# DBTITLE 1,Gold shared library
# MAGIC %run ../_gold_common

# COMMAND ----------

# DBTITLE 1,Gold inspection library
# MAGIC %run ../_gold_inspect

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "ga4"
COMPONENT = "gold/commerce/ga4_session"
TABLE = "ga4_session"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = gold_run_id()

# COMMAND ----------

# DBTITLE 1,Read Silver
ga4_event = read_silver("ga4_event")

# COMMAND ----------

# DBTITLE 1,Aggregate to one row per session
ga4_session = ga4_event.groupBy("user_pseudo_id", "session_id").agg(
    F.first("session_number", ignorenulls=True).alias("session_number"),
    F.min("event_timestamp_utc").alias("session_start_utc"),
    F.max("event_timestamp_utc").alias("session_end_utc"),
    F.count(F.lit(1)).alias("event_count"),
    F.array_distinct(F.collect_list("event_name")).alias("event_names"),
    F.countDistinct("transaction_id").alias("transaction_count"),
    F.sum("purchase_revenue").alias("total_revenue"),
    F.first("geo_country", ignorenulls=True).alias("geo_country"),
    F.first("source_system", ignorenulls=True).alias("source_system"),
    F.first("source_dataset", ignorenulls=True).alias("source_dataset"),
)
ga4_session = ga4_session.withColumn(
    "session_key",
    F.concat_ws("|", "user_pseudo_id", F.col("session_id").cast("string")),
)
ga4_session = add_gold_provenance(ga4_session, SOURCE, rid)

# COMMAND ----------

# DBTITLE 1,Grain gate
assert_unique_grain(
    ga4_session,
    ["user_pseudo_id", "session_id"],
    component=COMPONENT,
    source=SOURCE,
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,Write
write_gold(ga4_session, TABLE, source=SOURCE, component=COMPONENT, rid=rid)

# COMMAND ----------

# DBTITLE 1,Inspect + export findings
_blocks = inspect_gold_table(
    ga4_session,
    TABLE,
    source=SOURCE,
    component=COMPONENT,
    rid=rid,
    key_cols=["user_pseudo_id", "session_id"],
)
write_gold_findings(SOURCE, f"commerce__{TABLE}", TABLE, _blocks)
