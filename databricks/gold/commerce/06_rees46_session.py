# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # GOLD -- REES46_SESSION
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** `commerce_gold.rees46_session` -- `rees46_event` rolled up
# MAGIC to one row per (user_id, user_session). Grain: (user_id, user_session).

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
SOURCE = "rees46"
COMPONENT = "gold/commerce/rees46_session"
TABLE = "rees46_session"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = gold_run_id()

# COMMAND ----------

# DBTITLE 1,Read Silver
rees46_event = read_silver("rees46_event")

# COMMAND ----------

# DBTITLE 1,Aggregate to one row per session
rees46_session = rees46_event.groupBy("user_id", "user_session").agg(
    F.min("event_timestamp_utc").alias("session_start_utc"),
    F.max("event_timestamp_utc").alias("session_end_utc"),
    F.count(F.lit(1)).alias("event_count"),
    F.array_distinct(F.collect_list("event_type")).alias("event_types"),
    F.countDistinct("product_id").alias("distinct_product_count"),
    F.first("source_system", ignorenulls=True).alias("source_system"),
    F.first("source_dataset", ignorenulls=True).alias("source_dataset"),
)
rees46_session = rees46_session.withColumn(
    "session_key", F.concat_ws("|", "user_id", "user_session")
)
rees46_session = add_gold_provenance(rees46_session, SOURCE, rid)

# COMMAND ----------

# DBTITLE 1,Grain gate
assert_unique_grain(
    rees46_session,
    ["user_id", "user_session"],
    component=COMPONENT,
    source=SOURCE,
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,Write
write_gold(rees46_session, TABLE, source=SOURCE, component=COMPONENT, rid=rid)

# COMMAND ----------

# DBTITLE 1,Inspect + export findings
_blocks = inspect_gold_table(
    rees46_session,
    TABLE,
    source=SOURCE,
    component=COMPONENT,
    rid=rid,
    key_cols=["user_id", "user_session"],
)
write_gold_findings(SOURCE, f"commerce__{TABLE}", TABLE, _blocks)
