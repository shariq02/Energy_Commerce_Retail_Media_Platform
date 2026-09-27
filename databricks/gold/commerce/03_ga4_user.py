# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # GOLD -- GA4_USER
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** `commerce_gold.ga4_user` -- `ga4_event` rolled up to one
# MAGIC row per `user_pseudo_id`. Grain: user id.

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
COMPONENT = "gold/commerce/ga4_user"
TABLE = "ga4_user"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = gold_run_id()

# COMMAND ----------

# DBTITLE 1,Read Silver
ga4_event = read_silver("ga4_event")

# COMMAND ----------

# DBTITLE 1,Aggregate to one row per user
ga4_user = ga4_event.groupBy("user_pseudo_id").agg(
    F.countDistinct("session_id").alias("session_count"),
    F.count(F.lit(1)).alias("event_count"),
    F.min("event_timestamp_utc").alias("first_seen"),
    F.max("event_timestamp_utc").alias("last_seen"),
    F.countDistinct("transaction_id").alias("transaction_count"),
    F.sum("purchase_revenue").alias("total_revenue"),
    F.first("geo_country", ignorenulls=True).alias("geo_country"),
    F.first("source_system", ignorenulls=True).alias("source_system"),
    F.first("source_dataset", ignorenulls=True).alias("source_dataset"),
)
ga4_user = add_gold_provenance(ga4_user, SOURCE, rid)

# COMMAND ----------

# DBTITLE 1,Grain gate
assert_unique_grain(
    ga4_user, ["user_pseudo_id"], component=COMPONENT, source=SOURCE, rid=rid
)

# COMMAND ----------

# DBTITLE 1,Write
write_gold(ga4_user, TABLE, source=SOURCE, component=COMPONENT, rid=rid)

# COMMAND ----------

# DBTITLE 1,Inspect + export findings
_blocks = inspect_gold_table(
    ga4_user,
    TABLE,
    source=SOURCE,
    component=COMPONENT,
    rid=rid,
    key_cols=["user_pseudo_id"],
)
write_gold_findings(SOURCE, f"commerce__{TABLE}", TABLE, _blocks)
