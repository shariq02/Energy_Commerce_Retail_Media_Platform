# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # GOLD -- REES46_USER
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** `commerce_gold.rees46_user` -- `rees46_event` rolled up to
# MAGIC one row per `user_id`. Grain: user id.

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
COMPONENT = "gold/commerce/rees46_user"
TABLE = "rees46_user"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = gold_run_id()

# COMMAND ----------

# DBTITLE 1,Read Silver
rees46_event = read_silver("rees46_event")

# COMMAND ----------

# DBTITLE 1,Aggregate to one row per user
rees46_user = rees46_event.groupBy("user_id").agg(
    F.countDistinct("user_session").alias("session_count"),
    F.count(F.lit(1)).alias("event_count"),
    F.min("event_timestamp_utc").alias("first_seen"),
    F.max("event_timestamp_utc").alias("last_seen"),
    F.countDistinct("product_id").alias("distinct_product_count"),
    F.first("source_system", ignorenulls=True).alias("source_system"),
    F.first("source_dataset", ignorenulls=True).alias("source_dataset"),
)
rees46_user = add_gold_provenance(rees46_user, SOURCE, rid)

# COMMAND ----------

# DBTITLE 1,Grain gate
assert_unique_grain(
    rees46_user, ["user_id"], component=COMPONENT, source=SOURCE, rid=rid
)

# COMMAND ----------

# DBTITLE 1,Write
write_gold(rees46_user, TABLE, source=SOURCE, component=COMPONENT, rid=rid)

# COMMAND ----------

# DBTITLE 1,Inspect + export findings
_blocks = inspect_gold_table(
    rees46_user,
    TABLE,
    source=SOURCE,
    component=COMPONENT,
    rid=rid,
    key_cols=["user_id"],
)
write_gold_findings(SOURCE, f"commerce__{TABLE}", TABLE, _blocks)
