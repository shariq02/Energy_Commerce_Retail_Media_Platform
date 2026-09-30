# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # GOLD -- FACT CUSTOMER SESSION GA4
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Grain:** one row per (`user_pseudo_id`, `session_id`), grouping
# MAGIC existing event rows by GA4's own session boundary.
# MAGIC
# MAGIC **Sources:** `ga4_events` (Silver, commerce_silver).
# MAGIC
# MAGIC **Serves use case:** a GA4 visit as its own entity.
# MAGIC
# MAGIC **Purpose:** the GA4-scoped half of source-scoped session identity. Never
# MAGIC merged with the REES46 session fact.

# COMMAND ----------

# DBTITLE 1,Shared Silver library
# MAGIC %run ../../../silver/_silver_common

# COMMAND ----------

# DBTITLE 1,Gold shared library
# MAGIC %run ../../_gold_common

# COMMAND ----------

# DBTITLE 1,Inspection library
# MAGIC %run ../../_gold_inspect

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "ga4"
COMPONENT = "gold/commerce/journey/07_fact_customer_session_ga4"
RID = gold_run_id()
GOLD_TABLE = "fact_customer_session_ga4"

# COMMAND ----------

# DBTITLE 1,Read Silver -- ga4_events
_events_silver = read_silver("ga4_events")

# COMMAND ----------

# DBTITLE 1,Transform -- session boundary + event count per (user_pseudo_id, session_id)
fact = (
    _events_silver.filter(F.col("session_id").isNotNull())
    .groupBy("user_pseudo_id", "session_id")
    .agg(
        F.min("event_timestamp").alias("session_start_ts"),
        F.max("event_timestamp").alias("session_end_ts"),
        F.count(F.lit(1)).alias("event_count"),
    )
)

# COMMAND ----------

# DBTITLE 1,Transform -- surrogate key + Gold provenance
fact = fact.withColumn(
    "customer_session_ga4_key", surrogate_key("user_pseudo_id", "session_id")
)
fact = add_gold_provenance(fact, SOURCE, RID)

# COMMAND ----------

# DBTITLE 1,Grain assertion -- one row per (user_pseudo_id, session_id)
assert_unique_grain(
    fact,
    ["user_pseudo_id", "session_id"],
    component=COMPONENT,
    source=SOURCE,
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Write Gold -- fact_customer_session_ga4
write_gold(fact, GOLD_TABLE, source=SOURCE, component=COMPONENT, rid=RID)

# COMMAND ----------

# DBTITLE 1,Inspect fact_customer_session_ga4 + export findings
_findings_blocks = inspect_gold_table(
    fact,
    GOLD_TABLE,
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["user_pseudo_id", "session_id"],
)
write_gold_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__{GOLD_TABLE}",
    GOLD_TABLE,
    _findings_blocks,
)
