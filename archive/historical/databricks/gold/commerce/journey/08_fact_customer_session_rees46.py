# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # GOLD -- FACT CUSTOMER SESSION REES46
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Grain:** one row per (`user_id`, `user_session`), grouping existing
# MAGIC event rows by REES46's own session boundary.
# MAGIC
# MAGIC **Sources:** `rees46_events` (Silver, commerce_silver).
# MAGIC
# MAGIC **Serves use case:** a REES46 visit as its own entity.
# MAGIC
# MAGIC **Purpose:** the REES46-scoped half of source-scoped session identity.
# MAGIC Never merged with the GA4 session fact.

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
SOURCE = "rees46"
COMPONENT = "gold/commerce/journey/08_fact_customer_session_rees46"
RID = gold_run_id()
GOLD_TABLE = "fact_customer_session_rees46"

# COMMAND ----------

# DBTITLE 1,Read Silver -- rees46_events
_events_silver = read_silver("rees46_events")

# COMMAND ----------

# DBTITLE 1,Transform -- session boundary + event count per (user_id, user_session)
fact = (
    _events_silver.filter(
        F.col("user_id").isNotNull() & F.col("user_session").isNotNull()
    )
    .groupBy("user_id", "user_session")
    .agg(
        F.min("event_time").alias("session_start_ts"),
        F.max("event_time").alias("session_end_ts"),
        F.count(F.lit(1)).alias("event_count"),
    )
)

# COMMAND ----------

# DBTITLE 1,Transform -- surrogate key + Gold provenance
fact = fact.withColumn(
    "customer_session_rees46_key", surrogate_key("user_id", "user_session")
)
fact = add_gold_provenance(fact, SOURCE, RID)

# COMMAND ----------

# DBTITLE 1,Grain assertion -- one row per (user_id, user_session)
assert_unique_grain(
    fact, ["user_id", "user_session"], component=COMPONENT, source=SOURCE, rid=RID
)

# COMMAND ----------

# DBTITLE 1,Write Gold -- fact_customer_session_rees46
write_gold(fact, GOLD_TABLE, source=SOURCE, component=COMPONENT, rid=RID)

# COMMAND ----------

# DBTITLE 1,Inspect fact_customer_session_rees46 + export findings
_findings_blocks = inspect_gold_table(
    fact,
    GOLD_TABLE,
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["user_id", "user_session"],
)
write_gold_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__{GOLD_TABLE}",
    GOLD_TABLE,
    _findings_blocks,
)
