# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # ANALYTICS -- GA4_DEMAND_DAILY
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** `commerce_analytics.ga4_demand_daily` -- Gold `ga4_session`
# MAGIC rolled up from session to day and country: sessions, transactions and
# MAGIC revenue. No REES46 join -- GA4 and REES46 stay source-distinct at every
# MAGIC layer (no shared identity). Grain: country
# MAGIC x local date.

# COMMAND ----------

# DBTITLE 1,Analytics shared library
# MAGIC %run ../_analytics_common

# COMMAND ----------

# DBTITLE 1,Analytics inspection library
# MAGIC %run ../_analytics_inspect

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "ga4"
COMPONENT = "analytics/commerce/ga4_demand_daily"
TABLE = "ga4_demand_daily"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = analytics_run_id()

# COMMAND ----------

# DBTITLE 1,Read Gold
ga4_session = read_gold("ga4_session", source=SOURCE).withColumn(
    "local_date", F.to_date("session_start_utc")
)

# COMMAND ----------

# DBTITLE 1,Aggregate
ga4_demand_daily = ga4_session.groupBy("geo_country", "local_date").agg(
    F.count(F.lit(1)).alias("session_count"),
    F.sum("transaction_count").alias("transaction_count"),
    F.sum("total_revenue").alias("revenue"),
    F.sum("event_count").alias("event_count"),
)
ga4_demand_daily = add_analytics_provenance(ga4_demand_daily, SOURCE, rid)

# COMMAND ----------

# DBTITLE 1,Grain gate
_GRAIN = ["geo_country", "local_date"]
assert_unique_grain(
    ga4_demand_daily, _GRAIN, component=COMPONENT, source=SOURCE, rid=rid
)

# COMMAND ----------

# DBTITLE 1,Write
write_analytics(ga4_demand_daily, TABLE, source=SOURCE, component=COMPONENT, rid=rid)

# COMMAND ----------

# DBTITLE 1,Inspect + export findings
_blocks = inspect_analytics_table(
    ga4_demand_daily,
    TABLE,
    source=SOURCE,
    component=COMPONENT,
    rid=rid,
    key_cols=_GRAIN,
    df_before=ga4_session,
)
write_analytics_findings(SOURCE, TABLE, TABLE, _blocks)
