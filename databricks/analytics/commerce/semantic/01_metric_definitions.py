# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # ANALYTICS -- COMMERCE METRIC DEFINITIONS
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** `commerce_semantic.metric_definition` -- scoped to metric
# MAGIC definitions, not computed values. GA4 and REES46 metrics stay listed
# MAGIC separately -- neither source's metric implies the other's data model.

# COMMAND ----------

# DBTITLE 1,Analytics shared library
# MAGIC %run ../../_analytics_common

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "ga4"
COMPONENT = "analytics/commerce/semantic/metric_definition"
TABLE = "metric_definition"

METRICS = [
    (
        "ga4_sessions",
        "Daily GA4 session count, by country.",
        "count(*) grouped by geo_country, local_date",
        "ga4_demand_daily",
        "geo_country x local_date",
        "ga4",
    ),
    (
        "ga4_revenue",
        "Daily GA4 purchase revenue, by country.",
        "sum(total_revenue) grouped by geo_country, local_date",
        "ga4_demand_daily",
        "geo_country x local_date",
        "ga4",
    ),
    (
        "rees46_event_demand",
        "Daily REES46 event volume, by top-level category and event type. No currency -- relative measure only.",
        "count(*) grouped by local_date, category_l1, event_type",
        "rees46_demand_daily",
        "local_date x category_l1 x event_type",
        "rees46",
    ),
]

# COMMAND ----------

# DBTITLE 1,Run identity
rid = analytics_run_id()

# COMMAND ----------

# DBTITLE 1,Build metric_definition
metric_definition = spark.createDataFrame(
    METRICS,
    "metric_key string, definition string, formula string, source_mart string, "
    "grain string, origin_source_system string",
)
metric_definition = add_analytics_provenance(metric_definition, SOURCE, rid)

# COMMAND ----------

# DBTITLE 1,Grain gate
assert_unique_grain(
    metric_definition, ["metric_key"], component=COMPONENT, source=SOURCE, rid=rid
)

# COMMAND ----------

# DBTITLE 1,Write
write_analytics(
    metric_definition,
    TABLE,
    schema=semantic_schema_for(SOURCE),
    source=SOURCE,
    component=COMPONENT,
    rid=rid,
)
