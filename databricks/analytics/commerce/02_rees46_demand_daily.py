# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # ANALYTICS -- REES46_DEMAND_DAILY
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** `commerce_analytics.rees46_demand_daily` -- Gold `rees46_event`
# MAGIC rolled up to day x top-level category x event type. REES46 carries no
# MAGIC currency (`currency_unknown`), so this is an event/demand-volume mart, not
# MAGIC a revenue one -- relative measures only, per the source's own limitation.
# MAGIC Grain: local date x category_l1 x event_type.

# COMMAND ----------

# DBTITLE 1,Analytics shared library
# MAGIC %run ../_analytics_common

# COMMAND ----------

# DBTITLE 1,Analytics inspection library
# MAGIC %run ../_analytics_inspect

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "rees46"
COMPONENT = "analytics/commerce/rees46_demand_daily"
TABLE = "rees46_demand_daily"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = analytics_run_id()

# COMMAND ----------

# DBTITLE 1,Read Gold
rees46_event = read_gold("rees46_event", source=SOURCE)

# COMMAND ----------

# DBTITLE 1,Aggregate
rees46_demand_daily = rees46_event.groupBy(
    "local_date", "category_l1", "event_type"
).agg(
    F.count(F.lit(1)).alias("event_count"),
    F.countDistinct("user_session").alias("session_count"),
    F.countDistinct("product_id").alias("distinct_product_count"),
)
rees46_demand_daily = add_analytics_provenance(rees46_demand_daily, SOURCE, rid)

# COMMAND ----------

# DBTITLE 1,Grain gate
_GRAIN = ["local_date", "category_l1", "event_type"]
assert_unique_grain(
    rees46_demand_daily, _GRAIN, component=COMPONENT, source=SOURCE, rid=rid
)

# COMMAND ----------

# DBTITLE 1,Write
write_analytics(rees46_demand_daily, TABLE, source=SOURCE, component=COMPONENT, rid=rid)

# COMMAND ----------

# DBTITLE 1,Inspect + export findings
_blocks = inspect_analytics_table(
    rees46_demand_daily,
    TABLE,
    source=SOURCE,
    component=COMPONENT,
    rid=rid,
    key_cols=_GRAIN,
    df_before=rees46_event,
)
write_analytics_findings(SOURCE, TABLE, TABLE, _blocks)
