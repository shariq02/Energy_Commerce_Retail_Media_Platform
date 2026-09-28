# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # ANALYTICS -- SITE_ENERGY_DAILY
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** `energy_analytics.site_energy_daily` -- Gold `channel_reading`'s
# MAGIC `increment` rows (the true additive energy flow) rolled up from interval
# MAGIC to day. `cumulative` and `power` value kinds are a running total or an
# MAGIC instantaneous rate, not a quantity a day can sum -- left for a later
# MAGIC notebook once a stated need for them exists. Grain: site x local date x
# MAGIC subsystem x channel.

# COMMAND ----------

# DBTITLE 1,Analytics shared library
# MAGIC %run ../../_analytics_common

# COMMAND ----------

# DBTITLE 1,Analytics inspection library
# MAGIC %run ../../_analytics_inspect

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "honda_iot"
COMPONENT = "analytics/energy/site/site_energy_daily"
TABLE = "site_energy_daily"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = analytics_run_id()

# COMMAND ----------

# DBTITLE 1,Read Gold
channel_reading = read_gold("channel_reading", source=SOURCE)

# COMMAND ----------

# DBTITLE 1,Filter to the increment value kind
_increment = channel_reading.filter(F.col("value_kind") == "increment")

# COMMAND ----------

# DBTITLE 1,Aggregate
site_energy_daily = _increment.groupBy(
    "location_key", "local_date", "subsystem", "channel", "unit"
).agg(
    F.sum("value").alias("daily_energy"),
    F.count(F.lit(1)).alias("interval_count"),
)
site_energy_daily = add_analytics_provenance(site_energy_daily, SOURCE, rid)

# COMMAND ----------

# DBTITLE 1,Grain gate
_GRAIN = ["location_key", "local_date", "subsystem", "channel"]
assert_unique_grain(
    site_energy_daily, _GRAIN, component=COMPONENT, source=SOURCE, rid=rid
)

# COMMAND ----------

# DBTITLE 1,Write
write_analytics(site_energy_daily, TABLE, source=SOURCE, component=COMPONENT, rid=rid)

# COMMAND ----------

# DBTITLE 1,Inspect + export findings
_blocks = inspect_analytics_table(
    site_energy_daily,
    TABLE,
    source=SOURCE,
    component=COMPONENT,
    rid=rid,
    key_cols=_GRAIN,
    df_before=_increment,
)
write_analytics_findings(SOURCE, f"site__{TABLE}", TABLE, _blocks)
