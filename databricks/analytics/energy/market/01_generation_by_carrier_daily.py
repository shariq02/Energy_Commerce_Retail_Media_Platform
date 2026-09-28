# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # ANALYTICS -- GENERATION_BY_CARRIER_DAILY
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** `energy_analytics.generation_by_carrier_daily` -- Gold
# MAGIC `energy_balance_component`'s generation rows rolled up from interval to
# MAGIC day, by market area and resolved carrier. Grain: market area x local
# MAGIC date x carrier x resolution.

# COMMAND ----------

# DBTITLE 1,Analytics shared library
# MAGIC %run ../../_analytics_common

# COMMAND ----------

# DBTITLE 1,Analytics inspection library
# MAGIC %run ../../_analytics_inspect

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "smard"
COMPONENT = "analytics/energy/market/generation_by_carrier_daily"
TABLE = "generation_by_carrier_daily"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = analytics_run_id()

# COMMAND ----------

# DBTITLE 1,Read Gold
energy_balance_component = read_gold("energy_balance_component", source=SOURCE)

# COMMAND ----------

# DBTITLE 1,Filter to generation, roll up interval to day
_generation = energy_balance_component.filter(
    F.col("component_kind") == "generation"
).withColumn("resolution_seconds", F.col("interval_seconds"))

# COMMAND ----------

# DBTITLE 1,Aggregate
generation_by_carrier_daily = _generation.groupBy(
    "market_area_code", "local_date", "carrier_key", "resolution_seconds"
).agg(
    F.sum("energy_mwh").alias("generation_mwh"),
    F.countDistinct("origin_source_system").alias("source_count"),
    F.count(F.lit(1)).alias("interval_count"),
)
generation_by_carrier_daily = add_analytics_provenance(
    generation_by_carrier_daily, SOURCE, rid
)

# COMMAND ----------

# DBTITLE 1,Grain gate
_GRAIN = ["market_area_code", "local_date", "carrier_key", "resolution_seconds"]
assert_unique_grain(
    generation_by_carrier_daily, _GRAIN, component=COMPONENT, source=SOURCE, rid=rid
)

# COMMAND ----------

# DBTITLE 1,Write
write_analytics(
    generation_by_carrier_daily, TABLE, source=SOURCE, component=COMPONENT, rid=rid
)

# COMMAND ----------

# DBTITLE 1,Inspect + export findings
_blocks = inspect_analytics_table(
    generation_by_carrier_daily,
    TABLE,
    source=SOURCE,
    component=COMPONENT,
    rid=rid,
    key_cols=_GRAIN,
    df_before=_generation,
)
write_analytics_findings(SOURCE, f"market__{TABLE}", TABLE, _blocks)
