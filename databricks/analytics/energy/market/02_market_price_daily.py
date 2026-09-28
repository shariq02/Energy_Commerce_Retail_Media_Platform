# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # ANALYTICS -- MARKET_PRICE_DAILY
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** `energy_analytics.market_price_daily` -- Gold `market_price`
# MAGIC rolled up from interval to day: mean/min/max price and a volatility
# MAGIC measure (population stddev over the day's intervals). Grain: market area
# MAGIC x local date.

# COMMAND ----------

# DBTITLE 1,Analytics shared library
# MAGIC %run ../../_analytics_common

# COMMAND ----------

# DBTITLE 1,Analytics inspection library
# MAGIC %run ../../_analytics_inspect

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "smard"
COMPONENT = "analytics/energy/market/market_price_daily"
TABLE = "market_price_daily"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = analytics_run_id()

# COMMAND ----------

# DBTITLE 1,Read Gold
market_price = read_gold("market_price", source=SOURCE)

# COMMAND ----------

# DBTITLE 1,Aggregate
market_price_daily = market_price.groupBy("market_area_code", "local_date").agg(
    F.avg("price_eur_per_mwh").alias("mean_price_eur_per_mwh"),
    F.min("price_eur_per_mwh").alias("min_price_eur_per_mwh"),
    F.max("price_eur_per_mwh").alias("max_price_eur_per_mwh"),
    F.stddev_pop("price_eur_per_mwh").alias("price_volatility_eur_per_mwh"),
    F.count(F.lit(1)).alias("interval_count"),
)
market_price_daily = add_analytics_provenance(market_price_daily, SOURCE, rid)

# COMMAND ----------

# DBTITLE 1,Grain gate
_GRAIN = ["market_area_code", "local_date"]
assert_unique_grain(
    market_price_daily, _GRAIN, component=COMPONENT, source=SOURCE, rid=rid
)

# COMMAND ----------

# DBTITLE 1,Write
write_analytics(market_price_daily, TABLE, source=SOURCE, component=COMPONENT, rid=rid)

# COMMAND ----------

# DBTITLE 1,Inspect + export findings
_blocks = inspect_analytics_table(
    market_price_daily,
    TABLE,
    source=SOURCE,
    component=COMPONENT,
    rid=rid,
    key_cols=_GRAIN,
    df_before=market_price,
)
write_analytics_findings(SOURCE, f"market__{TABLE}", TABLE, _blocks)
