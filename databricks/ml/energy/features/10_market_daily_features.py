# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # MARKET DAILY FEATURES
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** market area by local date inputs for the price, bias and load datasets:
# MAGIC calendar, lagged realised price, generation and load (at least two days),
# MAGIC and day-ahead forecasts published before the day (vintage unknown).

# COMMAND ----------

# DBTITLE 1,ML shared library
# MAGIC %run ../../_ml_common

# COMMAND ----------

# DBTITLE 1,ML inspection library
# MAGIC %run ../../_ml_inspect

# COMMAND ----------

# DBTITLE 1,Configuration
ECO = "energy"
SOURCE = "smard"
COMPONENT = "ml/energy/features/market_daily_features"
TABLE = "features_market_daily"
DAY = "local_day_europe_berlin"
CARRIERS = [
    "onshore_wind",
    "offshore_wind",
    "photovoltaic",
    "biomass",
    "hydro",
    "lignite",
    "hard_coal",
    "natural_gas",
    "nuclear",
    "pumped_storage",
    "other_conventional",
    "other_renewable",
]
PRICE_LAGS = [2, 7, 14, 28]
GENERATION_LAGS = [2, 7]
LOAD_LAGS = [2, 7, 14]
DATE_FROM = SPLIT_CALENDARS["energy_daily"]["train"][0]

# COMMAND ----------

# DBTITLE 1,Run identity
rid = ml_run_id()

# COMMAND ----------

# DBTITLE 1,Session time zone
set_utc_session()

# COMMAND ----------

# DBTITLE 1,Read Gold
price = read_gold("market_price", source=SOURCE)
balance = read_gold("energy_balance_component", source=SOURCE)
forecast = read_gold("generation_forecast", source=SOURCE)

# COMMAND ----------

# DBTITLE 1,Preflight -- daily rows exist
assert_values_present(
    price, "interval_reference", [DAY], component=COMPONENT, source=SOURCE, rid=rid
)
assert_values_present(
    balance, "interval_reference", [DAY], component=COMPONENT, source=SOURCE, rid=rid
)

# COMMAND ----------

# DBTITLE 1,Daily price
price_daily = (
    price.filter(F.col("interval_reference") == DAY)
    .groupBy("market_area_code", "local_date")
    .agg(F.avg("price_eur_per_mwh").alias("price_eur_per_mwh"))
)

# COMMAND ----------

# DBTITLE 1,Daily realised generation by carrier
generation_daily = (
    balance.filter(
        (F.col("component_kind") == "generation") & (F.col("interval_reference") == DAY)
    )
    .groupBy("market_area_code", "local_date")
    .pivot("carrier_code", CARRIERS)
    .agg(F.sum("energy_mwh"))
)
for c in CARRIERS:
    generation_daily = generation_daily.withColumnRenamed(c, f"generation_{c}_mwh")

# COMMAND ----------

# DBTITLE 1,Daily consumption and residual load
load_daily = (
    balance.filter(
        F.col("component_kind").isin("consumption", "residual_load")
        & (F.col("interval_reference") == DAY)
    )
    .groupBy("market_area_code", "local_date")
    .pivot("component_kind", ["consumption", "residual_load"])
    .agg(F.sum("energy_mwh"))
    .withColumnRenamed("consumption", "consumption_mwh")
    .withColumnRenamed("residual_load", "residual_load_mwh")
)

# COMMAND ----------

# DBTITLE 1,Daily day-ahead forecasts by scope (the disputed PV scope is left out)
forecast_daily = (
    forecast.filter(F.col("interval_reference") == DAY)
    .select("market_area_code", "local_date", F.explode("components").alias("c"))
    .filter(F.col("c.forecast_scope") != "photovoltaic")
    .groupBy("market_area_code", "local_date")
    .pivot(
        "c.forecast_scope",
        ["onshore_wind", "offshore_wind", "other", "total", "wind_and_photovoltaic"],
    )
    .agg(F.sum("c.energy_mwh"))
)
for s in ["onshore_wind", "offshore_wind", "other", "total", "wind_and_photovoltaic"]:
    forecast_daily = forecast_daily.withColumnRenamed(s, f"forecast_{s}_mwh")

# COMMAND ----------

# DBTITLE 1,Spine, lags and calendar
calendar = read_ml("features_calendar_market_area", ecosystem=ECO).select(
    "market_area_code",
    "local_date",
    "day_of_week",
    "is_weekend",
    "month",
    "day_of_year",
    "iso_week",
    "year",
    "is_holiday",
    "is_bridge_day",
    "local_day_hours",
    "as_of_ts",
)
_areas = price_daily.select("market_area_code").distinct()
_last = price_daily.agg(F.max("local_date")).first()[0]
spine = calendar.join(_areas, "market_area_code").filter(
    F.col("local_date").between(DATE_FROM, str(_last))
)
raw = (
    spine.join(price_daily, ["market_area_code", "local_date"], "left")
    .join(generation_daily, ["market_area_code", "local_date"], "left")
    .join(load_daily, ["market_area_code", "local_date"], "left")
)
_gen_cols = [f"generation_{c}_mwh" for c in CARRIERS]
lagged = add_date_lags(
    raw,
    keys=["market_area_code"],
    date_col="local_date",
    cols=["price_eur_per_mwh"],
    lags=PRICE_LAGS,
)
lagged = add_date_lags(
    lagged,
    keys=["market_area_code"],
    date_col="local_date",
    cols=_gen_cols,
    lags=GENERATION_LAGS,
)
lagged = add_date_lags(
    lagged,
    keys=["market_area_code"],
    date_col="local_date",
    cols=["consumption_mwh", "residual_load_mwh"],
    lags=LOAD_LAGS,
)
out = lagged.drop(
    "price_eur_per_mwh", *_gen_cols, "consumption_mwh", "residual_load_mwh"
).join(forecast_daily, ["market_area_code", "local_date"], "left")
out = add_ml_provenance(out, TABLE, ECO, rid)

# COMMAND ----------

# DBTITLE 1,Grain and forbidden-column gate
_GRAIN = ["market_area_code", "local_date"]
assert_unique_grain(out, _GRAIN, component=COMPONENT, source=SOURCE, rid=rid)
assert_no_forbidden_columns(out, component=COMPONENT, source=SOURCE, rid=rid)
check(
    COMPONENT,
    SOURCE,
    "non_empty",
    out.limit(1).count() > 0,
    detail="no rows produced; check the input filters",
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,Write
write_ml(out, TABLE, ecosystem=ECO, source=SOURCE, component=COMPONENT, rid=rid)

# COMMAND ----------

# DBTITLE 1,Inspect and export findings
_blocks = inspect_ml_table(
    read_ml(TABLE, ecosystem=ECO), TABLE, ecosystem=ECO, key_cols=_GRAIN
)
write_ml_findings(ECO, "features__" + TABLE, TABLE, _blocks)
