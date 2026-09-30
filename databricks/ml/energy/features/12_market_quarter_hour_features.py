# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # MARKET QUARTER-HOUR FEATURES
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** market area by quarter-hour inputs from 2024-09-08: lagged realised price,
# MAGIC load and generation (at least two days), calendar and the day-ahead
# MAGIC forecasts of the day.

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
COMPONENT = "ml/energy/features/market_quarter_hour_features"
TABLE = "features_market_quarter_hour"
QH_FROM = SPLIT_CALENDARS["quarter_hour"]["train"][0]
LAG_DAYS = [2, 7]
GENERATION = ["onshore_wind", "offshore_wind", "photovoltaic"]

# COMMAND ----------

# DBTITLE 1,Run identity
rid = ml_run_id()

# COMMAND ----------

# DBTITLE 1,Session time zone
set_utc_session()

# COMMAND ----------

# DBTITLE 1,Quarter-hour price spine
prices = (
    read_gold("market_price", source=SOURCE)
    .filter(
        (F.col("interval_reference") == "clock")
        & (F.col("interval_seconds") == 900)
        & (F.col("local_date") >= QH_FROM)
    )
    .select(
        "market_area_code",
        F.col("observation_timestamp_utc").alias("interval_start_utc"),
        "local_date",
        "price_eur_per_mwh",
    )
)

# COMMAND ----------

# DBTITLE 1,Quarter-hour load and generation
_b = read_gold("energy_balance_component", source=SOURCE).filter(
    (F.col("interval_reference") == "clock")
    & (F.col("interval_seconds") == 900)
    & (F.col("local_date") >= QH_FROM)
)
load = (
    _b.filter(F.col("component_kind").isin("consumption", "residual_load"))
    .groupBy("market_area_code", "interval_start_utc")
    .pivot("component_kind", ["consumption", "residual_load"])
    .agg(F.sum("energy_mwh"))
    .withColumnRenamed("consumption", "consumption_mwh")
    .withColumnRenamed("residual_load", "residual_load_mwh")
)
generation = (
    _b.filter(
        (F.col("component_kind") == "generation")
        & F.col("carrier_code").isin(*GENERATION)
    )
    .groupBy("market_area_code", "interval_start_utc")
    .pivot("carrier_code", GENERATION)
    .agg(F.sum("energy_mwh"))
)
for c in GENERATION:
    generation = generation.withColumnRenamed(c, f"generation_{c}_mwh")

# COMMAND ----------

# DBTITLE 1,Join and lag
raw = prices.join(load, ["market_area_code", "interval_start_utc"], "left").join(
    generation, ["market_area_code", "interval_start_utc"], "left"
)
_cols = [
    "price_eur_per_mwh",
    "consumption_mwh",
    "residual_load_mwh",
    *[f"generation_{c}_mwh" for c in GENERATION],
]
lagged = add_timestamp_lags(
    raw,
    keys=["market_area_code"],
    ts_col="interval_start_utc",
    cols=_cols,
    lag_days=LAG_DAYS,
)
lagged = lagged.drop(*_cols)

# COMMAND ----------

# DBTITLE 1,Calendar and the day-ahead forecasts of the day
_local = F.from_utc_timestamp("interval_start_utc", PROJECT_TIMEZONE)
_cal = read_ml("features_calendar_market_area", ecosystem=ECO).select(
    "market_area_code",
    "local_date",
    "day_of_week",
    "is_weekend",
    "is_holiday",
    "is_bridge_day",
    "month",
)
_fc = read_ml("features_market_daily", ecosystem=ECO).select(
    "market_area_code",
    "local_date",
    *[
        c
        for c in read_ml("features_market_daily", ecosystem=ECO).columns
        if c.startswith("forecast_")
    ],
)
out = (
    lagged.withColumn("hour_of_day", F.hour(_local))
    .withColumn("quarter_of_hour", (F.minute(_local) / 15).cast("int"))
    .withColumn("as_of_ts", F.col("interval_start_utc"))
    .join(_cal, ["market_area_code", "local_date"], "left")
    .join(_fc, ["market_area_code", "local_date"], "left")
)
out = add_ml_provenance(out, TABLE, ECO, rid)

# COMMAND ----------

# DBTITLE 1,Grain and forbidden-column gate
_GRAIN = ["market_area_code", "interval_start_utc"]
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
