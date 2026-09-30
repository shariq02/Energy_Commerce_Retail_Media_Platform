# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # REDISPATCH FEATURES
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** transmission operator by local date inputs for the old redispatch regime:
# MAGIC calendar, own lagged event history (at least two days) and the day-ahead
# MAGIC forecasts where the market series exist.

# COMMAND ----------

# DBTITLE 1,ML shared library
# MAGIC %run ../../_ml_common

# COMMAND ----------

# DBTITLE 1,ML inspection library
# MAGIC %run ../../_ml_inspect

# COMMAND ----------

# DBTITLE 1,Configuration
ECO = "energy"
SOURCE = "redispatch"
COMPONENT = "ml/energy/features/redispatch_features"
TABLE = "features_redispatch_daily"
REGIME_START = SPLIT_CALENDARS["redispatch"]["train"][0]
REGIME_END = SPLIT_CALENDARS["redispatch"]["test"][1]
TSOS = ["fifty_hertz", "amprion", "tennet_de", "transnetbw"]
EVENT_LAGS = [2, 3, 7, 14]

# COMMAND ----------

# DBTITLE 1,Run identity
rid = ml_run_id()

# COMMAND ----------

# DBTITLE 1,Daily events per instructing operator
events = (
    read_gold("grid_intervention_event", source=SOURCE)
    .filter(
        F.col("instructing_market_area_code").isin(*TSOS)
        & (F.col("duration_hours") >= 0)
    )
    .withColumn("local_date", F.to_date("event_start_project"))
    .filter(F.col("local_date").between(REGIME_START, REGIME_END))
    .groupBy(
        F.col("instructing_market_area_code").alias("market_area_code"), "local_date"
    )
    .agg(
        F.count("*").alias("event_count"), F.sum("energy_mwh").alias("event_energy_mwh")
    )
)

# COMMAND ----------

# DBTITLE 1,Dense operator by date grid with the event history
_dates = spark.range(1).select(
    F.explode(
        F.sequence(F.lit(REGIME_START).cast("date"), F.lit(REGIME_END).cast("date"))
    ).alias("local_date")
)
_grid = _dates.crossJoin(
    spark.createDataFrame([(t,) for t in TSOS], "market_area_code string")
)
hist = (
    _grid.join(events, ["market_area_code", "local_date"], "left")
    .withColumn(
        "any_event", (F.coalesce(F.col("event_count"), F.lit(0)) > 0).cast("int")
    )
    .withColumn("event_energy_mwh", F.coalesce(F.col("event_energy_mwh"), F.lit(0.0)))
)

# COMMAND ----------

# DBTITLE 1,Lags and rolling shares ending two days before the day
_w7 = Window.partitionBy("market_area_code").orderBy("local_date").rowsBetween(-8, -2)
_w30 = Window.partitionBy("market_area_code").orderBy("local_date").rowsBetween(-31, -2)
lagged = add_date_lags(
    hist,
    keys=["market_area_code"],
    date_col="local_date",
    cols=["any_event", "event_energy_mwh"],
    lags=EVENT_LAGS,
)
lagged = (
    lagged.withColumn("event_share_7d", F.avg("any_event").over(_w7))
    .withColumn("event_share_30d", F.avg("any_event").over(_w30))
    .withColumn("event_energy_mean_30d", F.avg("event_energy_mwh").over(_w30))
    .drop("event_count", "any_event", "event_energy_mwh")
)

# COMMAND ----------

# DBTITLE 1,Calendar and forecasts where they exist
_cal = read_ml("features_calendar_market_area", ecosystem=ECO).select(
    "market_area_code",
    "local_date",
    "day_of_week",
    "is_weekend",
    "month",
    "is_holiday",
    "is_bridge_day",
    "as_of_ts",
)
_mf = read_ml("features_market_daily", ecosystem=ECO)
_fc = _mf.select(
    "market_area_code",
    "local_date",
    *[c for c in _mf.columns if c.startswith("forecast_")],
)
out = lagged.join(_cal, ["market_area_code", "local_date"], "left").join(
    _fc, ["market_area_code", "local_date"], "left"
)
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
