# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # PV FORECAST FEATURES
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** market area by local date PV inputs: the derived day-ahead PV forecast
# MAGIC (published before the day) and lagged realised PV generation.

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
COMPONENT = "ml/energy/features/pv_forecast_features"
TABLE = "features_pv_forecast_daily"
DAY = "local_day_europe_berlin"
PV_LAGS = [2, 7]

# COMMAND ----------

# DBTITLE 1,Run identity
rid = ml_run_id()

# COMMAND ----------

# DBTITLE 1,Read the derived PV view and realised PV
if not spark.catalog.tableExists(ml_fqn("derived_pv_forecast", ECO)):
    raise RuntimeError("run 03_derived_pv_forecast first")
derived = (
    read_ml("derived_pv_forecast", ecosystem=ECO)
    .filter(F.col("interval_reference") == DAY)
    .groupBy("market_area_code", "local_date")
    .agg(F.avg("forecast_photovoltaic_mwh").alias("forecast_photovoltaic_mwh"))
)
realised = (
    read_gold("energy_balance_component", source=SOURCE)
    .filter(
        (F.col("component_kind") == "generation")
        & (F.col("carrier_code") == "photovoltaic")
        & (F.col("interval_reference") == DAY)
    )
    .groupBy("market_area_code", "local_date")
    .agg(F.sum("energy_mwh").alias("generation_photovoltaic_mwh"))
)

# COMMAND ----------

# DBTITLE 1,Lag the realised PV and attach the forecast
_base = derived.join(realised, ["market_area_code", "local_date"], "outer")
_lagged = add_date_lags(
    _base,
    keys=["market_area_code"],
    date_col="local_date",
    cols=["generation_photovoltaic_mwh"],
    lags=PV_LAGS,
)
out = _lagged.drop("generation_photovoltaic_mwh").filter(
    F.col("forecast_photovoltaic_mwh").isNotNull()
)
out = out.withColumn("forecast_value_origin", F.lit("derived")).withColumn(
    "forecast_vintage", F.lit("published_before_day_unknown_issue_time")
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
