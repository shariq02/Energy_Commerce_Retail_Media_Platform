# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # GOLD -- FORECAST_ACTUAL
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** `energy_gold.forecast_actual` -- pairs each forecast
# MAGIC component (exploded from `electricity_generation_forecast`) with the
# MAGIC matching actual (`energy_balance_component`, built earlier this step) on
# MAGIC market area, native label and the same interval. This assumes forecast
# MAGIC and actual publish at the same resolution -- a real range-window
# MAGIC aggregation (actuals summed into a coarser forecast window) is not built
# MAGIC yet; `realised_window_coverage` is 1.0 for a matched interval, NULL for
# MAGIC an unmatched one, never a computed fraction. No error metric or score is
# MAGIC computed -- pairing only.

# COMMAND ----------

# DBTITLE 1,Shared Silver library
# MAGIC %run ../../../silver/_silver_common

# COMMAND ----------

# DBTITLE 1,Gold shared library
# MAGIC %run ../../_gold_common

# COMMAND ----------

# DBTITLE 1,Gold inspection library
# MAGIC %run ../../_gold_inspect

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "smard"
COMPONENT = "gold/energy/market/forecast_actual"
TABLE = "forecast_actual"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = gold_run_id()

# COMMAND ----------

# DBTITLE 1,Read Silver + the actual side already built this step
electricity_generation_forecast = read_silver("electricity_generation_forecast")
energy_balance_component = read_gold("energy_balance_component", source=SOURCE)

# COMMAND ----------

# DBTITLE 1,Explode forecast components
forecast = (
    electricity_generation_forecast.select(
        "location_key",
        "market_area_code",
        F.col("observation_timestamp_utc").alias("target_window_start"),
        "interval_seconds",
        "interval_reference",
        "forecast_issue_timestamp",
        F.explode("components").alias("_c"),
        "source_system",
        "source_dataset",
        "source_record_id",
    )
    .select(
        "*",
        F.col("_c.forecast_scope").alias("forecast_scope"),
        F.col("_c.native_label").alias("native_label"),
        F.col("_c.energy_mwh").alias("forecast_value"),
        F.col("_c.semantic_status").alias("semantic_status"),
        F.col("_c.semantic_issue_ref").alias("semantic_issue_ref"),
    )
    .drop("_c")
)

# COMMAND ----------

# DBTITLE 1,Aggregate the actual side to (place, market area, native label, interval)
actual = energy_balance_component.groupBy(
    "location_key",
    "interval_start_utc",
    "interval_seconds",
    "interval_reference",
    "native_label",
).agg(F.sum("energy_mwh").alias("realised_value"))

# COMMAND ----------

# DBTITLE 1,Pair forecast with actual on the same interval
forecast_actual = forecast.join(
    actual,
    (forecast.location_key == actual.location_key)
    & (forecast.target_window_start == actual.interval_start_utc)
    & (forecast.interval_seconds == actual.interval_seconds)
    & (forecast.interval_reference == actual.interval_reference)
    & (forecast.native_label == actual.native_label),
    "left",
).select(
    forecast["*"],
    "realised_value",
    F.when(F.col("realised_value").isNotNull(), F.lit(1.0)).alias(
        "realised_window_coverage"
    ),
)

# COMMAND ----------

# DBTITLE 1,Build the Gold row
forecast_actual = add_gold_provenance(forecast_actual, SOURCE, rid)

# COMMAND ----------

# DBTITLE 1,Grain gate
_GRAIN = ["location_key", "market_area_code", "target_window_start", "forecast_scope"]
assert_unique_grain(
    forecast_actual, _GRAIN, component=COMPONENT, source=SOURCE, rid=rid
)

# COMMAND ----------

# DBTITLE 1,Write
write_gold(forecast_actual, TABLE, source=SOURCE, component=COMPONENT, rid=rid)

# COMMAND ----------

# DBTITLE 1,Inspect + export findings
_blocks = inspect_gold_table(
    forecast_actual, TABLE, source=SOURCE, component=COMPONENT, rid=rid, key_cols=_GRAIN
)
write_gold_findings(SOURCE, f"market__{TABLE}", TABLE, _blocks)
