# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # MODEL LOAD
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** baseline and candidates for residual and zone load on the frozen dataset.

# COMMAND ----------

# DBTITLE 1,Model shared library
# MAGIC %run ../_model_common

# COMMAND ----------

# DBTITLE 1,Model metrics library
# MAGIC %run ../_model_metrics

# COMMAND ----------

# DBTITLE 1,Tabular model library
# MAGIC %run ../_fit_tabular

# COMMAND ----------

# DBTITLE 1,Smoke parameter
dbutils.widgets.text("smoke", "false")
SMOKE = dbutils.widgets.get("smoke").lower() == "true"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = model_run_id()

# COMMAND ----------

# DBTITLE 1,Task context (frozen version pinned)
ctx = TaskContext("load.load", rid, SMOKE)

# COMMAND ----------

# DBTITLE 1,Read the frozen dataset
df = read_frozen(ctx)

# COMMAND ----------

# DBTITLE 1,Configuration
LOAD_KEYS = ["market_area_code", "load_kind"]
CALENDAR_COLUMNS = [
    "day_of_week",
    "is_weekend",
    "month",
    "day_of_year",
    "iso_week",
    "year",
    "is_holiday",
    "is_bridge_day",
    "local_day_hours",
]
REALISED_LAGS = [2, 7, 14]

# COMMAND ----------

# DBTITLE 1,Helper -- calendar and realised lags for every market area


def complete_load_features(frame):
    """The frozen features exist for one market area only. Calendar columns come
    from the calendar table for every area; realised load at least two days back
    comes from the frame itself."""
    keys = ["market_area_code", "local_date"]
    have = [c for c in CALENDAR_COLUMNS if c in frame.columns]
    calendar = read_ml("features_calendar_market_area", ecosystem=ctx.ecosystem)
    out = frame.drop(*have).join(calendar.select(*keys, *have), keys, "left")
    realised = frame.select(
        *LOAD_KEYS, "local_date", F.col("target_value_mwh").alias("_realised")
    )
    for n in REALISED_LAGS:
        shifted = realised.select(
            *LOAD_KEYS,
            F.date_add("local_date", n).alias("local_date"),
            F.col("_realised").alias(f"realised_lag{n}"),
        )
        out = out.join(shifted, [*LOAD_KEYS, "local_date"], "left")
    return out


# COMMAND ----------

# DBTITLE 1,Complete the features for every market area
df = complete_load_features(df)

# COMMAND ----------

# DBTITLE 1,Task specification
SPEC = {
    "target": "target_value_mwh",
    "key_cols": ["market_area_code", "local_date", "load_kind"],
    "id_features": ["market_area_code", "load_kind"],
    "extra_features": ["realised_lag2", "realised_lag7", "realised_lag14"],
    "baseline": {
        "kind": "group_mean",
        "cols": ["market_area_code", "load_kind", "day_of_week", "month"],
        "name": "seasonal_mean",
    },
    "models": ["ridge", "gbt_lightgbm", "gbt_sklearn"],
    "fold_mode": "rolling",
    "date_col": "local_date",
    "gap_days": 2,
}

# COMMAND ----------

# DBTITLE 1,Run the regression candidates
run_regression(ctx, df, SPEC)
