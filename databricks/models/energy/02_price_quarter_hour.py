# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # MODEL PRICE QUARTER HOUR
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** price regression (point and quantile) and negative-price classification on the frozen
# MAGIC quarter-hour dataset.

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
ctx_0 = TaskContext("price_quarter_hour.price", rid, SMOKE)
ctx_1 = TaskContext("price_quarter_hour.negative_price", rid, SMOKE)

# COMMAND ----------

# DBTITLE 1,Read the frozen dataset
df = read_frozen(ctx_0)

# COMMAND ----------

# DBTITLE 1,Price task specification
SPEC_PRICE = {
    "target": "target_price_eur_per_mwh",
    "key_cols": ["market_area_code", "interval_start_utc"],
    "id_features": ["market_area_code"],
    "baseline": {
        "kind": "column",
        "column": "price_eur_per_mwh_lag2d",
        "name": "persistence",
    },
    "models": ["ridge", "gbt_lightgbm", "gbt_sklearn"],
    "quantile": ["quantile_gbt_lightgbm", "quantile_gbt_sklearn"],
    "quantile_cols": ["month"],
    "price": True,
    "fold_mode": "rolling",
    "date_col": "local_date",
    "gap_days": 2,
}

# COMMAND ----------

# DBTITLE 1,Run the price regression
run_regression(ctx_0, df, SPEC_PRICE)

# COMMAND ----------

# DBTITLE 1,Negative-price task specification
SPEC_NEGATIVE = {
    "target": "target_is_negative_price",
    "key_cols": ["market_area_code", "interval_start_utc"],
    "id_features": ["market_area_code"],
    "models": ["logistic", "gbt_lightgbm", "gbt_sklearn"],
    "fold_mode": "rolling",
    "date_col": "local_date",
    "gap_days": 2,
}

# COMMAND ----------

# DBTITLE 1,Run the negative-price classification
run_classification(ctx_1, df, SPEC_NEGATIVE)
