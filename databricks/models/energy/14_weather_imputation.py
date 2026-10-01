# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # MODEL WEATHER IMPUTATION
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** self-supervised reconstruction of masked weather blocks on the frozen dataset, scored
# MAGIC against a causal carry-forward baseline.

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

# DBTITLE 1,Reconstruction model library
# MAGIC %run ../_fit_reconstruction

# COMMAND ----------

# DBTITLE 1,Smoke parameter
dbutils.widgets.text("smoke", "false")
SMOKE = dbutils.widgets.get("smoke").lower() == "true"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = model_run_id()

# COMMAND ----------

# DBTITLE 1,Task context (frozen version pinned)
ctx = TaskContext("weather_imputation.reconstruction", rid, SMOKE)

# COMMAND ----------

# DBTITLE 1,Read the frozen dataset
df = read_frozen(ctx, apply_smoke=False)

# COMMAND ----------

# DBTITLE 1,Task specification
SPEC = {
    "sets": [
        {
            "name": "weather",
            "id_col": "location_key",
            "ts_col": "observation_timestamp_utc",
            "variables": [
                "air_temperature",
                "dew_point_temperature",
                "relative_humidity",
                "pressure_station",
                "pressure_sea_level",
                "wind_speed",
                "wind_direction",
                "cloud_cover",
                "visibility",
                "soil_temperature",
            ],
            "freq": "h",
            "period": 24,
            "step_hours": 1,
            "baseline": "carry_forward",
            "circular": ["wind_direction"],
        }
    ],
    "models": [
        "ridge_cross_variable",
        "gbt_cross_variable_lightgbm",
        "gbt_cross_variable_sklearn",
        "masked_sequence_model",
    ],
}

# COMMAND ----------

# DBTITLE 1,Run the reconstruction candidates
run_reconstruction(ctx, df, SPEC)
