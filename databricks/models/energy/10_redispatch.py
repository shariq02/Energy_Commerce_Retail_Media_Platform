# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # MODEL REDISPATCH
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** two-part redispatch model on the frozen dataset: any event per day (classification)
# MAGIC and log event energy on event days (regression).

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
ctx_0 = TaskContext("redispatch.any_event", rid, SMOKE)
ctx_1 = TaskContext("redispatch.event_energy", rid, SMOKE)

# COMMAND ----------

# DBTITLE 1,Read the frozen dataset
df = read_frozen(ctx_0)

# COMMAND ----------

# DBTITLE 1,Any-event task specification
SPEC_EVENT = {
    "target": "target_any_event",
    "key_cols": ["market_area_code", "local_date"],
    "id_features": ["market_area_code"],
    "models": ["logistic", "gbt_lightgbm", "gbt_sklearn"],
    "fold_mode": "rolling",
    "date_col": "local_date",
    "gap_days": 2,
}

# COMMAND ----------

# DBTITLE 1,Run the any-event classification
run_classification(ctx_0, df, SPEC_EVENT)

# COMMAND ----------

# DBTITLE 1,Event days only
df_events = df.filter(
    F.col("target_any_event") & F.col("target_log_event_energy_mwh").isNotNull()
)

# COMMAND ----------

# DBTITLE 1,Event-energy task specification
SPEC_ENERGY = {
    "target": "target_log_event_energy_mwh",
    "key_cols": ["market_area_code", "local_date"],
    "id_features": ["market_area_code"],
    "baseline": {
        "kind": "group_mean",
        "cols": ["market_area_code"],
        "name": "zone_mean",
    },
    "models": ["ridge", "gbt_lightgbm", "gbt_sklearn"],
    "fold_mode": "rolling",
    "date_col": "local_date",
    "gap_days": 2,
}

# COMMAND ----------

# DBTITLE 1,Run the event-energy regression
run_regression(ctx_1, df_events, SPEC_ENERGY)
