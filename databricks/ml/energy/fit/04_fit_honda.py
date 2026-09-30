# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # FIT HONDA
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** fit the datasets, applying the null rule, fitting imputers on the training
# MAGIC partition only and per fold, transforming every partition and
# MAGIC materialising the final tables.

# COMMAND ----------

# DBTITLE 1,ML shared library
# MAGIC %run ../../_ml_common

# COMMAND ----------

# DBTITLE 1,ML inspection library
# MAGIC %run ../../_ml_inspect

# COMMAND ----------

# DBTITLE 1,Configuration
ECO = "energy"
SOURCE = "honda_iot"
COMPONENT = "ml/energy/fit/04_fit_honda"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = ml_run_id()

# COMMAND ----------

# DBTITLE 1,ML fit library
# MAGIC %run ../../_ml_fit

# COMMAND ----------

# DBTITLE 1,Fit and transform honda_forecast
out_honda_forecast = fit_transform(
    "honda_forecast",
    ECO,
    group_cols=["location_key", "channel"],
    fold_mode="rolling",
    calendar_key="honda",
    date_col="local_date",
    impute=True,
    rid=rid,
    source=SOURCE,
)
out_honda_forecast = add_ml_provenance(out_honda_forecast, "honda_forecast", ECO, rid)

# COMMAND ----------

# DBTITLE 1,Gate honda_forecast
_grain_honda_forecast = list(
    read_ml("dataset_manifest", ecosystem=ECO)
    .filter(F.col("dataset_id") == "honda_forecast")
    .first()["grain_key"]
)
assert_unique_grain(
    out_honda_forecast,
    _grain_honda_forecast,
    component=COMPONENT + "/honda_forecast",
    source=SOURCE,
    rid=rid,
)
assert_no_forbidden_columns(
    out_honda_forecast, component=COMPONENT + "/honda_forecast", source=SOURCE, rid=rid
)

# COMMAND ----------

# DBTITLE 1,Write honda_forecast
write_ml(
    out_honda_forecast,
    "dataset_honda_forecast",
    ecosystem=ECO,
    source=SOURCE,
    component=COMPONENT + "/honda_forecast",
    rid=rid,
)
set_dataset_status("honda_forecast", ECO, "fitted")

# COMMAND ----------

# DBTITLE 1,Inspect and export findings for honda_forecast
_targets = contract_columns("honda_forecast", ECO, "target")
_blocks = inspect_ml_table(
    read_ml("dataset_honda_forecast", ecosystem=ECO),
    "dataset_honda_forecast",
    ecosystem=ECO,
    key_cols=_grain_honda_forecast,
    partition_col="partition",
    target_col=_targets[0] if _targets else None,
)
write_ml_findings(ECO, "fit__dataset_honda_forecast", "dataset_honda_forecast", _blocks)

# COMMAND ----------

# DBTITLE 1,Fit and transform honda_anomaly
out_honda_anomaly = fit_transform(
    "honda_anomaly",
    ECO,
    group_cols=["location_key", "channel"],
    fold_mode="rolling",
    calendar_key="honda",
    date_col="local_date",
    impute=True,
    rid=rid,
    source=SOURCE,
)
out_honda_anomaly = add_ml_provenance(out_honda_anomaly, "honda_anomaly", ECO, rid)

# COMMAND ----------

# DBTITLE 1,Gate honda_anomaly
_grain_honda_anomaly = list(
    read_ml("dataset_manifest", ecosystem=ECO)
    .filter(F.col("dataset_id") == "honda_anomaly")
    .first()["grain_key"]
)
assert_unique_grain(
    out_honda_anomaly,
    _grain_honda_anomaly,
    component=COMPONENT + "/honda_anomaly",
    source=SOURCE,
    rid=rid,
)
assert_no_forbidden_columns(
    out_honda_anomaly, component=COMPONENT + "/honda_anomaly", source=SOURCE, rid=rid
)

# COMMAND ----------

# DBTITLE 1,Write honda_anomaly
write_ml(
    out_honda_anomaly,
    "dataset_honda_anomaly",
    ecosystem=ECO,
    source=SOURCE,
    component=COMPONENT + "/honda_anomaly",
    rid=rid,
)
set_dataset_status("honda_anomaly", ECO, "fitted")

# COMMAND ----------

# DBTITLE 1,Inspect and export findings for honda_anomaly
_targets = contract_columns("honda_anomaly", ECO, "target")
_blocks = inspect_ml_table(
    read_ml("dataset_honda_anomaly", ecosystem=ECO),
    "dataset_honda_anomaly",
    ecosystem=ECO,
    key_cols=_grain_honda_anomaly,
    partition_col="partition",
    target_col=_targets[0] if _targets else None,
)
write_ml_findings(ECO, "fit__dataset_honda_anomaly", "dataset_honda_anomaly", _blocks)
