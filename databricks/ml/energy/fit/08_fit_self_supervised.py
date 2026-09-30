# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # FIT SELF-SUPERVISED
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** fit the datasets, applying the null rule, fitting imputers on the training
# MAGIC partition only and per fold, transforming every partition and
# MAGIC materialising the final tables. Self-supervised windows are not imputed;
# MAGIC nulls are the data and masks come from the stored specification.

# COMMAND ----------

# DBTITLE 1,ML shared library
# MAGIC %run ../../_ml_common

# COMMAND ----------

# DBTITLE 1,ML inspection library
# MAGIC %run ../../_ml_inspect

# COMMAND ----------

# DBTITLE 1,Configuration
ECO = "energy"
SOURCE = "dwd"
COMPONENT = "ml/energy/fit/08_fit_self_supervised"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = ml_run_id()

# COMMAND ----------

# DBTITLE 1,ML fit library
# MAGIC %run ../../_ml_fit

# COMMAND ----------

# DBTITLE 1,Fit and transform weather_imputation
out_weather_imputation = fit_transform(
    "weather_imputation",
    ECO,
    group_cols=[],
    fold_mode="none",
    calendar_key=None,
    date_col=None,
    impute=False,
    rid=rid,
    source=SOURCE,
)
out_weather_imputation = add_ml_provenance(
    out_weather_imputation, "weather_imputation", ECO, rid
)

# COMMAND ----------

# DBTITLE 1,Gate weather_imputation
_grain_weather_imputation = list(
    read_ml("dataset_manifest", ecosystem=ECO)
    .filter(F.col("dataset_id") == "weather_imputation")
    .first()["grain_key"]
)
assert_unique_grain(
    out_weather_imputation,
    _grain_weather_imputation,
    component=COMPONENT + "/weather_imputation",
    source=SOURCE,
    rid=rid,
)
assert_no_forbidden_columns(
    out_weather_imputation,
    component=COMPONENT + "/weather_imputation",
    source=SOURCE,
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,Write weather_imputation
write_ml(
    out_weather_imputation,
    "dataset_weather_imputation",
    ecosystem=ECO,
    source=SOURCE,
    component=COMPONENT + "/weather_imputation",
    rid=rid,
)
set_dataset_status("weather_imputation", ECO, "fitted")

# COMMAND ----------

# DBTITLE 1,Inspect and export findings for weather_imputation
_targets = contract_columns("weather_imputation", ECO, "target")
_blocks = inspect_ml_table(
    read_ml("dataset_weather_imputation", ecosystem=ECO),
    "dataset_weather_imputation",
    ecosystem=ECO,
    key_cols=_grain_weather_imputation,
    partition_col="partition",
    target_col=_targets[0] if _targets else None,
)
write_ml_findings(
    ECO, "fit__dataset_weather_imputation", "dataset_weather_imputation", _blocks
)

# COMMAND ----------

# DBTITLE 1,Fit and transform self_supervised_other
out_self_supervised_other = fit_transform(
    "self_supervised_other",
    ECO,
    group_cols=[],
    fold_mode="none",
    calendar_key=None,
    date_col=None,
    impute=False,
    rid=rid,
    source=SOURCE,
)
out_self_supervised_other = add_ml_provenance(
    out_self_supervised_other, "self_supervised_other", ECO, rid
)

# COMMAND ----------

# DBTITLE 1,Gate self_supervised_other
_grain_self_supervised_other = list(
    read_ml("dataset_manifest", ecosystem=ECO)
    .filter(F.col("dataset_id") == "self_supervised_other")
    .first()["grain_key"]
)
assert_unique_grain(
    out_self_supervised_other,
    _grain_self_supervised_other,
    component=COMPONENT + "/self_supervised_other",
    source=SOURCE,
    rid=rid,
)
assert_no_forbidden_columns(
    out_self_supervised_other,
    component=COMPONENT + "/self_supervised_other",
    source=SOURCE,
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,Write self_supervised_other
write_ml(
    out_self_supervised_other,
    "dataset_self_supervised_other",
    ecosystem=ECO,
    source=SOURCE,
    component=COMPONENT + "/self_supervised_other",
    rid=rid,
)
set_dataset_status("self_supervised_other", ECO, "fitted")

# COMMAND ----------

# DBTITLE 1,Inspect and export findings for self_supervised_other
_targets = contract_columns("self_supervised_other", ECO, "target")
_blocks = inspect_ml_table(
    read_ml("dataset_self_supervised_other", ecosystem=ECO),
    "dataset_self_supervised_other",
    ecosystem=ECO,
    key_cols=_grain_self_supervised_other,
    partition_col="partition",
    target_col=_targets[0] if _targets else None,
)
write_ml_findings(
    ECO, "fit__dataset_self_supervised_other", "dataset_self_supervised_other", _blocks
)

# COMMAND ----------

# DBTITLE 1,Fit and transform weak_supervision
out_weak_supervision = fit_transform(
    "weak_supervision",
    ECO,
    group_cols=[],
    fold_mode="none",
    calendar_key=None,
    date_col=None,
    impute=False,
    rid=rid,
    source=SOURCE,
)
out_weak_supervision = add_ml_provenance(
    out_weak_supervision, "weak_supervision", ECO, rid
)

# COMMAND ----------

# DBTITLE 1,Gate weak_supervision
_grain_weak_supervision = list(
    read_ml("dataset_manifest", ecosystem=ECO)
    .filter(F.col("dataset_id") == "weak_supervision")
    .first()["grain_key"]
)
assert_unique_grain(
    out_weak_supervision,
    _grain_weak_supervision,
    component=COMPONENT + "/weak_supervision",
    source=SOURCE,
    rid=rid,
)
assert_no_forbidden_columns(
    out_weak_supervision,
    component=COMPONENT + "/weak_supervision",
    source=SOURCE,
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,Write weak_supervision
write_ml(
    out_weak_supervision,
    "dataset_weak_supervision",
    ecosystem=ECO,
    source=SOURCE,
    component=COMPONENT + "/weak_supervision",
    rid=rid,
)
set_dataset_status("weak_supervision", ECO, "fitted")

# COMMAND ----------

# DBTITLE 1,Inspect and export findings for weak_supervision
_targets = contract_columns("weak_supervision", ECO, "target")
_blocks = inspect_ml_table(
    read_ml("dataset_weak_supervision", ecosystem=ECO),
    "dataset_weak_supervision",
    ecosystem=ECO,
    key_cols=_grain_weak_supervision,
    partition_col="partition",
    target_col=_targets[0] if _targets else None,
)
write_ml_findings(
    ECO, "fit__dataset_weak_supervision", "dataset_weak_supervision", _blocks
)
