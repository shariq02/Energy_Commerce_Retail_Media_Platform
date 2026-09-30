# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # FIT SESSION PURCHASE
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
ECO = "commerce"
SOURCE = "rees46"
COMPONENT = "ml/commerce/fit/01_fit_session_purchase"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = ml_run_id()

# COMMAND ----------

# DBTITLE 1,ML fit library
# MAGIC %run ../../_ml_fit

# COMMAND ----------

# DBTITLE 1,Fit and transform session_purchase_rees46
out_session_purchase_rees46 = fit_transform(
    "session_purchase_rees46",
    ECO,
    group_cols=[],
    fold_mode="grouped",
    calendar_key=None,
    date_col=None,
    impute=True,
    rid=rid,
    source=SOURCE,
)
out_session_purchase_rees46 = add_ml_provenance(
    out_session_purchase_rees46, "session_purchase_rees46", ECO, rid
)

# COMMAND ----------

# DBTITLE 1,Gate session_purchase_rees46
_grain_session_purchase_rees46 = list(
    read_ml("dataset_manifest", ecosystem=ECO)
    .filter(F.col("dataset_id") == "session_purchase_rees46")
    .first()["grain_key"]
)
assert_unique_grain(
    out_session_purchase_rees46,
    _grain_session_purchase_rees46,
    component=COMPONENT + "/session_purchase_rees46",
    source=SOURCE,
    rid=rid,
)
assert_no_forbidden_columns(
    out_session_purchase_rees46,
    component=COMPONENT + "/session_purchase_rees46",
    source=SOURCE,
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,Write session_purchase_rees46
write_ml(
    out_session_purchase_rees46,
    "dataset_session_purchase_rees46",
    ecosystem=ECO,
    source=SOURCE,
    component=COMPONENT + "/session_purchase_rees46",
    rid=rid,
)
set_dataset_status("session_purchase_rees46", ECO, "fitted")

# COMMAND ----------

# DBTITLE 1,Inspect and export findings for session_purchase_rees46
_targets = contract_columns("session_purchase_rees46", ECO, "target")
_blocks = inspect_ml_table(
    read_ml("dataset_session_purchase_rees46", ecosystem=ECO),
    "dataset_session_purchase_rees46",
    ecosystem=ECO,
    key_cols=_grain_session_purchase_rees46,
    partition_col="partition",
    target_col=_targets[0] if _targets else None,
)
write_ml_findings(
    ECO,
    "fit__dataset_session_purchase_rees46",
    "dataset_session_purchase_rees46",
    _blocks,
)

# COMMAND ----------

# DBTITLE 1,Fit and transform session_purchase_ga4
out_session_purchase_ga4 = fit_transform(
    "session_purchase_ga4",
    ECO,
    group_cols=[],
    fold_mode="grouped",
    calendar_key=None,
    date_col=None,
    impute=True,
    rid=rid,
    source=SOURCE,
)
out_session_purchase_ga4 = add_ml_provenance(
    out_session_purchase_ga4, "session_purchase_ga4", ECO, rid
)

# COMMAND ----------

# DBTITLE 1,Gate session_purchase_ga4
_grain_session_purchase_ga4 = list(
    read_ml("dataset_manifest", ecosystem=ECO)
    .filter(F.col("dataset_id") == "session_purchase_ga4")
    .first()["grain_key"]
)
assert_unique_grain(
    out_session_purchase_ga4,
    _grain_session_purchase_ga4,
    component=COMPONENT + "/session_purchase_ga4",
    source=SOURCE,
    rid=rid,
)
assert_no_forbidden_columns(
    out_session_purchase_ga4,
    component=COMPONENT + "/session_purchase_ga4",
    source=SOURCE,
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,Write session_purchase_ga4
write_ml(
    out_session_purchase_ga4,
    "dataset_session_purchase_ga4",
    ecosystem=ECO,
    source=SOURCE,
    component=COMPONENT + "/session_purchase_ga4",
    rid=rid,
)
set_dataset_status("session_purchase_ga4", ECO, "fitted")

# COMMAND ----------

# DBTITLE 1,Inspect and export findings for session_purchase_ga4
_targets = contract_columns("session_purchase_ga4", ECO, "target")
_blocks = inspect_ml_table(
    read_ml("dataset_session_purchase_ga4", ecosystem=ECO),
    "dataset_session_purchase_ga4",
    ecosystem=ECO,
    key_cols=_grain_session_purchase_ga4,
    partition_col="partition",
    target_col=_targets[0] if _targets else None,
)
write_ml_findings(
    ECO, "fit__dataset_session_purchase_ga4", "dataset_session_purchase_ga4", _blocks
)
