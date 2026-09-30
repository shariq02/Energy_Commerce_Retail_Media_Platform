# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # FIT RL
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
SOURCE = "smard"
COMPONENT = "ml/energy/fit/09_fit_rl"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = ml_run_id()

# COMMAND ----------

# DBTITLE 1,ML fit library
# MAGIC %run ../../_ml_fit

# COMMAND ----------

# DBTITLE 1,Fit and transform rl_pumped_storage
out_rl_pumped_storage = fit_transform(
    "rl_pumped_storage",
    ECO,
    group_cols=[],
    fold_mode="rolling",
    calendar_key="quarter_hour",
    date_col="local_date",
    impute=True,
    rid=rid,
    source=SOURCE,
)
out_rl_pumped_storage = add_ml_provenance(
    out_rl_pumped_storage, "rl_pumped_storage", ECO, rid
)

# COMMAND ----------

# DBTITLE 1,Gate rl_pumped_storage
_grain_rl_pumped_storage = list(
    read_ml("dataset_manifest", ecosystem=ECO)
    .filter(F.col("dataset_id") == "rl_pumped_storage")
    .first()["grain_key"]
)
assert_unique_grain(
    out_rl_pumped_storage,
    _grain_rl_pumped_storage,
    component=COMPONENT + "/rl_pumped_storage",
    source=SOURCE,
    rid=rid,
)
assert_no_forbidden_columns(
    out_rl_pumped_storage,
    component=COMPONENT + "/rl_pumped_storage",
    source=SOURCE,
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,Write rl_pumped_storage
write_ml(
    out_rl_pumped_storage,
    "dataset_rl_pumped_storage",
    ecosystem=ECO,
    source=SOURCE,
    component=COMPONENT + "/rl_pumped_storage",
    rid=rid,
)
set_dataset_status("rl_pumped_storage", ECO, "fitted")

# COMMAND ----------

# DBTITLE 1,Inspect and export findings for rl_pumped_storage
_targets = contract_columns("rl_pumped_storage", ECO, "target")
_blocks = inspect_ml_table(
    read_ml("dataset_rl_pumped_storage", ecosystem=ECO),
    "dataset_rl_pumped_storage",
    ecosystem=ECO,
    key_cols=_grain_rl_pumped_storage,
    partition_col="partition",
    target_col=_targets[0] if _targets else None,
)
write_ml_findings(
    ECO, "fit__dataset_rl_pumped_storage", "dataset_rl_pumped_storage", _blocks
)

# COMMAND ----------

# DBTITLE 1,Fit and transform rl_redispatch
out_rl_redispatch = fit_transform(
    "rl_redispatch",
    ECO,
    group_cols=["market_area_code"],
    fold_mode="rolling",
    calendar_key="redispatch",
    date_col="local_date",
    impute=True,
    rid=rid,
    source=SOURCE,
)
out_rl_redispatch = add_ml_provenance(out_rl_redispatch, "rl_redispatch", ECO, rid)

# COMMAND ----------

# DBTITLE 1,Gate rl_redispatch
_grain_rl_redispatch = list(
    read_ml("dataset_manifest", ecosystem=ECO)
    .filter(F.col("dataset_id") == "rl_redispatch")
    .first()["grain_key"]
)
assert_unique_grain(
    out_rl_redispatch,
    _grain_rl_redispatch,
    component=COMPONENT + "/rl_redispatch",
    source=SOURCE,
    rid=rid,
)
assert_no_forbidden_columns(
    out_rl_redispatch, component=COMPONENT + "/rl_redispatch", source=SOURCE, rid=rid
)

# COMMAND ----------

# DBTITLE 1,Write rl_redispatch
write_ml(
    out_rl_redispatch,
    "dataset_rl_redispatch",
    ecosystem=ECO,
    source=SOURCE,
    component=COMPONENT + "/rl_redispatch",
    rid=rid,
)
set_dataset_status("rl_redispatch", ECO, "fitted")

# COMMAND ----------

# DBTITLE 1,Inspect and export findings for rl_redispatch
_targets = contract_columns("rl_redispatch", ECO, "target")
_blocks = inspect_ml_table(
    read_ml("dataset_rl_redispatch", ecosystem=ECO),
    "dataset_rl_redispatch",
    ecosystem=ECO,
    key_cols=_grain_rl_redispatch,
    partition_col="partition",
    target_col=_targets[0] if _targets else None,
)
write_ml_findings(ECO, "fit__dataset_rl_redispatch", "dataset_rl_redispatch", _blocks)
