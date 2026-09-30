# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # FIT REDISPATCH
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
SOURCE = "redispatch"
COMPONENT = "ml/energy/fit/03_fit_redispatch"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = ml_run_id()

# COMMAND ----------

# DBTITLE 1,ML fit library
# MAGIC %run ../../_ml_fit

# COMMAND ----------

# DBTITLE 1,Fit and transform redispatch
out_redispatch = fit_transform(
    "redispatch",
    ECO,
    group_cols=["market_area_code"],
    fold_mode="rolling",
    calendar_key="redispatch",
    date_col="local_date",
    impute=True,
    rid=rid,
    source=SOURCE,
)
out_redispatch = add_ml_provenance(out_redispatch, "redispatch", ECO, rid)

# COMMAND ----------

# DBTITLE 1,Gate redispatch
_grain_redispatch = list(
    read_ml("dataset_manifest", ecosystem=ECO)
    .filter(F.col("dataset_id") == "redispatch")
    .first()["grain_key"]
)
assert_unique_grain(
    out_redispatch,
    _grain_redispatch,
    component=COMPONENT + "/redispatch",
    source=SOURCE,
    rid=rid,
)
assert_no_forbidden_columns(
    out_redispatch, component=COMPONENT + "/redispatch", source=SOURCE, rid=rid
)

# COMMAND ----------

# DBTITLE 1,Write redispatch
write_ml(
    out_redispatch,
    "dataset_redispatch",
    ecosystem=ECO,
    source=SOURCE,
    component=COMPONENT + "/redispatch",
    rid=rid,
)
set_dataset_status("redispatch", ECO, "fitted")

# COMMAND ----------

# DBTITLE 1,Inspect and export findings for redispatch
_targets = contract_columns("redispatch", ECO, "target")
_blocks = inspect_ml_table(
    read_ml("dataset_redispatch", ecosystem=ECO),
    "dataset_redispatch",
    ecosystem=ECO,
    key_cols=_grain_redispatch,
    partition_col="partition",
    target_col=_targets[0] if _targets else None,
)
write_ml_findings(ECO, "fit__dataset_redispatch", "dataset_redispatch", _blocks)

# COMMAND ----------

# DBTITLE 1,Fit and transform redispatch_matching
out_redispatch_matching = fit_transform(
    "redispatch_matching",
    ECO,
    group_cols=[],
    fold_mode="grouped",
    calendar_key=None,
    date_col=None,
    impute=True,
    rid=rid,
    source=SOURCE,
)
out_redispatch_matching = add_ml_provenance(
    out_redispatch_matching, "redispatch_matching", ECO, rid
)

# COMMAND ----------

# DBTITLE 1,Gate redispatch_matching
_grain_redispatch_matching = list(
    read_ml("dataset_manifest", ecosystem=ECO)
    .filter(F.col("dataset_id") == "redispatch_matching")
    .first()["grain_key"]
)
assert_unique_grain(
    out_redispatch_matching,
    _grain_redispatch_matching,
    component=COMPONENT + "/redispatch_matching",
    source=SOURCE,
    rid=rid,
)
assert_no_forbidden_columns(
    out_redispatch_matching,
    component=COMPONENT + "/redispatch_matching",
    source=SOURCE,
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,Write redispatch_matching
write_ml(
    out_redispatch_matching,
    "dataset_redispatch_matching",
    ecosystem=ECO,
    source=SOURCE,
    component=COMPONENT + "/redispatch_matching",
    rid=rid,
)
set_dataset_status("redispatch_matching", ECO, "fitted")

# COMMAND ----------

# DBTITLE 1,Inspect and export findings for redispatch_matching
_targets = contract_columns("redispatch_matching", ECO, "target")
_blocks = inspect_ml_table(
    read_ml("dataset_redispatch_matching", ecosystem=ECO),
    "dataset_redispatch_matching",
    ecosystem=ECO,
    key_cols=_grain_redispatch_matching,
    partition_col="partition",
    target_col=_targets[0] if _targets else None,
)
write_ml_findings(
    ECO, "fit__dataset_redispatch_matching", "dataset_redispatch_matching", _blocks
)
