# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # FIT SURVIVAL
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
SOURCE = "mastr"
COMPONENT = "ml/energy/fit/05_fit_survival"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = ml_run_id()

# COMMAND ----------

# DBTITLE 1,ML fit library
# MAGIC %run ../../_ml_fit

# COMMAND ----------

# DBTITLE 1,Fit and transform survival
out_survival = fit_transform(
    "survival",
    ECO,
    group_cols=["is_offshore"],
    fold_mode="grouped",
    calendar_key=None,
    date_col=None,
    impute=True,
    rid=rid,
    source=SOURCE,
)
out_survival = add_ml_provenance(out_survival, "survival", ECO, rid)

# COMMAND ----------

# DBTITLE 1,Gate survival
_grain_survival = list(
    read_ml("dataset_manifest", ecosystem=ECO)
    .filter(F.col("dataset_id") == "survival")
    .first()["grain_key"]
)
assert_unique_grain(
    out_survival,
    _grain_survival,
    component=COMPONENT + "/survival",
    source=SOURCE,
    rid=rid,
)
assert_no_forbidden_columns(
    out_survival, component=COMPONENT + "/survival", source=SOURCE, rid=rid
)

# COMMAND ----------

# DBTITLE 1,Write survival
write_ml(
    out_survival,
    "dataset_survival",
    ecosystem=ECO,
    source=SOURCE,
    component=COMPONENT + "/survival",
    rid=rid,
)
set_dataset_status("survival", ECO, "fitted")

# COMMAND ----------

# DBTITLE 1,Inspect and export findings for survival
_targets = contract_columns("survival", ECO, "target")
_blocks = inspect_ml_table(
    read_ml("dataset_survival", ecosystem=ECO),
    "dataset_survival",
    ecosystem=ECO,
    key_cols=_grain_survival,
    partition_col="partition",
    target_col=_targets[0] if _targets else None,
)
write_ml_findings(ECO, "fit__dataset_survival", "dataset_survival", _blocks)
