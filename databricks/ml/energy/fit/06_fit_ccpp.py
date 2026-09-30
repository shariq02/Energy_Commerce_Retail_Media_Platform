# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # FIT CCPP
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
SOURCE = "power_plant_ccpp"
COMPONENT = "ml/energy/fit/06_fit_ccpp"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = ml_run_id()

# COMMAND ----------

# DBTITLE 1,ML fit library
# MAGIC %run ../../_ml_fit

# COMMAND ----------

# DBTITLE 1,Fit and transform ccpp
out_ccpp = fit_transform(
    "ccpp",
    ECO,
    group_cols=[],
    fold_mode="grouped",
    calendar_key=None,
    date_col=None,
    impute=True,
    rid=rid,
    source=SOURCE,
)
out_ccpp = add_ml_provenance(out_ccpp, "ccpp", ECO, rid)

# COMMAND ----------

# DBTITLE 1,Gate ccpp
_grain_ccpp = list(
    read_ml("dataset_manifest", ecosystem=ECO)
    .filter(F.col("dataset_id") == "ccpp")
    .first()["grain_key"]
)
assert_unique_grain(
    out_ccpp, _grain_ccpp, component=COMPONENT + "/ccpp", source=SOURCE, rid=rid
)
assert_no_forbidden_columns(
    out_ccpp, component=COMPONENT + "/ccpp", source=SOURCE, rid=rid
)

# COMMAND ----------

# DBTITLE 1,Write ccpp
write_ml(
    out_ccpp,
    "dataset_ccpp",
    ecosystem=ECO,
    source=SOURCE,
    component=COMPONENT + "/ccpp",
    rid=rid,
)
set_dataset_status("ccpp", ECO, "fitted")

# COMMAND ----------

# DBTITLE 1,Inspect and export findings for ccpp
_targets = contract_columns("ccpp", ECO, "target")
_blocks = inspect_ml_table(
    read_ml("dataset_ccpp", ecosystem=ECO),
    "dataset_ccpp",
    ecosystem=ECO,
    key_cols=_grain_ccpp,
    partition_col="partition",
    target_col=_targets[0] if _targets else None,
)
write_ml_findings(ECO, "fit__dataset_ccpp", "dataset_ccpp", _blocks)
