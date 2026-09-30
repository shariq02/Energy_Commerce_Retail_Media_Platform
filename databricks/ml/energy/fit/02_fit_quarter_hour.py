# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # FIT QUARTER-HOUR
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
COMPONENT = "ml/energy/fit/02_fit_quarter_hour"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = ml_run_id()

# COMMAND ----------

# DBTITLE 1,ML fit library
# MAGIC %run ../../_ml_fit

# COMMAND ----------

# DBTITLE 1,Fit and transform price_quarter_hour
out_price_quarter_hour = fit_transform(
    "price_quarter_hour",
    ECO,
    group_cols=["market_area_code"],
    fold_mode="rolling",
    calendar_key="quarter_hour",
    date_col="local_date",
    impute=True,
    rid=rid,
    source=SOURCE,
)
out_price_quarter_hour = add_ml_provenance(
    out_price_quarter_hour, "price_quarter_hour", ECO, rid
)

# COMMAND ----------

# DBTITLE 1,Gate price_quarter_hour
_grain_price_quarter_hour = list(
    read_ml("dataset_manifest", ecosystem=ECO)
    .filter(F.col("dataset_id") == "price_quarter_hour")
    .first()["grain_key"]
)
assert_unique_grain(
    out_price_quarter_hour,
    _grain_price_quarter_hour,
    component=COMPONENT + "/price_quarter_hour",
    source=SOURCE,
    rid=rid,
)
assert_no_forbidden_columns(
    out_price_quarter_hour,
    component=COMPONENT + "/price_quarter_hour",
    source=SOURCE,
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,Write price_quarter_hour
write_ml(
    out_price_quarter_hour,
    "dataset_price_quarter_hour",
    ecosystem=ECO,
    source=SOURCE,
    component=COMPONENT + "/price_quarter_hour",
    rid=rid,
)
set_dataset_status("price_quarter_hour", ECO, "fitted")

# COMMAND ----------

# DBTITLE 1,Inspect and export findings for price_quarter_hour
_targets = contract_columns("price_quarter_hour", ECO, "target")
_blocks = inspect_ml_table(
    read_ml("dataset_price_quarter_hour", ecosystem=ECO),
    "dataset_price_quarter_hour",
    ecosystem=ECO,
    key_cols=_grain_price_quarter_hour,
    partition_col="partition",
    target_col=_targets[0] if _targets else None,
)
write_ml_findings(
    ECO, "fit__dataset_price_quarter_hour", "dataset_price_quarter_hour", _blocks
)
