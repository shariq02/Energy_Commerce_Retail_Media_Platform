# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # FIT MARKET DAILY
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
COMPONENT = "ml/energy/fit/01_fit_market_daily"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = ml_run_id()

# COMMAND ----------

# DBTITLE 1,ML fit library
# MAGIC %run ../../_ml_fit

# COMMAND ----------

# DBTITLE 1,Fit and transform price_daily
out_price_daily = fit_transform(
    "price_daily",
    ECO,
    group_cols=["market_area_code"],
    fold_mode="rolling",
    calendar_key="energy_daily",
    date_col="local_date",
    impute=True,
    rid=rid,
    source=SOURCE,
)
out_price_daily = add_ml_provenance(out_price_daily, "price_daily", ECO, rid)

# COMMAND ----------

# DBTITLE 1,Gate price_daily
_grain_price_daily = list(
    read_ml("dataset_manifest", ecosystem=ECO)
    .filter(F.col("dataset_id") == "price_daily")
    .first()["grain_key"]
)
assert_unique_grain(
    out_price_daily,
    _grain_price_daily,
    component=COMPONENT + "/price_daily",
    source=SOURCE,
    rid=rid,
)
assert_no_forbidden_columns(
    out_price_daily, component=COMPONENT + "/price_daily", source=SOURCE, rid=rid
)

# COMMAND ----------

# DBTITLE 1,Write price_daily
write_ml(
    out_price_daily,
    "dataset_price_daily",
    ecosystem=ECO,
    source=SOURCE,
    component=COMPONENT + "/price_daily",
    rid=rid,
)
set_dataset_status("price_daily", ECO, "fitted")

# COMMAND ----------

# DBTITLE 1,Inspect and export findings for price_daily
_targets = contract_columns("price_daily", ECO, "target")
_blocks = inspect_ml_table(
    read_ml("dataset_price_daily", ecosystem=ECO),
    "dataset_price_daily",
    ecosystem=ECO,
    key_cols=_grain_price_daily,
    partition_col="partition",
    target_col=_targets[0] if _targets else None,
)
write_ml_findings(ECO, "fit__dataset_price_daily", "dataset_price_daily", _blocks)

# COMMAND ----------

# DBTITLE 1,Fit and transform bias
out_bias = fit_transform(
    "bias",
    ECO,
    group_cols=["market_area_code", "forecast_scope"],
    fold_mode="rolling",
    calendar_key="energy_daily",
    date_col="local_date",
    impute=True,
    rid=rid,
    source=SOURCE,
)
out_bias = add_ml_provenance(out_bias, "bias", ECO, rid)

# COMMAND ----------

# DBTITLE 1,Gate bias
_grain_bias = list(
    read_ml("dataset_manifest", ecosystem=ECO)
    .filter(F.col("dataset_id") == "bias")
    .first()["grain_key"]
)
assert_unique_grain(
    out_bias, _grain_bias, component=COMPONENT + "/bias", source=SOURCE, rid=rid
)
assert_no_forbidden_columns(
    out_bias, component=COMPONENT + "/bias", source=SOURCE, rid=rid
)

# COMMAND ----------

# DBTITLE 1,Write bias
write_ml(
    out_bias,
    "dataset_bias",
    ecosystem=ECO,
    source=SOURCE,
    component=COMPONENT + "/bias",
    rid=rid,
)
set_dataset_status("bias", ECO, "fitted")

# COMMAND ----------

# DBTITLE 1,Inspect and export findings for bias
_targets = contract_columns("bias", ECO, "target")
_blocks = inspect_ml_table(
    read_ml("dataset_bias", ecosystem=ECO),
    "dataset_bias",
    ecosystem=ECO,
    key_cols=_grain_bias,
    partition_col="partition",
    target_col=_targets[0] if _targets else None,
)
write_ml_findings(ECO, "fit__dataset_bias", "dataset_bias", _blocks)

# COMMAND ----------

# DBTITLE 1,Fit and transform load
out_load = fit_transform(
    "load",
    ECO,
    group_cols=["market_area_code", "load_kind"],
    fold_mode="rolling",
    calendar_key="energy_daily",
    date_col="local_date",
    impute=True,
    rid=rid,
    source=SOURCE,
)
out_load = add_ml_provenance(out_load, "load", ECO, rid)

# COMMAND ----------

# DBTITLE 1,Gate load
_grain_load = list(
    read_ml("dataset_manifest", ecosystem=ECO)
    .filter(F.col("dataset_id") == "load")
    .first()["grain_key"]
)
assert_unique_grain(
    out_load, _grain_load, component=COMPONENT + "/load", source=SOURCE, rid=rid
)
assert_no_forbidden_columns(
    out_load, component=COMPONENT + "/load", source=SOURCE, rid=rid
)

# COMMAND ----------

# DBTITLE 1,Write load
write_ml(
    out_load,
    "dataset_load",
    ecosystem=ECO,
    source=SOURCE,
    component=COMPONENT + "/load",
    rid=rid,
)
set_dataset_status("load", ECO, "fitted")

# COMMAND ----------

# DBTITLE 1,Inspect and export findings for load
_targets = contract_columns("load", ECO, "target")
_blocks = inspect_ml_table(
    read_ml("dataset_load", ecosystem=ECO),
    "dataset_load",
    ecosystem=ECO,
    key_cols=_grain_load,
    partition_col="partition",
    target_col=_targets[0] if _targets else None,
)
write_ml_findings(ECO, "fit__dataset_load", "dataset_load", _blocks)

# COMMAND ----------

# DBTITLE 1,Fit and transform capacity_additions
out_capacity_additions = fit_transform(
    "capacity_additions",
    ECO,
    group_cols=["carrier_key"],
    fold_mode="rolling",
    calendar_key="energy_daily",
    date_col="commissioning_month",
    impute=True,
    rid=rid,
    source=SOURCE,
)
out_capacity_additions = add_ml_provenance(
    out_capacity_additions, "capacity_additions", ECO, rid
)

# COMMAND ----------

# DBTITLE 1,Gate capacity_additions
_grain_capacity_additions = list(
    read_ml("dataset_manifest", ecosystem=ECO)
    .filter(F.col("dataset_id") == "capacity_additions")
    .first()["grain_key"]
)
assert_unique_grain(
    out_capacity_additions,
    _grain_capacity_additions,
    component=COMPONENT + "/capacity_additions",
    source=SOURCE,
    rid=rid,
)
assert_no_forbidden_columns(
    out_capacity_additions,
    component=COMPONENT + "/capacity_additions",
    source=SOURCE,
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,Write capacity_additions
write_ml(
    out_capacity_additions,
    "dataset_capacity_additions",
    ecosystem=ECO,
    source=SOURCE,
    component=COMPONENT + "/capacity_additions",
    rid=rid,
)
set_dataset_status("capacity_additions", ECO, "fitted")

# COMMAND ----------

# DBTITLE 1,Inspect and export findings for capacity_additions
_targets = contract_columns("capacity_additions", ECO, "target")
_blocks = inspect_ml_table(
    read_ml("dataset_capacity_additions", ecosystem=ECO),
    "dataset_capacity_additions",
    ecosystem=ECO,
    key_cols=_grain_capacity_additions,
    partition_col="partition",
    target_col=_targets[0] if _targets else None,
)
write_ml_findings(
    ECO, "fit__dataset_capacity_additions", "dataset_capacity_additions", _blocks
)
