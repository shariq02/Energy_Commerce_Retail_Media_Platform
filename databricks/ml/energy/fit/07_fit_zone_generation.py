# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # FIT ZONE GENERATION
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
COMPONENT = "ml/energy/fit/07_fit_zone_generation"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = ml_run_id()

# COMMAND ----------

# DBTITLE 1,ML fit library
# MAGIC %run ../../_ml_fit

# COMMAND ----------

# DBTITLE 1,Fit and transform zone_generation
out_zone_generation = fit_transform(
    "zone_generation",
    ECO,
    group_cols=["market_area_code", "carrier"],
    fold_mode="rolling",
    calendar_key="energy_daily",
    date_col="local_date",
    impute=True,
    rid=rid,
    source=SOURCE,
)
out_zone_generation = add_ml_provenance(
    out_zone_generation, "zone_generation", ECO, rid
)

# COMMAND ----------

# DBTITLE 1,Gate zone_generation
_grain_zone_generation = list(
    read_ml("dataset_manifest", ecosystem=ECO)
    .filter(F.col("dataset_id") == "zone_generation")
    .first()["grain_key"]
)
assert_unique_grain(
    out_zone_generation,
    _grain_zone_generation,
    component=COMPONENT + "/zone_generation",
    source=SOURCE,
    rid=rid,
)
assert_no_forbidden_columns(
    out_zone_generation,
    component=COMPONENT + "/zone_generation",
    source=SOURCE,
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,Write zone_generation
write_ml(
    out_zone_generation,
    "dataset_zone_generation",
    ecosystem=ECO,
    source=SOURCE,
    component=COMPONENT + "/zone_generation",
    rid=rid,
)
set_dataset_status("zone_generation", ECO, "fitted")

# COMMAND ----------

# DBTITLE 1,Inspect and export findings for zone_generation
_targets = contract_columns("zone_generation", ECO, "target")
_blocks = inspect_ml_table(
    read_ml("dataset_zone_generation", ecosystem=ECO),
    "dataset_zone_generation",
    ecosystem=ECO,
    key_cols=_grain_zone_generation,
    partition_col="partition",
    target_col=_targets[0] if _targets else None,
)
write_ml_findings(
    ECO, "fit__dataset_zone_generation", "dataset_zone_generation", _blocks
)
