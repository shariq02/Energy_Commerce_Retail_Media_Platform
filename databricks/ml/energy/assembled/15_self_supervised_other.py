# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # ASSEMBLED SELF SUPERVISED OTHER
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** assemble the masked and shifted series windows dataset as a view: target
# MAGIC plus as-of features at the declared grain, with the feature contract and
# MAGIC null classes registered.

# COMMAND ----------

# DBTITLE 1,ML shared library
# MAGIC %run ../../_ml_common

# COMMAND ----------

# DBTITLE 1,ML inspection library
# MAGIC %run ../../_ml_inspect

# COMMAND ----------

# DBTITLE 1,Configuration
ECO = "energy"
SOURCE = "ml"
COMPONENT = "ml/energy/assembled/self_supervised_other"
TABLE = "assembled_self_supervised_other"
DATASET_ID = "self_supervised_other"
GRAIN = ["series_family", "series_id", "ts"]

# COMMAND ----------

# DBTITLE 1,Run identity
rid = ml_run_id()

# COMMAND ----------

# DBTITLE 1,Preflight -- every input structure exists
_needed = ["target_load", "target_honda_increment"]
_missing = [t for t in _needed if not spark.catalog.tableExists(ml_fqn(t, ECO))]
if _missing:
    raise RuntimeError(f"build these first: {_missing}")

# COMMAND ----------

# DBTITLE 1,Create the assembled view
write_ml_view(
    f"""
SELECT 'self_supervised_other' AS dataset_id, series_family, series_id, ts, local_date, series_value
FROM (
  SELECT 'daily_load' AS series_family, concat_ws('|', market_area_code, load_kind) AS series_id,
         CAST(local_date AS TIMESTAMP) AS ts, local_date, target_value_mwh AS series_value
  FROM {ml_fqn("target_load", ECO)}
  UNION ALL
  SELECT 'honda_hourly', concat_ws('|', location_key, channel), interval_start_utc, local_date, target_increment
  FROM {ml_fqn("target_honda_increment", ECO)} WHERE interval_seconds = 3600
)
""",
    TABLE,
    ecosystem=ECO,
)

# COMMAND ----------

# DBTITLE 1,Grain gate
assert_unique_grain(
    read_ml(TABLE, ecosystem=ECO), GRAIN, component=COMPONENT, source=SOURCE, rid=rid
)
assert_no_forbidden_columns(
    read_ml(TABLE, ecosystem=ECO), component=COMPONENT, source=SOURCE, rid=rid
)

# COMMAND ----------

# DBTITLE 1,Feature contract and null classes
_contract, _classes, _defaulted = derive_contract(
    read_ml(TABLE, ecosystem=ECO).columns,
    keys=GRAIN,
    time_col="local_date",
    static=(),
    registry=(),
    perfect_prognosis=(),
    context=("series_value",),
    overrides={
        "series_family": ("key", None, None),
        "series_id": ("key", None, None),
        "ts": ("key", None, None),
    },
)
_contract += [
    (
        c,
        "label_input",
        "observation_realised",
        0,
        None,
        "defines the target; excluded from the dataset",
    )
    for c in []
]
register_feature_contract(DATASET_ID, ECO, _contract)
register_null_classes(DATASET_ID, ECO, _classes)
if _defaulted:
    print("WARN columns given the default clock (derived_from_earlier):", _defaulted)

# COMMAND ----------

# DBTITLE 1,Register the dataset
register_dataset(
    DATASET_ID,
    ECO,
    capability="masked and shifted series windows",
    grain=GRAIN,
    dependency_group=1,
    target_name="masked_or_shifted_value",
    provenance_tier="constructed",
    status="assembled",
    notes="",
)

# COMMAND ----------

# DBTITLE 1,Inspect and export findings
_blocks = inspect_ml_table(
    read_ml(TABLE, ecosystem=ECO), TABLE, ecosystem=ECO, key_cols=GRAIN
)
write_ml_findings(ECO, "assembled__" + TABLE, TABLE, _blocks)
