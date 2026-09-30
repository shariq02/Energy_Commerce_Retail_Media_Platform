# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # ASSEMBLED LAPSE REES46
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** assemble the REES46 lapse (return in the 30 days after the cutoff) dataset
# MAGIC as a view: target plus as-of features at the declared grain, with the
# MAGIC feature contract and null classes registered.

# COMMAND ----------

# DBTITLE 1,ML shared library
# MAGIC %run ../../_ml_common

# COMMAND ----------

# DBTITLE 1,ML inspection library
# MAGIC %run ../../_ml_inspect

# COMMAND ----------

# DBTITLE 1,Configuration
ECO = "commerce"
SOURCE = "ml"
COMPONENT = "ml/commerce/assembled/lapse_rees46"
TABLE = "assembled_lapse_rees46"
DATASET_ID = "lapse_rees46"
GRAIN = ["user_id", "cutoff_date"]

# COMMAND ----------

# DBTITLE 1,Run identity
rid = ml_run_id()

# COMMAND ----------

# DBTITLE 1,Preflight -- every input structure exists
_needed = ["target_lapse_rees46", "features_lapse_rees46"]
_missing = [t for t in _needed if not spark.catalog.tableExists(ml_fqn(t, ECO))]
if _missing:
    raise RuntimeError(f"build these first: {_missing}")

# COMMAND ----------

# DBTITLE 1,Create the assembled view
write_ml_view(
    assembled_select(
        DATASET_ID,
        "target_lapse_rees46",
        ECO,
        [{"table": "features_lapse_rees46", "keys": ["user_id", "cutoff_date"]}],
        where=None,
        target_drop=None,
        extra_select=None,
    ),
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
    time_col="cutoff_date",
    static=(),
    registry=(),
    perfect_prognosis=(),
    context=(),
    group_col="user_id",
    overrides={
        "user_id": ("group", None, None),
        "cutoff_date": ("key", None, None),
        "label_window_days": ("flag", None, None),
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
    capability="REES46 lapse (return in the 30 days after the cutoff)",
    grain=GRAIN,
    dependency_group=1,
    target_name="target_returned",
    provenance_tier="constructed",
    status="assembled",
    notes="single cutoff, user-disjoint split with no temporal partition",
)

# COMMAND ----------

# DBTITLE 1,Inspect and export findings
_blocks = inspect_ml_table(
    read_ml(TABLE, ecosystem=ECO), TABLE, ecosystem=ECO, key_cols=GRAIN
)
write_ml_findings(ECO, "assembled__" + TABLE, TABLE, _blocks)
