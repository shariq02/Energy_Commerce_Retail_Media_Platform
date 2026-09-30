# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # ASSEMBLED REDISPATCH MATCHING
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** assemble the redispatch asset matching tier dataset as a view: target plus
# MAGIC as-of features at the declared grain, with the feature contract and null
# MAGIC classes registered.

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
COMPONENT = "ml/energy/assembled/redispatch_matching"
TABLE = "assembled_redispatch_matching"
DATASET_ID = "redispatch_matching"
GRAIN = ["affected_asset_text"]

# COMMAND ----------

# DBTITLE 1,Run identity
rid = ml_run_id()

# COMMAND ----------

# DBTITLE 1,Preflight -- every input structure exists
_needed = ["target_redispatch_match", "features_redispatch_match"]
_missing = [t for t in _needed if not spark.catalog.tableExists(ml_fqn(t, ECO))]
if _missing:
    raise RuntimeError(f"build these first: {_missing}")

# COMMAND ----------

# DBTITLE 1,Create the assembled view
write_ml_view(
    assembled_select(
        DATASET_ID,
        "target_redispatch_match",
        ECO,
        [{"table": "features_redispatch_match", "keys": ["affected_asset_text"]}],
        where=None,
        target_drop=["matched_unit_name", "match_confidence"],
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
    time_col=None,
    static=(
        "event_count",
        "first_event_date",
        "last_event_date",
        "max_power_mw",
        "primary_energy_type",
        "direction_count",
        "text_length",
        "token_count",
        "has_digit",
    ),
    registry=(),
    perfect_prognosis=(),
    context=(),
    overrides={},
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
    for c in ["matched_unit_name", "match_confidence"]
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
    capability="redispatch asset matching tier",
    grain=GRAIN,
    dependency_group=1,
    target_name="target_match_tier",
    provenance_tier="rule_derived",
    status="assembled",
    notes="",
)

# COMMAND ----------

# DBTITLE 1,Inspect and export findings
_blocks = inspect_ml_table(
    read_ml(TABLE, ecosystem=ECO), TABLE, ecosystem=ECO, key_cols=GRAIN
)
write_ml_findings(ECO, "assembled__" + TABLE, TABLE, _blocks)
