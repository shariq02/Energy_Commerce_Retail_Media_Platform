# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # ASSEMBLED RL REDISPATCH
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** assemble the redispatch event trajectories dataset as a view: target plus
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
COMPONENT = "ml/energy/assembled/rl_redispatch"
TABLE = "assembled_rl_redispatch"
DATASET_ID = "rl_redispatch"
GRAIN = ["episode_id", "step"]

# COMMAND ----------

# DBTITLE 1,Run identity
rid = ml_run_id()

# COMMAND ----------

# DBTITLE 1,Preflight -- every input structure exists
_needed = ["target_rl_redispatch_events", "features_redispatch_daily"]
_missing = [t for t in _needed if not spark.catalog.tableExists(ml_fqn(t, ECO))]
if _missing:
    raise RuntimeError(f"build these first: {_missing}")

# COMMAND ----------

# DBTITLE 1,Create the assembled view
write_ml_view(
    assembled_select(
        DATASET_ID,
        "target_rl_redispatch_events",
        ECO,
        [
            {
                "table": "features_redispatch_daily",
                "keys": ["market_area_code", "local_date"],
            }
        ],
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
    time_col="local_date",
    static=(),
    registry=(),
    perfect_prognosis=(),
    context=(),
    overrides={
        "event_key": ("flag", None, None),
        "event_start_utc": ("flag", None, None),
        "market_area_code": ("flag", None, None),
        "action_direction": ("target", None, None),
        "action_reason": ("target", None, None),
        "action_energy_mwh": ("target", None, None),
        "reward": ("target", None, None),
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
    capability="redispatch event trajectories",
    grain=GRAIN,
    dependency_group=1,
    target_name="reward",
    provenance_tier="rule_derived",
    status="assembled",
    notes="old regime only; reward is minus the energy volume, a proxy that ignores cost and reason",
)

# COMMAND ----------

# DBTITLE 1,Inspect and export findings
_blocks = inspect_ml_table(
    read_ml(TABLE, ecosystem=ECO), TABLE, ecosystem=ECO, key_cols=GRAIN
)
write_ml_findings(ECO, "assembled__" + TABLE, TABLE, _blocks)
