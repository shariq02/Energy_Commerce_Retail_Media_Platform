# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # ASSEMBLED SURVIVAL
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** assemble the wind unit survival dataset as a view: target plus as-of
# MAGIC features at the declared grain, with the feature contract and null classes
# MAGIC registered.

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
COMPONENT = "ml/energy/assembled/survival"
TABLE = "assembled_survival"
DATASET_ID = "survival"
GRAIN = ["unit_id"]

# COMMAND ----------

# DBTITLE 1,Run identity
rid = ml_run_id()

# COMMAND ----------

# DBTITLE 1,Preflight -- every input structure exists
_needed = ["target_unit_survival", "features_unit_static"]
_missing = [t for t in _needed if not spark.catalog.tableExists(ml_fqn(t, ECO))]
if _missing:
    raise RuntimeError(f"build these first: {_missing}")

# COMMAND ----------

# DBTITLE 1,Create the assembled view
write_ml_view(
    assembled_select(
        DATASET_ID,
        "target_unit_survival",
        ECO,
        [
            {
                "table": "features_unit_static",
                "keys": ["unit_id"],
                "drop": ["commissioning_date"],
            }
        ],
        where=None,
        target_drop=["stop_date"],
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
        "capacity_net_kw",
        "capacity_gross_kw",
        "rotor_diameter_m",
        "manufacturer",
        "model_designation",
        "latitude",
        "longitude",
        "federal_state",
        "operator_id",
        "is_offshore",
        "commissioning_year",
        "control_zone_code",
        "zone_status",
        "hub_height_m_filled",
        "hub_height_was_imputed",
    ),
    registry=(),
    perfect_prognosis=(),
    context=(),
    overrides={
        "commissioning_date": ("flag", None, None),
        "entry_date": ("flag", None, None),
        "entry_years": ("flag", None, None),
        "left_truncated": ("flag", None, None),
        "target_event": ("target", None, None),
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
    for c in ["stop_date"]
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
    capability="wind unit survival",
    grain=GRAIN,
    dependency_group=1,
    target_name="target_duration_years",
    provenance_tier="rule_derived",
    status="assembled",
    notes="fully deregistered units are absent from the registry (survivorship declared)",
)

# COMMAND ----------

# DBTITLE 1,Inspect and export findings
_blocks = inspect_ml_table(
    read_ml(TABLE, ecosystem=ECO), TABLE, ecosystem=ECO, key_cols=GRAIN
)
write_ml_findings(ECO, "assembled__" + TABLE, TABLE, _blocks)
