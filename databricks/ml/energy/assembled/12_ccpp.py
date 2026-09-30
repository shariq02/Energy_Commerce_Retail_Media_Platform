# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # ASSEMBLED CCPP
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** assemble the combined-cycle plant output dataset as a view: target plus
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
COMPONENT = "ml/energy/assembled/ccpp"
TABLE = "assembled_ccpp"
DATASET_ID = "ccpp"
GRAIN = ["sample_key"]

# COMMAND ----------

# DBTITLE 1,Run identity
rid = ml_run_id()

# COMMAND ----------

# DBTITLE 1,Preflight -- every input structure exists
_needed = ["target_ccpp"]
_missing = [t for t in _needed if not spark.catalog.tableExists(ml_fqn(t, ECO))]
if _missing:
    raise RuntimeError(f"build these first: {_missing}")

# COMMAND ----------

# DBTITLE 1,Create the assembled view
write_ml_view(
    f"""
SELECT 'ccpp' AS dataset_id, t.sample_key, t.target_net_output_mw, t.humidity_above_100_flag, t.provenance_tier,
       g.ambient_temperature_degc, g.exhaust_vacuum_cm_of_mercury, g.ambient_pressure_mbar, g.relative_humidity_percent
FROM {ml_fqn("target_ccpp", ECO)} t
JOIN {CATALOG}.energy_gold.plant_sensor_observation g ON g.sample_key = t.sample_key
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
    time_col=None,
    static=(),
    registry=(),
    perfect_prognosis=(),
    context=(
        "ambient_temperature_degc",
        "exhaust_vacuum_cm_of_mercury",
        "ambient_pressure_mbar",
        "relative_humidity_percent",
    ),
    overrides={"humidity_above_100_flag": ("flag", None, None)},
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
    for c in ["net_electrical_output_mw"]
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
    capability="combined-cycle plant output",
    grain=GRAIN,
    dependency_group=1,
    target_name="target_net_output_mw",
    provenance_tier="sourced",
    status="assembled",
    notes="no time column; random split grouped by repeat key; citation requirement applies",
)

# COMMAND ----------

# DBTITLE 1,Inspect and export findings
_blocks = inspect_ml_table(
    read_ml(TABLE, ecosystem=ECO), TABLE, ecosystem=ECO, key_cols=GRAIN
)
write_ml_findings(ECO, "assembled__" + TABLE, TABLE, _blocks)
