# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # ASSEMBLED ZONE GENERATION
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** assemble the zone wind and PV generation from weather dataset as a view:
# MAGIC target plus as-of features at the declared grain, with the feature
# MAGIC contract and null classes registered.

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
COMPONENT = "ml/energy/assembled/zone_generation"
TABLE = "assembled_zone_generation"
DATASET_ID = "zone_generation"
GRAIN = ["market_area_code", "local_date", "carrier"]

# COMMAND ----------

# DBTITLE 1,Run identity
rid = ml_run_id()

# COMMAND ----------

# DBTITLE 1,Preflight -- every input structure exists
_needed = [
    "target_zone_generation",
    "features_zone_weather",
    "features_zone_capacity_asof",
    "features_calendar_market_area",
]
_missing = [t for t in _needed if not spark.catalog.tableExists(ml_fqn(t, ECO))]
if _missing:
    raise RuntimeError(f"build these first: {_missing}")

# COMMAND ----------

# DBTITLE 1,Create the assembled view
write_ml_view(
    f"""
SELECT 'zone_generation' AS dataset_id,
       t.market_area_code, t.local_date, t.carrier, t.target_generation_mwh, t.provenance_tier,
       cap.capacity_net_mw,
       w.onshore_wind_speed_adjusted_mean, w.onshore_wind_speed_cubed_adjusted_mean,
       w.offshore_wind_speed_adjusted_mean, w.offshore_wind_speed_cubed_adjusted_mean,
       w.global_radiation_mean_w_per_m2, w.covered_weight_share,
       c.day_of_week, c.is_weekend, c.month, c.day_of_year, c.is_holiday,
       DATEDIFF(t.local_date, DATE'2018-10-01') / 365.25 AS trend_years
FROM {ml_fqn("target_zone_generation", ECO)} t
LEFT JOIN {ml_fqn("features_calendar_market_area", ECO)} c
  ON c.market_area_code = t.market_area_code AND c.local_date = t.local_date
LEFT JOIN (
  SELECT market_area_code, local_date,
    MAX(CASE WHEN variable = 'onshore_wind_speed_adjusted_mean' THEN value END) AS onshore_wind_speed_adjusted_mean,
    MAX(CASE WHEN variable = 'onshore_wind_speed_cubed_adjusted_mean' THEN value END) AS onshore_wind_speed_cubed_adjusted_mean,
    MAX(CASE WHEN variable = 'offshore_wind_speed_adjusted_mean' THEN value END) AS offshore_wind_speed_adjusted_mean,
    MAX(CASE WHEN variable = 'offshore_wind_speed_cubed_adjusted_mean' THEN value END) AS offshore_wind_speed_cubed_adjusted_mean,
    MAX(CASE WHEN variable = 'global_radiation_mean_w_per_m2' THEN value END) AS global_radiation_mean_w_per_m2,
    MIN(covered_weight_share) AS covered_weight_share
  FROM {ml_fqn("features_zone_weather", ECO)} GROUP BY market_area_code, local_date
) w ON w.market_area_code = t.market_area_code AND w.local_date = t.local_date
LEFT JOIN {ml_fqn("features_zone_capacity_asof", ECO)} cap
  ON cap.market_area_code = t.market_area_code AND cap.local_date = t.local_date
 AND cap.carrier = CASE t.carrier WHEN 'onshore_wind' THEN 'wind_onshore' WHEN 'offshore_wind' THEN 'wind_offshore' END
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
    registry=("capacity_net_mw",),
    perfect_prognosis=(
        "onshore_wind_speed_adjusted_mean",
        "onshore_wind_speed_cubed_adjusted_mean",
        "offshore_wind_speed_adjusted_mean",
        "offshore_wind_speed_cubed_adjusted_mean",
        "global_radiation_mean_w_per_m2",
        "covered_weight_share",
    ),
    context=(),
    overrides={
        "carrier": ("key", None, None),
        "trend_years": ("feature", "calendar", None),
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
    capability="zone wind and PV generation from weather",
    grain=GRAIN,
    dependency_group=2,
    target_name="target_generation_mwh",
    provenance_tier="sourced",
    status="assembled",
    notes="observed zone weather stands in for a forecast (perfect prognosis); waits on the zone structures",
)

# COMMAND ----------

# DBTITLE 1,Inspect and export findings
_blocks = inspect_ml_table(
    read_ml(TABLE, ecosystem=ECO), TABLE, ecosystem=ECO, key_cols=GRAIN
)
write_ml_findings(ECO, "assembled__" + TABLE, TABLE, _blocks)
