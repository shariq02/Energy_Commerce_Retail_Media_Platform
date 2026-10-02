# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # ML SETUP
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** create the two ML schemas and the registry tables, and assert every Gold
# MAGIC table the ML notebooks read exists. Idempotent.

# COMMAND ----------

# DBTITLE 1,ML shared library
# MAGIC %run ./_ml_common

# COMMAND ----------

# DBTITLE 1,Configuration
ML_ECOSYSTEMS = ("energy", "commerce")
ENERGY_ONLY_REGISTRY = ("gap_limits",)

# Every Gold table the ML notebooks read -- (schema, table).
_REQUIRED_GOLD = (
    ("energy_gold", "energy_balance_component"),
    ("energy_gold", "market_price"),
    ("energy_gold", "generation_forecast"),
    ("energy_gold", "generation_unit"),
    ("energy_gold", "grid_connection_point"),
    ("energy_gold", "grid_intervention_event"),
    ("energy_gold", "channel_reading"),
    ("energy_gold", "weather_observation"),
    ("energy_gold", "weather_observation_radiation"),
    ("energy_gold", "place_instrument_period"),
    ("energy_gold", "plant_sensor_observation"),
    ("shared_conformed", "series_coverage"),
    ("shared_conformed", "data_gap_period"),
    ("shared_conformed", "dim_place"),
    ("commerce_gold", "ga4_event"),
    ("commerce_gold", "ga4_session"),
    ("commerce_gold", "ga4_product"),
    ("commerce_gold", "rees46_event"),
    ("commerce_gold", "rees46_session"),
    ("commerce_gold", "rees46_product"),
)

# COMMAND ----------

# DBTITLE 1,Create the ML schemas
for eco in ML_ECOSYSTEMS:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{ml_schema_for(eco)}")
    print(f"OK  schema ready: {CATALOG}.{ml_schema_for(eco)}")

# COMMAND ----------

# DBTITLE 1,Create the registry tables
for eco in ML_ECOSYSTEMS:
    for name, ddl in REGISTRY_DDL.items():
        if name in ENERGY_ONLY_REGISTRY and eco != "energy":
            continue
        spark.sql(f"CREATE TABLE IF NOT EXISTS {ml_fqn(name, eco)} ({ddl}) USING delta")
        print(f"OK  registry table ready: {ml_fqn(name, eco)}")

# COMMAND ----------

# DBTITLE 1,Preflight -- every required Gold table exists
_missing = [
    f"{CATALOG}.{s}.{t}"
    for s, t in _REQUIRED_GOLD
    if not spark.catalog.tableExists(f"{CATALOG}.{s}.{t}")
]
if _missing:
    raise RuntimeError(f"ML reads Gold table(s) that do not exist yet: {_missing}")
print(f"OK  {len(_REQUIRED_GOLD)} required Gold table(s) all present")

# COMMAND ----------

# DBTITLE 1,Object count per schema against the quota
for eco in ML_ECOSYSTEMS:
    n = _schema_object_count(eco)
    flag = "WARN" if n >= SCHEMA_OBJECT_WARN else "OK"
    print(f"{flag}  {ml_schema_for(eco)}: {n} of {SCHEMA_OBJECT_QUOTA} objects")

# COMMAND ----------

# DBTITLE 1,Summary
print("=" * 70)
print("ML SETUP SUMMARY")
print("=" * 70)
for eco in ML_ECOSYSTEMS:
    print(f"Schema : {CATALOG}.{ml_schema_for(eco)}")
print("RESULT: PASS")
print("=" * 70)