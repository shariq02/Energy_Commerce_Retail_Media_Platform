# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # ML SETUP
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** November 2026
# MAGIC
# MAGIC **Purpose:** create the ML schemas and the registry tables, check that
# MAGIC every Gold table the ML notebooks read exists, and count the objects per
# MAGIC schema against the quota. Run once before any build notebook.

# COMMAND ----------

# DBTITLE 1,ML shared library
# MAGIC %run ./_ml_common

# COMMAND ----------

# DBTITLE 1,Configuration
ML_ECOSYSTEMS = tuple(ML_SCHEMAS)
# Every Gold table the ML notebooks read -- (schema, table).
_REQUIRED_GOLD = (
    ("energy_gold", "energy_component"),
    ("energy_gold", "market_price"),
    ("energy_gold", "generation_unit"),
    ("energy_gold", "grid_connection_point"),
    ("energy_gold", "grid_intervention_event"),
    ("energy_gold", "channel_reading"),
    ("energy_gold", "weather_observation"),
    ("energy_gold", "plant_sensor_observation"),
    ("shared_conformed", "series_coverage"),
    ("shared_conformed", "dim_place"),
    ("commerce_gold", "commerce_event"),
    ("commerce_gold", "commerce_event_item"),
    ("commerce_gold", "commerce_session"),
    ("commerce_gold", "commerce_product"),
    ("genomics_gold", "variant_profile"),
    ("genomics_gold", "gene_profile"),
    ("genomics_gold", "structural_variant_profile"),
    ("genomics_gold", "benchmark_variant"),
)

# COMMAND ----------

# DBTITLE 1,Create the ML schemas
for schema in (*ML_SCHEMAS.values(), ML_SHARED_SCHEMA):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}")
    print(f"OK  schema ready: {CATALOG}.{schema}")

# COMMAND ----------

# DBTITLE 1,Create the registry tables
for name, ddl in REGISTRY_DDL.items():
    print(f"OK  registry table ready: {create_ml_registry_table(name, ddl, 'energy')}")

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
print(f"Schema : {CATALOG}.{ML_SHARED_SCHEMA}")
print("RESULT: PASS")
print("=" * 70)
