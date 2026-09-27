# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # GOLD -- REF_MEASURE
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** `shared_conformed.ref_measure` -- the measured-quantity
# MAGIC vocabulary, from `weather_parameter_catalog` for now; market/site/IoT
# MAGIC measures extend it in a later build step. No canonical-unit conversion
# MAGIC layer exists in Silver, so `standard_unit` mirrors the native unit rather
# MAGIC than converting.

# COMMAND ----------

# DBTITLE 1,Shared Silver library
# MAGIC %run ../../silver/_silver_common

# COMMAND ----------

# DBTITLE 1,Gold shared library
# MAGIC %run ../_gold_common

# COMMAND ----------

# DBTITLE 1,Gold inspection library
# MAGIC %run ../_gold_inspect

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "dwd"
COMPONENT = "gold/shared/ref_measure"
TABLE = "ref_measure"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = gold_run_id()

# COMMAND ----------

# DBTITLE 1,Read Silver
weather_parameter_catalog = read_silver("weather_parameter_catalog")

# COMMAND ----------

# DBTITLE 1,Build ref_measure
ref_measure = weather_parameter_catalog.select(
    F.col("parameter_source_code").alias("measure_code"),
    F.col("parameter_business_name").alias("business_name"),
    F.col("parameter_unit").alias("native_unit"),
    F.col("parameter_unit").alias("standard_unit"),
    F.col("parameter_description_de").alias("description_de"),
    F.col("catalog_vintage"),
    F.lit("weather").alias("measure_domain"),
    F.col("source_system"),
    F.col("source_dataset"),
    F.col("source_record_id"),
)
ref_measure = add_gold_provenance(ref_measure, SOURCE, rid)

# COMMAND ----------

# DBTITLE 1,Grain gate
assert_unique_grain(
    ref_measure, ["measure_code"], component=COMPONENT, source=SOURCE, rid=rid
)

# COMMAND ----------

# DBTITLE 1,Write
write_gold(
    ref_measure,
    TABLE,
    schema=SHARED_CONFORMED_SCHEMA,
    source=SOURCE,
    component=COMPONENT,
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,Inspect + export findings
_blocks = inspect_gold_table(
    ref_measure,
    TABLE,
    source=SOURCE,
    component=COMPONENT,
    rid=rid,
    schema=SHARED_CONFORMED_SCHEMA,
    key_cols=["measure_code"],
    df_before=weather_parameter_catalog,
)
write_gold_findings(SOURCE, f"shared__{TABLE}", TABLE, _blocks)
