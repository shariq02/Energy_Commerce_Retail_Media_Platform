# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # GOLD -- PLACE_INSTRUMENT_PERIOD + PLACE_PARAMETER_PERIOD
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** `energy_gold.place_instrument_period` (from
# MAGIC `weather_station_instrument`) and `place_parameter_period` (from
# MAGIC `weather_parameter_period`) -- `location_key` renamed to `place_key`,
# MAGIC every other column carried through as-is.

# COMMAND ----------

# DBTITLE 1,Shared Silver library
# MAGIC %run ../../../silver/_silver_common

# COMMAND ----------

# DBTITLE 1,Gold shared library
# MAGIC %run ../../_gold_common

# COMMAND ----------

# DBTITLE 1,Gold inspection library
# MAGIC %run ../../_gold_inspect

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "dwd"
COMPONENT = "gold/energy/weather/place_measurement_metadata"
INSTRUMENT_TABLE = "place_instrument_period"
PARAMETER_TABLE = "place_parameter_period"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = gold_run_id()

# COMMAND ----------

# DBTITLE 1,Build place_instrument_period
place_instrument_period = (
    read_silver("weather_station_instrument")
    .withColumnRenamed("location_key", "place_key")
    .drop("_silver_loaded_at", "_silver_run_id")
)
place_instrument_period = add_gold_provenance(place_instrument_period, SOURCE, rid)

# COMMAND ----------

# DBTITLE 1,Build place_parameter_period
place_parameter_period = (
    read_silver("weather_parameter_period")
    .withColumnRenamed("location_key", "place_key")
    .drop("_silver_loaded_at", "_silver_run_id")
)
place_parameter_period = add_gold_provenance(place_parameter_period, SOURCE, rid)

# COMMAND ----------

# DBTITLE 1,Grain gates
assert_unique_grain(
    place_instrument_period,
    ["place_key", "parameter_category", "valid_from", "record_ordinal"],
    component=COMPONENT,
    source=SOURCE,
    rid=rid,
)
assert_unique_grain(
    place_parameter_period,
    ["place_key", "parameter_source_code", "valid_from", "record_ordinal"],
    component=COMPONENT,
    source=SOURCE,
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,Write
write_gold(
    place_instrument_period,
    INSTRUMENT_TABLE,
    source=SOURCE,
    component=COMPONENT,
    rid=rid,
)
write_gold(
    place_parameter_period, PARAMETER_TABLE, source=SOURCE, component=COMPONENT, rid=rid
)

# COMMAND ----------

# DBTITLE 1,Inspect + export findings
_blocks = inspect_gold_table(
    place_instrument_period,
    INSTRUMENT_TABLE,
    source=SOURCE,
    component=COMPONENT,
    rid=rid,
    key_cols=["place_key", "parameter_category", "valid_from", "record_ordinal"],
)
_blocks += inspect_gold_table(
    place_parameter_period,
    PARAMETER_TABLE,
    source=SOURCE,
    component=COMPONENT,
    rid=rid,
    key_cols=["place_key", "parameter_source_code", "valid_from", "record_ordinal"],
)
write_gold_findings(
    SOURCE, "weather__place_measurement_metadata", "place measurement metadata", _blocks
)
