# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # GOLD -- IOT_DEVICE_OBSERVATION
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** `energy_gold.iot_device_observation` -- Silver
# MAGIC `device_telemetry_snapshot`'s per-reading columns, the static device
# MAGIC fields left to `iot_device`. Grain: device x observation time.

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
SOURCE = "iot"
COMPONENT = "gold/energy/iot/iot_device_observation"
TABLE = "iot_device_observation"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = gold_run_id()

# COMMAND ----------

# DBTITLE 1,Read Silver
device_telemetry_snapshot = read_silver("device_telemetry_snapshot")

# COMMAND ----------

# DBTITLE 1,Build iot_device_observation
iot_device_observation = device_telemetry_snapshot.select(
    "device_key",
    "observation_timestamp_native",
    "observation_timestamp_utc",
    "temperature_degc",
    "humidity_percent",
    "co2_level",
    "battery_level",
    "data_origin",
    "measurement_basis",
    "source_system",
    "source_dataset",
    "source_record_id",
)
iot_device_observation = add_gold_provenance(iot_device_observation, SOURCE, rid)

# COMMAND ----------

# DBTITLE 1,Grain gate
assert_unique_grain(
    iot_device_observation,
    ["device_key", "observation_timestamp_utc"],
    component=COMPONENT,
    source=SOURCE,
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,Write
write_gold(iot_device_observation, TABLE, source=SOURCE, component=COMPONENT, rid=rid)

# COMMAND ----------

# DBTITLE 1,Inspect + export findings
_blocks = inspect_gold_table(
    iot_device_observation,
    TABLE,
    source=SOURCE,
    component=COMPONENT,
    rid=rid,
    key_cols=["device_key", "observation_timestamp_utc"],
)
write_gold_findings(SOURCE, f"iot__{TABLE}", TABLE, _blocks)
