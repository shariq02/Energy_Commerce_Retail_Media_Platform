# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # GOLD -- IOT_DEVICE
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** `energy_gold.iot_device` -- the device entity, aggregated
# MAGIC from Silver `device_telemetry_snapshot`'s own readings: static fields
# MAGIC (name, address, coordinates, country) taken from the first reading,
# MAGIC plus `first_seen`/`last_seen`/`reading_count`. Grain: device.

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
COMPONENT = "gold/energy/iot/iot_device"
TABLE = "iot_device"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = gold_run_id()

# COMMAND ----------

# DBTITLE 1,Read Silver
device_telemetry_snapshot = read_silver("device_telemetry_snapshot")

# COMMAND ----------

# DBTITLE 1,Aggregate the device entity
iot_device = device_telemetry_snapshot.groupBy("device_key").agg(
    F.first("source_device_id", ignorenulls=True).alias("source_device_id"),
    F.first("device_name", ignorenulls=True).alias("device_name"),
    F.first("ip_address", ignorenulls=True).alias("ip_address"),
    F.first("latitude", ignorenulls=True).alias("latitude"),
    F.first("longitude", ignorenulls=True).alias("longitude"),
    F.first("country_code", ignorenulls=True).alias("country_code"),
    F.first("country_code_alpha3", ignorenulls=True).alias("country_code_alpha3"),
    F.first("country_name", ignorenulls=True).alias("country_name"),
    F.first("lcd_label", ignorenulls=True).alias("lcd_label"),
    F.first("source_system", ignorenulls=True).alias("source_system"),
    F.first("source_dataset", ignorenulls=True).alias("source_dataset"),
    F.min("observation_timestamp_utc").alias("first_seen"),
    F.max("observation_timestamp_utc").alias("last_seen"),
    F.count(F.lit(1)).alias("reading_count"),
)
iot_device = add_gold_provenance(iot_device, SOURCE, rid)

# COMMAND ----------

# DBTITLE 1,Grain gate
assert_unique_grain(
    iot_device, ["device_key"], component=COMPONENT, source=SOURCE, rid=rid
)

# COMMAND ----------

# DBTITLE 1,Write
write_gold(iot_device, TABLE, source=SOURCE, component=COMPONENT, rid=rid)

# COMMAND ----------

# DBTITLE 1,Inspect + export findings
_blocks = inspect_gold_table(
    iot_device,
    TABLE,
    source=SOURCE,
    component=COMPONENT,
    rid=rid,
    key_cols=["device_key"],
)
write_gold_findings(SOURCE, f"iot__{TABLE}", TABLE, _blocks)
