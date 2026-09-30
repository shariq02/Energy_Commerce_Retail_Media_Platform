# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER -- DEVICE TELEMETRY SNAPSHOT (SAMPLES IOT)
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** the Samples `iot` device snapshot, one row per device, marked
# MAGIC `synthetic_suspected`; not weather and not the Honda site.

# COMMAND ----------

# DBTITLE 1,Shared library
# MAGIC %run ../../_silver_common

# COMMAND ----------

# DBTITLE 1,Inspection library
# MAGIC %run ../../_silver_inspect

# COMMAND ----------

# DBTITLE 1,Semantic structures library
# MAGIC %run ../../_semantic_common

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "iot"
COMPONENT = "silver/energy/iot/02_device_telemetry_iot"
RID = run_id()
FINDINGS = "energy"
TABLE = "device_telemetry_snapshot"
ROOT = "/Volumes/samples/databricks/datasets/iot"

# COMMAND ----------

# DBTITLE 1,Session time zone
ensure_utc_session()

# COMMAND ----------

# DBTITLE 1,Read Samples -- iot_devices.json
raw = (
    spark.read.option("recursiveFileLookup", True)
    .option("pathGlobFilter", "*.json")
    .json(ROOT)
)

# COMMAND ----------

# DBTITLE 1,Dedupe -- collapse identical rows, quarantine same-key value conflicts
_content = [c for c in raw.columns if c != "device_id"]
_kept, _q = resolve_conflicts(raw, ["device_id"], _content, bronze_table="iot_devices")
write_quarantine(_q.withColumn("source_system", F.lit(SOURCE)), RID)
RECONCILIATION = reconciliation_stats(raw, _kept, _q)

# COMMAND ----------

# DBTITLE 1,Transform -- one row per device (timestamp is epoch milliseconds)
devices = (
    _kept.withColumn("source_device_id", F.col("device_id").cast("string"))
    .withColumn("ip_address", F.col("ip"))
    .withColumn("latitude", F.col("latitude").cast("double"))
    .withColumn("longitude", F.col("longitude").cast("double"))
    .withColumn("country_code", F.col("cca2"))
    .withColumn("country_code_alpha3", F.col("cca3"))
    .withColumn("country_name", F.col("cn"))
    .withColumn("observation_timestamp_native", F.col("timestamp").cast("string"))
    .withColumn(
        "observation_timestamp_utc",
        (F.col("timestamp").cast("double") / 1000).cast("timestamp"),
    )
    .withColumn("temperature_degc", F.col("temp").cast("double"))
    .withColumn("humidity_percent", F.col("humidity").cast("double"))
    .withColumn("co2_level", F.col("c02_level").cast("double"))
    .withColumn("battery_level", F.col("battery_level").cast("double"))
    .withColumn("lcd_label", F.col("lcd"))
    .withColumn("data_origin", F.lit("synthetic_suspected"))
    .withColumn("measurement_basis", F.lit("device_snapshot"))
    .withColumn("device_key", sha_key(F.lit(SOURCE), "device_id"))
    .withColumn("source_record_id", F.col("device_key"))
)
devices = add_semantic_provenance(devices, SOURCE, "iot_devices", RID)

# COMMAND ----------

# DBTITLE 1,Write Silver -- device_telemetry_snapshot
write_semantic(
    conform(devices, DEVICE_TELEMETRY_SNAPSHOT_COLUMNS),
    TABLE,
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Inspect -- device_telemetry_snapshot
written = spark.table(semantic_table(TABLE))
findings_blocks = inspect_table(
    written,
    TABLE,
    source=FINDINGS,
    component=COMPONENT,
    rid=RID,
    key_cols=["device_key"],
    df_before=raw,
)

# COMMAND ----------

# DBTITLE 1,Export findings -- device_telemetry_snapshot
write_silver_findings(
    FINDINGS,
    f"{COMPONENT.split('/')[-1]}__{TABLE}",
    TABLE,
    [
        *findings_blocks,
        (
            "Bronze -> exact duplicates collapsed -> conflicts quarantined -> kept",
            dict_to_markdown_row(RECONCILIATION),
        ),
    ],
)
