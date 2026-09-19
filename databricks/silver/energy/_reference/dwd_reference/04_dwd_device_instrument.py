# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER -- DWD_DEVICE_INSTRUMENT
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** `dwd_device_instrument` into source-scoped Silver, source
# MAGIC (station, parameter_category, valid_from, `_src_id_ord`) grain -- the
# MAGIC Bronze contract documents (station_id, device_category, valid_from)
# MAGIC alone as not unique. One of 7 sibling notebooks in this folder, split
# MAGIC from a single `01_dwd_reference.py` -- see the folder's other files for
# MAGIC the rest. Runs before the DWD measurement notebooks.

# COMMAND ----------

# DBTITLE 1,Shared library
# MAGIC %run ../../../_silver_common

# COMMAND ----------

# DBTITLE 1,Inspection library
# MAGIC %run ../../../_silver_inspect

# COMMAND ----------

# DBTITLE 1,DWD reference shared helpers
# MAGIC %run ./_dwd_reference_common

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "dwd"
COMPONENT = "silver/energy/_reference/dwd_reference/04_dwd_device_instrument"
RID = run_id()

CONTRACT = load_contract(SOURCE)
MAPPING = load_mapping(SOURCE)
TABLES = contract_tables(CONTRACT)
META_FIELDS = (MAPPING.get("business_names", {}) or {}).get("metadata_fields", {})
STATION_IDS = [str(s) for s in CONTRACT["conventions"]["station_set"]["ids"]]

# COMMAND ----------

# DBTITLE 1,Read Bronze -- dwd_device_instrument
_bronze = read_bronze("dwd_device_instrument")

# COMMAND ----------

# DBTITLE 1,Transform -- dwd_device_instrument
di = keep_real_stations(_bronze, "Stations_ID", "dwd_device_instrument")
di = rename_meta(di, "dwd_device_instrument")
di = di.withColumn("valid_from", parse_ts("valid_from", ("yyyyMMdd",), "UTC"))
di = di.withColumn("valid_to", parse_ts("valid_to", ("yyyyMMdd",), "UTC"))
# Bronze contract documents (station_id, device_category, valid_from) as
# unique: false -- 4008 rows over 28 stations, more than one device record
# can share the same category and start date.
di = within_group_ordinal(
    di,
    ["station_id", "parameter_category", "valid_from"],
    [
        "station_name",
        "longitude",
        "latitude",
        "station_elevation_m",
        "sensor_height_m",
        "valid_to",
        "device_type",
        "measurement_method",
    ],
)
di = di.withColumn(
    "_srid", sha_key("station_id", "parameter_category", "valid_from", "_src_id_ord")
)
di = add_provenance(di, SOURCE, "_srid", RID)

# COMMAND ----------

# DBTITLE 1,Write Silver -- dwd_device_instrument
write_silver(di, "dwd_device_instrument", source=SOURCE, component=COMPONENT, rid=RID)

# COMMAND ----------

# DBTITLE 1,Inspect dwd_device_instrument + export findings
_findings_blocks = inspect_table(
    di,
    "dwd_device_instrument",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["station_id", "parameter_category", "valid_from", "_src_id_ord"],
    df_before=_bronze,
)
write_silver_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__dwd_device_instrument",
    "dwd_device_instrument",
    _findings_blocks,
)
