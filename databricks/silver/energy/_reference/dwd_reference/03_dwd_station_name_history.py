# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER -- DWD_STATION_NAME_HISTORY
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** `dwd_station_name_history` into source-scoped Silver, SCD
# MAGIC (station, valid_from, `_src_id_ord`) grain -- the Bronze contract
# MAGIC documents (station_id, valid_from) alone as not unique. One of 7
# MAGIC sibling notebooks in this folder, split
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
COMPONENT = "silver/energy/_reference/dwd_reference/03_dwd_station_name_history"
RID = run_id()

CONTRACT = load_contract(SOURCE)
STATION_IDS = [str(s) for s in CONTRACT["conventions"]["station_set"]["ids"]]

# COMMAND ----------

# DBTITLE 1,Read Bronze -- dwd_station_name_history
_bronze = read_bronze("dwd_station_name_history")

# COMMAND ----------

# DBTITLE 1,Transform -- dwd_station_name_history
snh = keep_real_stations(_bronze, "Stations_ID", "dwd_station_name_history")
snh = (
    snh.withColumnRenamed("Stationsname", "station_name")
    .withColumn("valid_from", parse_ts("Von_Datum", ("yyyyMMdd",), "UTC"))
    .withColumn("valid_to", parse_ts("Bis_Datum", ("yyyyMMdd",), "UTC"))
    .drop("Von_Datum", "Bis_Datum")
)
# Bronze contract documents (station_id, valid_from) as unique: false --
# same relocation-style overlap risk as dwd_station_geography.
snh = within_group_ordinal(
    snh, ["station_id", "valid_from"], ["station_name", "valid_to"]
)
snh = snh.withColumn("_srid", sha_key("station_id", "valid_from", "_src_id_ord"))
snh = add_provenance(snh, SOURCE, "_srid", RID)

# COMMAND ----------

# DBTITLE 1,Write Silver -- dwd_station_name_history
write_silver(
    snh, "dwd_station_name_history", source=SOURCE, component=COMPONENT, rid=RID
)

# COMMAND ----------

# DBTITLE 1,Inspect dwd_station_name_history + export findings
_findings_blocks = inspect_table(
    snh,
    "dwd_station_name_history",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["station_id", "valid_from", "_src_id_ord"],
    df_before=_bronze,
)
write_silver_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__dwd_station_name_history",
    "dwd_station_name_history",
    _findings_blocks,
)
