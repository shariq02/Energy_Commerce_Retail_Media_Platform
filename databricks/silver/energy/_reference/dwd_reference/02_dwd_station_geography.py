# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER -- DWD_STATION_GEOGRAPHY
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** `dwd_station_geography` into source-scoped Silver, SCD (station, valid_from) grain. One of 7 sibling notebooks in this folder, split
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

# DBTITLE 1,Imports
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "dwd"
COMPONENT = "silver/energy/_reference/dwd_reference/02_dwd_station_geography"
RID = run_id()

CONTRACT = load_contract(SOURCE)
STATION_IDS = [str(s) for s in CONTRACT["conventions"]["station_set"]["ids"]]

# COMMAND ----------

# DBTITLE 1,Read Bronze -- dwd_station_geography
_bronze = read_bronze("dwd_station_geography")

# COMMAND ----------

# DBTITLE 1,Transform -- dwd_station_geography (typing + renames)
sg = keep_real_stations(_bronze, "Stations_id", "dwd_station_geography")
sg = (
    sg.withColumn("latitude", F.col("`Geogr.Breite`").cast("double"))
    .withColumn("longitude", F.col("`Geogr.Laenge`").cast("double"))
    .withColumn("station_elevation_m", F.col("Stationshoehe").cast("double"))
    .withColumn("valid_from", parse_ts("von_datum", ("yyyyMMdd",), "UTC"))
    .withColumn("valid_to", parse_ts("bis_datum", ("yyyyMMdd",), "UTC"))
    .withColumnRenamed("Stationsname", "station_name")
    .drop("Geogr.Breite", "Geogr.Laenge", "Stationshoehe", "von_datum", "bis_datum")
)

# COMMAND ----------

# DBTITLE 1,Transform -- dwd_station_geography (bbox quarantine)
sg, q = value_quarantine(
    sg,
    bbox_outside_de("latitude", "longitude"),
    flag_col="_coord_outside_de_bbox",
    rule_id="coord_outside_de_bbox",
    reason="station coordinate outside the Germany bounding box",
    field_name="latitude,longitude",
    value_col="latitude",
    sr_id_col="station_id",
    source_system=SOURCE,
    bronze_table="dwd_station_geography",
)

# COMMAND ----------

# DBTITLE 1,Write Quarantine -- dwd_station_geography out-of-bbox rows
write_quarantine(q, RID)

# COMMAND ----------

# DBTITLE 1,Transform -- dwd_station_geography (provenance)
sg = sg.withColumn("_srid", sha_key("station_id", "valid_from"))
sg = add_provenance(sg, SOURCE, "_srid", RID)

# COMMAND ----------

# DBTITLE 1,Write Silver -- dwd_station_geography
write_silver(sg, "dwd_station_geography", source=SOURCE, component=COMPONENT, rid=RID)

# COMMAND ----------

# DBTITLE 1,Inspect dwd_station_geography + export findings
_findings_blocks = inspect_table(
    sg,
    "dwd_station_geography",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["station_id", "valid_from"],
    df_before=_bronze,
)
write_silver_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__dwd_station_geography",
    "dwd_station_geography",
    _findings_blocks,
)
