# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER -- DWD_MISSING_VALUE_PERIODS
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** `dwd_missing_value_periods` (~5.9M rows) into source-scoped Silver. One of 7 sibling notebooks in this folder, split
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
COMPONENT = "silver/energy/_reference/dwd_reference/07_dwd_missing_value_periods"
RID = run_id()

CONTRACT = load_contract(SOURCE)
STATION_IDS = [str(s) for s in CONTRACT["conventions"]["station_set"]["ids"]]

# COMMAND ----------

# DBTITLE 1,Read Bronze -- dwd_missing_value_periods (~5.9M rows, one scan)
_bronze = read_bronze("dwd_missing_value_periods")

# COMMAND ----------

# DBTITLE 1,Transform -- dwd_missing_value_periods (typing + renames)
mvp = keep_real_stations(_bronze, "Stations_ID", "dwd_missing_value_periods")
mvp = (
    mvp.withColumnRenamed("Stations_Name", "station_name")
    .withColumnRenamed("Parameter", "parameter_source_code")
    .withColumn("gap_start_ts", parse_ts("Von_Datum", ("dd.MM.yyyy-HH:mm",), "UTC"))
    .withColumn("gap_end_ts", parse_ts("Bis_Datum", ("dd.MM.yyyy-HH:mm",), "UTC"))
    .withColumn("missing_value_count", F.col("Anzahl_Fehlwerte").cast("bigint"))
    .withColumnRenamed("Beschreibung", "gap_description")
    .drop("Von_Datum", "Bis_Datum", "Anzahl_Fehlwerte")
    .dropDuplicates()
)

# COMMAND ----------

# DBTITLE 1,Transform -- dwd_missing_value_periods (provenance)
mvp = within_group_ordinal(
    mvp,
    ["station_id", "parameter_source_code", "gap_start_ts", "gap_end_ts"],
    ["missing_value_count", "gap_description", "eor"],
)
mvp = mvp.withColumn(
    "_srid",
    sha_key(
        "station_id",
        "parameter_source_code",
        "gap_start_ts",
        "gap_end_ts",
        "_src_id_ord",
    ),
)
mvp = add_provenance(mvp, SOURCE, "_srid", RID)

# COMMAND ----------

# DBTITLE 1,Write Silver -- dwd_missing_value_periods
write_silver(
    mvp, "dwd_missing_value_periods", source=SOURCE, component=COMPONENT, rid=RID
)

# COMMAND ----------

# DBTITLE 1,Inspect dwd_missing_value_periods + export findings
_findings_blocks = inspect_table(
    mvp,
    "dwd_missing_value_periods",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=[
        "station_id",
        "parameter_source_code",
        "gap_start_ts",
        "gap_end_ts",
        "_src_id_ord",
    ],
    df_before=_bronze,
)
write_silver_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__dwd_missing_value_periods",
    "dwd_missing_value_periods",
    _findings_blocks,
)
