# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER -- DWD_PARAMETER_UNIT
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** `dwd_parameter_unit` into source-scoped Silver, source
# MAGIC (station, parameter_source_code, valid_from, `_src_id_ord`) grain --
# MAGIC the Bronze contract documents (station_id, Parameter, valid_from) alone
# MAGIC as not unique. One of 7 sibling notebooks in this folder, split
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
COMPONENT = "silver/energy/_reference/dwd_reference/05_dwd_parameter_unit"
RID = run_id()

CONTRACT = load_contract(SOURCE)
MAPPING = load_mapping(SOURCE)
TABLES = contract_tables(CONTRACT)
META_FIELDS = (MAPPING.get("business_names", {}) or {}).get("metadata_fields", {})
STATION_IDS = [str(s) for s in CONTRACT["conventions"]["station_set"]["ids"]]

# COMMAND ----------

# DBTITLE 1,Read Bronze -- dwd_parameter_unit
_bronze = read_bronze("dwd_parameter_unit")

# COMMAND ----------

# DBTITLE 1,Transform -- dwd_parameter_unit
pu = keep_real_stations(_bronze, "Stations_ID", "dwd_parameter_unit")
pu = rename_meta(pu, "dwd_parameter_unit")
pu = pu.withColumn("valid_from", parse_ts("valid_from", ("yyyyMMdd",), "UTC"))
pu = pu.withColumn("valid_to", parse_ts("valid_to", ("yyyyMMdd",), "UTC"))
# Bronze contract documents (station_id, Parameter, valid_from) as unique:
# false -- 5418 rows, more than one parameter-unit record can share the same
# parameter code and start date.
pu = within_group_ordinal(
    pu,
    ["station_id", "parameter_source_code", "valid_from"],
    [
        "valid_to",
        "station_name",
        "parameter_description_de",
        "parameter_unit",
        "parameter_data_source",
        "parameter_extra_info",
        "parameter_special_notes",
        "parameter_reference",
    ],
)
pu = pu.withColumn(
    "_srid",
    sha_key("station_id", "parameter_source_code", "valid_from", "_src_id_ord"),
)
pu = add_provenance(pu, SOURCE, "_srid", RID)

# COMMAND ----------

# DBTITLE 1,Write Silver -- dwd_parameter_unit
write_silver(pu, "dwd_parameter_unit", source=SOURCE, component=COMPONENT, rid=RID)

# COMMAND ----------

# DBTITLE 1,Inspect dwd_parameter_unit + export findings
_findings_blocks = inspect_table(
    pu,
    "dwd_parameter_unit",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["station_id", "parameter_source_code", "valid_from", "_src_id_ord"],
    df_before=_bronze,
)
write_silver_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__dwd_parameter_unit",
    "dwd_parameter_unit",
    _findings_blocks,
)