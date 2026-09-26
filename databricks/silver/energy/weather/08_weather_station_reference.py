# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER -- WEATHER STATION REFERENCE (DWD)
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** DWD station metadata keyed to `weather_location`: name
# MAGIC history, instruments, parameter periods, the parameter catalog and the
# MAGIC reported missing-value periods.

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

# DBTITLE 1,DWD reference helpers (station filter, metadata renames)
# MAGIC %run ../_reference/dwd_reference/_dwd_reference_common

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "dwd"
COMPONENT = "silver/energy/weather/08_weather_station_reference"
RID = run_id()
FINDINGS = "weather"
CONTRACT = load_contract(SOURCE)
MAPPING = load_mapping(SOURCE)
TABLES = contract_tables(CONTRACT)
META_FIELDS = (MAPPING.get("business_names", {}) or {}).get("metadata_fields", {})
STATION_IDS = [str(s) for s in CONTRACT["conventions"]["station_set"]["ids"]]
DAY = ("yyyyMMdd",)

# COMMAND ----------

# DBTITLE 1,Session time zone
ensure_utc_session()

# COMMAND ----------

# DBTITLE 1,Helper -- station period rows (curated stations, renamed, ordinal)


def station_period(df, bronze_table: str, content_cols: list, extra_key=()):
    """Real stations only, business names, valid_from/valid_to as dates and a
    record_ordinal where several rows share one key."""
    df = rename_meta(keep_real_stations(df, "Stations_ID", bronze_table), bronze_table)
    df = (
        df.withColumn("valid_from", F.to_date(parse_ts("valid_from", DAY, "UTC")))
        .withColumn("valid_to", F.to_date(parse_ts("valid_to", DAY, "UTC")))
        .withColumnRenamed("station_id", "source_location_id")
        .withColumnRenamed("station_name", "name")
        .withColumnRenamed("station_elevation_m", "elevation_m")
        .withColumn("location_key", location_key(SOURCE, "source_location_id"))
    )
    key = ["source_location_id", *extra_key, "valid_from"]
    df = within_group_ordinal(df, key, content_cols)
    df = df.withColumnRenamed("_source_id_ordinal", "record_ordinal")
    return df.withColumn("_srid", sha_key(*key, "record_ordinal"))


# COMMAND ----------

# DBTITLE 1,Read Bronze -- dwd_station_name_history
name_history_bronze = read_bronze("dwd_station_name_history")

# COMMAND ----------

# DBTITLE 1,Read Bronze -- dwd_device_instrument
instrument_bronze = read_bronze("dwd_device_instrument")

# COMMAND ----------

# DBTITLE 1,Read Bronze -- dwd_parameter_unit
parameter_unit_bronze = read_bronze("dwd_parameter_unit")

# COMMAND ----------

# DBTITLE 1,Read Bronze -- dwd_missing_value_periods
missing_bronze = read_bronze("dwd_missing_value_periods")

# COMMAND ----------

# DBTITLE 1,Transform -- weather_station_name_history
name_history = station_period(
    name_history_bronze, "dwd_station_name_history", ["name", "valid_to"]
)
name_history = add_semantic_provenance(
    name_history, SOURCE, "dwd_station_name_history", RID, "_srid"
)

# COMMAND ----------

# DBTITLE 1,Transform -- weather_station_instrument
instrument = station_period(
    instrument_bronze,
    "dwd_device_instrument",
    [
        "name",
        "longitude",
        "latitude",
        "elevation_m",
        "sensor_height_m",
        "valid_to",
        "device_type",
        "measurement_method",
    ],
    extra_key=("parameter_category",),
)
instrument = add_semantic_provenance(
    instrument, SOURCE, "dwd_device_instrument", RID, "_srid"
)

# COMMAND ----------

# DBTITLE 1,Transform -- weather_parameter_period
parameter_period = station_period(
    parameter_unit_bronze,
    "dwd_parameter_unit",
    [
        "valid_to",
        "name",
        "parameter_description_de",
        "parameter_unit",
        "parameter_data_source",
        "parameter_extra_info",
        "parameter_special_notes",
        "parameter_reference",
    ],
    extra_key=("parameter_source_code",),
)
parameter_period = add_semantic_provenance(
    parameter_period, SOURCE, "dwd_parameter_unit", RID, "_srid"
)

# COMMAND ----------

# DBTITLE 1,Transform -- weather_parameter_catalog (code -> business name -> unit)
_business = (MAPPING.get("business_names", {}) or {}).get("parameters", {})
_catalog_names = spark.createDataFrame(
    [(c, spec["business_name"], spec.get("unit")) for c, spec in _business.items()],
    "parameter_source_code string, parameter_business_name string, mapped_unit string",
)
_catalog_units = (
    parameter_unit_bronze.select(
        F.trim("Parameter").alias("parameter_source_code"),
        F.trim("Einheit").alias("parameter_unit"),
        F.trim("Parameterbeschreibung").alias("parameter_description_de"),
    )
    .filter(F.col("parameter_source_code").isNotNull())
    .groupBy("parameter_source_code")
    .agg(F.min(F.struct("parameter_unit", "parameter_description_de")).alias("_pick"))
    .select("parameter_source_code", "_pick.*")
)
parameter_catalog = (
    _catalog_names.join(_catalog_units, "parameter_source_code", "left")
    .withColumn("parameter_unit", F.coalesce("parameter_unit", "mapped_unit"))
    .withColumn("catalog_vintage", F.lit("dwd_hourly_historical_20260904"))
)
parameter_catalog = add_semantic_provenance(
    parameter_catalog.withColumn("_srid", F.col("parameter_source_code")),
    SOURCE,
    "dwd_parameter_unit",
    RID,
    "_srid",
)

# COMMAND ----------

# DBTITLE 1,Transform -- weather_missing_value_period (79,551 exact repeats collapsed)
_gap_format = ("dd.MM.yyyy-HH:mm",)
missing = rename_meta(
    keep_real_stations(missing_bronze, "Stations_ID", "dwd_missing_value_periods"),
    "dwd_missing_value_periods",
)
missing = (
    missing.withColumn(
        "gap_start_timestamp", parse_ts("valid_from", _gap_format, "UTC")
    )
    .withColumn("gap_end_timestamp", parse_ts("valid_to", _gap_format, "UTC"))
    .withColumn("missing_value_count", F.col("missing_value_count").cast("bigint"))
    .withColumnRenamed("station_id", "source_location_id")
    .withColumnRenamed("station_name", "name")
    .drop("valid_from", "valid_to")
    .dropDuplicates()
    .withColumn("location_key", location_key(SOURCE, "source_location_id"))
)
_gap_key = [
    "source_location_id",
    "parameter_source_code",
    "gap_start_timestamp",
    "gap_end_timestamp",
]
missing = within_group_ordinal(
    missing, _gap_key, ["missing_value_count", "gap_description"]
).withColumnRenamed("_source_id_ordinal", "record_ordinal")
missing = add_semantic_provenance(
    missing.withColumn("_srid", sha_key(*_gap_key, "record_ordinal")),
    SOURCE,
    "dwd_missing_value_periods",
    RID,
    "_srid",
)

# COMMAND ----------

# DBTITLE 1,Configuration -- structure -> frame
STRUCTURES = {
    "weather_station_name_history": name_history,
    "weather_station_instrument": instrument,
    "weather_parameter_period": parameter_period,
    "weather_parameter_catalog": parameter_catalog,
    "weather_missing_value_period": missing,
}

# COMMAND ----------

# DBTITLE 1,Write Silver -- station reference structures
for name, frame in STRUCTURES.items():
    write_semantic(
        conform(frame, SEMANTIC_STRUCTURES[name]),
        name,
        source=SOURCE,
        component=COMPONENT,
        rid=RID,
    )

# COMMAND ----------

# DBTITLE 1,Inspect -- station reference structures
findings_blocks = {
    name: inspect_table(
        spark.table(semantic_table(name)),
        name,
        source=FINDINGS,
        component=COMPONENT,
        rid=RID,
        key_cols=["source_record_id"],
    )
    for name in STRUCTURES
}

# COMMAND ----------

# DBTITLE 1,Export findings -- station reference structures
for name, blocks in findings_blocks.items():
    write_silver_findings(FINDINGS, f"{COMPONENT.split('/')[-1]}__{name}", name, blocks)
