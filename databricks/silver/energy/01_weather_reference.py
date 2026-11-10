# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # SILVER -- WEATHER REFERENCE (ENERGY)
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** November 2026
# MAGIC
# MAGIC **Purpose:** the weather places (DWD stations, AccuWeather cities, the Honda site and
# MAGIC Seattle), the station periods (coordinates, names, instruments and parameters of
# MAGIC one table, one `period_kind` per kind), the missing-value periods that DWD
# MAGIC reports, the reconciliation of those reports with the gaps in the hourly data, and
# MAGIC the DWD parameter rows of `reference_code_list`.

# COMMAND ----------

# DBTITLE 1,Semantic structures library
# MAGIC %run ../_semantic_common

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "dwd"
COMPONENT = "silver/energy/01_weather_reference"
FINDINGS = "energy"
RID = run_id()

CONTRACT = load_contract(SOURCE)
MAPPING = load_mapping(SOURCE)
TABLES = contract_tables(CONTRACT)
META_FIELDS = (MAPPING.get("business_names", {}) or {}).get("metadata_fields", {})
_stations = CONTRACT["conventions"]["station_set"]
STATION_IDS = [str(s) for s in _stations["ids"]]
STATION_CITY = dict(zip(STATION_IDS, _stations["cities"], strict=True))
DAY = ("yyyyMMdd",)
NO_TEXT = F.lit(None).cast("string")

# DWD station city to Bundesland (curated, 28 stations).
CITY_REGION = {
    "berlin": "Berlin",
    "hamburg": "Hamburg",
    "munich": "Bayern",
    "cologne_bonn": "Nordrhein-Westfalen",
    "frankfurt_am_main": "Hessen",
    "stuttgart": "Baden-Wuerttemberg",
    "essen": "Nordrhein-Westfalen",
    "leipzig": "Sachsen",
    "dresden": "Sachsen",
    "nuremberg": "Bayern",
    "hannover": "Niedersachsen",
    "bremen": "Bremen",
    "potsdam": "Brandenburg",
    "magdeburg": "Sachsen-Anhalt",
    "erfurt": "Thueringen",
    "trier": "Rheinland-Pfalz",
    "saarbruecken": "Saarland",
    "kiel": "Schleswig-Holstein",
    "rostock_warnemuende": "Mecklenburg-Vorpommern",
    "norderney": "Niedersachsen",
    "sylt": "Schleswig-Holstein",
    "garmisch_partenkirchen": "Bayern",
    "zugspitze": "Bayern",
    "hohenpeissenberg": "Bayern",
    "feldberg_schwarzwald": "Baden-Wuerttemberg",
    "cottbus": "Brandenburg",
    "goerlitz": "Sachsen",
    "braunschweig": "Niedersachsen",
}
MEASUREMENT_TABLES = (
    "dwd_air_temperature",
    "dwd_cloudiness",
    "dwd_moisture",
    "dwd_precipitation",
    "dwd_pressure",
    "dwd_sun",
    "dwd_wind",
    "dwd_dew_point",
    "dwd_soil_temperature",
    "dwd_visibility",
    "dwd_cloud_type",
    "dwd_wind_synop",
    "dwd_extreme_wind",
    "dwd_weather_phenomena",
)

# COMMAND ----------

# DBTITLE 1,Session time zone
ensure_utc_session()

# COMMAND ----------

# DBTITLE 1,Transform -- DWD station coordinate periods (one row per station and validity)
dwd_validity = (
    read_bronze("dwd_station_geography")
    .withColumn("source_location_id", strip_float_suffix("Stations_id"))
    .filter(F.col("source_location_id").isin(STATION_IDS))
    .select(
        "source_location_id",
        F.col("Stationsname").alias("name"),
        F.col("`Geogr.Breite`").cast("double").alias("latitude"),
        F.col("`Geogr.Laenge`").cast("double").alias("longitude"),
        F.col("Stationshoehe").cast("double").alias("elevation_m"),
        F.to_date(parse_ts("von_datum", ("yyyyMMdd",), "UTC")).alias("valid_from"),
        F.to_date(parse_ts("bis_datum", ("yyyyMMdd",), "UTC")).alias("valid_to"),
    )
    .withColumn("location_key", location_key("dwd", "source_location_id"))
)
# Relocations can share a start date (2-17 rows): ordinal, never a drop.
dwd_validity = within_group_ordinal(
    dwd_validity,
    ["source_location_id", "valid_from"],
    ["latitude", "longitude", "elevation_m", "valid_to", "name"],
).withColumnRenamed("_source_id_ordinal", "record_ordinal")
dwd_validity = dwd_validity.withColumn(
    "coordinate_outside_germany_bounding_box",
    bbox_outside_de("latitude", "longitude"),
)
# Stations outside the Germany bounding box are recorded, not dropped.
write_quarantine(
    _q_rows(
        dwd_validity.filter(F.col("coordinate_outside_germany_bounding_box")),
        "coord_outside_de_bbox",
        "station coordinate outside the Germany bounding box",
        "latitude,longitude",
        "latitude",
        "source_location_id",
        "dwd",
        "dwd_station_geography",
    ),
    RID,
)
location_period = add_semantic_provenance(
    dwd_validity.withColumn("period_kind", F.lit("location")),
    "dwd",
    "dwd_station_geography",
    RID,
).withColumn(
    "source_record_id",
    sha_key("period_kind", "source_location_id", "valid_from", "record_ordinal"),
)

# COMMAND ----------

# DBTITLE 1,Transform -- weather_location (DWD stations, AccuWeather cities, single sites)
_latest = Window.partitionBy("source_location_id").orderBy(
    F.col("valid_to").isNull().desc(),
    F.col("valid_from").desc(),
    F.col("name").asc(),
)
_city_lookup = spark.createDataFrame(
    [(sid, slug, CITY_REGION[slug]) for sid, slug in STATION_CITY.items()],
    "source_location_id string, city_slug string, region string",
).withColumn("official_municipality_key", bundesland_ags("region"))
dwd_locations = (
    dwd_validity.withColumn("_rn", F.row_number().over(_latest))
    .filter(F.col("_rn") == 1)
    .drop(
        "_rn",
        "valid_from",
        "valid_to",
        "record_ordinal",
        "coordinate_outside_germany_bounding_box",
    )
    .join(F.broadcast(_city_lookup), "source_location_id", "left")
    .withColumn("city", F.initcap(F.regexp_replace("city_slug", "_", " ")))
    .withColumn("location_role", F.lit("station"))
    .withColumn("country_code", F.lit("DE"))
    .withColumn("continent", F.lit("EU"))
    .withColumn("geography_basis", F.lit("curated_city_lookup"))
    .drop("city_slug")
)
aw_locations = (
    spark.table("samples.accuweather.historical_hourly_metric")
    .select("city_name", "country_code", "latitude", "longitude")
    .distinct()
    .withColumn("source_location_id", F.col("city_name"))
    .withColumn("location_key", location_key("accuweather", "source_location_id"))
    .withColumn("name", F.initcap("city_name"))
    .withColumn("city", F.initcap("city_name"))
    .withColumn("country_code", F.upper("country_code"))
    .withColumn("continent", continent_of("country_code"))
    .withColumn("latitude", F.col("latitude").cast("double"))
    .withColumn("longitude", F.col("longitude").cast("double"))
    .withColumn("location_role", F.lit("city"))
    .withColumn("geography_basis", F.lit("source_field"))
    .drop("city_name")
)
site_rows = [
    ("honda_iot", "honda_site", "site", "Honda site", "EU", "DE", None, None),
    (
        "weather_seattle",
        "seattle",
        "city",
        "Seattle",
        "NA",
        "US",
        "Washington",
        "Seattle",
    ),
]
site_locations = (
    spark.createDataFrame(
        site_rows,
        "source_system string, source_location_id string, location_role string, "
        "name string, continent string, country_code string, region string, "
        "city string",
    )
    .withColumn("geography_basis", F.lit("source_documentation"))
    .withColumn("location_key", sha_key("source_system", "source_location_id"))
    .withColumn("source_dataset", F.lit("site_definition"))
    .withColumn("_silver_loaded_at", F.current_timestamp())
    .withColumn("_silver_run_id", F.lit(RID))
)
locations = (
    add_semantic_provenance(dwd_locations, "dwd", "dwd_station_geography", RID)
    .unionByName(
        add_semantic_provenance(
            aw_locations, "accuweather", "historical_hourly_metric", RID
        ),
        allowMissingColumns=True,
    )
    .unionByName(site_locations, allowMissingColumns=True)
    .withColumn("source_record_id", sha_key("source_system", "source_location_id"))
)
weather_location = conform(locations, WEATHER_LOCATION_COLUMNS)

# COMMAND ----------

# DBTITLE 1,Write, inspect and export -- weather_location
write_inspect_export(
    weather_location,
    "weather_location",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    findings=FINDINGS,
    key_cols=["location_key"],
    writer=write_semantic,
    extra_checks=structure_extra_checks,
)

# COMMAND ----------

# DBTITLE 1,Transform -- station periods: names, instruments and parameters
name_history_bronze = read_bronze("dwd_station_name_history")
instrument_bronze = read_bronze("dwd_device_instrument")
parameter_unit_bronze = read_bronze("dwd_parameter_unit")


def station_period(df, kind, bronze_table, content_cols, extra_key=()):
    """Real stations only, business names, validity as dates and a record_ordinal
    where several rows share one key; `kind` is the `period_kind`."""
    df = rename_meta(keep_real_stations(df, "Stations_ID", bronze_table), bronze_table)
    df = (
        df.withColumn("valid_from", F.to_date(parse_ts("valid_from", DAY, "UTC")))
        .withColumn("valid_to", F.to_date(parse_ts("valid_to", DAY, "UTC")))
        .withColumnRenamed("station_id", "source_location_id")
        .withColumnRenamed("station_name", "name")
        .withColumnRenamed("station_elevation_m", "elevation_m")
        .withColumn("location_key", location_key(SOURCE, "source_location_id"))
        .withColumn("period_kind", F.lit(kind))
    )
    key = ["source_location_id", *extra_key, "valid_from"]
    df = within_group_ordinal(df, key, content_cols).withColumnRenamed(
        "_source_id_ordinal", "record_ordinal"
    )
    df = df.withColumn("_srid", sha_key("period_kind", *key, "record_ordinal"))
    return add_semantic_provenance(df, SOURCE, bronze_table, RID, "_srid")


name_period = station_period(
    name_history_bronze, "name", "dwd_station_name_history", ["name", "valid_to"]
)
instrument_period = station_period(
    instrument_bronze,
    "instrument",
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
parameter_period = station_period(
    parameter_unit_bronze,
    "parameter",
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
station_periods = conform(location_period, WEATHER_STATION_PERIOD_COLUMNS)
for part in (name_period, instrument_period, parameter_period):
    station_periods = station_periods.unionByName(
        conform(part, WEATHER_STATION_PERIOD_COLUMNS)
    )

# COMMAND ----------

# DBTITLE 1,Write, inspect and export -- weather_station_period
write_inspect_export(
    station_periods,
    "weather_station_period",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    findings=FINDINGS,
    key_cols=["source_record_id"],
    writer=write_semantic,
    extra_checks=structure_extra_checks,
)

# COMMAND ----------

# DBTITLE 1,Transform -- DWD parameter rows of reference_code_list
business = (MAPPING.get("business_names", {}) or {}).get("parameters", {})
catalog_names = spark.createDataFrame(
    [(c, spec["business_name"], spec.get("unit")) for c, spec in business.items()],
    "code_id string, label string, mapped_unit string",
)
catalog_units = (
    parameter_unit_bronze.select(
        F.trim("Parameter").alias("code_id"),
        F.trim("Einheit").alias("parameter_unit"),
        F.trim("Parameterbeschreibung").alias("description_de"),
    )
    .filter(F.col("code_id").isNotNull())
    .groupBy("code_id")
    .agg(F.min(F.struct("parameter_unit", "description_de")).alias("_pick"))
    .select("code_id", "_pick.*")
)
parameter_codes = (
    catalog_names.join(catalog_units, "code_id", "left")
    .withColumn("unit", F.coalesce("parameter_unit", "mapped_unit"))
    .withColumn("catalog_kind", F.lit("dwd_parameter"))
    .withColumn("parent_id", NO_TEXT)
    .withColumn("catalog_vintage", F.lit("dwd_hourly_historical_20260904"))
    .withColumn("_srid", sha_key("catalog_kind", "code_id"))
)
parameter_codes = add_semantic_provenance(
    parameter_codes, SOURCE, "dwd_parameter_unit", RID, "_srid"
)
DWD_CODES = {r["code_id"] for r in parameter_codes.select("code_id").collect()}

# COMMAND ----------

# DBTITLE 1,Write, inspect and export -- reference_code_list (DWD rows)
write_inspect_export(
    conform(parameter_codes, REFERENCE_CODE_LIST_COLUMNS),
    "reference_code_list",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    findings=FINDINGS,
    key_cols=["catalog_kind", "code_id"],
    writer=write_semantic,
    replace_where="source_system = 'dwd'",
    extra_checks=structure_extra_checks,
    section="reference_code_list -- dwd",
)

# COMMAND ----------

# DBTITLE 1,Transform -- weather_missing_value_period (exact repeats collapsed)
_gap_format = ("dd.MM.yyyy-HH:mm",)
missing = rename_meta(
    keep_real_stations(
        read_bronze("dwd_missing_value_periods"),
        "Stations_ID",
        "dwd_missing_value_periods",
    ),
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
missing = conform(missing, WEATHER_MISSING_VALUE_PERIOD_COLUMNS)

# COMMAND ----------

# DBTITLE 1,Write, inspect and export -- weather_missing_value_period
write_inspect_export(
    missing,
    "weather_missing_value_period",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    findings=FINDINGS,
    key_cols=["source_record_id"],
    writer=write_semantic,
    extra_checks=structure_extra_checks,
)

# COMMAND ----------

# DBTITLE 1,Transform -- coverage of the hourly data and its observed gaps (from Bronze)
# Bronze column names are the DWD parameter codes the reports use. One pass per
# table: sentinels stripped and the timestamp parsed feed both the per-column
# coverage aggregate and the observed-missing explode (the gap signal).
_missing_parts = []
_coverage_parts = []
for _t in MEASUREMENT_TABLES:
    _df = read_bronze(_t)
    _value_cols = [c for c in _df.columns if c in DWD_CODES]
    if not _value_cols:
        continue
    _clean = (
        strip_sentinels(_df, _value_cols)
        .withColumn("source_location_id", strip_float_suffix("STATIONS_ID"))
        .withColumn("_ts", parse_mess_datum("MESS_DATUM"))
    )
    _missing_parts.append(
        _clean.select(
            "source_location_id",
            F.explode(
                F.array(*[F.when(F.col(c).isNull(), F.lit(c)) for c in _value_cols])
            ).alias("parameter_source_code"),
        )
        .filter(F.col("parameter_source_code").isNotNull())
        .distinct()
    )
    for _c in _value_cols:
        _coverage_parts.append(
            _clean.groupBy("source_location_id")
            .agg(
                F.count(F.col(_c)).alias("observed_count"),
                F.min(F.when(F.col(_c).isNotNull(), F.col("_ts"))).alias(
                    "time_span_start_utc"
                ),
                F.max(F.when(F.col(_c).isNotNull(), F.col("_ts"))).alias(
                    "time_span_end_utc"
                ),
            )
            .withColumn("parameter_source_code", F.lit(_c))
        )
observed = None
for _p in _missing_parts:
    observed = _p if observed is None else observed.unionByName(_p)
observed = observed.distinct().withColumn("observed", F.lit(True))
_coverage_union = None
for _p in _coverage_parts:
    _coverage_union = _p if _coverage_union is None else _coverage_union.unionByName(_p)
# A few codes are reported by more than one Bronze table (the shared quality
# column, V_N/V_N_I in cloudiness and cloud type): one row per (station, code).
coverage = _coverage_union.groupBy("source_location_id", "parameter_source_code").agg(
    F.sum("observed_count").alias("observed_count"),
    F.min("time_span_start_utc").alias("time_span_start_utc"),
    F.max("time_span_end_utc").alias("time_span_end_utc"),
)

# COMMAND ----------

# DBTITLE 1,Transform -- weather_missingness_reconciliation
_key = ["source_location_id", "parameter_source_code"]
reported = (
    missing.select("source_location_id", "parameter_source_code")
    .distinct()
    .withColumn("reported", F.lit(True))
)
recon = (
    coverage.join(observed, _key, "left")
    .join(reported, _key, "left")
    .na.fill(False, subset=["observed", "reported"])
    .withColumn(
        "reconciliation_status",
        F.when(F.col("observed") & F.col("reported"), F.lit("matched"))
        .when(F.col("observed"), F.lit("observed_only"))
        .when(F.col("reported"), F.lit("reported_only"))
        .otherwise(F.lit("no_gap_signal")),
    )
    .withColumn("location_key", location_key(SOURCE, "source_location_id"))
    .withColumn("_srid", sha_key(*_key))
)
recon = conform(
    add_semantic_provenance(recon, SOURCE, "dwd_hourly_bronze", RID, "_srid"),
    WEATHER_MISSINGNESS_RECONCILIATION_COLUMNS,
)

# COMMAND ----------

# DBTITLE 1,Write, inspect and export -- weather_missingness_reconciliation
write_inspect_export(
    recon,
    "weather_missingness_reconciliation",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    findings=FINDINGS,
    key_cols=_key,
    writer=write_semantic,
    extra_checks=lambda t: {
        "rows_by_status": {
            r["reconciliation_status"]: r["count"]
            for r in t.groupBy("reconciliation_status").count().collect()
        }
    },
)
