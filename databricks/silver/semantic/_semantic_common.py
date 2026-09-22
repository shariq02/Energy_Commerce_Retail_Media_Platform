# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER SEMANTIC STRUCTURES SHARED HELPER
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** building blocks for the cross-source Silver structures (time
# MAGIC columns, unit standardisation, place keys, column conformance, per-source
# MAGIC replace writes). Pulled in with `%run ../_semantic_common` after
# MAGIC `_silver_common`. Definitions only -- no side effects at import.

# COMMAND ----------

# DBTITLE 1,Configuration constants
SEMANTIC_SCHEMA = "energy_silver"
PROJECT_TZ = "Europe/Berlin"

# Provenance columns shared by every semantic structure. Field-level source
# attribution isn't tracked separately -- source_dataset already identifies
# exactly which product a row came from.
_PROVENANCE_HEAD = [("source_system", "string"), ("source_dataset", "string")]
_PROVENANCE_TAIL = [
    ("source_record_id", "string"),
    ("_silver_loaded_at", "timestamp"),
    ("_silver_run_id", "string"),
]

# Every weather structure needs place and time; this is genuinely universal
# scaffolding, not a measurement-shape decision -- the measurement columns
# below are designed per family, not shared.
_PLACE_HEAD = [
    ("observation_key", "string"),
    ("location_key", "string"),
    ("source_location_id", "string"),
]
_TIME_COLUMNS = [
    ("observation_ts_native", "string"),
    ("time_basis", "string"),
    ("utc_offset_hours", "double"),
    ("observation_ts_utc", "timestamp"),
    ("observation_ts_project", "timestamp"),
    ("local_date", "date"),
    ("interval_seconds", "int"),
]
# DWD's per-product quality classification (QN_*); NULL for sources that
# don't carry one (Honda, AccuWeather).
_QUALITY_COLUMNS = [
    ("quality_code", "string"),
    ("quality_label", "string"),
    ("quality_flag", "string"),
]
_TAIL = [("measurement_basis", "string"), *_PROVENANCE_HEAD, *_PROVENANCE_TAIL]

# One coherent semantic weather family = one Silver structure, designed
# around what that family's data actually is -- not a shared measurement
# shape partitioned by a label. `_is_primary` flags are per measurement
# field, only where a source genuinely has a competing alternate product for
# that specific field (not a blanket row-level flag).

WEATHER_TEMPERATURE_COLUMNS = [
    *_PLACE_HEAD,
    *_TIME_COLUMNS,
    ("air_temperature_degc", "double"),
    ("air_temperature_is_primary", "boolean"),
    ("dew_point_temperature_degc", "double"),
    ("dew_point_temperature_is_primary", "boolean"),
    ("wet_bulb_temperature_degc", "double"),
    *_QUALITY_COLUMNS,
    *_TAIL,
]

WEATHER_HUMIDITY_COLUMNS = [
    *_PLACE_HEAD,
    *_TIME_COLUMNS,
    ("relative_humidity_percent", "double"),
    ("relative_humidity_is_primary", "boolean"),
    ("absolute_humidity_g_per_m3", "double"),
    ("vapour_pressure_hpa", "double"),
    *_QUALITY_COLUMNS,
    *_TAIL,
]

# Native value/unit kept only for AccuWeather (Pa -> hPa); DWD is already
# hPa, so its native columns stay NULL (no unnecessary conversion recorded).
WEATHER_PRESSURE_COLUMNS = [
    *_PLACE_HEAD,
    *_TIME_COLUMNS,
    ("pressure_station_hpa", "double"),
    ("pressure_station_is_primary", "boolean"),
    ("pressure_station_native_value", "double"),
    ("pressure_station_native_unit", "string"),
    ("pressure_sea_level_hpa", "double"),
    ("pressure_sea_level_native_value", "double"),
    ("pressure_sea_level_native_unit", "string"),
    *_QUALITY_COLUMNS,
    *_TAIL,
]

# `statistic` is a real distinguishing fact here (mean/instant/max are three
# different DWD products reading the same station-hour), not a leftover
# generic column -- wind speed/direction/gust are already m/s and degrees
# everywhere, so no native pair is needed.
WEATHER_WIND_COLUMNS = [
    *_PLACE_HEAD,
    *_TIME_COLUMNS,
    ("statistic", "string"),
    ("wind_speed_m_per_s", "double"),
    ("wind_direction_degrees", "double"),
    ("wind_direction_variable", "boolean"),
    ("wind_gust_m_per_s", "double"),
    *_QUALITY_COLUMNS,
    *_TAIL,
]

# Amount, occurred-flag and form describe one precipitation event (DWD
# already reports all three in one Bronze row); both sources are already mm.
WEATHER_PRECIPITATION_COLUMNS = [
    *_PLACE_HEAD,
    *_TIME_COLUMNS,
    ("precipitation_mm", "double"),
    ("precipitation_occurred", "boolean"),
    ("precipitation_form_code", "string"),
    ("precipitation_form_text", "string"),
    *_QUALITY_COLUMNS,
    *_TAIL,
]

# Cloud layers are a genuinely repeated, variably-populated structure (0-4
# layers depending on sky conditions) -- kept as an array of structs, not
# flattened into fixed layer_1..layer_4 columns or exploded into long rows.
# Total cover needs a native pair on both sources (DWD eighths, AccuWeather
# fraction -- neither is already percent); base height is already metres
# everywhere.
WEATHER_CLOUD_COLUMNS = [
    *_PLACE_HEAD,
    *_TIME_COLUMNS,
    ("cloud_cover_total_percent", "double"),
    ("cloud_cover_total_is_primary", "boolean"),
    ("cloud_cover_total_native_value", "double"),
    ("cloud_cover_total_native_unit", "string"),
    ("cloud_base_height_m", "double"),
    ("observation_method", "string"),
    (
        "layers",
        (
            "array<struct<layer_number:int,genus_code:string,genus_text:string,"
            "base_height_m:double,cover_percent:double,cover_native_value:double,"
            "cover_native_unit:string>>"
        ),
    ),
    *_QUALITY_COLUMNS,
    *_TAIL,
]

# Kept separate from cloud: different physical basis (optical extinction
# distance vs. okta sky-cover fraction), and DWD keeps them as separate
# products. Native pair needed for AccuWeather (km -> m); DWD is already m.
WEATHER_VISIBILITY_COLUMNS = [
    *_PLACE_HEAD,
    *_TIME_COLUMNS,
    ("visibility_m", "double"),
    ("visibility_native_value", "double"),
    ("visibility_native_unit", "string"),
    ("observation_method", "string"),
    *_QUALITY_COLUMNS,
    *_TAIL,
]

# Purely categorical -- no numeric value/unit columns at all, unlike every
# other family. DWD only; no other source reports coded weather phenomena.
WEATHER_PRESENT_WEATHER_COLUMNS = [
    *_PLACE_HEAD,
    *_TIME_COLUMNS,
    ("present_weather_code", "string"),
    ("present_weather_text", "string"),
    *_QUALITY_COLUMNS,
    *_TAIL,
]

# Six depths, always reported together in one DWD row -- a small, fixed,
# always-jointly-measured set, so flat columns fit better than an array
# (unlike cloud layers, which vary in how many are populated). DWD only,
# already degC.
WEATHER_SOIL_TEMPERATURE_COLUMNS = [
    *_PLACE_HEAD,
    *_TIME_COLUMNS,
    ("soil_temperature_2cm_degc", "double"),
    ("soil_temperature_5cm_degc", "double"),
    ("soil_temperature_10cm_degc", "double"),
    ("soil_temperature_20cm_degc", "double"),
    ("soil_temperature_50cm_degc", "double"),
    ("soil_temperature_100cm_degc", "double"),
    *_QUALITY_COLUMNS,
    *_TAIL,
]

# Components a product reports together become columns of the same row
# (DWD's solar product gives all of longwave/diffuse/global/sunshine/zenith
# in one row). Native pair only on the three DWD radiation sums (J/cm2 over
# an interval -> mean W/m2, per interval_seconds); sunshine duration, zenith
# angle and UV index are never converted anywhere, so no native pair there.
WEATHER_SOLAR_RADIATION_COLUMNS = [
    *_PLACE_HEAD,
    *_TIME_COLUMNS,
    ("global_radiation_w_per_m2", "double"),
    ("global_radiation_native_value", "double"),
    ("global_radiation_native_unit", "string"),
    ("diffuse_radiation_w_per_m2", "double"),
    ("diffuse_radiation_native_value", "double"),
    ("diffuse_radiation_native_unit", "string"),
    ("longwave_downward_radiation_w_per_m2", "double"),
    ("longwave_downward_radiation_native_value", "double"),
    ("longwave_downward_radiation_native_unit", "string"),
    ("solar_zenith_angle_degrees", "double"),
    ("sunshine_duration_minutes", "double"),
    ("uv_index", "double"),
    *_QUALITY_COLUMNS,
    *_TAIL,
]

# name -> column list, one entry per Silver structure; not a fixed list by
# design -- these are the families the actual source parameters resolved
# into (see weather/_weather_specs.py for the per-source mapping).
WEATHER_FAMILY_COLUMNS = {
    "weather_temperature": WEATHER_TEMPERATURE_COLUMNS,
    "weather_humidity": WEATHER_HUMIDITY_COLUMNS,
    "weather_pressure": WEATHER_PRESSURE_COLUMNS,
    "weather_wind": WEATHER_WIND_COLUMNS,
    "weather_precipitation": WEATHER_PRECIPITATION_COLUMNS,
    "weather_cloud": WEATHER_CLOUD_COLUMNS,
    "weather_visibility": WEATHER_VISIBILITY_COLUMNS,
    "weather_present_weather": WEATHER_PRESENT_WEATHER_COLUMNS,
    "weather_soil_temperature": WEATHER_SOIL_TEMPERATURE_COLUMNS,
    "weather_solar_radiation": WEATHER_SOLAR_RADIATION_COLUMNS,
}
WEATHER_FAMILIES = list(WEATHER_FAMILY_COLUMNS)

WEATHER_DAILY_COLUMNS = [
    ("daily_key", "string"),
    ("location_key", "string"),
    ("source_location_id", "string"),
    ("local_date", "date"),
    ("variable", "string"),
    ("statistic", "string"),
    ("level", "string"),
    ("is_primary", "boolean"),
    ("date_native", "string"),
    ("time_basis", "string"),
    ("value_native", "double"),
    ("unit_native", "string"),
    ("value", "double"),
    ("unit", "string"),
    ("value_origin", "string"),
    ("derivation_rule", "string"),
    ("n_observations", "int"),
    ("measurement_basis", "string"),
    *_PROVENANCE_HEAD,
    ("source_column", "string"),
    *_PROVENANCE_TAIL,
]

WEATHER_LOCATION_COLUMNS = [
    ("location_key", "string"),
    ("source_location_id", "string"),
    ("location_role", "string"),
    ("name", "string"),
    ("latitude", "double"),
    ("longitude", "double"),
    ("elevation_m", "double"),
    ("continent", "string"),
    ("country_code", "string"),
    ("region", "string"),
    ("city", "string"),
    ("geography_basis", "string"),
    *_PROVENANCE_HEAD,
    *_PROVENANCE_TAIL,
]

WEATHER_LOCATION_VALIDITY_COLUMNS = [
    ("location_key", "string"),
    ("source_location_id", "string"),
    ("valid_from", "date"),
    ("valid_to", "date"),
    ("name", "string"),
    ("latitude", "double"),
    ("longitude", "double"),
    ("elevation_m", "double"),
    *_PROVENANCE_HEAD,
    *_PROVENANCE_TAIL,
]

# Continent by ISO 3166-1 alpha-2 (UN geoscheme; Russia -> Europe, Turkey and
# Cyprus -> Asia).
_CONTINENT_CODES = {
    "AF": (
        "DZ AO BJ BW BF BI CV CM CF TD KM CG CD CI DJ EG GQ ER SZ ET GA GM GH GN GW "
        "KE LS LR LY MG MW ML MR MU YT MA MZ NA NE NG RE RW SH ST SN SC SL SO ZA SS "
        "SD TZ TG TN UG EH ZM ZW"
    ),
    "AS": (
        "AF AM AZ BH BD BT BN KH CN CY GE HK IN ID IR IQ IL JP JO KZ KW KG LA LB MO "
        "MY MV MN MM NP KP OM PK PS PH QA SA SG KR LK SY TW TJ TH TL TR TM AE UZ VN "
        "YE"
    ),
    "EU": (
        "AL AD AT BY BE BA BG HR CZ DK EE FO FI FR DE GI GR GG VA HU IS IE IM IT JE "
        "XK LV LI LT LU MT MD MC ME NL MK NO PL PT RO RU SM RS SK SI ES SJ SE CH UA "
        "GB AX"
    ),
    "NA": (
        "AG BS BB BZ CA CR CU DM DO SV GD GT HT HN JM MX NI PA KN LC VC TT US AI AW "
        "BM BQ VG KY CW GL GP MQ MS PR BL MF PM SX TC VI"
    ),
    "SA": "AR BO BR CL CO EC FK GF GY PY PE SR UY VE",
    "OC": (
        "AU FJ KI MH FM NR NZ PW PG WS SB TO TV VU CK PF GU NC NU NF MP PN TK WF AS"
    ),
    "AN": "AQ",
}
CONTINENT_BY_ISO2 = {
    iso: cont for cont, codes in _CONTINENT_CODES.items() for iso in codes.split()
}

# COMMAND ----------

# DBTITLE 1,Helper -- semantic table name and session time zone


def semantic_table(name: str) -> str:
    return f"{CATALOG}.{SEMANTIC_SCHEMA}.{name}"


def ensure_utc_session() -> None:
    """Timestamps are handled as UTC wall-clock; pin the session zone once."""
    spark.conf.set("spark.sql.session.timeZone", "UTC")


# COMMAND ----------

# DBTITLE 1,Helper -- time columns


def add_project_time(df, utc_col: str = "observation_ts_utc"):
    """Project-time timestamp (Europe/Berlin wall clock) and local date from a
    UTC instant. NULL where the source has no instant."""
    local = F.from_utc_timestamp(F.col(utc_col), PROJECT_TZ)
    return df.withColumn("observation_ts_project", local).withColumn(
        "local_date", F.to_date(local)
    )


def local_time_to_utc(local_ts_col: str, offset_hours_col: str):
    """UTC instant from a local wall-clock timestamp and its GMT offset (hours)."""
    return (
        F.col(local_ts_col).cast("timestamp").cast("long")
        - (F.col(offset_hours_col) * 3600).cast("long")
    ).cast("timestamp")


# COMMAND ----------

# DBTITLE 1,Helper -- place keys and continent lookup


def location_key(source_system: str, source_location_id):
    """Deterministic place key; `source_location_id` is a column name or Column."""
    return sha_key(F.lit(source_system), source_location_id)


def continent_of(country_code_col: str):
    m = F.create_map([F.lit(x) for kv in CONTINENT_BY_ISO2.items() for x in kv])
    return m[F.upper(F.col(country_code_col))]


# COMMAND ----------

# DBTITLE 1,Helper -- keep rows where at least one named column is populated


def any_present(df, cols: list):
    """Filter to rows where at least one of `cols` is non-NULL, so a source
    row that populates none of a family's fields doesn't produce a junk row."""
    cond = F.lit(False)
    for c in cols:
        cond = cond | F.col(c).isNotNull()
    return df.filter(cond)


# COMMAND ----------

# DBTITLE 1,Helper -- provenance and column conformance


def add_semantic_provenance(df, source_system: str, source_dataset: str, rid: str):
    return (
        df.withColumn("source_system", F.lit(source_system))
        .withColumn("source_dataset", F.lit(source_dataset))
        .withColumn("_silver_loaded_at", F.current_timestamp())
        .withColumn("_silver_run_id", F.lit(rid))
    )


def conform(df, columns: list):
    """Select every structure column in order, casting; absent ones become NULL,
    so rows from different sources union cleanly."""
    return df.select(
        *[
            (F.col(n) if n in df.columns else F.lit(None)).cast(t).alias(n)
            for n, t in columns
        ]
    )


# COMMAND ----------

# DBTITLE 1,Helper -- write (full overwrite or replace one source's rows)


def write_semantic(
    df, table: str, *, source: str, component: str, rid: str, replace_where=None
) -> int | None:
    """Overwrite the table, or with `replace_where` (a SQL predicate on the
    table's own columns) replace only those rows and leave the rest. A schema
    change to an existing table needs the table dropped first."""
    started = now_utc()
    full = semantic_table(table)
    exists = spark.catalog.tableExists(full)
    if exists:
        v = spark.sql(f"DESCRIBE HISTORY {full} LIMIT 1").first()["version"]
        print(f"ROLLBACK IF NEEDED: RESTORE TABLE {full} TO VERSION AS OF {v}")
    writer = df.write.format("delta").mode("overwrite")
    if replace_where and exists:
        writer = writer.option("replaceWhere", replace_where)
    else:
        writer = writer.option("overwriteSchema", "true")
    writer.saveAsTable(full)
    n = _delta_rows_written(full)
    watermark(component, source, n, "COMPLETE", rid, started)
    audit(component, source, "rows_written", n, status="PASS", rid=rid)
    print(f"OK  {full}: {n if n is not None else '?'} rows written")
    return n


# COMMAND ----------

# DBTITLE 1,Configuration -- inspection additions
# Row identifiers and native strings: a distinct count is not diagnostic.
SEMANTIC_HIGH_CARDINALITY = {
    "observation_key",
    "daily_key",
    "location_key",
    "source_location_id",
    "observation_ts_native",
    "date_native",
}
FINDINGS_SOURCE = "weather"

# COMMAND ----------

# DBTITLE 1,Helper -- structure-specific checks for the findings export


def structure_extra_checks(df) -> dict:
    """`extra_checks` for `inspect_table`: row counts by the semantic columns the
    frame carries, plus the time span. One grouped pass and one aggregate."""
    HIGH_CARDINALITY_SKIP.update(SEMANTIC_HIGH_CARDINALITY)
    group_cols = [
        c
        for c in (
            "source_system",
            "location_role",
            "continent",
            "country_code",
            "source_dataset",
            "statistic",
            "time_basis",
            "measurement_basis",
        )
        if c in df.columns
    ]
    counts = df.groupBy(*group_cols).count().collect()
    out = {}
    for c in group_cols:
        agg = {}
        for r in counts:
            agg[r[c]] = agg.get(r[c], 0) + r["count"]
        out[f"rows_by_{c}"] = dict(sorted(agg.items(), key=lambda kv: str(kv[0])))
    span_col = next(
        (c for c in ("observation_ts_utc", "local_date") if c in df.columns), None
    )
    if span_col:
        span = df.agg(F.min(span_col).alias("lo"), F.max(span_col).alias("hi")).first()
        out[f"{span_col}_span"] = f"{span['lo']} .. {span['hi']}"
    return out


# COMMAND ----------

# DBTITLE 1,Helper -- DataFrame to markdown table


def rows_to_markdown(df, limit: int = 200) -> str:
    rows = df.limit(limit).collect()
    header = "| " + " | ".join(df.columns) + " |"
    rule = "|" + "|".join("---" for _ in df.columns) + "|"
    body = [
        "| " + " | ".join("" if v is None else str(v) for v in r) + " |" for r in rows
    ]
    return "\n".join([header, rule, *body])
