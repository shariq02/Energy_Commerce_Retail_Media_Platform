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
# MAGIC **Purpose:** building blocks for the cross-source Silver structures --
# MAGIC time columns, place keys, column conformance, per-source replace writes.
# MAGIC Pulled in with `%run ../_semantic_common` after `_silver_common`.

# COMMAND ----------

# DBTITLE 1,Configuration constants
SEMANTIC_SCHEMA = "energy_silver"
PROJECT_TZ = "Europe/Berlin"

# Provenance columns shared by every semantic structure.
_PROVENANCE_HEAD = [("source_system", "string"), ("source_dataset", "string")]
_PROVENANCE_TAIL = [
    ("source_record_id", "string"),
    ("_silver_loaded_at", "timestamp"),
    ("_silver_run_id", "string"),
]

# Place/time scaffolding shared by every family; measurement columns below
# are designed per family, not shared.
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

# One family = one structure, designed per family, not a shared shape.
# Grain: one row per family per (source_system, location, instant).

# One alternate estimate of a field's primary value, with its own native
# form, source and quality code.
_ALT_READING = (
    "array<struct<value:double,native_value:double,native_unit:string,"
    "source_dataset:string,quality_code:string>>"
)

WEATHER_TEMPERATURE_COLUMNS = [
    *_PLACE_HEAD,
    *_TIME_COLUMNS,
    ("air_temperature_degc", "double"),
    ("air_temperature_alt", _ALT_READING),
    ("dew_point_temperature_degc", "double"),
    ("dew_point_temperature_alt", _ALT_READING),
    ("wet_bulb_temperature_degc", "double"),
    *_QUALITY_COLUMNS,
    *_TAIL,
]

WEATHER_HUMIDITY_COLUMNS = [
    *_PLACE_HEAD,
    *_TIME_COLUMNS,
    ("relative_humidity_percent", "double"),
    ("relative_humidity_alt", _ALT_READING),
    ("absolute_humidity_g_per_m3", "double"),
    ("vapour_pressure_hpa", "double"),
    *_QUALITY_COLUMNS,
    *_TAIL,
]

# Native value/unit populated only where a source converts (AccuWeather Pa).
WEATHER_PRESSURE_COLUMNS = [
    *_PLACE_HEAD,
    *_TIME_COLUMNS,
    ("pressure_station_hpa", "double"),
    ("pressure_station_native_value", "double"),
    ("pressure_station_native_unit", "string"),
    ("pressure_station_alt", _ALT_READING),
    ("pressure_sea_level_hpa", "double"),
    ("pressure_sea_level_native_value", "double"),
    ("pressure_sea_level_native_unit", "string"),
    *_QUALITY_COLUMNS,
    *_TAIL,
]

# One `readings` array entry per statistic present (mean/instant/max/
# unspecified), not one row per product. Already m/s and degrees.
WEATHER_WIND_COLUMNS = [
    *_PLACE_HEAD,
    *_TIME_COLUMNS,
    (
        "readings",
        (
            "array<struct<statistic:string,wind_speed_m_per_s:double,"
            "wind_direction_degrees:double,wind_direction_variable:boolean,"
            "wind_gust_m_per_s:double,source_dataset:string>>"
        ),
    ),
    *_QUALITY_COLUMNS,
    *_TAIL,
]

# Amount, occurred-flag and form describe one precipitation event.
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

# Layers: array of structs (0-4, variably populated), not flat columns.
WEATHER_CLOUD_COLUMNS = [
    *_PLACE_HEAD,
    *_TIME_COLUMNS,
    ("cloud_cover_total_percent", "double"),
    ("cloud_cover_total_native_value", "double"),
    ("cloud_cover_total_native_unit", "string"),
    ("cloud_cover_total_alt", _ALT_READING),
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

# Separate from cloud: different physical basis (extinction distance).
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

# Purely categorical -- no numeric value/unit columns. DWD only.
WEATHER_PRESENT_WEATHER_COLUMNS = [
    *_PLACE_HEAD,
    *_TIME_COLUMNS,
    ("present_weather_code", "string"),
    ("present_weather_text", "string"),
    *_QUALITY_COLUMNS,
    *_TAIL,
]

# Six depths, always reported together -- a fixed set, so flat columns
# (unlike cloud's variably-populated layers). DWD only.
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

# dwd_solar's one row split by meaning into five families below: flux,
# longwave flux, duration, geometry and index.

# Shortwave flux only (global + diffuse). Native pair on DWD's two sums.
WEATHER_SOLAR_RADIATION_COLUMNS = [
    *_PLACE_HEAD,
    *_TIME_COLUMNS,
    ("global_radiation_w_per_m2", "double"),
    ("global_radiation_native_value", "double"),
    ("global_radiation_native_unit", "string"),
    ("diffuse_radiation_w_per_m2", "double"),
    ("diffuse_radiation_native_value", "double"),
    ("diffuse_radiation_native_unit", "string"),
    *_QUALITY_COLUMNS,
    *_TAIL,
]

# Atmospheric infrared, not sunlight -- a different physical process. DWD only.
WEATHER_LONGWAVE_RADIATION_COLUMNS = [
    *_PLACE_HEAD,
    *_TIME_COLUMNS,
    ("longwave_downward_radiation_w_per_m2", "double"),
    ("longwave_downward_radiation_native_value", "double"),
    ("longwave_downward_radiation_native_unit", "string"),
    *_QUALITY_COLUMNS,
    *_TAIL,
]

# A duration, not a flux. dwd_sun (hourly) and dwd_solar (10-min) are
# different grids, so genuinely separate instants, not fragmentation.
WEATHER_SUNSHINE_DURATION_COLUMNS = [
    *_PLACE_HEAD,
    *_TIME_COLUMNS,
    ("sunshine_duration_minutes", "double"),
    *_QUALITY_COLUMNS,
    *_TAIL,
]

# An astronomical position, not an atmospheric measurement. DWD only.
WEATHER_SOLAR_GEOMETRY_COLUMNS = [
    *_PLACE_HEAD,
    *_TIME_COLUMNS,
    ("solar_zenith_angle_degrees", "double"),
    *_QUALITY_COLUMNS,
    *_TAIL,
]

# An index, not a physical flux measurement -- AccuWeather only.
WEATHER_UV_INDEX_COLUMNS = [
    *_PLACE_HEAD,
    *_TIME_COLUMNS,
    ("uv_index", "double"),
    *_QUALITY_COLUMNS,
    *_TAIL,
]

# name -> column list, one entry per Silver structure.
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
    "weather_longwave_radiation": WEATHER_LONGWAVE_RADIATION_COLUMNS,
    "weather_sunshine_duration": WEATHER_SUNSHINE_DURATION_COLUMNS,
    "weather_solar_geometry": WEATHER_SOLAR_GEOMETRY_COLUMNS,
    "weather_uv_index": WEATHER_UV_INDEX_COLUMNS,
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


def _schema_signature(schema) -> set:
    """(name, type) pairs, ignoring field order and nullability -- the things
    that make a replaceWhere genuinely incompatible with the existing table."""
    return {(f.name, f.dataType.simpleString()) for f in schema.fields}


def write_semantic(
    df, table: str, *, source: str, component: str, rid: str, replace_where=None
) -> int | None:
    """Overwrite the table, or with `replace_where` replace only those rows.
    On a schema change, rows `replace_where` would leave untouched are
    conformed onto the new schema and committed with the incoming rows in
    one atomic write (Delta forbids combining `replaceWhere` with a schema
    change)."""
    started = now_utc()
    full = semantic_table(table)
    exists = spark.catalog.tableExists(full)
    if exists:
        v = spark.sql(f"DESCRIBE HISTORY {full} LIMIT 1").first()["version"]
        print(f"ROLLBACK IF NEEDED: RESTORE TABLE {full} TO VERSION AS OF {v}")

    if exists and replace_where:
        existing_schema = spark.table(full).schema
        if _schema_signature(existing_schema) != _schema_signature(df.schema):
            target_columns = [
                (f.name, f.dataType.simpleString()) for f in df.schema.fields
            ]
            kept = conform(
                spark.table(full).filter(f"NOT ({replace_where})"), target_columns
            )
            df = kept.unionByName(df)
            replace_where = None
            print(
                f"SCHEMA MIGRATION  {full}: pre-existing rows carried forward onto the new schema"
            )

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


def dict_to_markdown_row(stats: dict) -> str:
    """A single-row markdown table from a plain dict -- for a small numeric
    summary that doesn't need a Spark round-trip."""
    header = "| " + " | ".join(stats.keys()) + " |"
    rule = "|" + "|".join("---" for _ in stats) + "|"
    row = "| " + " | ".join(str(v) for v in stats.values()) + " |"
    return f"{header}\n{rule}\n{row}"


# COMMAND ----------

# DBTITLE 1,Helper -- dedup/conflict reconciliation proof (Genomics-style rule)


def reconciliation_stats(
    source_df, kept_df, quarantine_df, *, raw_bronze_df=None
) -> dict:
    """Proof of the collapse-identical / quarantine-conflicting rule: Bronze
    -> exact duplicates collapsed -> conflicts quarantined -> kept. Pass
    `raw_bronze_df` when `source_df` (resolve_conflicts' input) is already
    filtered, so `bronze_rows` reflects the true Bronze count."""
    dedup_input_rows = source_df.count()
    kept_rows = kept_df.count()
    quarantined_rows = quarantine_df.count()
    return {
        "bronze_rows": raw_bronze_df.count()
        if raw_bronze_df is not None
        else dedup_input_rows,
        "dedup_input_rows": dedup_input_rows,
        "exact_duplicates_collapsed": dedup_input_rows - kept_rows - quarantined_rows,
        "conflicts_quarantined": quarantined_rows,
        "kept_rows": kept_rows,
    }
