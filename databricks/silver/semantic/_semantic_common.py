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

# Provenance columns shared by every semantic structure; `source_column` only
# where a row comes from one named source column.
_PROVENANCE_HEAD = [("source_system", "string"), ("source_dataset", "string")]
_PROVENANCE_COLUMN = [("source_column", "string")]
_PROVENANCE_TAIL = [
    ("source_record_id", "string"),
    ("_silver_loaded_at", "timestamp"),
    ("_silver_run_id", "string"),
]

# One weather parameter/measurement family = one Silver structure; every
# genuinely compatible source contributes rows to it. Not a fixed list by
# design -- see the per-family assignment in weather/_weather_specs.py.
WEATHER_FAMILIES = [
    "weather_temperature",
    "weather_soil_temperature",
    "weather_humidity",
    "weather_pressure",
    "weather_wind",
    "weather_precipitation",
    "weather_cloud",
    "weather_visibility",
    "weather_present_weather",
    "weather_solar_radiation",
]

# Column shape shared by every table in WEATHER_FAMILIES.
WEATHER_MEASUREMENT_COLUMNS = [
    ("observation_key", "string"),
    ("location_key", "string"),
    ("source_location_id", "string"),
    ("variable", "string"),
    ("statistic", "string"),
    ("level", "string"),
    ("is_primary", "boolean"),
    ("observation_ts_native", "string"),
    ("time_basis", "string"),
    ("utc_offset_hours", "double"),
    ("observation_ts_utc", "timestamp"),
    ("observation_ts_project", "timestamp"),
    ("local_date", "date"),
    ("interval_seconds", "int"),
    ("value_native", "double"),
    ("unit_native", "string"),
    ("value", "double"),
    ("unit", "string"),
    ("value_code", "string"),
    ("value_text", "string"),
    ("value_origin", "string"),
    ("derivation_rule", "string"),
    ("missing_reason", "string"),
    ("quality_code", "string"),
    ("quality_label", "string"),
    ("quality_flag", "string"),
    ("observation_method", "string"),
    ("measurement_basis", "string"),
    *_PROVENANCE_HEAD,
    *_PROVENANCE_COLUMN,
    *_PROVENANCE_TAIL,
]

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
    *_PROVENANCE_COLUMN,
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

# DBTITLE 1,Helper -- wide source columns to standardised long rows

_SPEC_SCHEMA = (
    "source_column string, variable string, statistic string, level string, "
    "unit_native string, unit string, factor double, offset double, rule string, "
    "is_primary boolean, interval_seconds int, categorical boolean, "
    "null_value double, null_reason string, family string"
)


def spec(
    source_column,
    variable,
    statistic,
    unit_native,
    *,
    family,
    unit=None,
    factor=1.0,
    offset=0.0,
    level=None,
    interval_seconds=None,
    primary=True,
    rule=None,
    categorical=False,
    null_value=None,
    null_reason=None,
    text_col=None,
    method_col=None,
):
    """One source column -> one variable, routed to one `family` (the shared
    weather structure in WEATHER_FAMILIES it belongs in by meaning, regardless
    of which source it comes from). `factor`/`offset` give the standardised
    value (value_native * factor + offset); anything other than 1/0 marks the
    row `converted` and needs a `rule`."""
    return {
        "source_column": source_column,
        "variable": variable,
        "statistic": statistic,
        "level": level,
        "unit_native": unit_native,
        "unit": unit or unit_native,
        "factor": float(factor),
        "offset": float(offset),
        "rule": rule,
        "is_primary": primary,
        "interval_seconds": interval_seconds,
        "categorical": categorical,
        "null_value": null_value,
        "null_reason": null_reason,
        "text_col": text_col,
        "method_col": method_col,
        "family": family,
    }


def long_from_spec(df, specs: list, keep_cols: list):
    """Single-pass stack of the spec'd columns into long rows with native and
    standardised values. Missing (NULL) source values produce no row; a special
    value (`null_value`) keeps the row with a NULL standardised value and a
    `missing_reason`. Text and method columns named in the specs must exist in
    `df`."""
    cols = [s["source_column"] for s in specs]
    aux = sorted(
        {s[k] for s in specs for k in ("text_col", "method_col") if s[k]}
        - set(keep_cols)
    )
    pairs = ", ".join(f"'{c}', cast(`{c}` as double)" for c in cols)
    long = df.select(
        *keep_cols,
        *aux,
        F.expr(f"stack({len(cols)}, {pairs}) as (source_column, value_raw)"),
    ).filter(F.col("value_raw").isNotNull())

    def per_column(key):
        expr = F.lit(None).cast("string")
        for s in reversed(specs):
            if s[key]:
                expr = F.when(
                    F.col("source_column") == s["source_column"], F.col(s[key])
                ).otherwise(expr)
        return expr

    long = (
        long.withColumn("_text_raw", per_column("text_col"))
        .withColumn("observation_method", per_column("method_col"))
        .drop(*aux)
    )
    spec_df = spark.createDataFrame(
        [tuple(s[f.split()[0]] for f in _SPEC_SCHEMA.split(", ")) for s in specs],
        _SPEC_SCHEMA,
    )
    long = long.join(F.broadcast(spec_df), "source_column", "left")
    special = F.col("null_value").isNotNull() & (
        F.col("value_raw") == F.col("null_value")
    )
    converted = (F.col("factor") != 1.0) | (F.col("offset") != 0.0)
    return (
        long.withColumn("value_native", F.col("value_raw"))
        .withColumn("missing_reason", F.when(special, F.col("null_reason")))
        .withColumn(
            "value",
            F.when(
                F.col("categorical") | special, F.lit(None).cast("double")
            ).otherwise(F.col("value_raw") * F.col("factor") + F.col("offset")),
        )
        .withColumn(
            "value_code",
            F.when(
                F.col("categorical") & ~special,
                F.col("value_raw").cast("long").cast("string"),
            ),
        )
        .withColumn("value_text", F.when(~special, F.col("_text_raw")))
        .withColumn(
            "unit", F.when(F.col("categorical"), F.lit(None)).otherwise(F.col("unit"))
        )
        .withColumn(
            "value_origin", F.when(converted, "converted").otherwise("observed")
        )
        .withColumn("derivation_rule", F.when(converted, F.col("rule")))
        .drop(
            "value_raw",
            "_text_raw",
            "factor",
            "offset",
            "rule",
            "categorical",
            "null_value",
            "null_reason",
        )
    )


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

# DBTITLE 1,Helper -- split a long frame across its family structures


def write_semantic_families(
    df, families: list, *, source: str, component: str, rid: str, replace_where_fn
) -> dict:
    """Write one write_semantic() call per family present in `df`'s `family`
    column, each to its own WEATHER_FAMILIES table. `families` is the closed
    set of families the caller's specs can produce (known from the spec list,
    not discovered by scanning `df`). `replace_where_fn(family)` builds that
    family table's replaceWhere predicate."""
    return {
        fam: write_semantic(
            conform(df.filter(F.col("family") == fam), WEATHER_MEASUREMENT_COLUMNS),
            fam,
            source=source,
            component=component,
            rid=rid,
            replace_where=replace_where_fn(fam),
        )
        for fam in families
    }


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
            "variable",
            "level",
            "statistic",
            "unit",
            "value_origin",
            "missing_reason",
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
