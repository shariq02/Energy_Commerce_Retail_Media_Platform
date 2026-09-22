# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER -- WEATHER VALIDATION
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** read-only checks of the weather structures: key and grain
# MAGIC uniqueness, place coverage, time-basis consistency, per-family value
# MAGIC ranges, and regression against the existing source-scoped Silver tables.

# COMMAND ----------

# DBTITLE 1,Shared library
# MAGIC %run ../../_silver_common

# COMMAND ----------

# DBTITLE 1,Inspection library
# MAGIC %run ../../_silver_inspect

# COMMAND ----------

# DBTITLE 1,Semantic structures library
# MAGIC %run ../_semantic_common

# COMMAND ----------

# DBTITLE 1,Weather specifications
# MAGIC %run ./_weather_specs

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Configuration
DAILY = semantic_table("weather_daily")
LOC = semantic_table("weather_location")
BASELINE_SCHEMA = "energy_silver"
COMPONENT = "silver/semantic/weather/07_weather_validation"

# Columns every family table carries identically (the shared scaffolding);
# cross-family checks run on this projection, unioned across families.
COMMON_COLS = [
    "observation_key",
    "location_key",
    "source_location_id",
    "observation_ts_native",
    "time_basis",
    "utc_offset_hours",
    "observation_ts_utc",
    "observation_ts_project",
    "local_date",
    "interval_seconds",
    "measurement_basis",
    "source_system",
    "source_dataset",
    "source_record_id",
]

# Which family(ies) cover each DWD raw table's contribution, for the
# key-coverage regression against the old per-table baseline tables.
DWD_TABLE_TO_FAMILIES = {
    "dwd_air_temperature": ["weather_temperature", "weather_humidity"],
    "dwd_moisture": ["weather_temperature", "weather_humidity", "weather_pressure"],
    "dwd_dew_point": ["weather_temperature"],
    "dwd_pressure": ["weather_pressure"],
    "dwd_precipitation": ["weather_precipitation"],
    "dwd_sun": ["weather_sunshine_duration"],
    "dwd_wind": ["weather_wind"],
    "dwd_wind_synop": ["weather_wind"],
    "dwd_extreme_wind": ["weather_wind"],
    "dwd_visibility": ["weather_visibility"],
    "dwd_cloudiness": ["weather_cloud"],
    "dwd_cloud_type": ["weather_cloud"],
    "dwd_weather_phenomena": ["weather_present_weather"],
    "dwd_soil_temperature": ["weather_soil_temperature"],
    "dwd_solar": [
        "weather_solar_radiation",
        "weather_longwave_radiation",
        "weather_solar_geometry",
        "weather_sunshine_duration",
    ],
}

# Check results and result tables, exported to the findings file at the end.
CHECK_RESULTS = []
FINDINGS_BLOCKS = []

# COMMAND ----------

# DBTITLE 1,Helper -- print a check result


def report(name: str, ok: bool, detail: str = "", status: str | None = None) -> None:
    status = status or ("PASS" if ok else "FAIL")
    CHECK_RESULTS.append((name, status, detail))
    print(f"{status}  {name}  {detail}")


def keep(heading: str, df) -> None:
    """Show a result table and queue it for the findings export."""
    display(df)
    FINDINGS_BLOCKS.append((heading, rows_to_markdown(df)))


# COMMAND ----------

# DBTITLE 1,Read Silver -- every family table written so far
WRITTEN_FAMILIES = [
    f for f in WEATHER_FAMILIES if spark.catalog.tableExists(semantic_table(f))
]
for _f in WEATHER_FAMILIES:
    if _f not in WRITTEN_FAMILIES:
        report(f"{_f} table exists", True, "not written yet", status="SKIP")
FAMILY_FRAMES = {f: spark.table(semantic_table(f)) for f in WRITTEN_FAMILIES}

# COMMAND ----------

# DBTITLE 1,Read Silver -- common scaffolding, unioned across every family table
_common_frames = [
    df.select(*COMMON_COLS).withColumn("family", F.lit(fam))
    for fam, df in FAMILY_FRAMES.items()
]
obs_common = _common_frames[0]
for _cdf in _common_frames[1:]:
    obs_common = obs_common.unionByName(_cdf)

# COMMAND ----------

# DBTITLE 1,Check -- rows per family structure
keep(
    "rows per family structure",
    obs_common.groupBy("family")
    .agg(
        F.count("*").alias("rows"),
        F.min("observation_ts_utc").alias("first_ts"),
        F.max("observation_ts_utc").alias("last_ts"),
    )
    .orderBy("family"),
)

# COMMAND ----------

# DBTITLE 1,Check -- rows per source dataset
keep(
    "rows per source dataset",
    obs_common.groupBy("source_system", "source_dataset", "measurement_basis")
    .agg(
        F.count("*").alias("rows"),
        F.countDistinct("family").alias("families"),
        F.min("observation_ts_utc").alias("first_ts"),
        F.max("observation_ts_utc").alias("last_ts"),
    )
    .orderBy("source_system", "source_dataset"),
)

# COMMAND ----------

# DBTITLE 1,Check -- rows by time_basis (DWD legacy-local regime visible, not hidden as 'utc')
keep(
    "rows by time_basis",
    obs_common.groupBy("source_system", "time_basis")
    .agg(F.count("*").alias("rows"))
    .orderBy("source_system", "time_basis"),
)

# COMMAND ----------

# DBTITLE 1,Check -- observation_key is unique within each family table
for _fam, _fdf in FAMILY_FRAMES.items():
    _dup = _fdf.groupBy("observation_key").count().filter("count > 1").count()
    report(f"{_fam} observation_key unique", _dup == 0, f"duplicate keys: {_dup}")

# COMMAND ----------

# DBTITLE 1,Check -- one row per (source_system, location, instant) per family (grain, not product)
# A violation means a product fragmented the grain instead of landing in
# an _alt/readings array.
for _fam, _fdf in FAMILY_FRAMES.items():
    _dup_grain = (
        _fdf.groupBy("source_system", "location_key", "observation_ts_utc")
        .count()
        .filter("count > 1")
        .count()
    )
    report(
        f"{_fam} one row per (source, location, instant)",
        _dup_grain == 0,
        f"groups with >1 row: {_dup_grain}",
    )

# COMMAND ----------

# DBTITLE 1,Check -- every observation has a location
orphans = (
    obs_common.select("location_key")
    .distinct()
    .join(spark.table(LOC).select("location_key"), "location_key", "left_anti")
    .count()
)
report(
    "observation locations exist in weather_location",
    orphans == 0,
    f"orphans: {orphans}",
)

# COMMAND ----------

# DBTITLE 1,Check -- project time offset from UTC, by time_basis (Europe/Berlin)
offsets = (
    obs_common.filter(
        F.col("observation_ts_utc").isNotNull() & (F.col("source_system") == "dwd")
    )
    .withColumn(
        "offset_h",
        (
            F.col("observation_ts_project").cast("long")
            - F.col("observation_ts_utc").cast("long")
        )
        / 3600,
    )
    .groupBy("time_basis", "offset_h")
    .count()
)
keep("project time offset from UTC by time_basis (DWD, hours)", offsets)

# COMMAND ----------

# DBTITLE 1,Check -- value ranges per family, by measurement column and source
for _fam, _fdf in FAMILY_FRAMES.items():
    _numeric_cols = [
        name
        for name, dtype in _fdf.dtypes
        if dtype == "double" and name != "utc_offset_hours"
    ]
    if not _numeric_cols:
        continue
    _stack = ", ".join(f"'{c}', `{c}`" for c in _numeric_cols)
    _ranges = (
        _fdf.select(
            "source_system",
            F.expr(f"stack({len(_numeric_cols)}, {_stack}) as (field, value)"),
        )
        .filter(F.col("value").isNotNull())
        .groupBy("field", "source_system")
        .agg(
            F.count("*").alias("rows"),
            F.min("value").alias("min"),
            F.percentile_approx("value", 0.5).alias("median"),
            F.max("value").alias("max"),
        )
        .orderBy("field", "source_system")
    )
    keep(f"value ranges -- {_fam}", _ranges)

# COMMAND ----------

# DBTITLE 1,Check -- weather_wind readings (array, not a scalar column -- own check)
if "weather_wind" in FAMILY_FRAMES:
    _wind_readings = (
        FAMILY_FRAMES["weather_wind"]
        .select("source_system", F.explode("readings").alias("r"))
        .select(
            "source_system",
            F.col("r.statistic").alias("statistic"),
            F.col("r.wind_speed_m_per_s").alias("wind_speed_m_per_s"),
            F.col("r.wind_gust_m_per_s").alias("wind_gust_m_per_s"),
        )
    )
    keep(
        "weather_wind readings by statistic and source",
        _wind_readings.groupBy("source_system", "statistic").agg(
            F.count("*").alias("rows"),
            F.min("wind_speed_m_per_s").alias("min_speed"),
            F.max("wind_speed_m_per_s").alias("max_speed"),
            F.min("wind_gust_m_per_s").alias("min_gust"),
            F.max("wind_gust_m_per_s").alias("max_gust"),
        ),
    )

# COMMAND ----------

# DBTITLE 1,Regression -- key coverage per DWD table against the existing Silver tables
# Retire this cell and the two below with the old source-scoped Silver
# tables. Missing baseline table -> SKIP, not FAIL.
for _ds, _families in DWD_TABLE_TO_FAMILIES.items():
    _base_name = f"{CATALOG}.{BASELINE_SCHEMA}.{_ds}"
    if not spark.catalog.tableExists(_base_name):
        report(f"{_ds} key coverage", True, "baseline table absent", status="SKIP")
        continue
    _present_families = [f for f in _families if f in FAMILY_FRAMES]
    if not _present_families:
        report(f"{_ds} key coverage", True, "not written yet", status="SKIP")
        continue
    _new_frames = [
        FAMILY_FRAMES[f]
        .filter(F.col("source_system") == "dwd")
        .select("source_location_id", "observation_ts_utc")
        .distinct()
        for f in _present_families
    ]
    _new = _new_frames[0]
    for _extra in _new_frames[1:]:
        _new = _new.union(_extra)
    _new = _new.distinct()
    _base = (
        spark.table(_base_name)
        .select(
            F.col("STATIONS_ID").alias("source_location_id"),
            F.col("observation_ts").alias("observation_ts_utc"),
        )
        .distinct()
    )
    _keys = ["source_location_id", "observation_ts_utc"]
    _only_new = _new.join(_base, _keys, "left_anti").count()
    _only_base = _base.join(_new, _keys, "left_anti").count()
    report(
        f"{_ds} key coverage (via {', '.join(_present_families)})",
        _only_new == 0,
        f"only_new={_only_new} baseline_only={_only_base} (all-null baseline rows)",
    )

# COMMAND ----------

# DBTITLE 1,Regression -- DWD air temperature values against the existing Silver table
_base_name = f"{CATALOG}.{BASELINE_SCHEMA}.dwd_air_temperature"
if spark.catalog.tableExists(_base_name) and "weather_temperature" in FAMILY_FRAMES:
    _base_dwd = (
        spark.table(_base_name)
        .select(
            F.col("STATIONS_ID").alias("source_location_id"),
            "observation_ts",
            F.col("air_temperature_2m").alias("base_value"),
        )
        .filter("base_value is not null")
    )
    _new_dwd = (
        FAMILY_FRAMES["weather_temperature"]
        .filter(F.col("source_system") == "dwd")
        .select(
            "source_location_id",
            F.col("observation_ts_utc").alias("observation_ts"),
            F.col("air_temperature_degc").alias("new_value"),
        )
    )
    _res = (
        _base_dwd.join(_new_dwd, ["source_location_id", "observation_ts"], "full_outer")
        .agg(
            F.count("*").alias("keys"),
            F.sum(F.col("base_value").isNull().cast("int")).alias("only_new"),
            F.sum(F.col("new_value").isNull().cast("int")).alias("only_baseline"),
            F.sum(
                (
                    F.col("base_value").isNotNull()
                    & F.col("new_value").isNotNull()
                    & (F.abs(F.col("base_value") - F.col("new_value")) > 1e-9)
                ).cast("int")
            ).alias("value_mismatch"),
        )
        .first()
    )
    report(
        "dwd air temperature values match the existing Silver table",
        _res["only_new"] == 0
        and _res["only_baseline"] == 0
        and _res["value_mismatch"] == 0,
        str(_res.asDict()),
    )
else:
    report("dwd air temperature values", True, "baseline table absent", status="SKIP")

# COMMAND ----------

# DBTITLE 1,Regression -- Honda weather against the existing Silver table
_base_name = f"{CATALOG}.{BASELINE_SCHEMA}.honda_weather"
if spark.catalog.tableExists(_base_name) and "weather_temperature" in FAMILY_FRAMES:
    _base_h = (
        spark.table(_base_name)
        .select(
            "frequency",
            "datetime_utc",
            F.col("air_temperature_2m").alias("base_value"),
        )
        .filter("base_value is not null")
    )
    _new_h = (
        FAMILY_FRAMES["weather_temperature"]
        .filter(F.col("source_dataset") == "honda_iot_weather")
        .select(
            F.col("observation_ts_utc").alias("datetime_utc"),
            F.when(F.col("interval_seconds") == 60, "1min")
            .when(F.col("interval_seconds") == 900, "15min")
            .otherwise("1h")
            .alias("frequency"),
            F.col("air_temperature_degc").alias("new_value"),
        )
    )
    _hres = (
        _base_h.join(_new_h, ["frequency", "datetime_utc"], "full_outer")
        .agg(
            F.count("*").alias("keys"),
            F.sum(F.col("base_value").isNull().cast("int")).alias("only_new"),
            F.sum(F.col("new_value").isNull().cast("int")).alias("only_baseline"),
            F.sum(
                (
                    F.col("base_value").isNotNull()
                    & F.col("new_value").isNotNull()
                    & (F.abs(F.col("base_value") - F.col("new_value")) > 1e-9)
                ).cast("int")
            ).alias("value_mismatch"),
        )
        .first()
    )
    report(
        "honda air temperature matches the existing Silver table",
        _hres["only_new"] == 0
        and _hres["only_baseline"] == 0
        and _hres["value_mismatch"] == 0,
        str(_hres.asDict()),
    )
else:
    report("honda air temperature", True, "baseline table absent", status="SKIP")

# COMMAND ----------

# DBTITLE 1,Check -- primary vs alternate DWD air temperature agreement (array-based)
if "weather_temperature" in FAMILY_FRAMES:
    _t = FAMILY_FRAMES["weather_temperature"].filter(F.col("source_system") == "dwd")
    _alt = _t.select(
        "location_key",
        "observation_ts_utc",
        F.explode("air_temperature_alt").alias("a"),
    ).select(
        "location_key",
        "observation_ts_utc",
        F.col("a.value").alias("alt_value"),
    )
    _p = _t.filter(F.col("air_temperature_degc").isNotNull()).select(
        "location_key",
        "observation_ts_utc",
        F.col("air_temperature_degc").alias("primary_value"),
    )
    keep(
        "primary vs alternate DWD air temperature agreement",
        _p.join(_alt, ["location_key", "observation_ts_utc"]).agg(
            F.count("*").alias("pairs"),
            F.avg(
                (F.abs(F.col("primary_value") - F.col("alt_value")) < 1e-9).cast("int")
            ).alias("share_equal"),
        ),
    )

# COMMAND ----------

# DBTITLE 1,Check -- weather_daily rows and derived-day sanity
daily = spark.table(DAILY)
keep(
    "weather_daily rows",
    daily.groupBy("source_system", "source_dataset", "statistic", "value_origin")
    .agg(F.count("*").alias("rows"), F.countDistinct("local_date").alias("days"))
    .orderBy("source_system", "statistic"),
)

# COMMAND ----------

# DBTITLE 1,Check -- derived min <= mean <= max
_d = (
    daily.filter(
        (F.col("value_origin") == "derived") & (F.col("variable") == "air_temperature")
    )
    .groupBy("location_key", "local_date")
    .pivot("statistic", ["min", "mean", "max"])
    .agg(F.first("value"))
)
_bad = _d.filter(
    ~((F.col("min") <= F.col("mean")) & (F.col("mean") <= F.col("max")))
).count()
report("derived daily min <= mean <= max", _bad == 0, f"violations: {_bad}")

# COMMAND ----------

# DBTITLE 1,Check -- Seattle daily high above low on every date
_s = (
    daily.filter(F.col("source_system") == "weather_seattle")
    .groupBy("local_date")
    .pivot("statistic", ["min", "max"])
    .agg(F.first("value"))
)
_bad_s = _s.filter(~(F.col("max") > F.col("min"))).count()
report("seattle max above min on every date", _bad_s == 0, f"violations: {_bad_s}")

# COMMAND ----------

# DBTITLE 1,Build findings -- check results table
_checks_md = "\n".join(
    [
        "| check | status | detail |",
        "|---|---|---|",
        *[f"| {n} | {s} | {d} |" for n, s, d in CHECK_RESULTS],
    ]
)
findings_blocks = [("checks", _checks_md), *FINDINGS_BLOCKS]

# COMMAND ----------

# DBTITLE 1,Export findings -- weather validation
write_silver_findings(
    FINDINGS_SOURCE,
    f"{COMPONENT.split('/')[-1]}__validation",
    "weather validation",
    findings_blocks,
)