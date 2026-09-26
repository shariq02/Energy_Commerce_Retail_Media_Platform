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
# MAGIC ranges.

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

# DBTITLE 1,Weather specifications
# MAGIC %run ./_weather_specs

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Configuration
DAILY = semantic_table("weather_daily")
LOC = semantic_table("weather_location")
COMPONENT = "silver/energy/weather/07_weather_validation"

# Columns every family table carries identically (the shared scaffolding);
# cross-family checks run on this projection, unioned across families.
COMMON_COLS = [
    "observation_key",
    "location_key",
    "source_location_id",
    "observation_timestamp_native",
    "time_basis",
    "utc_offset_hours",
    "observation_timestamp_utc",
    "observation_timestamp_project",
    "local_date",
    "interval_seconds",
    "interval_reference",
    "measurement_basis",
    "source_system",
    "source_dataset",
    "source_record_id",
]

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
        F.min("observation_timestamp_utc").alias("first_timestamp"),
        F.max("observation_timestamp_utc").alias("last_timestamp"),
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
        F.min("observation_timestamp_utc").alias("first_timestamp"),
        F.max("observation_timestamp_utc").alias("last_timestamp"),
    )
    .orderBy("source_system", "source_dataset"),
)

# COMMAND ----------

# DBTITLE 1,Check -- rows by time_basis
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

# DBTITLE 1,Check -- one row per grain key per family (not per product)
# A violation means a product fragmented the grain instead of landing in
# an _alternate/readings array.
for _fam, _fdf in FAMILY_FRAMES.items():
    _dup_grain = _fdf.groupBy(*WEATHER_GRAIN).count().filter("count > 1").count()
    report(
        f"{_fam} one row per grain key",
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
        F.col("observation_timestamp_utc").isNotNull()
        & (F.col("source_system") == "dwd")
    )
    .withColumn(
        "offset_h",
        (
            F.col("observation_timestamp_project").cast("long")
            - F.col("observation_timestamp_utc").cast("long")
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

# DBTITLE 1,Check -- primary vs alternate DWD air temperature agreement (array-based)
if "weather_temperature" in FAMILY_FRAMES:
    _t = FAMILY_FRAMES["weather_temperature"].filter(F.col("source_system") == "dwd")
    _alt = _t.select(
        "location_key",
        "observation_timestamp_utc",
        F.explode("air_temperature_alternate").alias("a"),
    ).select(
        "location_key",
        "observation_timestamp_utc",
        F.col("a.value").alias("alt_value"),
    )
    _p = _t.filter(F.col("air_temperature_degc").isNotNull()).select(
        "location_key",
        "observation_timestamp_utc",
        F.col("air_temperature_degc").alias("primary_value"),
    )
    keep(
        "primary vs alternate DWD air temperature agreement",
        _p.join(_alt, ["location_key", "observation_timestamp_utc"]).agg(
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
findings_blocks = checks_blocks()

# COMMAND ----------

# DBTITLE 1,Export findings -- weather validation
write_silver_findings(
    FINDINGS_SOURCE,
    f"{COMPONENT.split('/')[-1]}__validation",
    "weather validation",
    findings_blocks,
)
