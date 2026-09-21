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
# MAGIC **Purpose:** read-only checks of the weather structures: key uniqueness,
# MAGIC place coverage, time consistency, and regression against the existing
# MAGIC source-scoped Silver tables.

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
OBS = semantic_table("weather_observation")
DAILY = semantic_table("weather_daily")
LOC = semantic_table("weather_location")
BASELINE_SCHEMA = "energy_silver"
COMPONENT = "silver/semantic/weather/07_weather_validation"

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

# DBTITLE 1,Read Silver -- weather_observation
obs = spark.table(OBS)

# COMMAND ----------

# DBTITLE 1,Read Silver -- loaded source datasets
LOADED_DATASETS = {
    r["source_dataset"] for r in obs.select("source_dataset").distinct().collect()
}

# COMMAND ----------

# DBTITLE 1,Check -- rows per source dataset
keep(
    "rows per source dataset",
    obs.groupBy("source_system", "source_dataset", "measurement_basis")
    .agg(
        F.count("*").alias("rows"),
        F.countDistinct("variable").alias("variables"),
        F.min("observation_ts_utc").alias("first_ts"),
        F.max("observation_ts_utc").alias("last_ts"),
    )
    .orderBy("source_system", "source_dataset"),
)

# COMMAND ----------

# DBTITLE 1,Check -- observation_key is unique
dup_keys = obs.groupBy("observation_key").count().filter("count > 1").count()
report("observation_key unique", dup_keys == 0, f"duplicate keys: {dup_keys}")

# COMMAND ----------

# DBTITLE 1,Check -- every observation has a location
orphans = (
    obs.select("location_key")
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

# DBTITLE 1,Check -- primary variables are unique per place, instant, statistic, level
primary_dups = (
    obs.filter("is_primary")
    .groupBy(
        "source_system",
        "location_key",
        "variable",
        "statistic",
        "level",
        "observation_ts_utc",
        "interval_seconds",
    )
    .count()
    .filter("count > 1")
    .count()
)
report(
    "primary rows unique on the natural grain",
    primary_dups == 0,
    f"groups: {primary_dups}",
)

# COMMAND ----------

# DBTITLE 1,Check -- project time offset from UTC is +1 or +2 hours (Europe/Berlin)
offsets = (
    obs.filter(
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
    .groupBy("offset_h")
    .count()
)
keep("project time offset from UTC (DWD, hours)", offsets)

# COMMAND ----------

# DBTITLE 1,Check -- value ranges by variable, unit and source
keep(
    "value ranges by variable, unit and source",
    obs.filter(F.col("value").isNotNull())
    .groupBy("variable", "unit", "source_system")
    .agg(
        F.count("*").alias("rows"),
        F.min("value").alias("min"),
        F.percentile_approx("value", 0.5).alias("median"),
        F.max("value").alias("max"),
    )
    .orderBy("variable", "source_system"),
)

# COMMAND ----------

# DBTITLE 1,Regression -- key coverage per DWD table against the existing Silver tables
# Retire this cell and the two below with the old source-scoped Silver tables;
# a missing baseline table is reported SKIP, not FAIL.
_key_rows = []
for _ds in [d for d in DWD_TABLES if d in LOADED_DATASETS]:
    _base_name = f"{CATALOG}.{BASELINE_SCHEMA}.{_ds}"
    if not spark.catalog.tableExists(_base_name):
        report(f"{_ds} key coverage", True, "baseline table absent", status="SKIP")
        continue
    _new = (
        obs.filter(F.col("source_dataset") == _ds)
        .select("source_location_id", "observation_ts_utc")
        .distinct()
    )
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
    _key_rows.append((_ds, _only_new, _only_base))
    report(
        f"{_ds} key coverage",
        _only_new == 0,
        f"only_new={_only_new} baseline_only={_only_base} (all-null baseline rows)",
    )

# COMMAND ----------

# DBTITLE 1,Regression -- DWD air temperature values against the existing Silver table
_base_name = f"{CATALOG}.{BASELINE_SCHEMA}.dwd_air_temperature"
if spark.catalog.tableExists(_base_name):
    _base_dwd = (
        spark.table(_base_name)
        .select(
            F.col("STATIONS_ID").alias("source_location_id"),
            "observation_ts",
            F.col("air_temperature_2m").alias("base_value"),
        )
        .filter("base_value is not null")
    )
    _new_dwd = obs.filter(
        (F.col("source_dataset") == "dwd_air_temperature")
        & (F.col("source_column") == "TT_TU")
    ).select(
        "source_location_id",
        F.col("observation_ts_utc").alias("observation_ts"),
        F.col("value_native").alias("new_value"),
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
if spark.catalog.tableExists(_base_name):
    _base_h = (
        spark.table(_base_name)
        .select(
            "frequency",
            "datetime_utc",
            F.col("air_temperature_2m").alias("base_value"),
        )
        .filter("base_value is not null")
    )
    _new_h = obs.filter(
        (F.col("source_dataset") == "honda_iot_weather")
        & (F.col("variable") == "air_temperature")
    ).select(
        F.col("observation_ts_utc").alias("datetime_utc"),
        F.when(F.col("interval_seconds") == 60, "1min")
        .when(F.col("interval_seconds") == 900, "15min")
        .otherwise("1h")
        .alias("frequency"),
        F.col("value_native").alias("new_value"),
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

# DBTITLE 1,Check -- primary vs alternate DWD air temperature agreement
_alt = obs.filter(
    (F.col("source_system") == "dwd") & (F.col("variable") == "air_temperature")
).select("location_key", "observation_ts_utc", "is_primary", "value_native")
_p = _alt.filter("is_primary").select(
    "location_key", "observation_ts_utc", F.col("value_native").alias("primary_value")
)
_a = _alt.filter("not is_primary").select(
    "location_key", "observation_ts_utc", F.col("value_native").alias("alt_value")
)
keep(
    "primary vs alternate DWD air temperature agreement",
    _p.join(_a, ["location_key", "observation_ts_utc"]).agg(
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
