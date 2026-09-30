# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # GOLD -- DIM_DATE COVERAGE
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** `shared_conformed.dim_date` already exists
# MAGIC (`databricks/setup/03_create_shared_conformed.py`, range 2016-01-01 ..
# MAGIC 2031-12-31). This notebook checks that range against the earliest and
# MAGIC latest dates Gold actually needs (DWD's historical weather goes back well
# MAGIC before 2016) and regenerates the table with a wider range only if the
# MAGIC real data requires it -- idempotent, no-op when coverage is already
# MAGIC sufficient.

# COMMAND ----------

# DBTITLE 1,Shared Silver library
# MAGIC %run ../../silver/_silver_common

# COMMAND ----------

# DBTITLE 1,Gold shared library
# MAGIC %run ../_gold_common

# COMMAND ----------

# DBTITLE 1,Imports
import datetime as _dt

# COMMAND ----------

# DBTITLE 1,Configuration
FQ = f"{CATALOG}.{SHARED_CONFORMED_SCHEMA}.dim_date"
HORIZON_END = _dt.date(2031, 12, 31)
_MONTH_NAMES = [
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
]
_DAY_NAMES = [
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
    "Sunday",
]

# COMMAND ----------

# DBTITLE 1,Assert dim_date already exists
if not spark.catalog.tableExists(FQ):
    raise RuntimeError(
        f"{FQ} missing -- run databricks/setup/03_create_shared_conformed.py first"
    )

# COMMAND ----------

# DBTITLE 1,Current coverage
_current = (
    spark.table(FQ)
    .agg(F.min("calendar_date").alias("min_d"), F.max("calendar_date").alias("max_d"))
    .first()
)
current_min, current_max = _current["min_d"], _current["max_d"]
print(f"current dim_date coverage: {current_min} .. {current_max}")

# COMMAND ----------

# DBTITLE 1,Earliest date Gold actually needs
_needed_min = read_silver("weather_daily").agg(F.min("local_date")).first()[0]
_needed_min_2 = (
    read_silver("generation_unit")
    .agg(F.least(F.min("commissioning_date"), F.min("registration_date")))
    .first()[0]
)
_candidates = [d for d in (_needed_min, _needed_min_2) if d is not None]
needed_min = min(d.date() if hasattr(d, "date") else d for d in _candidates)
print(f"earliest date Gold needs (weather_daily, generation_unit): {needed_min}")

# COMMAND ----------

# DBTITLE 1,Decide: no-op or regenerate with a wider range
range_start = min(current_min, needed_min)
range_end = max(current_max, HORIZON_END)
if range_start >= current_min and range_end <= current_max:
    print(
        f"OK  {FQ}: {current_min} .. {current_max} already covers {needed_min} .. {HORIZON_END}"
    )
    dbutils.notebook.exit("unchanged")

# COMMAND ----------

# DBTITLE 1,Regenerate dim_date over the wider range
_date_rows = []
_d = range_start
while _d <= range_end:
    _iso_year, _iso_week, _iso_weekday = _d.isocalendar()
    _date_rows.append(
        (
            int(_d.strftime("%Y%m%d")),
            _d,
            _d.year,
            (_d.month - 1) // 3 + 1,
            _d.month,
            _MONTH_NAMES[_d.month - 1],
            _d.day,
            _d.timetuple().tm_yday,
            _iso_week,
            _iso_weekday,
            _DAY_NAMES[_iso_weekday - 1],
            _iso_weekday in (6, 7),
        )
    )
    _d += _dt.timedelta(days=1)

_date_df = spark.createDataFrame(
    _date_rows,
    "date_key int, calendar_date date, year short, quarter short, month short, "
    "month_name string, day_of_month short, day_of_year short, iso_week short, "
    "day_of_week short, day_name string, is_weekend boolean",
)
_date_df.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(FQ)
print(f"OK  {FQ}: regenerated, {len(_date_rows)} rows ({range_start} .. {range_end})")
