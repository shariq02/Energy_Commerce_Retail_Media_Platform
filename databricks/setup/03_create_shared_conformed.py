# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # CREATE THE SHARED_CONFORMED SCHEMA
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** create the platform-level `shared_conformed` schema and its
# MAGIC five conformed dimension tables, and populate the two that need no
# MAGIC acquired-source evidence:
# MAGIC
# MAGIC - `dim_date` -- one row per calendar day, generated here (pure calendar math)
# MAGIC - `dim_time` -- one row per minute of day (0..1439), generated here
# MAGIC - `dim_geography` -- Nation + 16 Bundeslaender, fixed official AGS codes
# MAGIC   (inline below). Regierungsbezirk / Kreis / Gemeinde rows are NOT loaded
# MAGIC   -- they need an acquired BKG/Destatis boundary dataset.
# MAGIC - `geo_plz_gemeinde_xref` -- structure only, no rows (needs an acquired
# MAGIC   PLZ crosswalk)
# MAGIC - `dim_weather_context` -- structure only, no rows (published later from
# MAGIC   the energy weather Gold layer once it exists)
# MAGIC
# MAGIC Idempotent: schemas/tables use `IF NOT EXISTS`; the three seeded tables
# MAGIC are deterministically overwritten. Delta tables carry no enforced
# MAGIC PK/FK/index -- the key and reference intent is in each table `COMMENT`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Imports and configuration

# COMMAND ----------

import datetime as _dt

CATALOG = "energy_commerce_retail_media"
SCHEMA = "shared_conformed"
FQ = f"{CATALOG}.{SCHEMA}"

# dim_date range: wide enough for every wave's history plus a planning horizon,
# without an unbounded table.
DATE_RANGE_START = _dt.date(2016, 1, 1)
DATE_RANGE_END = _dt.date(2031, 12, 31)

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

# The 16 German Bundeslaender -- official AGS 2-digit "Land" codes. A fixed,
# unchanging public administrative fact, not ecosystem evidence.
_BUNDESLAENDER = [
    ("01", "Schleswig-Holstein"),
    ("02", "Hamburg"),
    ("03", "Niedersachsen"),
    ("04", "Bremen"),
    ("05", "Nordrhein-Westfalen"),
    ("06", "Hessen"),
    ("07", "Rheinland-Pfalz"),
    ("08", "Baden-Wuerttemberg"),
    ("09", "Bayern"),
    ("10", "Saarland"),
    ("11", "Berlin"),
    ("12", "Brandenburg"),
    ("13", "Mecklenburg-Vorpommern"),
    ("14", "Sachsen"),
    ("15", "Sachsen-Anhalt"),
    ("16", "Thueringen"),
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Schema

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {FQ}")
print(f"OK  schema ready: {FQ}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Table structures

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FQ}.dim_date (
    date_key       INT     NOT NULL,
    calendar_date  DATE    NOT NULL,
    year           SMALLINT NOT NULL,
    quarter        SMALLINT NOT NULL,
    month          SMALLINT NOT NULL,
    month_name     STRING  NOT NULL,
    day_of_month   SMALLINT NOT NULL,
    day_of_year    SMALLINT NOT NULL,
    iso_week       SMALLINT NOT NULL,
    day_of_week    SMALLINT NOT NULL,
    day_name       STRING  NOT NULL,
    is_weekend     BOOLEAN NOT NULL
)
USING DELTA
COMMENT 'Conformed calendar dimension. Natural + surrogate key: date_key (YYYYMMDD). calendar_date is unique. One row per consecutive day, no gaps.'
""")
print(f"OK  table ready: {FQ}.dim_date")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FQ}.dim_time (
    time_key      SMALLINT NOT NULL,
    hour_24       SMALLINT NOT NULL,
    minute        SMALLINT NOT NULL,
    hour_12       SMALLINT NOT NULL,
    am_pm         STRING  NOT NULL,
    quarter_hour  SMALLINT NOT NULL,
    daypart       STRING  NOT NULL
)
USING DELTA
COMMENT 'Conformed time-of-day dimension. Key: time_key (0..1439, minutes since midnight). One row per minute of day.'
""")
print(f"OK  table ready: {FQ}.dim_time")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FQ}.dim_geography (
    ags_code         STRING NOT NULL,
    ars_code         STRING,
    level            STRING NOT NULL,
    name             STRING NOT NULL,
    parent_ags_code  STRING,
    nuts_code        STRING,
    valid_from       DATE,
    valid_to         DATE,
    source_system    STRING NOT NULL,
    created_at       TIMESTAMP NOT NULL
)
USING DELTA
COMMENT 'Conformed German administrative geography spine. Key: ags_code (Amtlicher Gemeindeschluessel). parent_ags_code references ags_code. level in (nation,bundesland,regierungsbezirk,kreis,gemeinde). Seeded to Nation + 16 Bundeslaender only.'
""")
print(f"OK  table ready: {FQ}.dim_geography")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FQ}.geo_plz_gemeinde_xref (
    plz             STRING NOT NULL,
    ags_code        STRING NOT NULL,
    coverage_share  DECIMAL(5,4),
    source_system   STRING NOT NULL,
    valid_from      DATE,
    valid_to        DATE,
    created_at      TIMESTAMP NOT NULL
)
USING DELTA
COMMENT 'Postal-code crosswalk (many-to-many). Key: (plz, ags_code); ags_code references dim_geography.ags_code (level=gemeinde). STRUCTURE ONLY -- no rows until an official PLZ->Gemeinde dataset is acquired. Every PLZ lift is approximate.'
""")
print(f"OK  table ready: {FQ}.geo_plz_gemeinde_xref")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FQ}.dim_weather_context (
    weather_context_key    STRING NOT NULL,
    ags_code               STRING NOT NULL,
    date_key               INT    NOT NULL,
    weather_regime         STRING,
    avg_temperature_c      DECIMAL(5,2),
    total_precipitation_mm DECIMAL(6,2),
    source_system          STRING NOT NULL,
    published_from         STRING NOT NULL,
    created_at             TIMESTAMP NOT NULL
)
USING DELTA
COMMENT 'Conformed weather/environmental regime attributes -- NOT the full fact_weather (that stays energy-local). Key: weather_context_key; ags_code references dim_geography.ags_code, date_key references dim_date.date_key. STRUCTURE ONLY -- published later from the energy weather Gold layer.'
""")
print(f"OK  table ready: {FQ}.dim_weather_context")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Populate dim_date

# COMMAND ----------

_date_rows = []
_d = DATE_RANGE_START
while _d <= DATE_RANGE_END:
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
).saveAsTable(f"{FQ}.dim_date")
print(
    f"OK  {FQ}.dim_date: {len(_date_rows)} rows "
    f"({DATE_RANGE_START} .. {DATE_RANGE_END})"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Populate dim_time

# COMMAND ----------

_time_rows = []
for _m in range(24 * 60):
    _h24 = _m // 60
    _mm = _m % 60
    _h12 = _h24 % 12 or 12
    _ampm = "AM" if _h24 < 12 else "PM"
    if 5 <= _h24 < 12:
        _daypart = "morning"
    elif 12 <= _h24 < 17:
        _daypart = "afternoon"
    elif 17 <= _h24 < 21:
        _daypart = "evening"
    else:
        _daypart = "night"
    _time_rows.append((_m, _h24, _mm, _h12, _ampm, _m % 60 // 15, _daypart))

_time_df = spark.createDataFrame(
    _time_rows,
    "time_key short, hour_24 short, minute short, hour_12 short, am_pm string, "
    "quarter_hour short, daypart string",
)
_time_df.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(f"{FQ}.dim_time")
print(f"OK  {FQ}.dim_time: {len(_time_rows)} rows (0..1439 minutes)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Populate dim_geography (Nation + 16 Bundeslaender)

# COMMAND ----------

_now = _dt.datetime.now(_dt.UTC)
_vf = _dt.date(1990, 10, 3)
_geo_rows = [
    (
        "00",
        "00",
        "nation",
        "Deutschland",
        None,
        "DE",
        _vf,
        None,
        "bkg_destatis_ags",
        _now,
    )
]
for _ags, _name in _BUNDESLAENDER:
    _geo_rows.append(
        (
            _ags,
            _ags,
            "bundesland",
            _name,
            "00",
            None,
            _vf,
            None,
            "bkg_destatis_ags",
            _now,
        )
    )

_geo_df = spark.createDataFrame(
    _geo_rows,
    "ags_code string, ars_code string, level string, name string, "
    "parent_ags_code string, nuts_code string, valid_from date, valid_to date, "
    "source_system string, created_at timestamp",
)
_geo_df.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(f"{FQ}.dim_geography")
print(f"OK  {FQ}.dim_geography: {len(_geo_rows)} rows (nation + 16 Bundeslaender)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Verify

# COMMAND ----------

_expected = {
    "dim_date",
    "dim_time",
    "dim_geography",
    "geo_plz_gemeinde_xref",
    "dim_weather_context",
}
_found = {row.tableName for row in spark.sql(f"SHOW TABLES IN {FQ}").collect()}
_missing = _expected - _found
if _missing:
    raise RuntimeError(f"FAIL  missing tables: {sorted(_missing)}")

_n_date = spark.table(f"{FQ}.dim_date").count()
_n_time = spark.table(f"{FQ}.dim_time").count()
_n_geo = spark.table(f"{FQ}.dim_geography").count()
if _n_time != 24 * 60:
    raise RuntimeError(f"FAIL  dim_time has {_n_time} rows, expected {24 * 60}")
if _n_geo != 17:
    raise RuntimeError(f"FAIL  dim_geography has {_n_geo} rows, expected 17")

print("=" * 70)
print("SHARED_CONFORMED SETUP SUMMARY")
print("=" * 70)
print(f"Schema                : {FQ}")
print(f"Tables                : {len(_expected)}  -> {sorted(_expected)}")
print(f"dim_date rows         : {_n_date}")
print(f"dim_time rows         : {_n_time}")
print(f"dim_geography rows    : {_n_geo}  (nation + 16 Bundeslaender)")
print(
    "geo_plz_gemeinde_xref : structure only (0 rows -- needs an acquired PLZ dataset)"
)
print(
    "dim_weather_context   : structure only (0 rows -- published later from energy weather Gold)"
)
print("RESULT: PASS")
print("=" * 70)
