# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER -- WEATHER OBSERVATION (ACCUWEATHER HISTORICAL HOURLY)
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** one AccuWeather hourly observation per (city, instant) split
# MAGIC across the family structures its columns belong to, alongside DWD/Honda's
# MAGIC own rows in each. The metric and imperial hourly tables are two unit
# MAGIC systems for the *same* underlying observations, not two sources: matched
# MAGIC (city, instant) pairs are consolidated to one row (the metric side, needing
# MAGIC no conversion for most fields); the `historical_hourly_imperial`-only
# MAGIC pairs (58 of 6,550, confirmed by EDA) are not dropped -- they survive as
# MAGIC their own rows, converted from imperial units. Column names match between
# MAGIC the two tables except imperial's time column (`date`, here treated as the
# MAGIC same local instant as metric's `datetime_valid_local` -- EDA found a
# MAGIC 0-hour alignment shift is the best match) and its missing `gmt_offset`
# MAGIC (looked up from metric by city, which is per-city constant).
# MAGIC
# MAGIC Unit handling is per field, independently: humidity, wind direction,
# MAGIC cloud cover (fraction in both tables), sunshine minutes and UV index need
# MAGIC no conversion from either table; temperature/pressure/wind speed-gust/
# MAGIC precipitation/solar irradiance/cloud base height/visibility are already
# MAGIC standard from the metric table and converted from the imperial table
# MAGIC (degF, inHg, mph, in, BTU/(h*ft2), ft, mi respectively -- factors
# MAGIC confirmed against `src/schemas/profiling/accuweather.md`'s measured
# MAGIC imperial/metric ratios).

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
SOURCE = "accuweather"
COMPONENT = "silver/semantic/weather/04_weather_observation_accuweather"
RID = run_id()
METRIC_DATASET = "historical_hourly_metric"
IMPERIAL_DATASET = "historical_hourly_imperial"
METRIC_TABLE = f"samples.accuweather.{METRIC_DATASET}"
IMPERIAL_TABLE = f"samples.accuweather.{IMPERIAL_DATASET}"

AW_KEY = ["city_name", "datetime_valid_local"]
AW_FIELDS = [
    "temperature",
    "temperature_dew_point",
    "humidity_relative",
    "pressure",
    "pressure_msl",
    "wind_speed",
    "wind_gust",
    "wind_direction",
    "precipitation_lwe",
    "solar_irradiance",
    "cloud_cover_total",
    "cloud_base_height",
    "visibility",
    "minutes_of_sun",
    "index_uv",
]
# field -> (metric_factor, metric_offset, metric_native_unit_or_None,
#           imperial_factor, imperial_offset, imperial_native_unit_or_None).
# standard = raw * factor + offset; native tracked only where that source's
# raw value differs from the standard unit (factors/offsets confirmed
# against src/schemas/profiling/accuweather.md's measured imperial/metric
# ratios, or the exact physical constant where the ratio is a round one).
AW_CONVERSION = {
    "temperature": (1.0, 0.0, None, 5 / 9, -160 / 9, "degF"),
    "temperature_dew_point": (1.0, 0.0, None, 5 / 9, -160 / 9, "degF"),
    "humidity_relative": (1.0, 0.0, None, 1.0, 0.0, None),
    "pressure": (0.01, 0.0, "Pa", 33.8639, 0.0, "inHg"),
    "pressure_msl": (0.01, 0.0, "Pa", 33.8639, 0.0, "inHg"),
    "wind_speed": (1.0, 0.0, None, 0.44704, 0.0, "mph"),
    "wind_gust": (1.0, 0.0, None, 0.44704, 0.0, "mph"),
    "wind_direction": (1.0, 0.0, None, 1.0, 0.0, None),
    "precipitation_lwe": (1.0, 0.0, None, 25.4, 0.0, "in"),
    "solar_irradiance": (1.0, 0.0, None, 3.15459, 0.0, "BTU_per_hr_ft2"),
    "cloud_cover_total": (100.0, 0.0, "fraction", 100.0, 0.0, "fraction"),
    "cloud_base_height": (1.0, 0.0, None, 0.3048, 0.0, "ft"),
    "visibility": (1000.0, 0.0, "km", 1609.344, 0.0, "mi"),
    "minutes_of_sun": (1.0, 0.0, None, 1.0, 0.0, None),
    "index_uv": (1.0, 0.0, None, 1.0, 0.0, None),
}

# COMMAND ----------

# DBTITLE 1,Session time zone
ensure_utc_session()

# COMMAND ----------

# DBTITLE 1,Read Samples -- metric and imperial hourly tables
metric_src = spark.table(METRIC_TABLE)
imperial_src = spark.table(IMPERIAL_TABLE).withColumnRenamed(
    "date", "datetime_valid_local"
)

# COMMAND ----------

# DBTITLE 1,Dedupe -- collapse identical rows, quarantine same-key value conflicts (per table)
_metric_kept, _metric_q = resolve_conflicts(
    metric_src, AW_KEY, AW_FIELDS, bronze_table=METRIC_DATASET
)
write_quarantine(_metric_q.withColumn("source_system", F.lit(SOURCE)), RID)

_imperial_kept, _imperial_q = resolve_conflicts(
    imperial_src, AW_KEY, AW_FIELDS, bronze_table=IMPERIAL_DATASET
)
write_quarantine(_imperial_q.withColumn("source_system", F.lit(SOURCE)), RID)

RECONCILIATION = {
    METRIC_DATASET: reconciliation_stats(metric_src, _metric_kept, _metric_q),
    IMPERIAL_DATASET: reconciliation_stats(imperial_src, _imperial_kept, _imperial_q),
}
metric_src, imperial_src = _metric_kept, _imperial_kept

# COMMAND ----------

# DBTITLE 1,Consolidate -- imperial rows with no matching metric (city, instant) survive
_gmt_offset_by_city = metric_src.select("city_name", "gmt_offset").distinct()
imperial_only = (
    imperial_src.join(metric_src.select(*AW_KEY), AW_KEY, "left_anti")
    .join(_gmt_offset_by_city, "city_name", "left")
    .withColumn("source_dataset", F.lit(IMPERIAL_DATASET))
    .withColumn("_is_imperial", F.lit(True))
)
metric_tagged = metric_src.withColumn(
    "source_dataset", F.lit(METRIC_DATASET)
).withColumn("_is_imperial", F.lit(False))
aw_source = metric_tagged.unionByName(imperial_only, allowMissingColumns=True)

# COMMAND ----------

# DBTITLE 1,Transform -- standardised value + native value/unit per field, independently
for _field in AW_FIELDS:
    m_factor, m_offset, m_unit, i_factor, i_offset, i_unit = AW_CONVERSION[_field]
    _raw = F.col(_field).cast("double")
    _factor = F.when(F.col("_is_imperial"), F.lit(i_factor)).otherwise(F.lit(m_factor))
    _offset = F.when(F.col("_is_imperial"), F.lit(i_offset)).otherwise(F.lit(m_offset))
    _native_unit = F.when(F.col("_is_imperial"), F.lit(i_unit)).otherwise(F.lit(m_unit))
    aw_source = (
        aw_source.withColumn(
            f"{_field}__value", F.when(_raw.isNotNull(), _raw * _factor + _offset)
        )
        .withColumn(f"{_field}__native_value", F.when(_native_unit.isNotNull(), _raw))
        .withColumn(f"{_field}__native_unit", _native_unit)
    )

# COMMAND ----------

# DBTITLE 1,Transform -- place, time, provenance (shared by every family)
# Not add_semantic_provenance(): source_dataset here is per-row (metric vs
# imperial-only), already set above -- a fixed literal would overwrite it.
aw_base = (
    aw_source.withColumn("source_location_id", F.col("city_name"))
    .withColumn("location_key", location_key(SOURCE, "source_location_id"))
    .withColumn("observation_ts_native", F.col("datetime_valid_local").cast("string"))
    .withColumn("time_basis", F.lit("local_with_offset"))
    .withColumn("utc_offset_hours", F.col("gmt_offset").cast("double"))
    .withColumn(
        "observation_ts_utc",
        local_time_to_utc("datetime_valid_local", "utc_offset_hours"),
    )
    .withColumn("measurement_basis", F.lit("provider_historical"))
    .withColumn("source_system", F.lit(SOURCE))
    .withColumn("source_record_id", sha_key(F.lit("historical_hourly"), *AW_KEY))
    .withColumn("observation_key", sha_key(F.lit("historical_hourly"), *AW_KEY))
    .withColumn("_silver_loaded_at", F.current_timestamp())
    .withColumn("_silver_run_id", F.lit(RID))
)
aw_base = add_project_time(aw_base)

# COMMAND ----------

# DBTITLE 1,Transform -- weather_temperature and weather_humidity rows
aw_temperature = any_present(
    aw_base.withColumn("air_temperature_degc", F.col("temperature__value")).withColumn(
        "dew_point_temperature_degc", F.col("temperature_dew_point__value")
    ),
    ["air_temperature_degc", "dew_point_temperature_degc"],
)
aw_humidity = any_present(
    aw_base.withColumn("relative_humidity_percent", F.col("humidity_relative__value")),
    ["relative_humidity_percent"],
)

# COMMAND ----------

# DBTITLE 1,Transform -- weather_pressure rows
aw_pressure = any_present(
    aw_base.withColumn("pressure_station_hpa", F.col("pressure__value"))
    .withColumn("pressure_station_native_value", F.col("pressure__native_value"))
    .withColumn("pressure_station_native_unit", F.col("pressure__native_unit"))
    .withColumn("pressure_sea_level_hpa", F.col("pressure_msl__value"))
    .withColumn("pressure_sea_level_native_value", F.col("pressure_msl__native_value"))
    .withColumn("pressure_sea_level_native_unit", F.col("pressure_msl__native_unit")),
    ["pressure_station_hpa", "pressure_sea_level_hpa"],
)

# COMMAND ----------

# DBTITLE 1,Transform -- weather_wind rows (one reading per instant, statistic unspecified)
_wind_reading = F.struct(
    F.lit("unspecified").alias("statistic"),
    F.col("wind_speed__value").alias("wind_speed_m_per_s"),
    F.col("wind_direction__value").alias("wind_direction_degrees"),
    F.lit(None).cast("boolean").alias("wind_direction_variable"),
    F.col("wind_gust__value").alias("wind_gust_m_per_s"),
    F.col("source_dataset").alias("source_dataset"),
)
aw_wind = aw_base.withColumn(
    "readings",
    F.filter(
        F.array(
            F.when(
                F.col("wind_speed__value").isNotNull()
                | F.col("wind_direction__value").isNotNull()
                | F.col("wind_gust__value").isNotNull(),
                _wind_reading,
            )
        ),
        lambda x: x.isNotNull(),
    ),
).filter(F.size("readings") > 0)

# COMMAND ----------

# DBTITLE 1,Transform -- weather_precipitation rows
aw_precipitation = any_present(
    aw_base.withColumn("precipitation_mm", F.col("precipitation_lwe__value")),
    ["precipitation_mm"],
)

# COMMAND ----------

# DBTITLE 1,Transform -- weather_cloud rows
aw_cloud = any_present(
    aw_base.withColumn("cloud_cover_total_percent", F.col("cloud_cover_total__value"))
    .withColumn(
        "cloud_cover_total_native_value", F.col("cloud_cover_total__native_value")
    )
    .withColumn(
        "cloud_cover_total_native_unit", F.col("cloud_cover_total__native_unit")
    )
    .withColumn("cloud_base_height_m", F.col("cloud_base_height__value")),
    ["cloud_cover_total_percent", "cloud_base_height_m"],
)

# COMMAND ----------

# DBTITLE 1,Transform -- weather_visibility rows
aw_visibility = any_present(
    aw_base.withColumn("visibility_m", F.col("visibility__value"))
    .withColumn("visibility_native_value", F.col("visibility__native_value"))
    .withColumn("visibility_native_unit", F.col("visibility__native_unit")),
    ["visibility_m"],
)

# COMMAND ----------

# DBTITLE 1,Transform -- weather_solar_radiation, weather_sunshine_duration, weather_uv_index rows
aw_solar = any_present(
    aw_base.withColumn("global_radiation_w_per_m2", F.col("solar_irradiance__value")),
    ["global_radiation_w_per_m2"],
)
aw_sunshine = any_present(
    aw_base.withColumn("sunshine_duration_minutes", F.col("minutes_of_sun__value")),
    ["sunshine_duration_minutes"],
)
aw_uv = any_present(
    aw_base.withColumn("uv_index", F.col("index_uv__value")), ["uv_index"]
)

# COMMAND ----------

# DBTITLE 1,Configuration -- family -> frame
AW_FAMILIES = {
    "weather_temperature": aw_temperature,
    "weather_humidity": aw_humidity,
    "weather_pressure": aw_pressure,
    "weather_wind": aw_wind,
    "weather_precipitation": aw_precipitation,
    "weather_cloud": aw_cloud,
    "weather_visibility": aw_visibility,
    "weather_solar_radiation": aw_solar,
    "weather_sunshine_duration": aw_sunshine,
    "weather_uv_index": aw_uv,
}

# COMMAND ----------

# DBTITLE 1,Write Silver -- family structures (accuweather)
for fam, fdf in AW_FAMILIES.items():
    write_semantic(
        conform(fdf, WEATHER_FAMILY_COLUMNS[fam]),
        fam,
        source=SOURCE,
        component=COMPONENT,
        rid=RID,
        replace_where=f"source_system = '{SOURCE}'",
    )

# COMMAND ----------

# DBTITLE 1,Inspect -- each family structure (aw)
findings_blocks = {}
for fam in AW_FAMILIES:
    written = spark.table(semantic_table(fam)).filter(F.col("source_system") == SOURCE)
    findings_blocks[fam] = inspect_table(
        written,
        fam,
        source=FINDINGS_SOURCE,
        component=COMPONENT,
        rid=RID,
        key_cols=["observation_key"],
        extra_checks=structure_extra_checks(written),
    )

# COMMAND ----------

# DBTITLE 1,Export findings -- each family structure (aw)
for fam, blocks in findings_blocks.items():
    write_silver_findings(
        FINDINGS_SOURCE,
        f"{COMPONENT.split('/')[-1]}__{fam}",
        fam,
        blocks,
    )

# COMMAND ----------

# DBTITLE 1,Export findings -- dedup/conflict reconciliation proof, per table
for dataset, stats in RECONCILIATION.items():
    write_silver_findings(
        FINDINGS_SOURCE,
        f"{COMPONENT.split('/')[-1]}__{dataset}__reconciliation",
        f"dedup/conflict reconciliation -- {dataset}",
        [
            (
                "Bronze -> exact duplicates collapsed -> conflicts quarantined -> kept",
                dict_to_markdown_row(stats),
            )
        ],
    )
