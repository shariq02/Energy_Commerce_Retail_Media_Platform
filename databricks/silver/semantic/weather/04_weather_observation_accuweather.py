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
# MAGIC **Purpose:** one AccuWeather hourly record split across the eight family
# MAGIC structures its columns belong to, alongside DWD/Honda's own rows in each;
# MAGIC the imperial variant is the same record in other units and is not
# MAGIC loaded. Provider values, so `measurement_basis` differs from station
# MAGIC sources. Pressure (Pa), cloud cover (fraction) and visibility (km) are
# MAGIC the only fields here that need conversion to the Silver standard unit;
# MAGIC everything else is already standard.

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
DATASET = "historical_hourly_metric"
SAMPLES_TABLE = f"samples.accuweather.{DATASET}"

# COMMAND ----------

# DBTITLE 1,Session time zone
ensure_utc_session()

# COMMAND ----------

# DBTITLE 1,Read Samples -- historical_hourly_metric
aw_hourly_src = spark.table(SAMPLES_TABLE)

# COMMAND ----------

# DBTITLE 1,Transform -- place, time, provenance (shared by every family)
aw_base = (
    aw_hourly_src.withColumn("source_location_id", F.col("city_name"))
    .withColumn("location_key", location_key(SOURCE, "source_location_id"))
    .withColumn("observation_ts_native", F.col("datetime_valid_local").cast("string"))
    .withColumn("time_basis", F.lit("local_with_offset"))
    .withColumn("utc_offset_hours", F.col("gmt_offset").cast("double"))
    .withColumn(
        "observation_ts_utc",
        local_time_to_utc("datetime_valid_local", "utc_offset_hours"),
    )
    .withColumn("measurement_basis", F.lit("provider_historical"))
    .withColumn(
        "source_record_id", sha_key(F.lit(DATASET), "city_name", "datetime_valid_local")
    )
    .withColumn(
        "observation_key", sha_key(F.lit(DATASET), "city_name", "datetime_valid_local")
    )
)
aw_base = add_project_time(aw_base)
aw_base = add_semantic_provenance(aw_base, SOURCE, DATASET, RID)

# COMMAND ----------

# DBTITLE 1,Transform -- weather_temperature and weather_humidity rows
aw_temperature = any_present(
    aw_base.withColumn(
        "air_temperature_degc", F.col("temperature").cast("double")
    ).withColumn(
        "dew_point_temperature_degc", F.col("temperature_dew_point").cast("double")
    ),
    ["air_temperature_degc", "dew_point_temperature_degc"],
)
aw_humidity = any_present(
    aw_base.withColumn(
        "relative_humidity_percent", F.col("humidity_relative").cast("double")
    ),
    ["relative_humidity_percent"],
)

# COMMAND ----------

# DBTITLE 1,Transform -- weather_pressure rows (Pa -> hPa)
_pressure_station = F.col("pressure").cast("double")
_pressure_sea_level = F.col("pressure_msl").cast("double")
aw_pressure = any_present(
    aw_base.withColumn(
        "pressure_station_hpa",
        F.when(_pressure_station.isNotNull(), _pressure_station * 0.01),
    )
    .withColumn("pressure_station_native_value", _pressure_station)
    .withColumn(
        "pressure_station_native_unit",
        F.when(_pressure_station.isNotNull(), F.lit("Pa")),
    )
    .withColumn(
        "pressure_sea_level_hpa",
        F.when(_pressure_sea_level.isNotNull(), _pressure_sea_level * 0.01),
    )
    .withColumn("pressure_sea_level_native_value", _pressure_sea_level)
    .withColumn(
        "pressure_sea_level_native_unit",
        F.when(_pressure_sea_level.isNotNull(), F.lit("Pa")),
    ),
    ["pressure_station_hpa", "pressure_sea_level_hpa"],
)

# COMMAND ----------

# DBTITLE 1,Transform -- weather_wind rows (already m/s and degrees)
aw_wind = any_present(
    aw_base.withColumn("statistic", F.lit("unspecified"))
    .withColumn("wind_speed_m_per_s", F.col("wind_speed").cast("double"))
    .withColumn("wind_gust_m_per_s", F.col("wind_gust").cast("double"))
    .withColumn("wind_direction_degrees", F.col("wind_direction").cast("double")),
    ["wind_speed_m_per_s", "wind_gust_m_per_s", "wind_direction_degrees"],
)

# COMMAND ----------

# DBTITLE 1,Transform -- weather_precipitation rows (already mm, liquid water equivalent)
aw_precipitation = any_present(
    aw_base.withColumn("precipitation_mm", F.col("precipitation_lwe").cast("double")),
    ["precipitation_mm"],
)

# COMMAND ----------

# DBTITLE 1,Transform -- weather_cloud rows (fraction -> percent; base height already m)
_cloud_fraction = F.col("cloud_cover_total").cast("double")
aw_cloud = any_present(
    aw_base.withColumn(
        "cloud_cover_total_percent",
        F.when(_cloud_fraction.isNotNull(), _cloud_fraction * 100.0),
    )
    .withColumn("cloud_cover_total_native_value", _cloud_fraction)
    .withColumn(
        "cloud_cover_total_native_unit",
        F.when(_cloud_fraction.isNotNull(), F.lit("fraction")),
    )
    .withColumn("cloud_base_height_m", F.col("cloud_base_height").cast("double")),
    ["cloud_cover_total_percent", "cloud_base_height_m"],
)

# COMMAND ----------

# DBTITLE 1,Transform -- weather_visibility rows (km -> m)
_visibility_km = F.col("visibility").cast("double")
aw_visibility = any_present(
    aw_base.withColumn(
        "visibility_m", F.when(_visibility_km.isNotNull(), _visibility_km * 1000.0)
    )
    .withColumn("visibility_native_value", _visibility_km)
    .withColumn(
        "visibility_native_unit", F.when(_visibility_km.isNotNull(), F.lit("km"))
    ),
    ["visibility_m"],
)

# COMMAND ----------

# DBTITLE 1,Transform -- weather_solar_radiation rows (already W/m2, minutes, index)
aw_solar = any_present(
    aw_base.withColumn(
        "global_radiation_w_per_m2", F.col("solar_irradiance").cast("double")
    )
    .withColumn("sunshine_duration_minutes", F.col("minutes_of_sun").cast("double"))
    .withColumn("uv_index", F.col("index_uv").cast("double")),
    ["global_radiation_w_per_m2", "sunshine_duration_minutes", "uv_index"],
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
        replace_where=f"source_dataset = '{DATASET}'",
    )

# COMMAND ----------

# DBTITLE 1,Inspect -- each family structure (aw)
findings_blocks = {}
for fam in AW_FAMILIES:
    written = spark.table(semantic_table(fam)).filter(
        F.col("source_dataset") == DATASET
    )
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
        f"{COMPONENT.split('/')[-1]}__{DATASET}__{fam}",
        f"{fam} -- {DATASET}",
        blocks,
    )
