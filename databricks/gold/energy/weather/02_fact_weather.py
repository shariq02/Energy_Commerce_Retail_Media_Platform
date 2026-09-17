# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # GOLD -- FACT WEATHER (SPLIT, ONE TABLE PER DWD PARAMETER)
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Grain:** one row per (`STATIONS_ID`, `observation_ts`), per table.
# MAGIC
# MAGIC **Sources:** the 14 DWD hourly-measurement Silver tables + `dwd_solar`
# MAGIC (Silver, energy_silver); `dim_weather_station` (Gold, this run).
# MAGIC
# MAGIC **Serves use case:** any Energy use case needing DWD weather observations
# MAGIC attributed to the station's own attributes AS OF the observation time.
# MAGIC
# MAGIC **Purpose:** promote each DWD parameter Silver table to its own Gold
# MAGIC fact table -- kept split, one table per source table, NOT conformed into
# MAGIC a single long `fact_weather`. Each fact resolves `weather_station_key` via
# MAGIC `pit_join()` against `dim_weather_station`'s SCD validity window, never a
# MAGIC plain equi-join on `station_id` alone.

# COMMAND ----------

# DBTITLE 1,Shared Silver library
# MAGIC %run ../../../silver/_silver_common

# COMMAND ----------

# DBTITLE 1,Gold shared library
# MAGIC %run ../../_gold_common

# COMMAND ----------

# DBTITLE 1,Inspection library
# MAGIC %run ../../_gold_inspect

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "dwd"
COMPONENT = "gold/energy/weather/02_fact_weather"
RID = gold_run_id()

# COMMAND ----------

# DBTITLE 1,Read Silver -- dwd_air_temperature
_air_temperature_silver = read_silver("dwd_air_temperature")

# COMMAND ----------

# DBTITLE 1,Read Silver -- dwd_cloudiness
_cloudiness_silver = read_silver("dwd_cloudiness")

# COMMAND ----------

# DBTITLE 1,Read Silver -- dwd_moisture
_moisture_silver = read_silver("dwd_moisture")

# COMMAND ----------

# DBTITLE 1,Read Silver -- dwd_precipitation
_precipitation_silver = read_silver("dwd_precipitation")

# COMMAND ----------

# DBTITLE 1,Read Silver -- dwd_pressure
_pressure_silver = read_silver("dwd_pressure")

# COMMAND ----------

# DBTITLE 1,Read Silver -- dwd_sun
_sun_silver = read_silver("dwd_sun")

# COMMAND ----------

# DBTITLE 1,Read Silver -- dwd_wind
_wind_silver = read_silver("dwd_wind")

# COMMAND ----------

# DBTITLE 1,Read Silver -- dwd_dew_point
_dew_point_silver = read_silver("dwd_dew_point")

# COMMAND ----------

# DBTITLE 1,Read Silver -- dwd_soil_temperature
_soil_temperature_silver = read_silver("dwd_soil_temperature")

# COMMAND ----------

# DBTITLE 1,Read Silver -- dwd_visibility
_visibility_silver = read_silver("dwd_visibility")

# COMMAND ----------

# DBTITLE 1,Read Silver -- dwd_cloud_type
_cloud_type_silver = read_silver("dwd_cloud_type")

# COMMAND ----------

# DBTITLE 1,Read Silver -- dwd_wind_synop
_wind_synop_silver = read_silver("dwd_wind_synop")

# COMMAND ----------

# DBTITLE 1,Read Silver -- dwd_extreme_wind
_extreme_wind_silver = read_silver("dwd_extreme_wind")

# COMMAND ----------

# DBTITLE 1,Read Silver -- dwd_weather_phenomena
_weather_phenomena_silver = read_silver("dwd_weather_phenomena")

# COMMAND ----------

# DBTITLE 1,Read Silver -- dwd_solar
_solar_silver = read_silver("dwd_solar")

# COMMAND ----------

# DBTITLE 1,Read Gold -- dim_weather_station
_weather_station_dim = read_gold("dim_weather_station", source="dwd")

# COMMAND ----------

# DBTITLE 1,Transform -- fact_weather_air_temperature (point-in-time station key)
air_temperature = pit_join(
    _air_temperature_silver,
    _weather_station_dim,
    fact_key_col="STATIONS_ID",
    fact_ts_col="observation_ts",
    dim_key_col="station_id",
    dim_select=["station_id", "valid_from", "valid_to", "weather_station_key"],
)

# COMMAND ----------

# DBTITLE 1,Transform -- fact_weather_air_temperature (surrogate key + Gold provenance)
air_temperature = air_temperature.withColumn(
    "air_temperature_key", surrogate_key("STATIONS_ID", "observation_ts")
)
air_temperature = add_gold_provenance(air_temperature, SOURCE, RID)

# COMMAND ----------

# DBTITLE 1,Grain assertion -- fact_weather_air_temperature
assert_unique_grain(
    air_temperature,
    ["STATIONS_ID", "observation_ts"],
    component=COMPONENT,
    source=SOURCE,
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Write Gold -- fact_weather_air_temperature
write_gold(
    air_temperature,
    "fact_weather_air_temperature",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Inspect fact_weather_air_temperature + export findings
_findings_blocks = inspect_gold_table(
    air_temperature,
    "fact_weather_air_temperature",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["STATIONS_ID", "observation_ts"],
    df_before=_air_temperature_silver,
    extra_checks={
        "weather_station_key_unmatched_count": air_temperature.filter(
            F.col("weather_station_key").isNull()
        ).count(),
    },
)
write_gold_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__fact_weather_air_temperature",
    "fact_weather_air_temperature",
    _findings_blocks,
)

# COMMAND ----------

# DBTITLE 1,Transform -- fact_weather_cloudiness (point-in-time station key)
cloudiness = pit_join(
    _cloudiness_silver,
    _weather_station_dim,
    fact_key_col="STATIONS_ID",
    fact_ts_col="observation_ts",
    dim_key_col="station_id",
    dim_select=["station_id", "valid_from", "valid_to", "weather_station_key"],
)

# COMMAND ----------

# DBTITLE 1,Transform -- fact_weather_cloudiness (surrogate key + Gold provenance)
cloudiness = cloudiness.withColumn(
    "cloudiness_key", surrogate_key("STATIONS_ID", "observation_ts")
)
cloudiness = add_gold_provenance(cloudiness, SOURCE, RID)

# COMMAND ----------

# DBTITLE 1,Grain assertion -- fact_weather_cloudiness
assert_unique_grain(
    cloudiness,
    ["STATIONS_ID", "observation_ts"],
    component=COMPONENT,
    source=SOURCE,
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Write Gold -- fact_weather_cloudiness
write_gold(
    cloudiness, "fact_weather_cloudiness", source=SOURCE, component=COMPONENT, rid=RID
)

# COMMAND ----------

# DBTITLE 1,Inspect fact_weather_cloudiness + export findings
_findings_blocks = inspect_gold_table(
    cloudiness,
    "fact_weather_cloudiness",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["STATIONS_ID", "observation_ts"],
    df_before=_cloudiness_silver,
    extra_checks={
        "weather_station_key_unmatched_count": cloudiness.filter(
            F.col("weather_station_key").isNull()
        ).count(),
    },
)
write_gold_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__fact_weather_cloudiness",
    "fact_weather_cloudiness",
    _findings_blocks,
)

# COMMAND ----------

# DBTITLE 1,Transform -- fact_weather_moisture (point-in-time station key)
moisture = pit_join(
    _moisture_silver,
    _weather_station_dim,
    fact_key_col="STATIONS_ID",
    fact_ts_col="observation_ts",
    dim_key_col="station_id",
    dim_select=["station_id", "valid_from", "valid_to", "weather_station_key"],
)

# COMMAND ----------

# DBTITLE 1,Transform -- fact_weather_moisture (surrogate key + Gold provenance)
moisture = moisture.withColumn(
    "moisture_key", surrogate_key("STATIONS_ID", "observation_ts")
)
moisture = add_gold_provenance(moisture, SOURCE, RID)

# COMMAND ----------

# DBTITLE 1,Grain assertion -- fact_weather_moisture
assert_unique_grain(
    moisture,
    ["STATIONS_ID", "observation_ts"],
    component=COMPONENT,
    source=SOURCE,
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Write Gold -- fact_weather_moisture
write_gold(
    moisture, "fact_weather_moisture", source=SOURCE, component=COMPONENT, rid=RID
)

# COMMAND ----------

# DBTITLE 1,Inspect fact_weather_moisture + export findings
_findings_blocks = inspect_gold_table(
    moisture,
    "fact_weather_moisture",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["STATIONS_ID", "observation_ts"],
    df_before=_moisture_silver,
    extra_checks={
        "weather_station_key_unmatched_count": moisture.filter(
            F.col("weather_station_key").isNull()
        ).count(),
    },
)
write_gold_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__fact_weather_moisture",
    "fact_weather_moisture",
    _findings_blocks,
)

# COMMAND ----------

# DBTITLE 1,Transform -- fact_weather_precipitation (point-in-time station key)
precipitation = pit_join(
    _precipitation_silver,
    _weather_station_dim,
    fact_key_col="STATIONS_ID",
    fact_ts_col="observation_ts",
    dim_key_col="station_id",
    dim_select=["station_id", "valid_from", "valid_to", "weather_station_key"],
)

# COMMAND ----------

# DBTITLE 1,Transform -- fact_weather_precipitation (surrogate key + Gold provenance)
precipitation = precipitation.withColumn(
    "precipitation_key", surrogate_key("STATIONS_ID", "observation_ts")
)
precipitation = add_gold_provenance(precipitation, SOURCE, RID)

# COMMAND ----------

# DBTITLE 1,Grain assertion -- fact_weather_precipitation
assert_unique_grain(
    precipitation,
    ["STATIONS_ID", "observation_ts"],
    component=COMPONENT,
    source=SOURCE,
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Write Gold -- fact_weather_precipitation
write_gold(
    precipitation,
    "fact_weather_precipitation",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Inspect fact_weather_precipitation + export findings
_findings_blocks = inspect_gold_table(
    precipitation,
    "fact_weather_precipitation",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["STATIONS_ID", "observation_ts"],
    df_before=_precipitation_silver,
    extra_checks={
        "weather_station_key_unmatched_count": precipitation.filter(
            F.col("weather_station_key").isNull()
        ).count(),
    },
)
write_gold_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__fact_weather_precipitation",
    "fact_weather_precipitation",
    _findings_blocks,
)

# COMMAND ----------

# DBTITLE 1,Transform -- fact_weather_pressure (point-in-time station key)
pressure = pit_join(
    _pressure_silver,
    _weather_station_dim,
    fact_key_col="STATIONS_ID",
    fact_ts_col="observation_ts",
    dim_key_col="station_id",
    dim_select=["station_id", "valid_from", "valid_to", "weather_station_key"],
)

# COMMAND ----------

# DBTITLE 1,Transform -- fact_weather_pressure (surrogate key + Gold provenance)
pressure = pressure.withColumn(
    "pressure_key", surrogate_key("STATIONS_ID", "observation_ts")
)
pressure = add_gold_provenance(pressure, SOURCE, RID)

# COMMAND ----------

# DBTITLE 1,Grain assertion -- fact_weather_pressure
assert_unique_grain(
    pressure,
    ["STATIONS_ID", "observation_ts"],
    component=COMPONENT,
    source=SOURCE,
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Write Gold -- fact_weather_pressure
write_gold(
    pressure, "fact_weather_pressure", source=SOURCE, component=COMPONENT, rid=RID
)

# COMMAND ----------

# DBTITLE 1,Inspect fact_weather_pressure + export findings
_findings_blocks = inspect_gold_table(
    pressure,
    "fact_weather_pressure",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["STATIONS_ID", "observation_ts"],
    df_before=_pressure_silver,
    extra_checks={
        "weather_station_key_unmatched_count": pressure.filter(
            F.col("weather_station_key").isNull()
        ).count(),
    },
)
write_gold_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__fact_weather_pressure",
    "fact_weather_pressure",
    _findings_blocks,
)

# COMMAND ----------

# DBTITLE 1,Transform -- fact_weather_sun (point-in-time station key)
sun = pit_join(
    _sun_silver,
    _weather_station_dim,
    fact_key_col="STATIONS_ID",
    fact_ts_col="observation_ts",
    dim_key_col="station_id",
    dim_select=["station_id", "valid_from", "valid_to", "weather_station_key"],
)

# COMMAND ----------

# DBTITLE 1,Transform -- fact_weather_sun (surrogate key + Gold provenance)
sun = sun.withColumn("sun_key", surrogate_key("STATIONS_ID", "observation_ts"))
sun = add_gold_provenance(sun, SOURCE, RID)

# COMMAND ----------

# DBTITLE 1,Grain assertion -- fact_weather_sun
assert_unique_grain(
    sun,
    ["STATIONS_ID", "observation_ts"],
    component=COMPONENT,
    source=SOURCE,
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Write Gold -- fact_weather_sun
write_gold(sun, "fact_weather_sun", source=SOURCE, component=COMPONENT, rid=RID)

# COMMAND ----------

# DBTITLE 1,Inspect fact_weather_sun + export findings
_findings_blocks = inspect_gold_table(
    sun,
    "fact_weather_sun",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["STATIONS_ID", "observation_ts"],
    df_before=_sun_silver,
    extra_checks={
        "weather_station_key_unmatched_count": sun.filter(
            F.col("weather_station_key").isNull()
        ).count(),
    },
)
write_gold_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__fact_weather_sun",
    "fact_weather_sun",
    _findings_blocks,
)

# COMMAND ----------

# DBTITLE 1,Transform -- fact_weather_wind (point-in-time station key)
wind = pit_join(
    _wind_silver,
    _weather_station_dim,
    fact_key_col="STATIONS_ID",
    fact_ts_col="observation_ts",
    dim_key_col="station_id",
    dim_select=["station_id", "valid_from", "valid_to", "weather_station_key"],
)

# COMMAND ----------

# DBTITLE 1,Transform -- fact_weather_wind (surrogate key + Gold provenance)
wind = wind.withColumn("wind_key", surrogate_key("STATIONS_ID", "observation_ts"))
wind = add_gold_provenance(wind, SOURCE, RID)

# COMMAND ----------

# DBTITLE 1,Grain assertion -- fact_weather_wind
assert_unique_grain(
    wind,
    ["STATIONS_ID", "observation_ts"],
    component=COMPONENT,
    source=SOURCE,
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Write Gold -- fact_weather_wind
write_gold(wind, "fact_weather_wind", source=SOURCE, component=COMPONENT, rid=RID)

# COMMAND ----------

# DBTITLE 1,Inspect fact_weather_wind + export findings
_findings_blocks = inspect_gold_table(
    wind,
    "fact_weather_wind",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["STATIONS_ID", "observation_ts"],
    df_before=_wind_silver,
    extra_checks={
        "weather_station_key_unmatched_count": wind.filter(
            F.col("weather_station_key").isNull()
        ).count(),
    },
)
write_gold_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__fact_weather_wind",
    "fact_weather_wind",
    _findings_blocks,
)

# COMMAND ----------

# DBTITLE 1,Transform -- fact_weather_dew_point (point-in-time station key)
dew_point = pit_join(
    _dew_point_silver,
    _weather_station_dim,
    fact_key_col="STATIONS_ID",
    fact_ts_col="observation_ts",
    dim_key_col="station_id",
    dim_select=["station_id", "valid_from", "valid_to", "weather_station_key"],
)

# COMMAND ----------

# DBTITLE 1,Transform -- fact_weather_dew_point (surrogate key + Gold provenance)
dew_point = dew_point.withColumn(
    "dew_point_key", surrogate_key("STATIONS_ID", "observation_ts")
)
dew_point = add_gold_provenance(dew_point, SOURCE, RID)

# COMMAND ----------

# DBTITLE 1,Grain assertion -- fact_weather_dew_point
assert_unique_grain(
    dew_point,
    ["STATIONS_ID", "observation_ts"],
    component=COMPONENT,
    source=SOURCE,
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Write Gold -- fact_weather_dew_point
write_gold(
    dew_point, "fact_weather_dew_point", source=SOURCE, component=COMPONENT, rid=RID
)

# COMMAND ----------

# DBTITLE 1,Inspect fact_weather_dew_point + export findings
_findings_blocks = inspect_gold_table(
    dew_point,
    "fact_weather_dew_point",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["STATIONS_ID", "observation_ts"],
    df_before=_dew_point_silver,
    extra_checks={
        "weather_station_key_unmatched_count": dew_point.filter(
            F.col("weather_station_key").isNull()
        ).count(),
    },
)
write_gold_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__fact_weather_dew_point",
    "fact_weather_dew_point",
    _findings_blocks,
)

# COMMAND ----------

# DBTITLE 1,Transform -- fact_weather_soil_temperature (point-in-time station key)
soil_temperature = pit_join(
    _soil_temperature_silver,
    _weather_station_dim,
    fact_key_col="STATIONS_ID",
    fact_ts_col="observation_ts",
    dim_key_col="station_id",
    dim_select=["station_id", "valid_from", "valid_to", "weather_station_key"],
)

# COMMAND ----------

# DBTITLE 1,Transform -- fact_weather_soil_temperature (surrogate key + Gold provenance)
soil_temperature = soil_temperature.withColumn(
    "soil_temperature_key", surrogate_key("STATIONS_ID", "observation_ts")
)
soil_temperature = add_gold_provenance(soil_temperature, SOURCE, RID)

# COMMAND ----------

# DBTITLE 1,Grain assertion -- fact_weather_soil_temperature
assert_unique_grain(
    soil_temperature,
    ["STATIONS_ID", "observation_ts"],
    component=COMPONENT,
    source=SOURCE,
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Write Gold -- fact_weather_soil_temperature
write_gold(
    soil_temperature,
    "fact_weather_soil_temperature",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Inspect fact_weather_soil_temperature + export findings
_findings_blocks = inspect_gold_table(
    soil_temperature,
    "fact_weather_soil_temperature",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["STATIONS_ID", "observation_ts"],
    df_before=_soil_temperature_silver,
    extra_checks={
        "weather_station_key_unmatched_count": soil_temperature.filter(
            F.col("weather_station_key").isNull()
        ).count(),
    },
)
write_gold_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__fact_weather_soil_temperature",
    "fact_weather_soil_temperature",
    _findings_blocks,
)

# COMMAND ----------

# DBTITLE 1,Transform -- fact_weather_visibility (point-in-time station key)
visibility = pit_join(
    _visibility_silver,
    _weather_station_dim,
    fact_key_col="STATIONS_ID",
    fact_ts_col="observation_ts",
    dim_key_col="station_id",
    dim_select=["station_id", "valid_from", "valid_to", "weather_station_key"],
)

# COMMAND ----------

# DBTITLE 1,Transform -- fact_weather_visibility (surrogate key + Gold provenance)
visibility = visibility.withColumn(
    "visibility_key", surrogate_key("STATIONS_ID", "observation_ts")
)
visibility = add_gold_provenance(visibility, SOURCE, RID)

# COMMAND ----------

# DBTITLE 1,Grain assertion -- fact_weather_visibility
assert_unique_grain(
    visibility,
    ["STATIONS_ID", "observation_ts"],
    component=COMPONENT,
    source=SOURCE,
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Write Gold -- fact_weather_visibility
write_gold(
    visibility, "fact_weather_visibility", source=SOURCE, component=COMPONENT, rid=RID
)

# COMMAND ----------

# DBTITLE 1,Inspect fact_weather_visibility + export findings
_findings_blocks = inspect_gold_table(
    visibility,
    "fact_weather_visibility",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["STATIONS_ID", "observation_ts"],
    df_before=_visibility_silver,
    extra_checks={
        "weather_station_key_unmatched_count": visibility.filter(
            F.col("weather_station_key").isNull()
        ).count(),
    },
)
write_gold_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__fact_weather_visibility",
    "fact_weather_visibility",
    _findings_blocks,
)

# COMMAND ----------

# DBTITLE 1,Transform -- fact_weather_cloud_type (point-in-time station key)
cloud_type = pit_join(
    _cloud_type_silver,
    _weather_station_dim,
    fact_key_col="STATIONS_ID",
    fact_ts_col="observation_ts",
    dim_key_col="station_id",
    dim_select=["station_id", "valid_from", "valid_to", "weather_station_key"],
)

# COMMAND ----------

# DBTITLE 1,Transform -- fact_weather_cloud_type (surrogate key + Gold provenance)
cloud_type = cloud_type.withColumn(
    "cloud_type_key", surrogate_key("STATIONS_ID", "observation_ts")
)
cloud_type = add_gold_provenance(cloud_type, SOURCE, RID)

# COMMAND ----------

# DBTITLE 1,Grain assertion -- fact_weather_cloud_type
assert_unique_grain(
    cloud_type,
    ["STATIONS_ID", "observation_ts"],
    component=COMPONENT,
    source=SOURCE,
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Write Gold -- fact_weather_cloud_type
write_gold(
    cloud_type, "fact_weather_cloud_type", source=SOURCE, component=COMPONENT, rid=RID
)

# COMMAND ----------

# DBTITLE 1,Inspect fact_weather_cloud_type + export findings
_findings_blocks = inspect_gold_table(
    cloud_type,
    "fact_weather_cloud_type",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["STATIONS_ID", "observation_ts"],
    df_before=_cloud_type_silver,
    extra_checks={
        "weather_station_key_unmatched_count": cloud_type.filter(
            F.col("weather_station_key").isNull()
        ).count(),
    },
)
write_gold_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__fact_weather_cloud_type",
    "fact_weather_cloud_type",
    _findings_blocks,
)

# COMMAND ----------

# DBTITLE 1,Transform -- fact_weather_wind_synop (point-in-time station key)
wind_synop = pit_join(
    _wind_synop_silver,
    _weather_station_dim,
    fact_key_col="STATIONS_ID",
    fact_ts_col="observation_ts",
    dim_key_col="station_id",
    dim_select=["station_id", "valid_from", "valid_to", "weather_station_key"],
)

# COMMAND ----------

# DBTITLE 1,Transform -- fact_weather_wind_synop (surrogate key + Gold provenance)
wind_synop = wind_synop.withColumn(
    "wind_synop_key", surrogate_key("STATIONS_ID", "observation_ts")
)
wind_synop = add_gold_provenance(wind_synop, SOURCE, RID)

# COMMAND ----------

# DBTITLE 1,Grain assertion -- fact_weather_wind_synop
assert_unique_grain(
    wind_synop,
    ["STATIONS_ID", "observation_ts"],
    component=COMPONENT,
    source=SOURCE,
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Write Gold -- fact_weather_wind_synop
write_gold(
    wind_synop, "fact_weather_wind_synop", source=SOURCE, component=COMPONENT, rid=RID
)

# COMMAND ----------

# DBTITLE 1,Inspect fact_weather_wind_synop + export findings
_findings_blocks = inspect_gold_table(
    wind_synop,
    "fact_weather_wind_synop",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["STATIONS_ID", "observation_ts"],
    df_before=_wind_synop_silver,
    extra_checks={
        "weather_station_key_unmatched_count": wind_synop.filter(
            F.col("weather_station_key").isNull()
        ).count(),
    },
)
write_gold_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__fact_weather_wind_synop",
    "fact_weather_wind_synop",
    _findings_blocks,
)

# COMMAND ----------

# DBTITLE 1,Transform -- fact_weather_extreme_wind (point-in-time station key)
extreme_wind = pit_join(
    _extreme_wind_silver,
    _weather_station_dim,
    fact_key_col="STATIONS_ID",
    fact_ts_col="observation_ts",
    dim_key_col="station_id",
    dim_select=["station_id", "valid_from", "valid_to", "weather_station_key"],
)

# COMMAND ----------

# DBTITLE 1,Transform -- fact_weather_extreme_wind (surrogate key + Gold provenance)
extreme_wind = extreme_wind.withColumn(
    "extreme_wind_key", surrogate_key("STATIONS_ID", "observation_ts")
)
extreme_wind = add_gold_provenance(extreme_wind, SOURCE, RID)

# COMMAND ----------

# DBTITLE 1,Grain assertion -- fact_weather_extreme_wind
assert_unique_grain(
    extreme_wind,
    ["STATIONS_ID", "observation_ts"],
    component=COMPONENT,
    source=SOURCE,
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Write Gold -- fact_weather_extreme_wind
write_gold(
    extreme_wind,
    "fact_weather_extreme_wind",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Inspect fact_weather_extreme_wind + export findings
_findings_blocks = inspect_gold_table(
    extreme_wind,
    "fact_weather_extreme_wind",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["STATIONS_ID", "observation_ts"],
    df_before=_extreme_wind_silver,
    extra_checks={
        "weather_station_key_unmatched_count": extreme_wind.filter(
            F.col("weather_station_key").isNull()
        ).count(),
    },
)
write_gold_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__fact_weather_extreme_wind",
    "fact_weather_extreme_wind",
    _findings_blocks,
)

# COMMAND ----------

# DBTITLE 1,Transform -- fact_weather_weather_phenomena (point-in-time station key)
weather_phenomena = pit_join(
    _weather_phenomena_silver,
    _weather_station_dim,
    fact_key_col="STATIONS_ID",
    fact_ts_col="observation_ts",
    dim_key_col="station_id",
    dim_select=["station_id", "valid_from", "valid_to", "weather_station_key"],
)

# COMMAND ----------

# DBTITLE 1,Transform -- fact_weather_weather_phenomena (surrogate key + Gold provenance)
weather_phenomena = weather_phenomena.withColumn(
    "weather_phenomena_key", surrogate_key("STATIONS_ID", "observation_ts")
)
weather_phenomena = add_gold_provenance(weather_phenomena, SOURCE, RID)

# COMMAND ----------

# DBTITLE 1,Grain assertion -- fact_weather_weather_phenomena
assert_unique_grain(
    weather_phenomena,
    ["STATIONS_ID", "observation_ts"],
    component=COMPONENT,
    source=SOURCE,
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Write Gold -- fact_weather_weather_phenomena
write_gold(
    weather_phenomena,
    "fact_weather_weather_phenomena",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Inspect fact_weather_weather_phenomena + export findings
_findings_blocks = inspect_gold_table(
    weather_phenomena,
    "fact_weather_weather_phenomena",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["STATIONS_ID", "observation_ts"],
    df_before=_weather_phenomena_silver,
    extra_checks={
        "weather_station_key_unmatched_count": weather_phenomena.filter(
            F.col("weather_station_key").isNull()
        ).count(),
    },
)
write_gold_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__fact_weather_weather_phenomena",
    "fact_weather_weather_phenomena",
    _findings_blocks,
)

# COMMAND ----------

# DBTITLE 1,Transform -- fact_weather_solar (point-in-time station key)
solar = pit_join(
    _solar_silver,
    _weather_station_dim,
    fact_key_col="STATIONS_ID",
    fact_ts_col="observation_ts",
    dim_key_col="station_id",
    dim_select=["station_id", "valid_from", "valid_to", "weather_station_key"],
)

# COMMAND ----------

# DBTITLE 1,Transform -- fact_weather_solar (surrogate key + Gold provenance)
solar = solar.withColumn("solar_key", surrogate_key("STATIONS_ID", "observation_ts"))
solar = add_gold_provenance(solar, SOURCE, RID)

# COMMAND ----------

# DBTITLE 1,Grain assertion -- fact_weather_solar
assert_unique_grain(
    solar,
    ["STATIONS_ID", "observation_ts"],
    component=COMPONENT,
    source=SOURCE,
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Write Gold -- fact_weather_solar
write_gold(solar, "fact_weather_solar", source=SOURCE, component=COMPONENT, rid=RID)

# COMMAND ----------

# DBTITLE 1,Inspect fact_weather_solar + export findings
_findings_blocks = inspect_gold_table(
    solar,
    "fact_weather_solar",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["STATIONS_ID", "observation_ts"],
    df_before=_solar_silver,
    extra_checks={
        "weather_station_key_unmatched_count": solar.filter(
            F.col("weather_station_key").isNull()
        ).count(),
    },
)
write_gold_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__fact_weather_solar",
    "fact_weather_solar",
    _findings_blocks,
)
