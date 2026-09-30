# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # ML ZONE WEATHER LIBRARY
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** the station, unit-weight and zone-day computations shared by
# MAGIC the zone-weather parameter fit, the zone-weather build and its acceptance
# MAGIC test. Pulled in with `%run ../../_ml_zone_weather` after `_ml_common`.
# MAGIC Definitions only.

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# DBTITLE 1,Configuration constants
MOUNTAIN_STATION_PATTERN = "(?i)zugspitze|feldberg|hohenpei"
COASTAL_STATION_PATTERN = "(?i)list|norderney|kiel|warnem"
WIND_CATEGORY_PATTERN = "(?i)windgeschwindigkeit|wind_?speed"
WIND_SPEED_EXPR = (
    "coalesce(filter(wind__readings, r -> r.statistic = 'mean')[0], "
    "wind__readings[0]).wind_speed_m_per_s"
)
MIN_STATION_HOURS = 18
EARTH_RADIUS_KM = 6371.0

# COMMAND ----------

# DBTITLE 1,Distance


def haversine_km(lat1, lon1, lat2, lon2):
    p1, p2 = F.radians(lat1), F.radians(lat2)
    dphi = F.radians(lat2 - lat1)
    dlmb = F.radians(lon2 - lon1)
    a = F.sin(dphi / 2) ** 2 + F.cos(p1) * F.cos(p2) * F.sin(dlmb / 2) ** 2
    return F.lit(2 * EARTH_RADIUS_KM) * F.asin(F.sqrt(a))


# COMMAND ----------

# DBTITLE 1,Stations and sensor heights


def station_table(dim_place: DataFrame, place_periods: DataFrame, *, names_like=None):
    """One row per weather station with coordinates and name."""
    st = (
        dim_place.filter(F.col("latitude").isNotNull() & F.col("longitude").isNotNull())
        .select("place_key", "name", "latitude", "longitude")
        .join(
            place_periods.select("place_key").distinct(),
            "place_key",
            "inner",
        )
    )
    if names_like:
        st = st.filter(F.col("name").rlike(names_like))
    return st


def wind_height_periods(place_periods: DataFrame) -> DataFrame:
    """Sensor height periods of the wind-speed category (one per station and day)."""
    return place_periods.filter(
        F.col("parameter_category").rlike(WIND_CATEGORY_PATTERN)
        & F.col("sensor_height_m").isNotNull()
    ).select(
        "place_key",
        "valid_from",
        F.coalesce(F.col("valid_to"), F.lit("9999-12-31").cast("date")).alias(
            "valid_to"
        ),
        "sensor_height_m",
    )


def station_day_wind(
    weather: DataFrame, heights: DataFrame, date_from: str, date_to: str
):
    """Mean wind speed and mean speed cubed per station and local date, with the
    sensor height valid on that date."""
    hourly = weather.filter(
        (F.col("origin_source_system") == "dwd")
        & (F.col("interval_seconds") == 3600)
        & F.col("local_date").between(date_from, date_to)
    ).select(
        F.col("location_key").alias("place_key"),
        "local_date",
        F.expr(WIND_SPEED_EXPR).alias("speed"),
    )
    daily = (
        hourly.filter(F.col("speed").isNotNull())
        .groupBy("place_key", "local_date")
        .agg(
            F.count("*").alias("n_hours"),
            F.avg("speed").alias("speed_mean"),
            F.avg(F.pow("speed", 3)).alias("speed_cubed_mean"),
        )
        .filter(F.col("n_hours") >= MIN_STATION_HOURS)
    )
    j = daily.join(
        heights,
        (daily["place_key"] == heights["place_key"])
        & (daily["local_date"] >= heights["valid_from"])
        & (daily["local_date"] <= heights["valid_to"]),
        "inner",
    ).select(
        daily["place_key"],
        "local_date",
        "speed_mean",
        "speed_cubed_mean",
        "sensor_height_m",
        "valid_from",
    )
    w = F.row_number().over(
        Window.partitionBy("place_key", "local_date").orderBy(
            F.col("valid_from").desc()
        )
    )
    return (
        j.withColumn("_rn", w)
        .filter(F.col("_rn") == 1)
        .drop("_rn", "valid_from")
        .withColumn("month", F.trunc("local_date", "month"))
    )


# COMMAND ----------

# DBTITLE 1,Unit to station weights and monthly capacity


def unit_station_weights(
    units: DataFrame, stations: DataFrame, *, k: int, cap_km: float, decay_km=None
):
    """Weight of each of a unit's k nearest stations within cap_km; weights sum
    to one per unit. Inverse distance, or exp(-d/decay_km) when decay_km is set."""
    s = stations.select(
        F.col("place_key"),
        F.col("latitude").alias("s_lat"),
        F.col("longitude").alias("s_lon"),
    )
    d = units.select("unit_id", "latitude", "longitude").crossJoin(F.broadcast(s))
    d = d.withColumn(
        "distance_km",
        haversine_km(
            F.col("latitude"), F.col("longitude"), F.col("s_lat"), F.col("s_lon")
        ),
    ).filter(F.col("distance_km") <= cap_km)
    w = Window.partitionBy("unit_id").orderBy("distance_km")
    d = d.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") <= k)
    raw = (
        F.exp(-F.col("distance_km") / F.lit(float(decay_km)))
        if decay_km
        else F.lit(1.0) / F.greatest(F.col("distance_km"), F.lit(1.0))
    )
    d = d.withColumn("_w", raw)
    tot = Window.partitionBy("unit_id")
    return d.withColumn("weight", F.col("_w") / F.sum("_w").over(tot)).select(
        "unit_id", "place_key", "distance_km", "weight"
    )


def unit_months(units: DataFrame, date_from: str, date_to: str) -> DataFrame:
    """Units alive at the start of each month (commissioned, not yet decommissioned)."""
    months = units.select(
        "unit_id",
        "market_area_code",
        "capacity_net_kw",
        "hub_height_m",
        "commissioning_date",
        "final_decommissioning_date",
        F.explode(
            F.sequence(
                F.trunc(F.lit(date_from).cast("date"), "month"),
                F.trunc(F.lit(date_to).cast("date"), "month"),
                F.expr("INTERVAL 1 MONTH"),
            )
        ).alias("month"),
    )
    return months.filter(
        (F.col("commissioning_date").cast("date") <= F.col("month"))
        & (
            F.col("final_decommissioning_date").isNull()
            | (F.col("final_decommissioning_date").cast("date") > F.col("month"))
        )
    ).drop("commissioning_date", "final_decommissioning_date")


def zone_station_weights(
    umonths: DataFrame, weights: DataFrame, heights: DataFrame, alpha: float
) -> DataFrame:
    """Per zone, station, sensor height and month: capacity weight (w0), shear
    adjusted weight (w1) and cubed adjusted weight (w3)."""
    h = heights.select("place_key", "sensor_height_m").distinct()
    j = (
        umonths.join(weights, "unit_id", "inner")
        .join(h, "place_key", "inner")
        .withColumn("_cw", F.col("capacity_net_kw") * F.col("weight"))
        .withColumn(
            "_f", F.pow(F.col("hub_height_m") / F.col("sensor_height_m"), F.lit(alpha))
        )
    )
    return j.groupBy("market_area_code", "place_key", "sensor_height_m", "month").agg(
        F.sum("_cw").alias("w0"),
        F.sum(F.col("_cw") * F.col("_f")).alias("w1"),
        F.sum(F.col("_cw") * F.pow(F.col("_f"), F.lit(3.0))).alias("w3"),
    )


# COMMAND ----------

# DBTITLE 1,Zone-day wind and radiation


def zone_wind_daily(
    sday: DataFrame, zw: DataFrame, *, min_covered: float = 0.5
) -> DataFrame:
    """Capacity-weighted, shear-adjusted zone wind per local date. Weights are
    renormalised over the stations that reported that day (zone level)."""
    total = zw.groupBy("market_area_code", "month").agg(F.sum("w0").alias("w0_all"))
    j = sday.join(zw, ["place_key", "sensor_height_m", "month"], "inner")
    agg = j.groupBy("market_area_code", "local_date", "month").agg(
        F.sum(F.col("w1") * F.col("speed_mean")).alias("_num1"),
        F.sum(F.col("w3") * F.col("speed_cubed_mean")).alias("_num3"),
        F.sum("w0").alias("_w0"),
        F.countDistinct("place_key").alias("stations_used"),
    )
    out = agg.join(total, ["market_area_code", "month"], "inner")
    cov = F.col("_w0") / F.col("w0_all")
    ok = cov >= F.lit(min_covered)
    return out.select(
        "market_area_code",
        "local_date",
        F.when(ok, F.col("_num1") / F.col("_w0")).alias("wind_speed_adjusted_mean"),
        F.when(ok, F.col("_num3") / F.col("_w0")).alias(
            "wind_speed_cubed_adjusted_mean"
        ),
        cov.alias("covered_weight_share"),
        "stations_used",
    )


def station_day_radiation(radiation: DataFrame, date_from: str, date_to: str):
    hourly = radiation.filter(
        (F.col("origin_source_system") == "dwd")
        & F.col("local_date").between(date_from, date_to)
    ).select(
        F.col("location_key").alias("place_key"),
        "local_date",
        F.col("solar_radiation__global_radiation_w_per_m2").alias("radiation"),
    )
    return (
        hourly.filter(F.col("radiation").isNotNull())
        .groupBy("place_key", "local_date")
        .agg(F.count("*").alias("n_hours"), F.avg("radiation").alias("radiation_mean"))
        .filter(F.col("n_hours") >= 20)
    )


def zone_radiation_daily(
    rday: DataFrame, centroids: DataFrame, stations: DataFrame, *, k: int, cap_km: float
):
    """Zone radiation per local date from the k nearest stations of the zone
    centroid, weights renormalised over the stations present that day."""
    s = stations.select(
        "place_key",
        F.col("latitude").alias("s_lat"),
        F.col("longitude").alias("s_lon"),
    )
    d = centroids.crossJoin(F.broadcast(s)).withColumn(
        "distance_km",
        haversine_km(
            F.col("centroid_lat"), F.col("centroid_lon"), F.col("s_lat"), F.col("s_lon")
        ),
    )
    d = d.filter(F.col("distance_km") <= cap_km)
    w = Window.partitionBy("market_area_code").orderBy("distance_km")
    d = d.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") <= k)
    d = d.withColumn(
        "weight", F.lit(1.0) / F.greatest(F.col("distance_km"), F.lit(1.0))
    )
    tot = d.groupBy("market_area_code").agg(F.sum("weight").alias("w_all"))
    j = rday.join(
        d.select("market_area_code", "place_key", "weight"), "place_key", "inner"
    )
    agg = j.groupBy("market_area_code", "local_date").agg(
        F.sum(F.col("weight") * F.col("radiation_mean")).alias("_num"),
        F.sum("weight").alias("_w"),
        F.countDistinct("place_key").alias("stations_used"),
    )
    out = agg.join(tot, "market_area_code", "inner")
    return out.select(
        "market_area_code",
        "local_date",
        (F.col("_num") / F.col("_w")).alias("global_radiation_mean_w_per_m2"),
        (F.col("_w") / F.col("w_all")).alias("covered_weight_share"),
        "stations_used",
    )
