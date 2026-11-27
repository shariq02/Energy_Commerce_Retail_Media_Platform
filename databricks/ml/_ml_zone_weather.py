# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # ML ZONE WEATHER LIBRARY
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** November 2026
# MAGIC
# MAGIC **Purpose:** the cell, unit-weight and zone-day computations shared by
# MAGIC the zone-weather parameter fit, the zone-weather build and its acceptance
# MAGIC test. Pulled in with `%run ../_ml_zone_weather` after `_ml_common`.
# MAGIC Definitions only. The wind comes from the ERA5 grid cells (`dim_place` rows of
# MAGIC the source `era5`, daily 100 m wind of `weather_daily`); the radiation comes
# MAGIC from the DWD stations.

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# DBTITLE 1,Configuration constants
ERA5_HEIGHT_M = 100.0  # the height of the ERA5 wind speed
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

# DBTITLE 1,ERA5 cells and the daily wind of a cell


def era5_cells(dim_place: DataFrame) -> DataFrame:
    """One row per ERA5 grid cell with its coordinates and the 100 m height."""
    return dim_place.filter(
        (F.col("source_system") == "era5")
        & F.col("latitude").isNotNull()
        & F.col("longitude").isNotNull()
    ).select(
        F.col("location_key").alias("place_key"),
        "name",
        "latitude",
        "longitude",
        F.lit(ERA5_HEIGHT_M).alias("sensor_height_m"),
    )


def cell_day_wind(daily: DataFrame, date_from: str, date_to: str) -> DataFrame:
    """Mean wind speed and mean speed cubed per ERA5 cell and local date (the long
    rows of `weather_daily` as two columns), with the height of the reanalysis."""
    return (
        daily.filter(
            (F.col("source_system") == "era5")
            & (F.col("variable") == "wind_speed_100m")
            & F.col("local_date").between(date_from, date_to)
        )
        .groupBy(F.col("location_key").alias("place_key"), "local_date")
        .agg(
            F.max(F.when(F.col("statistic") == "mean", F.col("value"))).alias(
                "speed_mean"
            ),
            F.max(F.when(F.col("statistic") == "cubed_mean", F.col("value"))).alias(
                "speed_cubed_mean"
            ),
        )
        .filter(F.col("speed_mean").isNotNull() & F.col("speed_cubed_mean").isNotNull())
        .withColumn("sensor_height_m", F.lit(ERA5_HEIGHT_M))
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
    total = (
        zw.groupBy("market_area_code", "place_key", "month")
        .agg(F.max("w0").alias("w0"))
        .groupBy("market_area_code", "month")
        .agg(F.sum("w0").alias("w0_all"))
    )
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
        (F.col("source_system") == "dwd")
        & F.col("local_date").between(date_from, date_to)
    ).select(
        F.col("location_key").alias("place_key"),
        "local_date",
        F.col("global_radiation_w_per_m2").alias("radiation"),
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
