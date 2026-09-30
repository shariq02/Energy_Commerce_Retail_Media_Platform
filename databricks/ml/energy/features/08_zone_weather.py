# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # ZONE WEATHER
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** zone by local date derived weather: capacity-weighted shear-adjusted
# MAGIC onshore wind, coastal offshore wind and station-weighted global radiation,
# MAGIC with covered weight share. Uses the parameters fitted on the training
# MAGIC calendar.

# COMMAND ----------

# DBTITLE 1,ML shared library
# MAGIC %run ../../_ml_common

# COMMAND ----------

# DBTITLE 1,ML inspection library
# MAGIC %run ../../_ml_inspect

# COMMAND ----------

# DBTITLE 1,Configuration
ECO = "energy"
SOURCE = "mastr"
COMPONENT = "ml/energy/features/zone_weather"
TABLE = "features_zone_weather"
MIN_COVERED = 0.5
RADIATION_K = 5
RADIATION_CAP_KM = 500.0
OFFSHORE_K = 2
OFFSHORE_CAP_KM = 500.0
OFFSHORE_DECAY_KM = 100.0
ZONES = ("fifty_hertz", "amprion", "tennet_de", "transnetbw")
DATE_FROM = SPLIT_CALENDARS["energy_daily"]["train"][0]

# COMMAND ----------

# DBTITLE 1,Run identity
rid = ml_run_id()

# COMMAND ----------

# DBTITLE 1,Zone weather library
# MAGIC %run ../../_ml_zone_weather

# COMMAND ----------

# DBTITLE 1,Selected parameters
_p = read_ml("zone_weather_parameters", ecosystem=ECO).filter(F.col("selected")).first()
if _p is None:
    raise RuntimeError("run 07_zone_weather_parameter_fit first")
K, CAP_KM, ALPHA = (
    int(_p["station_count"]),
    float(_p["distance_cap_km"]),
    float(_p["shear_exponent"]),
)
print(f"parameters: k={K} cap_km={CAP_KM} alpha={ALPHA}")

# COMMAND ----------

# DBTITLE 1,Read inputs
sday = read_ml("features_station_day_wind", ecosystem=ECO)
rday = read_ml("features_station_day_radiation", ecosystem=ECO)
periods = read_gold("place_instrument_period", source="dwd")
heights = wind_height_periods(periods)
dim_place = read_shared_conformed("dim_place")
_date_to = sday.agg(F.max("local_date")).first()[0]

# COMMAND ----------

# DBTITLE 1,Wind units with a control zone and a filled hub height
_gu = wind_units(read_gold("generation_unit", source=SOURCE)).filter(
    F.col("lifecycle_state").isin(*OPERATING_STATES)
    & F.col("commissioning_date").isNotNull()
    & F.col("capacity_net_kw").isNotNull()
    & F.col("latitude").isNotNull()
    & F.col("longitude").isNotNull()
)
_zm = unit_zone_map(
    _gu,
    read_gold("grid_connection_point", source=SOURCE),
    read_ml("zone_code_map", ecosystem=ECO),
)
_hub = read_ml("features_hub_height_fill", ecosystem=ECO).select(
    "unit_id", F.col("hub_height_m_filled").alias("hub_height_m")
)
units_all = (
    _gu.select(
        "unit_id",
        "is_offshore",
        "latitude",
        "longitude",
        "capacity_net_kw",
        "commissioning_date",
        "final_decommissioning_date",
    )
    .join(_zm.filter(F.col("zone_status") == "zoned"), "unit_id")
    .join(_hub, "unit_id")
    .filter(F.col("hub_height_m").isNotNull())
)
onshore_units = units_all.filter(~F.col("is_offshore"))
offshore_units = units_all.filter(F.col("is_offshore"))

# COMMAND ----------

# DBTITLE 1,Onshore wind by zone and day
_periods_wind = periods.filter(F.col("parameter_category").rlike(WIND_CATEGORY_PATTERN))
_st = station_table(dim_place, _periods_wind).filter(
    ~F.col("name").rlike(MOUNTAIN_STATION_PATTERN)
)
_w = unit_station_weights(onshore_units, _st, k=K, cap_km=CAP_KM)
_zw = zone_station_weights(
    unit_months(onshore_units, DATE_FROM, str(_date_to)), _w, heights, ALPHA
)
onshore = zone_wind_daily(sday, _zw, min_covered=MIN_COVERED)

# COMMAND ----------

# DBTITLE 1,Offshore wind by zone and day from the coastal stations
_coastal = station_table(dim_place, _periods_wind, names_like=COASTAL_STATION_PATTERN)
_wo = unit_station_weights(
    offshore_units,
    _coastal,
    k=OFFSHORE_K,
    cap_km=OFFSHORE_CAP_KM,
    decay_km=OFFSHORE_DECAY_KM,
)
_zwo = zone_station_weights(
    unit_months(offshore_units, DATE_FROM, str(_date_to)), _wo, heights, ALPHA
)
offshore = zone_wind_daily(sday, _zwo, min_covered=MIN_COVERED)

# COMMAND ----------

# DBTITLE 1,Zone centroids for radiation
centroids = onshore_units.groupBy("market_area_code").agg(
    (
        F.sum(F.col("latitude") * F.col("capacity_net_kw")) / F.sum("capacity_net_kw")
    ).alias("centroid_lat"),
    (
        F.sum(F.col("longitude") * F.col("capacity_net_kw")) / F.sum("capacity_net_kw")
    ).alias("centroid_lon"),
)

# COMMAND ----------

# DBTITLE 1,Radiation by zone and day
_rad_stations = dim_place.join(rday.select("place_key").distinct(), "place_key").filter(
    F.col("latitude").isNotNull() & F.col("longitude").isNotNull()
)
radiation = zone_radiation_daily(
    rday, centroids, _rad_stations, k=RADIATION_K, cap_km=RADIATION_CAP_KM
)

# COMMAND ----------


# DBTITLE 1,Stack the variables into one long table
def _long(df, prefix, mapping):
    parts = []
    for col, name in mapping.items():
        parts.append(
            df.select(
                "market_area_code",
                "local_date",
                F.lit(f"{prefix}{name}").alias("variable"),
                F.col(col).alias("value"),
                "covered_weight_share",
                "stations_used",
            )
        )
    out = parts[0]
    for p in parts[1:]:
        out = out.unionByName(p)
    return out


out = (
    _long(
        onshore,
        "onshore_",
        {
            "wind_speed_adjusted_mean": "wind_speed_adjusted_mean",
            "wind_speed_cubed_adjusted_mean": "wind_speed_cubed_adjusted_mean",
        },
    )
    .unionByName(
        _long(
            offshore,
            "offshore_",
            {
                "wind_speed_adjusted_mean": "wind_speed_adjusted_mean",
                "wind_speed_cubed_adjusted_mean": "wind_speed_cubed_adjusted_mean",
            },
        )
    )
    .unionByName(
        _long(
            radiation,
            "",
            {"global_radiation_mean_w_per_m2": "global_radiation_mean_w_per_m2"},
        )
    )
    .filter(F.col("market_area_code").isin(*ZONES))
)
out = add_ml_provenance(out, TABLE, ECO, rid)

# COMMAND ----------

# DBTITLE 1,Grain and forbidden-column gate
_GRAIN = ["market_area_code", "local_date", "variable"]
assert_unique_grain(out, _GRAIN, component=COMPONENT, source=SOURCE, rid=rid)
assert_no_forbidden_columns(out, component=COMPONENT, source=SOURCE, rid=rid)
check(
    COMPONENT,
    SOURCE,
    "non_empty",
    out.limit(1).count() > 0,
    detail="no rows produced; check the input filters",
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,Write
write_ml(out, TABLE, ecosystem=ECO, source=SOURCE, component=COMPONENT, rid=rid)

# COMMAND ----------

# DBTITLE 1,Inspect and export findings
_blocks = inspect_ml_table(
    read_ml(TABLE, ecosystem=ECO), TABLE, ecosystem=ECO, key_cols=_GRAIN
)
write_ml_findings(ECO, "features__" + TABLE, TABLE, _blocks)
