# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # ZONE WEATHER PARAMETER FIT
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** choose the station count, distance cap and shear exponent of the zone wind
# MAGIC method on the training calendar and score them on the validation year. The
# MAGIC test partition is never read.

# COMMAND ----------

# DBTITLE 1,ML shared library
# MAGIC %run ../../_ml_common

# COMMAND ----------

# DBTITLE 1,Configuration
ECO = "energy"
SOURCE = "mastr"
COMPONENT = "ml/energy/features/zone_weather_parameter_fit"
TABLE = "zone_weather_parameters"
FIT_FROM, FIT_TO = SPLIT_CALENDARS["energy_daily"]["train"]
VALID_FROM, VALID_TO = SPLIT_CALENDARS["energy_daily"]["validation"]
GRID_K = (2, 3, 5)
GRID_CAP_KM = (150.0, 300.0)
GRID_ALPHA = (0.11, 0.143, 0.2)
MIN_COVERED = 0.5
ZONES = ("fifty_hertz", "amprion", "tennet_de", "transnetbw")

# COMMAND ----------

# DBTITLE 1,Run identity
rid = ml_run_id()

# COMMAND ----------

# DBTITLE 1,Zone weather library
# MAGIC %run ../../_ml_zone_weather

# COMMAND ----------

# DBTITLE 1,Read inputs
sday = read_ml("features_station_day_wind", ecosystem=ECO).filter(
    F.col("local_date") <= VALID_TO
)
periods = read_gold("place_instrument_period", source="dwd")
heights = wind_height_periods(periods)
stations = station_table(
    read_shared_conformed("dim_place"),
    periods.filter(F.col("parameter_category").rlike(WIND_CATEGORY_PATTERN)),
).filter(~F.col("name").rlike(MOUNTAIN_STATION_PATTERN))

# COMMAND ----------

# DBTITLE 1,Onshore wind units in a control zone with a filled hub height
_gu = wind_units(read_gold("generation_unit", source=SOURCE)).filter(
    F.col("lifecycle_state").isin(*OPERATING_STATES)
    & F.col("commissioning_date").isNotNull()
    & F.col("capacity_net_kw").isNotNull()
    & F.col("latitude").isNotNull()
    & F.col("longitude").isNotNull()
    & ~F.col("is_offshore")
)
_zm = unit_zone_map(
    _gu,
    read_gold("grid_connection_point", source=SOURCE),
    read_ml("zone_code_map", ecosystem=ECO),
)
_hub = read_ml("features_hub_height_fill", ecosystem=ECO).select(
    "unit_id", F.col("hub_height_m_filled").alias("hub_height_m")
)
units = (
    _gu.select(
        "unit_id",
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

# COMMAND ----------

# DBTITLE 1,Capacity factor target per zone and day
_gen = (
    read_gold("energy_balance_component", source="smard")
    .filter(
        (F.col("component_kind") == "generation")
        & (F.col("carrier_code") == "onshore_wind")
        & (F.col("interval_reference") == "local_day_europe_berlin")
        & F.col("market_area_code").isin(*ZONES)
    )
    .groupBy("market_area_code", "local_date")
    .agg(F.sum("energy_mwh").alias("generation_mwh"))
)
_cap = read_ml("features_zone_capacity_asof", ecosystem=ECO).filter(
    F.col("carrier") == "wind_onshore"
)
capacity_factor = (
    _gen.join(_cap, ["market_area_code", "local_date"])
    .withColumn(
        "capacity_factor",
        F.col("generation_mwh") / (F.col("capacity_net_mw") * F.lit(24.0)),
    )
    .select("market_area_code", "local_date", "capacity_factor")
)

# COMMAND ----------


# DBTITLE 1,Helper -- score one parameter set
def score_parameters(k, cap_km, alpha):
    weights = unit_station_weights(units, stations, k=k, cap_km=cap_km)
    umonths = unit_months(units, FIT_FROM, VALID_TO)
    zw = zone_station_weights(umonths, weights, heights, alpha)
    zdaily = zone_wind_daily(sday, zw, min_covered=MIN_COVERED).join(
        capacity_factor, ["market_area_code", "local_date"]
    )
    part = F.when(F.col("local_date") <= F.lit(FIT_TO).cast("date"), "train").when(
        F.col("local_date").between(VALID_FROM, VALID_TO), "validation"
    )
    rows = (
        zdaily.withColumn("partition", part)
        .filter(
            F.col("partition").isNotNull()
            & F.col("wind_speed_cubed_adjusted_mean").isNotNull()
        )
        .groupBy("market_area_code", "partition")
        .agg(F.corr("wind_speed_cubed_adjusted_mean", "capacity_factor").alias("corr"))
        .collect()
    )

    def mean_corr(p):
        v = [r["corr"] for r in rows if r["partition"] == p and r["corr"] is not None]
        return sum(v) / len(v) if v else None

    return mean_corr("train"), mean_corr("validation")


# COMMAND ----------

# DBTITLE 1,Evaluate the parameter grid
_results = []
for k in GRID_K:
    for cap_km in GRID_CAP_KM:
        for alpha in GRID_ALPHA:
            tr, va = score_parameters(k, cap_km, alpha)
            _results.append((k, cap_km, alpha, tr, va))
            print(
                f"k={k} cap={cap_km} alpha={alpha}: train corr={tr} validation corr={va}"
            )

# COMMAND ----------

# DBTITLE 1,Stop with row counts when no parameter set scored
if all(r[3] is None for r in _results):
    _k, _cap, _alpha = GRID_K[0], GRID_CAP_KM[-1], GRID_ALPHA[0]
    _w = unit_station_weights(units, stations, k=_k, cap_km=_cap)
    _um = unit_months(units, FIT_FROM, VALID_TO)
    _zw = zone_station_weights(_um, _w, heights, _alpha)
    _zd = zone_wind_daily(sday, _zw, min_covered=MIN_COVERED)
    _counts = {
        "units": units.count(),
        "stations": stations.count(),
        "unit-station weights": _w.count(),
        "unit months": _um.count(),
        "zone station weights": _zw.count(),
        "station days": sday.count(),
        "capacity factor days": capacity_factor.count(),
        "zone wind days": _zd.count(),
        "zone wind days with capacity factor": _zd.join(
            capacity_factor, ["market_area_code", "local_date"]
        ).count(),
        "zone wind days with a wind value": _zd.filter(
            F.col("wind_speed_cubed_adjusted_mean").isNotNull()
        ).count(),
        "coverage share min / median": _zd.agg(
            F.min("covered_weight_share"),
            F.percentile_approx("covered_weight_share", 0.5),
        ).first(),
    }
    raise RuntimeError(f"no parameter set scored; row counts: {_counts}")

# COMMAND ----------

# DBTITLE 1,Select on the training score
_scored = [r for r in _results if r[3] is not None]
_best = max(_scored, key=lambda r: r[3])
rows = [
    (
        f"k{k}_cap{int(c)}_alpha{a}",
        k,
        c,
        a,
        MIN_COVERED,
        tr,
        va,
        (k, c, a) == _best[:3],
        now_utc(),
    )
    for k, c, a, tr, va in _results
]
out = spark.createDataFrame(
    rows,
    "parameter_set_id string, station_count int, distance_cap_km double, shear_exponent double, "
    "min_covered_weight double, train_correlation double, validation_correlation double, "
    "selected boolean, evaluated_at timestamp",
)

# COMMAND ----------

# DBTITLE 1,Gate
assert_unique_grain(
    out, ["parameter_set_id"], component=COMPONENT, source=SOURCE, rid=rid
)

# COMMAND ----------

# DBTITLE 1,Write
write_ml(out, TABLE, ecosystem=ECO, source=SOURCE, component=COMPONENT, rid=rid)

# COMMAND ----------

# DBTITLE 1,Report the selected parameters
print(
    f"selected: station_count={_best[0]} distance_cap_km={_best[1]} shear_exponent={_best[2]} train corr={_best[3]}"
)
