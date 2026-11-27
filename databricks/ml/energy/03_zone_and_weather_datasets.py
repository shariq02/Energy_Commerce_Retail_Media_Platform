# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # ML ENERGY -- ZONE GENERATION, WEATHER IMPUTATION AND SERIES DATASETS
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** November 2026
# MAGIC
# MAGIC **Purpose:** the zone weather method (the ERA5 cell-day wind and the DWD
# MAGIC station-day radiation tables, the parameter fit stored as imputer rows, the
# MAGIC zone wind and radiation, the acceptance test),
# MAGIC the zone generation dataset (family `unit`), the weather imputation and
# MAGIC series datasets (family `self_supervised_rl`) and their mask specification.
# MAGIC Run after the market notebook and the unit notebook (the calendar, the load
# MAGIC and Honda targets and the hub heights are read).

# COMMAND ----------

# DBTITLE 1,ML shared library
# MAGIC %run ../_ml_common

# COMMAND ----------

# DBTITLE 1,ML inspection library
# MAGIC %run ../_ml_inspect

# COMMAND ----------

# DBTITLE 1,ML build library
# MAGIC %run ../_ml_fit

# COMMAND ----------

# DBTITLE 1,Zone weather library
# MAGIC %run ../_ml_zone_weather

# COMMAND ----------

# DBTITLE 1,Configuration
ECO = "energy"
SOURCE = "mastr"
COMPONENT = "ml/energy/zone_and_weather"
DAY = "local_day_europe_berlin"
ZONES = ("fifty_hertz", "amprion", "tennet_de", "transnetbw")
CARRIERS = ["onshore_wind", "offshore_wind", "photovoltaic"]
DATE_FROM = SPLIT_CALENDARS["energy_daily"]["train"][0]
TRAIN_FROM, TRAIN_TO = SPLIT_CALENDARS["energy_daily"]["train"]
VALID_FROM, VALID_TO = SPLIT_CALENDARS["energy_daily"]["validation"]
# ERA5 cells: a unit takes its k nearest cells within the cap (the cell spacing is
# about 28 km); alpha is the shear exponent from 100 m to the hub height (0 = none)
GRID_K, GRID_CAP_KM, GRID_ALPHA = (1, 2, 3), (40.0, 80.0), (0.0, 0.11, 0.143, 0.2)
MIN_COVERED = 0.5
RADIATION_K, RADIATION_CAP_KM = 5, 500.0
OFFSHORE_K, OFFSHORE_CAP_KM, OFFSHORE_DECAY_KM = 2, 100.0, 25.0
# Declared before scoring: minimum validation correlation and minimum MAE
# improvement over the better of the two baselines.
ACCEPT = {
    "onshore_wind": {"corr_min": 0.75, "improvement_min": 0.10},
    "offshore_wind": {"corr_min": 0.60, "improvement_min": 0.10},
    "photovoltaic": {"corr_min": 0.80, "improvement_min": 0.10},
}
MASK_SEED, HELD_OUT_STATION_PERCENT = 42, 15
LENGTH_BUCKETS_HOURS = [1, 2, 3, 6, 12, 24, 48, 96]
FRAMES, BLOCKS = {}, {}

# COMMAND ----------

# DBTITLE 1,Run identity and session time zone
rid = ml_run_id()
set_utc_session()

# COMMAND ----------

# DBTITLE 1,Read Gold -- weather, ERA5 daily wind, places, units, generation
weather = read_gold("weather_observation", source="dwd")
daily = read_gold("weather_daily", source="era5")
coverage = read_shared_conformed("series_coverage")
dim_place = read_shared_conformed("dim_place")
cells = era5_cells(dim_place)
# the reanalysis wind has one height for every cell
heights = cells.select("place_key", "sensor_height_m")
_date_to = read_gold("market_price", source="smard").agg(F.max("local_date")).first()[0]
generation = (
    read_gold("energy_component", source="smard")
    .filter(
        (F.col("component_group") == "balance")
        & (F.col("component_kind") == "generation")
        & F.col("carrier_code").isin(*CARRIERS)
        & (F.col("interval_reference") == DAY)
        & F.col("market_area_code").isin(*ZONES)
    )
    .groupBy("market_area_code", "local_date", "carrier_code")
    .agg(F.sum("energy_mwh").alias("generation_mwh"))
)
units_all = read_gold("generation_unit", source=SOURCE)
connection_points = read_gold("grid_connection_point", source=SOURCE)
hub = read_ml("features_hub_height_fill", ecosystem=ECO)
calendar = read_ml("features_calendar_market_area", ecosystem=ECO)

# COMMAND ----------

# DBTITLE 1,Cell-day wind and station-day radiation -- two kept tables
write_stored_table(
    cell_day_wind(daily, DATE_FROM, str(_date_to)),
    "features_cell_day_wind",
    ECO,
    key_cols=["place_key", "local_date"],
    source="era5",
    component=f"{COMPONENT}/cell_day_wind",
    rid=rid,
    section="features",
)
write_stored_table(
    station_day_radiation(weather, DATE_FROM, str(_date_to)),
    "features_station_day_radiation",
    ECO,
    key_cols=["place_key", "local_date"],
    source="dwd",
    component=f"{COMPONENT}/station_day_radiation",
    rid=rid,
    section="features",
)
sday = read_ml("features_cell_day_wind", ecosystem=ECO)
rday = read_ml("features_station_day_radiation", ecosystem=ECO)

# COMMAND ----------

# DBTITLE 1,Wind units with a control zone, and the capacity as of each date
_wind = wind_units(units_all).filter(
    F.col("lifecycle_state").isin(*OPERATING_STATES)
    & F.col("commissioning_date").isNotNull()
    & F.col("capacity_net_kw").isNotNull()
)
_zones = unit_zone_map(_wind, connection_points, zone_code_frame())
_zoned = (
    _wind.join(_zones, "unit_id")
    .filter(F.col("zone_status") == "zoned")
    .withColumn(
        "carrier",
        F.when(F.col("is_offshore"), "wind_offshore").otherwise("wind_onshore"),
    )
)
_changes = _zoned.select(
    "market_area_code",
    "carrier",
    F.to_date("commissioning_date").alias("local_date"),
    (F.col("capacity_net_kw") / 1000).alias("delta_mw"),
    F.lit(1).alias("delta_units"),
).unionByName(
    _zoned.filter(F.col("final_decommissioning_date").isNotNull()).select(
        "market_area_code",
        "carrier",
        F.to_date("final_decommissioning_date").alias("local_date"),
        (-F.col("capacity_net_kw") / 1000).alias("delta_mw"),
        F.lit(-1).alias("delta_units"),
    )
)
_spine = (
    _changes.select("market_area_code", "carrier")
    .distinct()
    .crossJoin(
        spark.range(1).select(
            F.explode(F.sequence(F.lit(DATE_FROM).cast("date"), F.lit(_date_to))).alias(
                "local_date"
            )
        )
    )
    .withColumn("delta_mw", F.lit(0.0))
    .withColumn("delta_units", F.lit(0))
)
_keys = ["market_area_code", "carrier"]
_capacity_window = (
    Window.partitionBy(*_keys)
    .orderBy("local_date")
    .rowsBetween(Window.unboundedPreceding, 0)
)
capacity_asof = (
    _changes.unionByName(_spine)
    .groupBy(*_keys, "local_date")
    .agg(F.sum("delta_mw").alias("delta_mw"), F.sum("delta_units").alias("delta_units"))
    .withColumn("capacity_net_mw", F.sum("delta_mw").over(_capacity_window))
    .withColumn("unit_count", F.sum("delta_units").over(_capacity_window))
    .filter(F.col("local_date") >= F.lit(DATE_FROM).cast("date"))
    .select(*_keys, "local_date", "capacity_net_mw", "unit_count")
)
_total_kw = _wind.agg(F.sum("capacity_net_kw")).first()[0] or 0.0
_exclusions = [
    (
        r["zone_status"],
        r["n"],
        round((r["kw"] or 0.0) / 1000, 1),
        round(100 * (r["kw"] or 0.0) / _total_kw, 2),
    )
    for r in _wind.join(_zones, "unit_id")
    .groupBy("zone_status")
    .agg(F.count("*").alias("n"), F.sum("capacity_net_kw").alias("kw"))
    .collect()
]
BLOCKS["zone_generation"] = [
    (
        "zone_chain_exclusions",
        markdown_table(
            ["zone_status", "units", "capacity_mw", "share_percent"], _exclusions
        ),
    )
]

# COMMAND ----------

# DBTITLE 1,Units with a zone and a filled hub height, and the parameter grid
_located = _wind.filter(F.col("latitude").isNotNull() & F.col("longitude").isNotNull())
_zone_units = unit_zone_map(_located, connection_points, zone_code_frame())
units_zoned = (
    _located.select(
        "unit_id",
        "is_offshore",
        "latitude",
        "longitude",
        "capacity_net_kw",
        "commissioning_date",
        "final_decommissioning_date",
    )
    .join(_zone_units.filter(F.col("zone_status") == "zoned"), "unit_id")
    .join(
        hub.select("unit_id", F.col("hub_height_m_filled").alias("hub_height_m")),
        "unit_id",
    )
    .filter(F.col("hub_height_m").isNotNull())
)
onshore_units = units_zoned.filter(~F.col("is_offshore"))
offshore_units = units_zoned.filter(F.col("is_offshore"))
_gen_onshore = (
    generation.filter(F.col("carrier_code") == "onshore_wind")
    .join(
        capacity_asof.filter(F.col("carrier") == "wind_onshore"),
        ["market_area_code", "local_date"],
    )
    .withColumn(
        "capacity_factor",
        F.col("generation_mwh") / (F.col("capacity_net_mw") * F.lit(24.0)),
    )
    .select("market_area_code", "local_date", "capacity_factor")
)
_sday_fit = sday.filter(F.col("local_date") <= VALID_TO)


def score_parameters(k, cap_km, alpha):
    weights = unit_station_weights(onshore_units, cells, k=k, cap_km=cap_km)
    umonths = unit_months(onshore_units, TRAIN_FROM, VALID_TO)
    zw = zone_station_weights(umonths, weights, heights, alpha)
    zdaily = zone_wind_daily(_sday_fit, zw, min_covered=MIN_COVERED).join(
        _gen_onshore, ["market_area_code", "local_date"]
    )
    part = F.when(F.col("local_date") <= F.lit(TRAIN_TO).cast("date"), "train").when(
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

# DBTITLE 1,Fit the parameters on the training score -- stored as imputer rows
_results = []
for _k in GRID_K:
    for _cap in GRID_CAP_KM:
        for _alpha in GRID_ALPHA:
            _train, _valid = score_parameters(_k, _cap, _alpha)
            _results.append((_k, _cap, _alpha, _train, _valid))
            print(f"k={_k} cap={_cap} alpha={_alpha}: train={_train} valid={_valid}")
_scored = [r for r in _results if r[3] is not None]
if not _scored:
    raise RuntimeError("no parameter set scored; check the ERA5 cells and the units")
_best = max(_scored, key=lambda r: r[3])
K, CAP_KM, ALPHA = _best[0], _best[1], _best[2]
record_imputer(
    "zone_weather",
    ECO,
    fold_id=-2,
    rows=[
        (
            f"k{k}_cap{int(c)}_alpha{a}",
            "grid_search",
            {
                "cell_count": k,
                "distance_cap_km": c,
                "shear_exponent": a,
                "min_covered_weight": MIN_COVERED,
                "train_correlation": tr,
                "validation_correlation": va,
                "selected": (k, c, a) == _best[:3],
            },
        )
        for k, c, a, tr, va in _results
    ],
)
print(f"selected: k={K} cap_km={CAP_KM} alpha={ALPHA} train corr={_best[3]}")

# COMMAND ----------

# DBTITLE 1,Zone wind (onshore and offshore) and zone radiation per day
_umonths_on = unit_months(onshore_units, DATE_FROM, str(_date_to))
_weights_on = unit_station_weights(onshore_units, cells, k=K, cap_km=CAP_KM)
onshore = zone_wind_daily(
    sday,
    zone_station_weights(_umonths_on, _weights_on, heights, ALPHA),
    min_covered=MIN_COVERED,
)
_weights_off = unit_station_weights(
    offshore_units,
    cells,
    k=OFFSHORE_K,
    cap_km=OFFSHORE_CAP_KM,
    decay_km=OFFSHORE_DECAY_KM,
)
offshore = zone_wind_daily(
    sday,
    zone_station_weights(
        unit_months(offshore_units, DATE_FROM, str(_date_to)),
        _weights_off,
        heights,
        ALPHA,
    ),
    min_covered=MIN_COVERED,
)
_centroids = onshore_units.groupBy("market_area_code").agg(
    (
        F.sum(F.col("latitude") * F.col("capacity_net_kw")) / F.sum("capacity_net_kw")
    ).alias("centroid_lat"),
    (
        F.sum(F.col("longitude") * F.col("capacity_net_kw")) / F.sum("capacity_net_kw")
    ).alias("centroid_lon"),
)
_radiation_stations = dim_place.select(
    F.col("location_key").alias("place_key"), "latitude", "longitude"
).join(rday.select("place_key").distinct(), "place_key")
radiation = zone_radiation_daily(
    rday,
    _centroids,
    _radiation_stations.filter(
        F.col("latitude").isNotNull() & F.col("longitude").isNotNull()
    ),
    k=RADIATION_K,
    cap_km=RADIATION_CAP_KM,
)
_stacked = None
for _frame, _prefix, _names in (
    (
        onshore,
        "onshore_",
        ["wind_speed_adjusted_mean", "wind_speed_cubed_adjusted_mean"],
    ),
    (
        offshore,
        "offshore_",
        ["wind_speed_adjusted_mean", "wind_speed_cubed_adjusted_mean"],
    ),
    (radiation, "", ["global_radiation_mean_w_per_m2"]),
):
    for _name in _names:
        _part = _frame.select(
            "market_area_code",
            "local_date",
            F.lit(f"{_prefix}{_name}").alias("variable"),
            F.col(_name).alias("value"),
            "covered_weight_share",
            "stations_used",
        )
        _stacked = _part if _stacked is None else _stacked.unionByName(_part)
_stacked = _stacked.filter(F.col("market_area_code").isin(*ZONES))
_variables = [
    "onshore_wind_speed_adjusted_mean",
    "onshore_wind_speed_cubed_adjusted_mean",
    "offshore_wind_speed_adjusted_mean",
    "offshore_wind_speed_cubed_adjusted_mean",
    "global_radiation_mean_w_per_m2",
]
zone_weather = _stacked.groupBy("market_area_code", "local_date").agg(
    *[
        F.max(F.when(F.col("variable") == v, F.col("value"))).alias(v)
        for v in _variables
    ],
    F.min("covered_weight_share").alias("covered_weight_share"),
)

# COMMAND ----------

# DBTITLE 1,Acceptance test -- a linear fit on train, the error on validation
import numpy as np
import pandas as pd

_VARIABLE = {
    "onshore_wind": "onshore_wind_speed_cubed_adjusted_mean",
    "offshore_wind": "offshore_wind_speed_cubed_adjusted_mean",
    "photovoltaic": "global_radiation_mean_w_per_m2",
}
_CAP_CARRIER = {"onshore_wind": "wind_onshore", "offshore_wind": "wind_offshore"}
frames = {}
for _carrier, _variable in _VARIABLE.items():
    g = generation.filter(F.col("carrier_code") == _carrier)
    if _carrier in _CAP_CARRIER:
        c = capacity_asof.filter(F.col("carrier") == _CAP_CARRIER[_carrier]).select(
            "market_area_code", "local_date", "capacity_net_mw"
        )
        g = g.join(c, ["market_area_code", "local_date"]).withColumn(
            "target", F.col("generation_mwh") / (F.col("capacity_net_mw") * 24.0)
        )
    else:
        g = g.withColumn("target", F.col("generation_mwh"))
    v = zone_weather.select(
        "market_area_code", "local_date", F.col(_variable).alias("proxy")
    ).filter(F.col("proxy").isNotNull())
    nat = zone_weather.groupBy("local_date").agg(
        F.avg(_variable).alias("national_mean_proxy")
    )
    frames[_carrier] = (
        g.join(v, ["market_area_code", "local_date"]).join(nat, "local_date").toPandas()
    )


def fit_and_score(pdf, xcols):
    pdf = pdf.copy()
    d = pd.to_datetime(pdf["local_date"])
    pdf["trend"] = (d - pd.Timestamp(TRAIN_FROM)).dt.days / 365.25
    pdf["sin"] = np.sin(2 * np.pi * d.dt.dayofyear / 365.25)
    pdf["cos"] = np.cos(2 * np.pi * d.dt.dayofyear / 365.25)
    keep = pdf[[*xcols, "target"]].notna().all(axis=1)
    pdf, d = pdf[keep], d[keep]
    tr = pdf[d <= pd.Timestamp(TRAIN_TO)]
    va = pdf[(d >= pd.Timestamp(VALID_FROM)) & (d <= pd.Timestamp(VALID_TO))]
    if len(tr) < 50 or len(va) < 50:
        return None
    a = np.column_stack([tr[xcols].values, np.ones(len(tr))])
    b = np.column_stack([va[xcols].values, np.ones(len(va))])
    coef, *_ = np.linalg.lstsq(a, tr["target"].values, rcond=None)
    pred = b @ coef
    return {
        "mae": float(np.mean(np.abs(pred - va["target"].values))),
        "corr": float(np.corrcoef(pred, va["target"].values)[0, 1]),
        "n": len(va),
    }


_rows = []
for _carrier, pdf in frames.items():
    for zone in sorted(pdf["market_area_code"].unique()):
        z = pdf[pdf["market_area_code"] == zone]
        model = fit_and_score(
            z, ["proxy", "trend"] if _carrier == "photovoltaic" else ["proxy"]
        )
        calendar_fit = fit_and_score(z, ["trend", "sin", "cos"])
        national = fit_and_score(z, ["national_mean_proxy"])
        if not (model and calendar_fit and national):
            _rows.append((_carrier, zone, None, None, None, "insufficient data"))
            continue
        base = min(calendar_fit["mae"], national["mae"])
        improvement = (base - model["mae"]) / base if base else 0.0
        rule = ACCEPT[_carrier]
        ok = (
            model["corr"] >= rule["corr_min"] and improvement >= rule["improvement_min"]
        )
        _rows.append(
            (
                _carrier,
                zone,
                round(model["corr"], 3),
                round(model["mae"], 4),
                round(improvement, 3),
                "PASS" if ok else "FAIL",
            )
        )
        record_check(
            f"{COMPONENT}/{_carrier}/{zone}",
            "smard",
            "acceptance",
            ok,
            detail=f"corr={model['corr']:.3f} improvement={improvement:.3f}",
            rid=rid,
        )
for r in _rows:
    print(r)

# COMMAND ----------

# DBTITLE 1,Acceptance -- the verdict per carrier, and the findings
_verdict = {
    c: "PASS" if all(r[5] == "PASS" for r in _rows if r[0] == c) else "FAIL"
    for c in _VARIABLE
}
print(_verdict)
write_ml_findings(
    ECO,
    "features__zone_weather_acceptance",
    "zone_weather_acceptance",
    [
        (
            "declared_thresholds",
            markdown_table(
                ["carrier", "corr_min", "improvement_min"],
                [(c, r["corr_min"], r["improvement_min"]) for c, r in ACCEPT.items()],
            ),
        ),
        (
            "validation_scores",
            markdown_table(
                [
                    "carrier",
                    "zone",
                    "correlation",
                    "mae",
                    "improvement_over_baseline",
                    "status",
                ],
                _rows,
            ),
        ),
    ],
)

# COMMAND ----------

# DBTITLE 1,Zone generation -- the target, the zone weather, the capacity and the calendar
_candidates = generation.filter(F.col("carrier_code").isin(*CARRIERS))
_target_zone = (
    _candidates.select(
        "market_area_code",
        "local_date",
        F.col("carrier_code").alias("carrier"),
        F.col("generation_mwh").alias("target_generation_mwh"),
    )
    .filter(F.col("target_generation_mwh").isNotNull())
    .withColumn("provenance_tier", F.lit("sourced"))
)
_capacity_carrier = F.when(F.col("carrier") == "onshore_wind", "wind_onshore").when(
    F.col("carrier") == "offshore_wind", "wind_offshore"
)
zone_generation = (
    _target_zone.join(
        calendar.select(
            "market_area_code",
            "local_date",
            "day_of_week",
            "is_weekend",
            "month",
            "day_of_year",
            "is_holiday",
        ),
        ["market_area_code", "local_date"],
        "left",
    )
    .join(
        zone_weather.select(
            "market_area_code", "local_date", *_variables, "covered_weight_share"
        ),
        ["market_area_code", "local_date"],
        "left",
    )
    .withColumn("_capacity_carrier", _capacity_carrier)
    .join(
        capacity_asof.select(
            "market_area_code",
            "local_date",
            F.col("carrier").alias("_capacity_carrier"),
            "capacity_net_mw",
        ),
        ["market_area_code", "local_date", "_capacity_carrier"],
        "left",
    )
    .drop("_capacity_carrier")
    .withColumn(
        "trend_years",
        F.datediff("local_date", F.lit("2018-10-01").cast("date")) / 365.25,
    )
)
BLOCKS["zone_generation"].append(
    (
        "dropped_rows",
        markdown_table(
            ["rows_dropped"],
            [
                (
                    _candidates.groupBy(
                        "market_area_code", "local_date", "carrier_code"
                    )
                    .count()
                    .count()
                    - _target_zone.count(),
                )
            ],
        ),
    )
)
FRAMES["zone_generation"] = zone_generation

# COMMAND ----------

# DBTITLE 1,Weather imputation and series datasets
_hourly = weather.filter(
    (F.col("source_system") == "dwd")
    & (F.col("interval_seconds") == 3600)
    & (F.col("interval_reference") == "clock")
    & (F.col("local_date") >= F.lit("2018-10-01").cast("date"))
)
FRAMES["weather_imputation"] = _hourly.select(
    "location_key",
    "observation_timestamp_utc",
    "local_date",
    F.col("air_temperature_degc").alias("air_temperature"),
    F.col("air_temperature_dew_point_degc").alias("dew_point_temperature"),
    F.col("relative_humidity_percent").alias("relative_humidity"),
    F.col("pressure_station_hpa").alias("pressure_station"),
    F.col("pressure_sea_level_hpa").alias("pressure_sea_level"),
    F.expr(
        "coalesce(wind_mean_speed_m_per_s, wind_instant_speed_m_per_s, "
        "wind_unspecified_speed_m_per_s)"
    ).alias("wind_speed"),
    F.expr(
        "coalesce(wind_mean_direction_degrees, wind_instant_direction_degrees, "
        "wind_unspecified_direction_degrees)"
    ).alias("wind_direction"),
    F.col("cloud_cover_total_percent").alias("cloud_cover"),
    F.col("visibility_m").alias("visibility"),
    F.col("soil_temperature_5cm_degc").alias("soil_temperature"),
)
_load = read_ml("target_load", ecosystem=ECO)
_honda = read_ml("target_honda_increment", ecosystem=ECO).filter(
    F.col("interval_seconds") == 3600
)
FRAMES["self_supervised_other"] = _load.select(
    F.lit("daily_load").alias("series_family"),
    F.concat_ws("|", "market_area_code", "load_kind").alias("series_id"),
    F.col("local_date").cast("timestamp").alias("ts"),
    "local_date",
    F.col("target_value_mwh").alias("series_value"),
).unionByName(
    _honda.select(
        F.lit("honda_hourly").alias("series_family"),
        F.concat_ws("|", "location_key", "channel").alias("series_id"),
        F.col("interval_start_utc").alias("ts"),
        "local_date",
        F.col("target_increment").alias("series_value"),
    )
)

# COMMAND ----------

# DBTITLE 1,Build the datasets -- contract, split, fit and the family rows
build_datasets(FRAMES, rid=rid, source=SOURCE, blocks=BLOCKS)

# COMMAND ----------

# DBTITLE 1,Mask specification -- gap lengths, held-out stations, injected anomalies
import json

_gaps = (
    coverage.filter(F.col("record_kind") == "missing_value_period")
    .withColumn(
        "hours",
        (
            F.col("gap_end_timestamp").cast("long")
            - F.col("gap_start_timestamp").cast("long")
        )
        / 3600.0,
    )
    .filter(F.col("hours") > 0)
)
_bucket = F.lit(None).cast("int")
for _b in reversed(LENGTH_BUCKETS_HOURS):
    _bucket = F.when(F.col("hours") <= _b, F.lit(_b)).otherwise(_bucket)
_dist = (
    _gaps.withColumn("bucket", F.coalesce(_bucket, F.lit(LENGTH_BUCKETS_HOURS[-1])))
    .groupBy("bucket")
    .count()
    .collect()
)
_total = sum(r["count"] for r in _dist) or 1
weights = {int(r["bucket"]): r["count"] / _total for r in _dist}
_stations = [
    r[0]
    for r in read_dataset("weather_imputation")
    .select("location_key")
    .distinct()
    .collect()
]
_bucket_of = {
    r["location_key"]: r["b"]
    for r in spark.createDataFrame([(s,) for s in _stations], "location_key string")
    .select("location_key", user_hash_bucket(["location_key"]).alias("b"))
    .collect()
}
held_out = sorted(s for s, b in _bucket_of.items() if b < HELD_OUT_STATION_PERCENT)
print(f"declared gap length weights: {weights}")
print(f"{len(held_out)} of {len(_stations)} stations held out")
_cal = SPLIT_CALENDARS["energy_daily"]
_mask_rows = [
    (
        "weather_imputation",
        SPLIT_VERSION,
        MASK_SEED,
        length,
        float(weight),
        _cal["validation"][0],
        _cal["validation"][1],
        json.dumps(held_out),
    )
    for length, weight in sorted(weights.items())
]
_mask_rows += [
    (
        "self_supervised_other",
        SPLIT_VERSION,
        MASK_SEED,
        length,
        1.0 / 3,
        _cal["validation"][0],
        _cal["validation"][1],
        None,
    )
    for length in (24, 72, 168)
]
_mask_rows += [
    (
        "honda_anomaly",
        SPLIT_VERSION,
        MASK_SEED,
        length,
        1.0 / 3,
        SPLIT_CALENDARS["honda"]["validation"][0],
        SPLIT_CALENDARS["honda"]["validation"][1],
        json.dumps({"kinds": ["spike", "drop", "flatline"], "magnitude_sd": [3, 5]}),
    )
    for length in (1, 3, 24)
]
replace_rows(
    spark.createDataFrame(_mask_rows, REGISTRY_DDL["mask_specification"]),
    "mask_specification",
    ecosystem=ECO,
    predicate=(
        f"split_version = '{SPLIT_VERSION}' AND dataset_id IN "
        "('weather_imputation', 'self_supervised_other', 'honda_anomaly')"
    ),
)
register_waiting_datasets(ECO)
