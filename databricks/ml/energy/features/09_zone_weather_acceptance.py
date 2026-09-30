# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # ZONE WEATHER ACCEPTANCE
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** score the zone weather against the published zone generation series on the
# MAGIC validation year, against a calendar-plus-trend baseline and a national-
# MAGIC mean-station baseline, with and without the mountain stations. Thresholds
# MAGIC are declared before scoring.

# COMMAND ----------

# DBTITLE 1,ML shared library
# MAGIC %run ../../_ml_common

# COMMAND ----------

# DBTITLE 1,ML inspection library
# MAGIC %run ../../_ml_inspect

# COMMAND ----------

# DBTITLE 1,Configuration
ECO = "energy"
SOURCE = "smard"
COMPONENT = "ml/energy/features/zone_weather_acceptance"
TABLE = "zone_weather_acceptance"
TRAIN_FROM, TRAIN_TO = SPLIT_CALENDARS["energy_daily"]["train"]
VALID_FROM, VALID_TO = SPLIT_CALENDARS["energy_daily"]["validation"]
ZONES = ("fifty_hertz", "amprion", "tennet_de", "transnetbw")
# Declared before scoring: minimum validation correlation and minimum MAE
# improvement over the better of the two baselines.
ACCEPT = {
    "onshore_wind": {"corr_min": 0.75, "improvement_min": 0.10},
    "offshore_wind": {"corr_min": 0.60, "improvement_min": 0.10},
    "photovoltaic": {"corr_min": 0.80, "improvement_min": 0.10},
}

# COMMAND ----------

# DBTITLE 1,Run identity
rid = ml_run_id()

# COMMAND ----------

# DBTITLE 1,Zone weather library
# MAGIC %run ../../_ml_zone_weather

# COMMAND ----------

# DBTITLE 1,Read zone weather and the published zone series
zw = read_ml("features_zone_weather", ecosystem=ECO)
_gen = (
    read_gold("energy_balance_component", source="smard")
    .filter(
        (F.col("component_kind") == "generation")
        & F.col("carrier_code").isin("onshore_wind", "offshore_wind", "photovoltaic")
        & (F.col("interval_reference") == "local_day_europe_berlin")
        & F.col("market_area_code").isin(*ZONES)
    )
    .groupBy("market_area_code", "local_date", "carrier_code")
    .agg(F.sum("energy_mwh").alias("generation_mwh"))
)
_cap = read_ml("features_zone_capacity_asof", ecosystem=ECO)

# COMMAND ----------

# DBTITLE 1,Assemble one pandas frame per carrier (small)
import numpy as np
import pandas as pd

_VARIABLE = {
    "onshore_wind": "onshore_wind_speed_cubed_adjusted_mean",
    "offshore_wind": "offshore_wind_speed_cubed_adjusted_mean",
    "photovoltaic": "global_radiation_mean_w_per_m2",
}
_CAP_CARRIER = {"onshore_wind": "wind_onshore", "offshore_wind": "wind_offshore"}
frames = {}
for carrier, variable in _VARIABLE.items():
    g = _gen.filter(F.col("carrier_code") == carrier)
    if carrier in _CAP_CARRIER:
        c = _cap.filter(F.col("carrier") == _CAP_CARRIER[carrier]).select(
            "market_area_code", "local_date", "capacity_net_mw"
        )
        g = g.join(c, ["market_area_code", "local_date"]).withColumn(
            "target", F.col("generation_mwh") / (F.col("capacity_net_mw") * 24.0)
        )
    else:
        g = g.withColumn("target", F.col("generation_mwh"))
    v = zw.filter(F.col("variable") == variable).select(
        "market_area_code", "local_date", F.col("value").alias("proxy")
    )
    nat = (
        zw.filter(F.col("variable") == variable)
        .groupBy("local_date")
        .agg(F.avg("value").alias("national_mean_proxy"))
    )
    frames[carrier] = (
        g.join(v, ["market_area_code", "local_date"]).join(nat, "local_date").toPandas()
    )

# COMMAND ----------


# DBTITLE 1,Helper -- linear fit on train, error on validation
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


# COMMAND ----------

# DBTITLE 1,Score each carrier and zone
_rows = []
for carrier, pdf in frames.items():
    for zone in sorted(pdf["market_area_code"].unique()):
        z = pdf[pdf["market_area_code"] == zone]
        model = fit_and_score(
            z, ["proxy", "trend"] if carrier == "photovoltaic" else ["proxy"]
        )
        calendar = fit_and_score(z, ["trend", "sin", "cos"])
        national = fit_and_score(z, ["national_mean_proxy"])
        if not (model and calendar and national):
            _rows.append((carrier, zone, None, None, None, "insufficient data"))
            continue
        base = min(calendar["mae"], national["mae"])
        improvement = (base - model["mae"]) / base if base else 0.0
        rule = ACCEPT[carrier]
        ok = (
            model["corr"] >= rule["corr_min"] and improvement >= rule["improvement_min"]
        )
        _rows.append(
            (
                carrier,
                zone,
                round(model["corr"], 3),
                round(model["mae"], 4),
                round(improvement, 3),
                "PASS" if ok else "FAIL",
            )
        )
        record_check(
            f"{COMPONENT}/{carrier}/{zone}",
            SOURCE,
            "acceptance",
            ok,
            detail=f"corr={model['corr']:.3f} improvement={improvement:.3f}",
            rid=rid,
        )
for r in _rows:
    print(r)

# COMMAND ----------

# DBTITLE 1,Verdict per carrier
_verdict = {}
for carrier in _VARIABLE:
    statuses = [r[5] for r in _rows if r[0] == carrier]
    _verdict[carrier] = (
        "PASS" if statuses and all(s == "PASS" for s in statuses) else "FAIL"
    )
for carrier, v in _verdict.items():
    print(
        f"{carrier}: {v}"
        + (
            "  -> report unsupported"
            if v == "FAIL" and carrier == "offshore_wind"
            else ""
        )
    )

# COMMAND ----------

# DBTITLE 1,Onshore wind with the mountain stations included
_p = read_ml("zone_weather_parameters", ecosystem=ECO).filter(F.col("selected")).first()
_k, _cap_km, _alpha = (
    int(_p["station_count"]),
    float(_p["distance_cap_km"]),
    float(_p["shear_exponent"]),
)
_periods = read_gold("place_instrument_period", source="dwd")
_heights = wind_height_periods(_periods)
_st_all = station_table(
    read_shared_conformed("dim_place"),
    _periods.filter(F.col("parameter_category").rlike(WIND_CATEGORY_PATTERN)),
)
_gu = wind_units(read_gold("generation_unit", source="mastr")).filter(
    F.col("lifecycle_state").isin(*OPERATING_STATES)
    & F.col("commissioning_date").isNotNull()
    & F.col("capacity_net_kw").isNotNull()
    & F.col("latitude").isNotNull()
    & F.col("longitude").isNotNull()
    & ~F.col("is_offshore")
)
_zm = unit_zone_map(
    _gu,
    read_gold("grid_connection_point", source="mastr"),
    read_ml("zone_code_map", ecosystem=ECO),
)
_hub = read_ml("features_hub_height_fill", ecosystem=ECO).select(
    "unit_id", F.col("hub_height_m_filled").alias("hub_height_m")
)
_units = (
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
)
_sday = read_ml("features_station_day_wind", ecosystem=ECO)
_end = _sday.agg(F.max("local_date")).first()[0]
_w = unit_station_weights(_units, _st_all, k=_k, cap_km=_cap_km)
_zw = zone_station_weights(
    unit_months(_units, TRAIN_FROM, str(_end)), _w, _heights, _alpha
)
with_mountain = (
    zone_wind_daily(_sday, _zw)
    .select(
        "market_area_code",
        "local_date",
        F.col("wind_speed_cubed_adjusted_mean").alias("proxy_mountain"),
    )
    .toPandas()
)

# COMMAND ----------

# DBTITLE 1,Compare with and without the mountain stations
_base = frames["onshore_wind"]
_cmp = []
for zone in sorted(_base["market_area_code"].unique()):
    z = _base[_base["market_area_code"] == zone].merge(
        with_mountain[with_mountain["market_area_code"] == zone][
            ["local_date", "proxy_mountain"]
        ],
        on="local_date",
        how="left",
    )
    excluded = fit_and_score(z, ["proxy"])
    z = z.assign(proxy=z["proxy_mountain"])
    included = fit_and_score(z, ["proxy"])
    if excluded and included:
        _cmp.append(
            (
                zone,
                round(excluded["corr"], 3),
                round(included["corr"], 3),
                round(excluded["mae"], 4),
                round(included["mae"], 4),
            )
        )
for r in _cmp:
    print(r)

# COMMAND ----------

# DBTITLE 1,Export findings
_blocks = [
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
    (
        "mountain_stations",
        markdown_table(
            ["zone", "corr_excluded", "corr_included", "mae_excluded", "mae_included"],
            _cmp,
        ),
    ),
]
write_ml_findings(ECO, "features__" + TABLE, TABLE, _blocks)
