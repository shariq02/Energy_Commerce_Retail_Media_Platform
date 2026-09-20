# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # EDA -- SMARD ENERGY TIME SERIES
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** Profile smard_energy_timeseries (one long-format Bronze
# MAGIC table: metric / filter_id / region / resolution / timestamp_utc / value)
# MAGIC -- schema, missingness, constant columns, metric x region x resolution
# MAGIC coverage, per-series temporal continuity against an independent calendar,
# MAGIC value ranges and sign, exact-copy / sign-mirror metrics (a forecast
# MAGIC series that is the negative of another), per-(series, ts) duplicates
# MAGIC (identical vs conflicting), and the layered modelling-risk checklist --
# MAGIC as evidence for Silver design.

# COMMAND ----------

# DBTITLE 1,Imports
import numpy as np
import pandas as pd
from pyspark.sql import Window
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %run ../_eda_common

# COMMAND ----------

# DBTITLE 1,Configuration
CATALOG = "energy_commerce_retail_media"
BRONZE_SCHEMA = "bronze"
SOURCE = "smard"
NB_KEY = "01_smard"
SECTION_TITLE = "SMARD energy time series (smard_energy_timeseries)"
TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.smard_energy_timeseries"
SERIES_KEY = ["metric", "filter_id", "region", "resolution"]
# SMARD publishes on Europe/Berlin wall-clock; the Bronze column is named
# timestamp_utc, so the loader is expected to have converted -- flagged for
# verification in Temporal Semantics.
TS_TZ = "UTC (per column name; SMARD source is Europe/Berlin -- verify the load)"

RESOLUTION_SECONDS = {
    "quarterhour": 900,
    "quarter_hour": 900,
    "15min": 900,
    "hour": 3600,
    "hourly": 3600,
    "day": 86400,
    "daily": 86400,
    "week": 604800,
}

# COMMAND ----------

# DBTITLE 1,Helpers -- SMARD timestamp (epoch-ms / epoch-s / ISO) + resolution step

def as_ts(col):
    # SMARD timestamps arrive as ISO, epoch-ms or epoch-s. coalesce evaluates
    # every branch, so the numeric branches use safe_num (an ANSI-safe .cast
    # would otherwise throw on the ISO string).
    c = F.col(col).cast("string")
    n = safe_num(col)
    return F.coalesce(
        F.try_to_timestamp(c),
        (n / 1000).cast("timestamp"),
        n.cast("timestamp"),
    )


def step_col():
    e = F.lit(None).cast("long")
    for label, secs in RESOLUTION_SECONDS.items():
        e = F.when(F.lower(F.col("resolution")) == label, F.lit(secs)).otherwise(e)
    return e


# COMMAND ----------

# DBTITLE 1,Validate profiling export path
REPO_ROOT = _repo_root()
PROFILING_DIR = _profiling_dir()
print(f"OK  repo root: {REPO_ROOT}")
print(f"OK  profiling directory: {PROFILING_DIR}")

# COMMAND ----------

# DBTITLE 1,Profile -- rows, missingness, approx distinct, constant columns (one agg)
df = spark.table(TABLE)
COLS = df.columns
exprs = [F.count(F.lit(1)).alias("__rows")]
for c in COLS:
    miss = F.col(c).isNull() | (F.trim(F.col(c).cast("string")) == "")
    exprs += [
        F.sum(miss.cast("long")).alias(c + "__m"),
        F.approx_count_distinct(c).alias(c + "__d"),
    ]
r = df.agg(*exprs).first().asDict()
total = r["__rows"]
acd = {c: r[c + "__d"] for c in COLS}
constant_cands = [c for c in COLS if acd[c] <= 1]
constant_cols = (
    sorted(
        c
        for c in constant_cands
        if (df.agg(F.countDistinct(F.col(c)).alias(c)).first()[c] or 0) <= 1
    )
    if constant_cands
    else []
)
distinct_rows = df.distinct().count()
print(f"rows={total}  columns={len(COLS)}  ->  {COLS}")
for c in COLS:
    print(
        f"  {c:<14} missing={r[c + '__m']:>12} "
        f"rate={r[c + '__m'] / total:.4f} approx_distinct={acd[c]}"
    )
print("constant columns (exact):", constant_cols)
print("exact full-row duplicates:", total - distinct_rows)

# COMMAND ----------

# DBTITLE 1,metric x region x resolution coverage (one groupBy -> all marginals)
combos = (
    df.groupBy("metric", "region", "resolution")
    .agg(
        F.count(F.lit(1)).alias("rows"),
        F.approx_count_distinct("filter_id").alias("filter_ids"),
    )
    .collect()
)
present = {(x["metric"], x["region"], x["resolution"]) for x in combos}
metrics = sorted({x["metric"] for x in combos})
regions = sorted({x["region"] for x in combos})
resolutions = sorted({x["resolution"] for x in combos})
dist = {c: {} for c in ("metric", "region", "resolution")}
for x in combos:
    for c in ("metric", "region", "resolution"):
        dist[c][x[c]] = dist[c].get(x[c], 0) + x["rows"]
dist = {c: sorted(v.items(), key=lambda p: -p[1]) for c, v in dist.items()}
missing_combos = [
    (m, rg, rs)
    for m in metrics
    for rg in regions
    for rs in resolutions
    if (m, rg, rs) not in present
]
metric_regions = {m: sorted({rg for mm, rg, rs in present if mm == m}) for m in metrics}
metric_res = {m: sorted({rs for mm, rg, rs in present if mm == m}) for m in metrics}
print(
    f"metrics={len(metrics)} regions={len(regions)} resolutions={len(resolutions)}  "
    f"present={len(present)}  absent={len(missing_combos)}"
)

# COMMAND ----------

# DBTITLE 1,Distinct series + per-(series, ts) duplicates identical vs conflicting (one groupBy)
dk = df.groupBy(*SERIES_KEY, "timestamp_utc").agg(
    F.count(F.lit(1)).alias("n"),
    F.countDistinct("value").alias("value_variants"),
)
db = (
    dk.agg(
        F.count(F.lit(1)).alias("series_ts_keys"),
        F.sum((F.col("n") > 1).cast("long")).alias("dup_groups"),
        F.sum(((F.col("n") > 1) & (F.col("value_variants") == 1)).cast("long")).alias(
            "identical"
        ),
        F.sum(((F.col("n") > 1) & (F.col("value_variants") > 1)).cast("long")).alias(
            "conflicting"
        ),
    )
    .first()
    .asDict()
)
series = (
    df.groupBy(*SERIES_KEY)
    .agg(
        F.count(F.lit(1)).alias("rows"),
        F.min("timestamp_utc").alias("min_ts"),
        F.max("timestamp_utc").alias("max_ts"),
        F.countDistinct("timestamp_utc").alias("distinct_ts"),
    )
    .orderBy(*SERIES_KEY)
    .collect()
)
series_rows = [("|".join(str(x[k]) for k in SERIES_KEY), x["rows"]) for x in series]
print(f"distinct series = {len(series)}  (series,ts) dups: {db}")

# COMMAND ----------

# DBTITLE 1,Value stats per metric (one groupBy)
v = safe_num("value")
bm = (
    df.groupBy("metric")
    .agg(
        F.min(v).alias("min"),
        F.max(v).alias("max"),
        F.avg(v).alias("mean"),
        F.stddev(v).alias("sd"),
        F.percentile_approx(v, [0.01, 0.25, 0.5, 0.75, 0.99]).alias("p01_25_50_75_99"),
        F.sum((v == 0).cast("long")).alias("zero_rows"),
        F.sum((v < 0).cast("long")).alias("negative_rows"),
        F.sum(
            (F.col("value").isNull() | (F.trim(F.col("value")) == "")).cast("long")
        ).alias("missing"),
        F.sum(
            (
                F.col("value").isNotNull() & (F.trim(F.col("value")) != "") & v.isNull()
            ).cast("long")
        ).alias("non_numeric"),
        F.approx_count_distinct("filter_id").alias("filter_ids"),
        F.approx_count_distinct("region").alias("regions"),
    )
    .collect()
)
by_metric = {x["metric"]: x.asDict() for x in bm}
for m, x in by_metric.items():
    print(m, x)

# COMMAND ----------

# DBTITLE 1,Exact-copy / sign-mirror metrics (a forecast series that is -1x another)
metric_stats = {
    m: {"mean": x["mean"], "sd": x["sd"], "min": x["min"], "max": x["max"]}
    for m, x in by_metric.items()
    if x["sd"] is not None
}
mirrors = mirror_columns(metric_stats)
print("mirror / duplicate metric pairs:")
for a, b, why in mirrors:
    print(f"  {a}  <->  {b}  : {why}")

# COMMAND ----------

# DBTITLE 1,5-sigma outliers per metric (one agg using collected mean/sd)
oe = [
    F.sum(
        ((F.col("metric") == m) & (F.abs(v - x["mean"]) > 5 * x["sd"])).cast("long")
    ).alias(m)
    for m, x in by_metric.items()
    if x["sd"] and x["sd"] > 0
]
outliers = df.agg(*oe).first().asDict() if oe else {}
print("rows beyond 5 sigma per metric:", outliers)

# COMMAND ----------

# DBTITLE 1,Per-series temporal continuity vs an INDEPENDENT calendar (one windowed pass)
w = Window.partitionBy(*SERIES_KEY).orderBy("ts")
sd_rows = (
    df.select(*SERIES_KEY, as_ts("timestamp_utc").alias("ts"), step_col().alias("step"))
    .where(F.col("ts").isNotNull() & F.col("step").isNotNull())
    .distinct()
    .withColumn(
        # Round before subtracting 1: SMARD day/hour timestamps carry DST jumps
        # (25h / 23h steps) that otherwise yield fractional "missing steps".
        "gap_steps",
        F.round(
            (F.col("ts").cast("long") - F.lag("ts").over(w).cast("long"))
            / F.col("step")
        )
        - 1,
    )
    .groupBy(*SERIES_KEY, "step")
    .agg(
        F.min("ts").alias("min_ts"),
        F.max("ts").alias("max_ts"),
        F.count(F.lit(1)).alias("observed"),
        F.max(F.when(F.col("gap_steps") > 0, F.col("gap_steps")))
        .cast("long")
        .alias("longest_gap"),
        F.sum(F.when(F.col("gap_steps") > 0, F.col("gap_steps")).otherwise(0))
        .cast("long")
        .alias("missing_steps"),
    )
).collect()
continuity = []
for x in sd_rows:
    exp = round((x["max_ts"].timestamp() - x["min_ts"].timestamp()) / x["step"]) + 1
    continuity.append(
        {
            "series": "|".join(str(x[k]) for k in SERIES_KEY),
            "resolution": x["resolution"],
            "observed": x["observed"],
            "expected": exp,
            "coverage_pct": round(x["observed"] / exp * 100, 2) if exp else None,
            "longest_gap": x["longest_gap"] or 0,
            "missing_steps": x["missing_steps"] or 0,
        }
    )
for c in continuity:
    print(c)

# COMMAND ----------

# DBTITLE 1,Temporal activity -- rows per year (one groupBy)
ym = (
    df.groupBy(F.year(as_ts("timestamp_utc")).alias("year"))
    .count()
    .orderBy("year")
    .collect()
)
# F.year(...) is NULL for any unparsed timestamp -> drop that key so sorting
# and min() over the years never hit None vs int.
by_year = {x["year"]: x["count"] for x in ym if x["year"] is not None}
print("by year:", sorted(by_year.items()))

# COMMAND ----------

# DBTITLE 1,Categorical / domain validation -- resolution + region against known sets
KNOWN_REGIONS = {
    "DE",
    "DE-LU",
    "AT",
    "LU",
    "50Hertz",
    "Amprion",
    "TenneT",
    "TransnetBW",
    "DE-AT-LU",
}
res_domain = categorical_domain(
    df, "resolution", RESOLUTION_SECONDS.keys(), name="resolution"
)
region_domain = categorical_domain(df, "region", KNOWN_REGIONS, name="region")
print("resolution domain:", res_domain)
print("region domain:", region_domain)

# COMMAND ----------

# DBTITLE 1,Temporal consistency -- does every forecast metric have a realised counterpart on the same grid?
forecast_metrics = [m for m in metrics if str(m).startswith("forecast_")]
forecast_pairing = []
for fm in forecast_metrics:
    rm = fm[len("forecast_") :]
    realised = (
        rm
        if rm in metrics
        else next((m for m in metrics if rm in str(m) and m != fm), None)
    )
    if realised is None:
        forecast_pairing.append(
            {"forecast": fm, "realised": None, "note": "no realised metric by name"}
        )
        continue
    fk = df.where(F.col("metric") == fm).select(*SERIES_KEY, "timestamp_utc")
    rk = df.where(F.col("metric") == realised).select(
        *[F.col(k).alias(k) for k in ("filter_id", "region", "resolution")],
        "timestamp_utc",
    )
    fk2 = fk.select("region", "resolution", "timestamp_utc").distinct()
    matched = fk2.join(
        rk.select("region", "resolution", "timestamp_utc").distinct(),
        on=["region", "resolution", "timestamp_utc"],
        how="left_semi",
    ).count()
    total_fk = fk2.count()
    forecast_pairing.append(
        {
            "forecast": fm,
            "realised": realised,
            "forecast_points": total_fk,
            "with_realised_same_grid": matched,
            "coverage_pct": round(matched / total_fk * 100, 2) if total_fk else None,
        }
    )
    print(forecast_pairing[-1])

# COMMAND ----------

# DBTITLE 1,Physical consistency -- residual_load against load, wind and solar (variants tested)

def _pick(*names):
    return next((m for m in names if m in metrics), None)


m_resid = _pick("residual_load")
m_load = _pick("total_power_consumption", "total_load", "grid_load")
m_on = _pick("generation_onshore_wind")
m_off = _pick("generation_offshore_wind")
m_pv = _pick("generation_photovoltaic")
m_ps = _pick("pumped_storage_consumption")
print(
    f"resid={m_resid} load={m_load} onshore={m_on} offshore={m_off} pv={m_pv} pumped={m_ps}"
)
identity_variants, identity_levels = [], []
if m_resid and m_load and m_pv and (m_on or m_off):
    wind = [m for m in (m_on, m_off) if m]
    variants = {
        "load - wind (on + off) - pv": (
            [m_load, *wind, m_pv],
            [1] + [-1] * (len(wind) + 1),
        ),
    }
    if m_on and m_off:
        variants["load - onshore wind - pv"] = ([m_load, m_on, m_pv], [1, -1, -1])
    if m_ps:
        variants["load + pumped-storage consumption - wind - pv"] = (
            [m_load, m_ps, *wind, m_pv],
            [1, 1] + [-1] * (len(wind) + 1),
        )
    cols_needed = sorted({m_resid, *[c for cs, _ in variants.values() for c in cs]})
    piv_all = (
        df.where((F.col("region") == "DE-LU") & F.col("metric").isin(cols_needed))
        .groupBy("resolution", "timestamp_utc")
        .pivot("metric", cols_needed)
        .agg(F.first(safe_num("value")))
    )
    identity_levels = (
        piv_all.groupBy("resolution")
        .agg(*[F.avg(F.col(f"`{c}`")).alias(c) for c in cols_needed])
        .collect()
    )
    identity_levels = [x.asDict() for x in identity_levels]
    for res in [r_ for r_ in resolutions if r_ in metric_res.get(m_resid, [])]:
        piv_r = piv_all.where(F.col("resolution") == res)
        for name, (cs, sg) in variants.items():
            ok = F.lit(True)
            for c in [m_resid, *cs]:
                ok = ok & F.col(f"`{c}`").isNotNull()
            out = additive_identity_check(
                piv_r.where(ok), m_resid, cs, signs=sg, rel_tol=0.02, abs_floor=100.0
            )
            out.update({"variant": name, "resolution": res})
            identity_variants.append(out)
            print(res, name, out)
identity_best = min(
    (x for x in identity_variants if x["violation_pct"] is not None),
    key=lambda x: x["violation_pct"],
    default=None,
)

# COMMAND ----------

# DBTITLE 1,Day-resolution timestamps -- hour of day of the stored value
day_hours = (
    df.where(F.col("resolution") == "day")
    .groupBy(F.hour(as_ts("timestamp_utc")).alias("hour"))
    .count()
    .orderBy("hour")
    .limit(30)
    .collect()
    if "day" in resolutions
    else []
)
day_hours = [(x["hour"], x["count"]) for x in day_hours]
print(day_hours)

# COMMAND ----------

# DBTITLE 1,DE-LU daily series -- collected once for pattern analysis
PATTERN_METRICS = [
    m
    for m in (
        "total_power_consumption",
        "residual_load",
        "day_ahead_prices",
        "generation_onshore_wind",
        "generation_offshore_wind",
        "generation_photovoltaic",
        "generation_biomass",
        "generation_hydro",
        "generation_lignite",
        "generation_hard_coal",
        "generation_natural_gas",
        "generation_nuclear",
    )
    if m in metrics
]
FORECAST_PAIRS = [
    (f, f[len("forecast_") :])
    for f in metrics
    if str(f).startswith("forecast_") and f[len("forecast_") :] in metrics
]
day_metrics = sorted({*PATTERN_METRICS, *[m for pair in FORECAST_PAIRS for m in pair]})
daily = pd.DataFrame()
if day_metrics and "day" in resolutions:
    daily = (
        df.where(
            (F.col("region") == "DE-LU")
            & (F.col("resolution") == "day")
            & F.col("metric").isin(day_metrics)
        )
        .select(
            F.to_date(
                F.from_utc_timestamp(as_ts("timestamp_utc"), "Europe/Berlin")
            ).alias("day"),
            "metric",
            safe_num("value").alias("value"),
        )
        .groupBy("day")
        .pivot("metric", day_metrics)
        .agg(F.avg("value"))
        .orderBy("day")
        .toPandas()
        .set_index("day")
    )
    daily.index = pd.to_datetime(daily.index)
print(daily.shape)

# COMMAND ----------

# DBTITLE 1,Seasonal, weekday and annual profiles (DE-LU, day resolution)
profiles = {"month": {}, "weekday": {}, "year": {}}
if not daily.empty:
    for key, grouper in (
        ("month", daily.index.month),
        ("weekday", daily.index.dayofweek),
        ("year", daily.index.year),
    ):
        g = daily[PATTERN_METRICS].groupby(grouper).mean()
        profiles[key] = {
            m: [(int(i), round(float(v), 1)) for i, v in g[m].items() if pd.notna(v)]
            for m in PATTERN_METRICS
        }
print({k: list(v) for k, v in profiles.items()})

# COMMAND ----------

# DBTITLE 1,Persistence -- lag-1 and lag-7 autocorrelation of the daily series
autocorr = {}
for m in PATTERN_METRICS:
    sr = daily[m].dropna() if m in daily else pd.Series(dtype=float)
    if len(sr) > 30:
        autocorr[m] = (round(float(sr.autocorr(1)), 3), round(float(sr.autocorr(7)), 3))
print(autocorr)

# COMMAND ----------

# DBTITLE 1,Diurnal profile (DE-LU, quarter-hour, Europe/Berlin hour)
DIURNAL = [
    m
    for m in (
        "total_power_consumption",
        "residual_load",
        "day_ahead_prices",
        "generation_onshore_wind",
        "generation_offshore_wind",
        "generation_photovoltaic",
    )
    if m in metrics and "quarterhour" in metric_res.get(m, [])
]
diurnal, neg_share_by_hour = {}, {}
if DIURNAL:
    hh = F.hour(F.from_utc_timestamp(as_ts("timestamp_utc"), "Europe/Berlin"))
    rows = (
        df.where(
            (F.col("region") == "DE-LU")
            & (F.col("resolution") == "quarterhour")
            & F.col("metric").isin(DIURNAL)
        )
        .groupBy("metric", hh.alias("h"))
        .agg(
            F.avg(safe_num("value")).alias("mean"),
            F.avg((safe_num("value") < 0).cast("double")).alias("neg_share"),
        )
        .orderBy("metric", "h")
        .collect()
    )
    for x in rows:
        diurnal.setdefault(x["metric"], []).append((x["h"], round(x["mean"], 1)))
        if x["neg_share"]:
            neg_share_by_hour.setdefault(x["metric"], []).append(
                (x["h"], round(x["neg_share"], 3))
            )
print({m: len(v) for m, v in diurnal.items()})

# COMMAND ----------

# DBTITLE 1,Price against load, wind, solar and residual load (day resolution)
rel = {"corr": None, "spearman": None, "by_year": {}, "residual_deciles": [], "n": 0}
if not daily.empty and "day_ahead_prices" in daily and m_load:
    wind_cols = [c for c in (m_on, m_off) if c]
    tmp = pd.DataFrame(
        {
            "price": daily["day_ahead_prices"],
            "load": daily[m_load],
            "wind": daily[wind_cols].sum(axis=1, min_count=len(wind_cols))
            if wind_cols
            else np.nan,
            "solar": daily[m_pv] if m_pv else np.nan,
            "residual": daily[m_resid] if m_resid else np.nan,
        }
    )
    tmp = tmp.dropna(axis=1, how="all").dropna()
    rel["n"] = len(tmp)
    if len(tmp) > 30:
        rel["corr"] = {
            c: round(float(tmp["price"].corr(tmp[c])), 3)
            for c in tmp.columns
            if c != "price"
        }
        rel["spearman"] = {
            c: round(float(tmp["price"].corr(tmp[c], method="spearman")), 3)
            for c in tmp.columns
            if c != "price"
        }
        for y, g in tmp.groupby(tmp.index.year):
            if len(g) > 30:
                rel["by_year"][int(y)] = {
                    c: round(float(g["price"].corr(g[c])), 3)
                    for c in tmp.columns
                    if c != "price"
                }
        q = (
            pd.qcut(tmp["residual"], 10, duplicates="drop")
            if "residual" in tmp
            else None
        )
        rel["residual_deciles"] = (
            [
                (str(k), round(float(v), 1))
                for k, v in tmp.groupby(q, observed=True)["price"].mean().items()
            ]
            if q is not None
            else []
        )
print(rel)

# COMMAND ----------

# DBTITLE 1,Forecast against realised values (day resolution, DE-LU)
forecast_acc = []
for f, rm in FORECAST_PAIRS:
    if f in daily and rm in daily:
        pair = daily[[f, rm]].dropna()
        if len(pair) > 30:
            err = pair[f] - pair[rm]
            base = pair[rm].abs().mean()
            forecast_acc.append(
                {
                    "forecast": f,
                    "realised": rm,
                    "n": len(pair),
                    "corr": round(float(pair[f].corr(pair[rm])), 3),
                    "bias": round(float(err.mean()), 1),
                    "mae": round(float(err.abs().mean()), 1),
                    "mae_pct_of_mean": round(float(err.abs().mean() / base * 100), 1)
                    if base
                    else None,
                }
            )
print(forecast_acc)

# COMMAND ----------

# DBTITLE 1,Generation mix by year (share of summed realised generation, DE-LU day resolution)
GEN = [m for m in PATTERN_METRICS if m.startswith("generation_")]
mix_by_year = {}
if GEN and not daily.empty:
    sums = daily[GEN].groupby(daily.index.year).sum(min_count=1)
    shares = sums.div(sums.sum(axis=1), axis=0)
    for y, row in shares.iterrows():
        mix_by_year[int(y)] = {
            m.replace("generation_", ""): round(float(v), 3)
            for m, v in row.items()
            if pd.notna(v) and v > 0
        }
print(mix_by_year)

# COMMAND ----------

# DBTITLE 1,Regime evidence -- metric availability + total volume per year (generation-mix shift)
my = (
    df.groupBy(F.year(as_ts("timestamp_utc")).alias("year"), "metric")
    .agg(
        F.count(F.lit(1)).alias("rows"),
        F.sum(safe_num("value")).alias("sum_value"),
    )
    .collect()
)
metric_years = {}
for x in my:
    if x["year"] is None:
        continue
    metric_years.setdefault(x["metric"], {})[x["year"]] = (x["rows"], x["sum_value"])
metric_first_year = {m: min(yr) for m, yr in metric_years.items() if yr}
print(
    "metric first-seen year:",
    sorted(metric_first_year.items(), key=lambda p: p[1] or 0),
)

# COMMAND ----------

# DBTITLE 1,Samples -- value histogram + ONE representative series per metric (not mixed)
value_pdf = (
    df.select("metric", v.alias("value"))
    .where(F.col("value").isNotNull())
    .sample(0.1, seed=42)
    .limit(200_000)
    .toPandas()
)
# Pick the highest-resolution series per metric so the time-series figure plots
# a SINGLE (metric, filter_id, region, resolution) series, not several
# interleaved -- the earlier "per metric" plot mixed day + quarterhour + regions.
rep_series = {}
for x in series:
    m = x["metric"]
    cur = rep_series.get(m)
    if cur is None or (x["distinct_ts"] or 0) > (cur["distinct_ts"] or 0):
        rep_series[m] = x
ts_pdf = {}
for m, s in rep_series.items():
    cond = F.lit(True)
    for k in SERIES_KEY:
        cond = cond & (F.col(k) == s[k])
    ts_pdf[m] = (
        df.where(cond)
        .select("timestamp_utc", v.alias("value"))
        .orderBy(as_ts("timestamp_utc"))
        .limit(4000)
        .toPandas()
    )

# COMMAND ----------

# DBTITLE 1,Figure -- series overview (rows per metric / year, coverage, longest gap)
figs = []
_cov_pairs = [
    (c["series"], c["coverage_pct"])
    for c in continuity
    if c["coverage_pct"] is not None and c["coverage_pct"] < 100
]
if facet_bars(
    {
        "rows per metric": dist.get("metric", []),
        "rows per year": sorted(by_year.items()),
        "series with <100% coverage": _cov_pairs or [("all series 100%", 0)],
        "series longest gap (steps)": [
            (c["series"], c["longest_gap"]) for c in continuity if c["longest_gap"]
        ]
        or [("no gaps", 0)],
    },
    "SMARD -- series overview",
    "smard_series_overview.png",
    rot=90,
    ncols=2,
):
    figs.append(("SMARD series overview", "smard_series_overview.png"))

# COMMAND ----------

# DBTITLE 1,Figure -- value distribution + one representative series per metric
if facet_hists(
    {m: value_pdf.loc[value_pdf["metric"] == m, "value"].tolist() for m in metrics},
    "SMARD -- value distribution per metric (sampled)",
    "smard_value_distributions.png",
    ncols=4,
):
    figs.append(
        (
            "SMARD value distribution per metric (sampled)",
            "smard_value_distributions.png",
        )
    )


def _ts_draw(pdf):
    def draw(ax):
        ax.plot(range(len(pdf)), pdf["value"], linewidth=0.7)

    return draw


if _facet_grid(
    [
        (
            f"{m}\n{rep_series[m]['region']}|{rep_series[m]['resolution']}",
            _ts_draw(ts_pdf[m]),
        )
        for m in metrics
        if not ts_pdf[m].empty
    ],
    "SMARD -- first 4000 points of one representative series per metric",
    "smard_time_series.png",
    ncols=4,
):
    figs.append(
        (
            "SMARD -- one representative series per metric (chronological)",
            "smard_time_series.png",
        )
    )

# COMMAND ----------

# DBTITLE 1,Figure -- metric|region x resolution coverage heatmap
labels_mr = [
    f"{m}|{rg}"
    for m in metrics
    for rg in regions
    if any((m, rg, rs) in present for rs in resolutions)
]
grid = np.array(
    [
        [1 if (m, rg, rs) in present else 0 for rs in resolutions]
        for m in metrics
        for rg in regions
        if any((m, rg, rs2) in present for rs2 in resolutions)
    ],
    dtype=float,
)
if grid.size:
    fig, ax = plt.subplots(
        figsize=(max(4, 1.1 * len(resolutions)), max(3, 0.28 * len(labels_mr)))
    )
    ax.imshow(grid, aspect="auto", cmap="Greens")
    ax.set_xticks(range(len(resolutions)))
    ax.set_xticklabels(resolutions, rotation=45, ha="right")
    ax.set_yticks(range(len(labels_mr)))
    ax.set_yticklabels(labels_mr, fontsize=6)
    ax.set_title("SMARD -- present metric|region x resolution")
    fig.tight_layout()
    _save_and_show(fig, "smard_coverage_matrix.png")
    figs.append(
        ("SMARD metric|region x resolution presence", "smard_coverage_matrix.png")
    )

# COMMAND ----------

# DBTITLE 1,Findings
print("constant columns:", constant_cols)
print(
    "exact duplicates:",
    total - distinct_rows,
    " conflicting (series,ts):",
    db["conflicting"],
)
print("mirror metrics:", mirrors)
print("5-sigma outliers per metric:", outliers)

# COMMAND ----------

# DBTITLE 1,Export profiling findings -> src/schemas/profiling/smard.md
_prof = ["| column | missing | rate | approx_distinct |", "|---|---|---|---|"]
for c in COLS:
    _prof.append(f"| {c} | {r[c + '__m']} | {r[c + '__m'] / total:.4f} | {acd[c]} |")
_prof += [
    "",
    para(
        f"Rows: {total}. Long format, series key = {SERIES_KEY}.",
        f"Constant columns: {constant_cols or 'none'}. Distinct series: {len(series)}.",
    ),
]

_dq = [
    f"Exact full-row duplicates: {total - distinct_rows}.",
    para(
        f"(series, timestamp_utc) duplicate groups: {db['dup_groups']}",
        f"(identical: {db['identical']}, conflicting: {db['conflicting']}).",
    ),
]
if db["conflicting"]:
    _dq.append(
        para(
            "Conflicting duplicate series points exist -- Silver needs a deterministic",
            "value-selection rule per (series, timestamp).",
        )
    )
_nn = {m: x["non_numeric"] for m, x in by_metric.items() if x["non_numeric"]}
if _nn:
    _dq.append(f"Non-numeric values in `value` per metric: {_nn}.")

_unit = [
    para(
        "`value` is a physical quantity whose unit is metric-dependent (MW / MWh for",
        "generation and load, EUR/MWh for prices) -- the Bronze table does not carry the unit,",
        "so it must be attached per metric at Silver from the SMARD filter catalog.",
    ),
]
_neg = {m: x["negative_rows"] for m, x in by_metric.items() if x["negative_rows"]}
if _neg:
    _unit.append(
        para(
            f"Negative values per metric: {_neg}.",
            "Negative is physical for residual_load, day_ahead_prices and net cross-border",
            "flows; a negative realised generation is an error.",
        )
    )
_unit.append("")
_unit.append("Exact-copy / sign-mirror metric pairs (circular feature/target risk):")
if mirrors:
    for a, b, why in mirrors:
        _unit.append(
            f"- `{a}` <-> `{b}`: {why}. If one is a forecast and the other its "
            "components' sum, they are not independent series -- do not use one to predict "
            "the other, and check the Bronze/staging layer for a sign or labelling error."
        )
else:
    _unit.append("- none detected across the metrics.")

_temporal = [
    para(
        f"timestamp_utc timezone: {TS_TZ}.",
        "SMARD's own export is Europe/Berlin wall-clock and has 23h / 25h DST days;",
        "the continuity check rounds the step ratio so DST transitions do not show as gaps,",
        "but the load's UTC conversion must be verified before any hourly join.",
    ),
    "",
    "Per-series continuity (fixed-step resolutions; expected = span / step + 1, independent of the data):",
    "",
    "| series | resolution | observed | expected | coverage % | longest gap (steps) | missing steps |",
    "|---|---|---|---|---|---|---|",
]
for c in continuity:
    _temporal.append(
        f"| {c['series']} | {c['resolution']} | {c['observed']} | {c['expected']} | "
        f"{c['coverage_pct']} | {c['longest_gap']} | {c['missing_steps']} |"
    )
_temporal += [
    "",
    (
        f"Rows per year: {sorted(by_year.items())} -- heavily back-loaded: series added in "
        "later years are dense from their own start, older series are short. A per-series min/max "
        "is the right span, NOT the table-wide 2013..now."
    ),
]

_entities = [
    f"Distinct series (metric|filter_id|region|resolution): {len(series)}.",
    f"Metrics ({len(metrics)}): {metrics}",
    f"Regions ({len(regions)}): {regions}",
    f"Resolutions ({len(resolutions)}): {resolutions}",
]

_coverage = [
    para(
        f"metric x region x resolution: {len(present)} present,",
        f"{len(missing_combos)} absent of",
        f"{len(metrics) * len(regions) * len(resolutions)} possible.",
    ),
    "",
    f"Regions per metric: {metric_regions}",
    "",
    f"Resolutions per metric: {metric_res}",
    "",
    para(
        "A model must not pool a metric's regional breakdowns with its DE-LU total, nor its",
        "day and quarterhour resolutions -- those are the same quantity at different",
        "aggregations and a random split leaks between them.",
    ),
]

_domain = [
    para(
        "`resolution` vs the known step set:",
        f"unexpected={res_domain['unexpected'] or 'none'}",
        "(an unexpected step parses to null and is dropped from the continuity check);",
        f"unused={res_domain['unused_allowed'] or 'none'}.",
    ),
    para(
        "`region` vs the known SMARD zone set:",
        f"unexpected={region_domain['unexpected'] or 'none'},",
        f"unused={region_domain['unused_allowed'] or 'none'}.",
    ),
    para(
        "An unexpected `resolution` or `region` is an ingestion/parse issue (SMARD filter",
        "mapping), not a source-data finding. The known-region list is a best-effort reference,",
        "not authoritative -- an 'unexpected' region may just be missing from it.",
    ),
]

_tcons = [
    para(
        "Does every forecast_* metric have a realised counterpart on the same",
        "(region, resolution, timestamp) grid? A gap means the forecast cannot be",
        "scored against an outcome from this table alone.",
    ),
    "",
]
for fp in forecast_pairing:
    if fp.get("realised") is None:
        _tcons.append(f"- `{fp['forecast']}`: {fp['note']}.")
    else:
        _tcons.append(
            f"- `{fp['forecast']}` -> `{fp['realised']}`: "
            f"{fp['with_realised_same_grid']}/{fp['forecast_points']} forecast grid points have a "
            f"realised value ({fp['coverage_pct']}%)."
        )
if not forecast_pairing:
    _tcons.append("- no forecast_* metrics present.")

_pcons = [
    para(
        "residual_load is tested against load minus wind and solar at the same (DE-LU, resolution,",
        "timestamp), with the load series named explicitly and several candidate definitions compared.",
    ),
    f"Series used: residual={m_resid}, load={m_load}, onshore wind={m_on}, offshore wind={m_off}, solar={m_pv}, pumped-storage consumption={m_ps}.",
]
for x in identity_variants:
    _pcons.append(
        f"- {x['resolution']}: `{x['identity']}`: {x['violations']}/{x['comparable_rows']} rows exceed "
        f"{int(x['rel_tol'] * 100)}% relative residual ({x['violation_pct']}%); residual p01/p50/p99 "
        f"{x['residual_p01_p50_p99']}, max abs {x['max_abs_residual']}."
    )
if identity_levels:
    _pcons.append(f"Mean level of each series used, per resolution: {identity_levels}.")
if identity_best and identity_best["violation_pct"] is not None:
    if identity_best["violation_pct"] < 5:
        _pcons.append(
            f"Best-fitting definition: `{identity_best['identity']}` at {identity_best['resolution']} "
            f"({identity_best['violation_pct']}% of rows outside 2%)."
        )
    else:
        _pcons.append(
            para(
                f"No tested combination reproduces residual_load within 2% (best: `{identity_best['identity']}` at",
                f"{identity_best['resolution']}, {identity_best['violation_pct']}% of rows outside). The cause is not",
                "established here (candidates: the source's definition of load, series vintages, or a unit /",
                "aggregation difference between the series); the ambiguity is recorded, not resolved.",
            )
        )
if not identity_variants:
    _pcons.append(
        f"- LIMITATION: the required series were not all present (resid={m_resid}, load={m_load}, wind={m_on}/{m_off}, pv={m_pv}) -- identity not tested."
    )

_patterns = []
if day_hours:
    _patterns.append(
        f"Hour of day (UTC) of the stored day-resolution timestamps (hour, rows): {day_hours}."
    )
for key, title in (
    ("month", "calendar month"),
    ("weekday", "weekday (0 = Monday)"),
    ("year", "year"),
):
    _patterns.append(f"Mean by {title}, DE-LU day resolution (period, mean):")
    for m, vs in profiles[key].items():
        _patterns.append(f"- `{m}`: {vs}")
_patterns.append(f"Lag-1 / lag-7 autocorrelation of the daily series: {autocorr}.")
_patterns.append(
    "Diurnal profile, DE-LU quarter-hour, Europe/Berlin hour (hour, mean):"
)
for m, vs in diurnal.items():
    _patterns.append(f"- `{m}`: {vs}")
if neg_share_by_hour:
    _patterns.append(
        f"Share of negative values by hour (hour, share): {neg_share_by_hour}."
    )
if not daily.empty:
    _patterns.append(
        f"Daily frame: {daily.shape[0]} days x {daily.shape[1]} series, {daily.index.min().date()} .. {daily.index.max().date()}."
    )

_rels = []
if rel["corr"]:
    _rels.append(
        f"Day-ahead price against load, wind, solar and residual load on {rel['n']} days (Pearson): {rel['corr']}; (Spearman): {rel['spearman']}."
    )
    _rels.append(f"The same correlations by year: {rel['by_year']}.")
    _rels.append(
        f"Mean price by decile of residual load (decile, mean price): {rel['residual_deciles']}."
    )
else:
    _rels.append(
        "- Price / load / generation relationships not computed (series missing or too few common days)."
    )
for a in forecast_acc:
    _rels.append(
        f"- `{a['forecast']}` against `{a['realised']}` on {a['n']} days: corr {a['corr']}, bias {a['bias']}, mean abs error {a['mae']} ({a['mae_pct_of_mean']}% of the mean level)."
    )
if not forecast_acc:
    _rels.append(
        "- No forecast series with a realised counterpart of the same name and enough common days."
    )
_rels.append(
    f"Share of summed realised generation by year (technology: share): {mix_by_year}."
)

_regime2 = [
    para(
        "Metric availability and total annual volume per year -- the measurable",
        "form of the 2013-2026 regime change (nuclear phase-out, coal exit, PV",
        "growth). `first-seen year` flags metrics that do not span the full table.",
    ),
    "",
    f"Metric first-seen year: {sorted(metric_first_year.items(), key=lambda p: p[1] or 0)}",
    "",
]
_first_year = min(by_year) if by_year else None
_late = [
    m
    for m, y in metric_first_year.items()
    if y and _first_year is not None and y > _first_year
]
if _late:
    _regime2.append(
        f"- {len(_late)} metric(s) start after the table's first year {_first_year}: {_late}."
    )
_regime2.append(
    para(
        "Any model pooling across years sees multiple generation-mix regimes and a changing set",
        "of available series -- a year/era indicator and a per-metric availability window are",
        "warranted (not chosen here).",
    )
)

_dist = [
    "| metric | min | max | mean | sd | p01/25/50/75/99 | zero | negative | 5-sigma |",
    "|---|---|---|---|---|---|---|---|---|",
]
for m, x in by_metric.items():
    _dist.append(
        f"| {m} | {x['min']} | {x['max']} | {x['mean']} | {x['sd']} | "
        f"{x['p01_25_50_75_99']} | {x['zero_rows']} | {x['negative_rows']} | "
        f"{outliers.get(m)} |"
    )

_findings = []
if constant_cols:
    _findings.append(f"- Constant columns: {constant_cols}.")
if total - distinct_rows:
    _findings.append(f"- {total - distinct_rows} exact duplicate rows.")
if db["conflicting"]:
    _findings.append(
        f"- {db['conflicting']} (series, timestamp) keys have conflicting values."
    )
if mirrors:
    _findings.append(
        f"- Sign-mirror / duplicate metric pairs: {[(a, b) for a, b, _ in mirrors]}."
    )
_low_cov = [
    c["series"]
    for c in continuity
    if c["coverage_pct"] is not None and c["coverage_pct"] < 99
]
if _low_cov:
    _findings.append(f"- Series with <99% temporal coverage: {_low_cov[:20]}.")
if _neg:
    _findings.append(f"- Negative values present per metric: {_neg}.")
if any(outliers.values()):
    _findings.append(
        f"- 5-sigma value outliers per metric: { {m: x for m, x in outliers.items() if x} }."
    )
if missing_combos:
    _findings.append(
        f"- {len(missing_combos)} metric x region x resolution combinations carry no data."
    )
_findings_md = (
    "\n".join(_findings) if _findings else "No material data-quality issues found."
)

_silver = []
if constant_cols:
    _silver.append(f"- Drop constant columns {constant_cols} from Silver.")
if db["conflicting"]:
    _silver.append(
        "- De-duplicate (series key, timestamp_utc) with a deterministic value-selection rule."
    )
elif total - distinct_rows:
    _silver.append("- Apply distinct on load to drop exact duplicate rows.")
if _nn:
    _silver.append("- Cast `value` to double; quarantine non-numeric values.")
if mirrors:
    _silver.append(
        "- Sign-mirror metrics identified above -> keep one, or an explicit note on the "
        "derivation; investigate the load/staging for a sign error before Silver."
    )
_silver += [
    "- Silver grain: one row per (metric, filter_id, region, resolution, timestamp_utc).",
    "- Attach the physical unit per metric from the SMARD filter catalog at Silver.",
    "- Preserve gaps; do not forward-fill without a stated, per-resolution rule.",
]

_ml = ml_readiness_block(
    [
        (
            "Grain / grain drift",
            (
                f"Long format, series key = {SERIES_KEY}. One physical quantity appears at several "
                "resolutions and regional splits -- pooling them drifts the grain and mixes aggregation "
                "levels."
            ),
        ),
        (
            "Join multiplication (1:N / M:N expansion)",
            (
                "Single table -- no join here. A future join to weather / grid-operator data is "
                "unassessed; verify cardinality on (region, timestamp) before using SMARD as a feature."
            ),
        ),
        (
            "Target contamination",
            (
                f"Forecast metrics ({[m for m in metrics if m.startswith('forecast_')]}) predict a "
                "realised metric -- using the realised value at or after the forecast's target time as a "
                "feature for that forecast (or vice versa) is target contamination."
            ),
        ),
        (
            "Temporal / post-event leakage",
            (
                "A forecast series is published BEFORE its target time; a realised series is known only "
                "after. Any feature/target pair must respect each series' own availability time, not just "
                "the timestamp label."
            ),
        ),
        (
            "Proxy leakage",
            (
                "residual_load = load - (wind + pv); day-ahead price is a near-deterministic function of "
                "the residual-load forecast -- a model given both is partly seeing its own target."
            ),
        ),
        (
            "Split / entity leakage",
            (
                "Split by series (metric, filter_id, region, resolution), never by row or shuffled "
                "timestamp -- adjacent points in a series are highly correlated, and the same quantity's "
                "day/quarterhour variants must stay on one side."
            ),
        ),
        (
            "Historical-reference (point-in-time) leakage",
            (
                "SMARD revises published values (a preliminary figure is later corrected). Bronze holds "
                "the snapshot as downloaded; if re-downloaded, an as-of column is needed to avoid using a "
                "later revision as a historical feature."
            ),
        ),
        (
            "Survivorship / coverage bias",
            (
                "Rows-per-year is back-loaded (series added over time). A study window must be the "
                "intersection of the series it uses, not the table-wide span."
            ),
        ),
        (
            "Missingness leakage",
            (
                f"`value` missing per metric: { {m: x['missing'] for m, x in by_metric.items() if x['missing']} }. "
                "A missing point often marks a data-publication outage -- an 'is-missing' flag can leak it."
            ),
        ),
        (
            "Duplicate-event leakage",
            f"(series, ts) duplicates: {db} -- de-duplicate before treating a point as one observation.",
        ),
        (
            "Target / feature temporal misalignment",
            (
                "Forecast timestamp = target time, not publication time. Aligning a forecast feature to a "
                "realised target by timestamp alone silently uses a same-time forecast that was actually "
                "published earlier -- fine -- but a LATER-vintage forecast for the same target is leakage."
            ),
        ),
        (
            "Unit / sign / circular-feature leakage",
            (
                f"Mirror/copy metrics: {[(a, b) for a, b, _ in mirrors] or 'none'}. Units are not in "
                "Bronze -- combining metrics before attaching units risks adding MW to MWh to EUR/MWh."
            ),
        ),
        (
            "Data-generation-process leakage",
            (
                "`filter_id` and `resolution` describe how SMARD aggregated and published the series, not "
                "the physical system -- a feature keyed on them encodes the publication process."
            ),
        ),
        ("Class / label instability", "Not applicable -- `value` is continuous."),
        (
            "Label availability lag",
            (
                "Realised generation/load is published with a lag (preliminary then final); a nowcast "
                "cannot use a value that is not yet published at prediction time."
            ),
        ),
        (
            "Source / version / regime change",
            (
                "SMARD's methodology and the German generation mix both changed materially over 2013-2026 "
                "(nuclear phase-out, coal exit, PV growth). Regime / Version Evidence above gives the "
                "per-metric first-seen year and annual volume -- a regime/era indicator and per-metric "
                "availability window are warranted."
            ),
        ),
        (
            "Sample-vs-full divergence",
            (
                "value_pdf is a 10% sample capped at 200k rows and the time-series figure shows the first "
                "4000 points of ONE representative series per metric -- neither represents later history; "
                "use the full-table by_metric aggregates for any feature-quality decision."
            ),
        ),
    ]
)

_areas = {
    "Domain understanding": [
        f"{len(metrics)} metrics: realised generation by technology, consumption, residual load, day-ahead price and forecasts, for {regions}",
        f"generation mix by year: {mix_by_year}",
        f"residual load identity best result: {identity_best['identity'] + ' ' + str(identity_best['violation_pct']) + '% outside 2%' if identity_best else 'not testable'}",
    ],
    "Structure and engineering": [
        "one long-format table (series key metric | filter_id | region | resolution); units are not stored",
        f"day-resolution timestamps stored at UTC hours {day_hours[:4]}",
        f"{len(present)} of {len(metrics) * len(regions) * len(resolutions)} metric x region x resolution combinations exist",
    ],
    "Temporal": [
        f"daily frame {daily.shape if not daily.empty else 'not built'}; year profiles show the trend in {list(profiles['year'])[:3]}",
        f"persistence (lag-1, lag-7): {autocorr}",
        f"per-series coverage: {sum(1 for c in continuity if c['coverage_pct'] == 100)} of {len(continuity)} series complete",
    ],
    "Spatial": [
        f"regions {regions}; regional series exist only for {[m for m, rg in metric_regions.items() if len(rg) > 1]}"
    ],
    "Data quality": [
        f"exact duplicates {total - distinct_rows}; conflicting (series, ts) duplicates {db['conflicting']}",
        f"missing values {sum(x['missing'] for x in by_metric.values())}; mirrored metric pairs {[(a, b) for a, b, _ in mirrors] or 'none'}",
    ],
    "Statistical patterns": [
        f"weekday and month profiles for {len(PATTERN_METRICS)} metrics; diurnal profiles for {list(diurnal)}",
        f"negative-value hours: {list(neg_share_by_hour)}",
    ],
    "Relationships": [
        f"price vs load / wind / solar / residual (Pearson): {rel['corr']}",
        f"forecast accuracy pairs: {[(a['forecast'], a['corr']) for a in forecast_acc]}",
    ],
    "Analytics use": [
        "measures at two resolutions for one region total, with a few regional breakdowns; supports profile, trend and relationship analysis"
    ],
    "ML use": [
        f"a price series with explanatory series available on the same days ({rel['n']} common days) and forecasts to compare with realised values",
    ],
    "AI / knowledge use": [
        "metric names and filter ids form a small catalog; no free text"
    ],
}

write_profiling(
    SOURCE,
    NB_KEY,
    SECTION_TITLE,
    [
        ("Profile", "\n".join(_prof)),
        ("Data Quality", "\n".join(_dq)),
        ("Unit & Semantic Validation", "\n".join(_unit)),
        ("Categorical / Domain Validation", "\n".join(_domain)),
        ("Temporal Semantics", "\n".join(_temporal)),
        ("Temporal Consistency", "\n".join(_tcons)),
        ("Physical Consistency", "\n".join(_pcons)),
        ("Temporal Patterns", "\n".join(_patterns)),
        ("Relationships", "\n".join(_rels)),
        ("Regime / Version Evidence", "\n".join(_regime2)),
        ("Entities / Keys", "\n".join(_entities)),
        ("Coverage & Sampling Bias", "\n".join(_coverage)),
        ("Distributions", "\n".join(_dist)),
        ("EDA Findings", _findings_md),
        ("ML-Readiness Evidence", _ml),
        ("Observations by Area", area_block(_areas)),
        ("Silver Implications", "\n".join(_silver)),
    ],
    figures=figs,
)