# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # EDA -- ENERGY (SMARD, HONDA IOT, IOT DEVICES)
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** exploratory analysis of the energy data in one notebook. Each section profiles one group of
# MAGIC tables and writes its findings to `src/schemas/profiling/<source>.md` (one section key per group).

# COMMAND ----------

# MAGIC %run ./_eda_common

# COMMAND ----------

# MAGIC %md
# MAGIC ## EDA -- SMARD ENERGY TIME SERIES
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
P = profile_frame(df)
total = P["total"]
acd = P["acd"]
constant_cols = constant_column_names(df, P)
duplicate_rows = full_row_dup_count(df, total)
print(f"rows={total}  columns={len(COLS)}  ->  {COLS}")
for c in COLS:
    print(
        f"  {c:<14} missing={P['miss'][c]:>12} "
        f"rate={P['miss'][c] / total:.4f} approx_distinct={acd[c]}"
    )
print("constant columns (exact):", constant_cols)
print("exact full-row duplicates:", duplicate_rows)

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
    def draw_series(ax):
        ax.plot(range(len(pdf)), pdf["value"], linewidth=0.7)

    return draw_series


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
    duplicate_rows,
    " conflicting (series,ts):",
    db["conflicting"],
)
print("mirror metrics:", mirrors)
print("5-sigma outliers per metric:", outliers)

# COMMAND ----------

# DBTITLE 1,Export profiling findings -> src/schemas/profiling/smard.md
_prof = ["| column | missing | rate | approx_distinct |", "|---|---|---|---|"]
for c in COLS:
    _prof.append(f"| {c} | {P['miss'][c]} | {P['miss'][c] / total:.4f} | {acd[c]} |")
_prof += [
    "",
    para(
        f"Rows: {total}. Long format, series key = {SERIES_KEY}.",
        f"Constant columns: {constant_cols or 'none'}. Distinct series: {len(series)}.",
    ),
]

_dq = [
    f"Exact full-row duplicates: {duplicate_rows}.",
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
if duplicate_rows:
    _findings.append(f"- {duplicate_rows} exact duplicate rows.")
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
elif duplicate_rows:
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
        f"exact duplicates {duplicate_rows}; conflicting (series, ts) duplicates {db['conflicting']}",
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## EDA -- HONDA IOT ENERGY
# MAGIC
# MAGIC **Purpose:** Profile the six Honda IoT energy Bronze tables (electricity
# MAGIC / heating / cooling, each P and W) -- schema, missingness, constant
# MAGIC columns, per-frequency continuity measured against an independent
# MAGIC calendar, value ranges and sign, duplicate keys (identical vs
# MAGIC conflicting), stuck runs and outliers, exact-copy / sign-mirror columns
# MAGIC across all tables, the P<->W relationship, and the layered modelling-risk
# MAGIC checklist -- as evidence for Silver design.

# COMMAND ----------

# DBTITLE 1,Imports
import numpy as np
from pyspark.sql import Window
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Configuration
CATALOG = "energy_commerce_retail_media"
BRONZE_SCHEMA = "bronze"
SOURCE = "honda_iot"
NB_KEY = "01_energy"
SECTION_TITLE = "Energy tables (electricity / heating / cooling, each P and W)"
ENERGY = [
    "electricity_p",
    "electricity_w",
    "heating_p",
    "heating_w",
    "cooling_p",
    "cooling_w",
]
# P and W of a metric share one table; column measurement_type holds p or w.
TABLES = {e: f"{CATALOG}.{BRONZE_SCHEMA}.honda_iot_{e[:-2]}" for e in ENERGY}
KEY_COLS = ["frequency", "datetime_utc"]
VALUE_EXCLUDE = {"frequency", "datetime_utc"}
FREQ_SECONDS = {"1min": 60, "15min": 900, "1h": 3600}
# The dataset name says datetime_utc is UTC; W tables carry a running energy
# meter, P tables an instantaneous power / flow -- both are assumptions that the
# checks below test against the values; units are not stated in the data.
TS_TZ = "UTC"

# COMMAND ----------

# DBTITLE 1,Profile -- rows, missingness, approx distinct, constant columns (one agg per table)
frames = {
    e: bronze_frame(t)
    .where(F.col("measurement_type") == e[-1])
    .drop("measurement_type")
    for e, t in TABLES.items()
}
prof = {}
for e in ENERGY:
    df = frames[e]
    prof[e] = profile_frame(df)
    cols = prof[e]["cols"]
    print("=" * 88, f"\n{e}  rows={prof[e]['total']}  ->  {cols}")
    for c in cols:
        print(
            f"  {c:<28} missing={prof[e]['miss'][c]:>12} "
            f"rate={prof[e]['miss'][c] / prof[e]['total']:.4f} "
            f"approx_distinct={prof[e]['acd'][c]}"
        )
totals = {e: prof[e]["total"] for e in ENERGY}
VCOLS = {e: [c for c in prof[e]["cols"] if c not in VALUE_EXCLUDE] for e in ENERGY}

# COMMAND ----------

# DBTITLE 1,Confirm constant columns exactly
constant_cols = {}
for e in ENERGY:
    constant_cols[e] = constant_column_names(frames[e], prof[e])
    prof[e]["constant"] = constant_cols[e]
    print(f"{e}: constant columns (exact) = {constant_cols[e]}")

# COMMAND ----------

# DBTITLE 1,Rows per frequency + timestamp span (one groupBy per table)
freq_cov = {}
for e in ENERGY:
    d = (
        frames[e]
        .groupBy("frequency")
        .agg(
            F.count(F.lit(1)).alias("rows"),
            F.min("datetime_utc").alias("min_ts"),
            F.max("datetime_utc").alias("max_ts"),
            F.countDistinct("datetime_utc").alias("distinct_ts"),
        )
        .orderBy("frequency")
        .collect()
    )
    freq_cov[e] = {x["frequency"]: x.asDict() for x in d}
    print(f"{e}:", [x.asDict() for x in d])
freq_rows = {e: [(k, v["rows"]) for k, v in freq_cov[e].items()] for e in ENERGY}

# COMMAND ----------

# DBTITLE 1,Duplicate (frequency, datetime_utc) key -- identical vs conflicting (one groupBy per table)
dup = {}
for e in ENERGY:
    dup[e] = dup_key_composition(frames[e], KEY_COLS)
    print(f"{e:<16} {dup[e]}")

# COMMAND ----------

# DBTITLE 1,Value columns -- range, sign, sentinels, percentiles (one agg per table)
value_stats = {}
for e in ENERGY:
    df = frames[e]
    exprs = []
    for c in VCOLS[e]:
        v = safe_num(c)
        exprs += [
            F.min(v).alias(c + "_min"),
            F.max(v).alias(c + "_max"),
            F.avg(v).alias(c + "_avg"),
            F.stddev(v).alias(c + "_sd"),
            F.percentile_approx(v, [0.01, 0.25, 0.5, 0.75, 0.99]).alias(c + "_p"),
            F.sum((v < 0).cast("long")).alias(c + "_negative"),
            F.sum((v == 0).cast("long")).alias(c + "_zero"),
            F.sum(
                (F.col(c).isNotNull() & (F.trim(F.col(c)) != "") & v.isNull()).cast(
                    "long"
                )
            ).alias(c + "_non_numeric"),
        ]
    value_stats[e] = df.agg(*exprs).first().asDict()
    for c in VCOLS[e]:
        print(
            f"{e}.{c:<12}",
            {
                k[len(c) + 1 :]: value_stats[e][k]
                for k in value_stats[e]
                if k.startswith(c + "_")
            },
        )

# COMMAND ----------

# DBTITLE 1,Exact-copy / sign-mirror columns across ALL tables (circular-feature detector)
all_stats = {
    f"{e}.{c}": {
        "mean": value_stats[e][c + "_avg"],
        "sd": value_stats[e][c + "_sd"],
        "min": value_stats[e][c + "_min"],
        "max": value_stats[e][c + "_max"],
    }
    for e in ENERGY
    for c in VCOLS[e]
    if value_stats[e][c + "_sd"] is not None
}
mirrors = mirror_columns(all_stats)
print("mirror / duplicate column pairs:")
for a, b, why in mirrors:
    print(f"  {a}  <->  {b}  : {why}")

# COMMAND ----------

# DBTITLE 1,5-sigma outliers + stuck runs (two passes per table)
outliers = {}
stuck = {}
for e in ENERGY:
    df = frames[e]
    vs = value_stats[e]
    oor_exprs = []
    for c in VCOLS[e]:
        v = safe_num(c)
        m, sd = vs[c + "_avg"], vs[c + "_sd"]
        oor_exprs.append(
            F.sum((F.abs(v - F.lit(m)) > 5 * F.lit(sd)).cast("long")).alias(c)
            if sd
            else F.lit(0).alias(c)
        )
    outliers[e] = df.agg(*oor_exprs).first().asDict()
    w = Window.partitionBy("frequency").orderBy("datetime_utc")
    df_1h = df.where(F.col("frequency") == "1h")
    for c in VCOLS[e]:
        v = safe_num(c)
        df_1h = df_1h.withColumn(
            f"{c}_stuck",
            (
                v.isNotNull() & (v == F.lag(v, 1).over(w)) & (v == F.lag(v, 9).over(w))
            ).cast("long"),
        )
    stuck[e] = (
        df_1h.agg(*[F.sum(F.col(f"{c}_stuck")).alias(c) for c in VCOLS[e]])
        .first()
        .asDict()
    )
    print(f"{e}: 5sigma_outliers={outliers[e]}  stuck>=10run(1h)={stuck[e]}")

# COMMAND ----------

# DBTITLE 1,Temporal continuity -- coverage vs an INDEPENDENT per-frequency calendar
continuity = {}
for e in ENERGY:
    cg = continuity_grid(
        frames[e], "datetime_utc", FREQ_SECONDS, entity_col="frequency"
    )
    continuity[e] = cg["per_entity"]
    print(f"{e}: {cg['per_entity']}")

# COMMAND ----------

# DBTITLE 1,P <-> W value relationship per metric (one join + one agg per metric)
pw_rel = {}
pw_scatter = {}
for metric in ("electricity", "heating", "cooling"):
    p, w = frames[f"{metric}_p"], frames[f"{metric}_w"]
    shared = [c for c in p.columns if c not in VALUE_EXCLUDE and c in w.columns]
    if not shared:
        continue
    j = p.select(*KEY_COLS, *[safe_num(c).alias(f"p_{c}") for c in shared]).join(
        w.select(*KEY_COLS, *[safe_num(c).alias(f"w_{c}") for c in shared]),
        on=KEY_COLS,
        how="inner",
    )
    pw_rel[metric] = (
        j.agg(
            F.count(F.lit(1)).alias("matched"),
            *[F.corr(f"p_{c}", f"w_{c}").alias(f"corr_{c}") for c in shared],
        )
        .first()
        .asDict()
    )
    pw_scatter[metric] = (
        shared[0],
        j.select(f"p_{shared[0]}", f"w_{shared[0]}")
        .where(
            F.col(f"p_{shared[0]}").isNotNull() & F.col(f"w_{shared[0]}").isNotNull()
        )
        .sample(0.1, seed=42)
        .limit(20_000)
        .toPandas(),
    )
    print(f"{metric}: {pw_rel[metric]}")

schema_parity = {
    m: frames[f"{m}_p"].columns == frames[f"{m}_w"].columns
    for m in ("electricity", "heating", "cooling")
}
print("P/W schema parity:", schema_parity)

# COMMAND ----------

# DBTITLE 1,Categorical domain -- `frequency` against the known resolution set
freq_domain = {
    e: categorical_domain(
        frames[e], "frequency", FREQ_SECONDS.keys(), name=f"{e}.frequency"
    )
    for e in ENERGY
}
for e, d in freq_domain.items():
    if d["unexpected_count"]:
        print(
            f"{e}: UNEXPECTED frequency label(s) {d['unexpected']} -- ingestion defect"
        )

# COMMAND ----------

# DBTITLE 1,Temporal consistency -- the W meter must be non-decreasing (cumulative energy)
w_monotonic = {}
for e in ENERGY:
    if not e.endswith("_w"):
        continue
    w_monotonic[e] = {}
    for c in VCOLS[e]:
        res = monotonic_series_check(
            frames[e], c, "datetime_utc", partition_col="frequency"
        )
        w_monotonic[e][c] = res
        print(
            f"{e}.{c}: {res['decreasing_steps']}/{res['comparable_steps']} steps decrease "
            f"({res['decreasing_pct']}%), largest drop {res['largest_drop']}"
        )

# COMMAND ----------

# DBTITLE 1,W meters -- direction of change, sign-aware
w_direction = {}
for e in ENERGY:
    if not e.endswith("_w"):
        continue
    win = Window.partitionBy("frequency").orderBy("datetime_utc")
    sel = []
    for c in VCOLS[e]:
        v = safe_num(c)
        sel += [
            v.alias(f"_cur_{c}"),
            F.lag(v).over(win).alias(f"_prev_{c}"),
        ]
    exprs = []
    for c in VCOLS[e]:
        cur = F.col(f"_cur_{c}")
        prev = F.col(f"_prev_{c}")
        exprs += [
            F.sum(((cur - prev) > 0).cast("long")).alias(f"{c}__up"),
            F.sum(((cur - prev) < 0).cast("long")).alias(f"{c}__down"),
            F.sum(((cur - prev) == 0).cast("long")).alias(f"{c}__flat"),
            F.sum((F.abs(cur) < F.abs(prev) - 1e-6).cast("long")).alias(
                f"{c}__mag_down"
            ),
        ]
    r = frames[e].select(*sel).agg(*exprs).first().asDict()
    w_direction[e] = {}
    for c in VCOLS[e]:
        lo, hi = value_stats[e][c + "_min"], value_stats[e][c + "_max"]
        w_direction[e][c] = {
            "up": r[f"{c}__up"],
            "down": r[f"{c}__down"],
            "flat": r[f"{c}__flat"],
            "magnitude_decreasing": r[f"{c}__mag_down"],
            "sign": "non-negative"
            if lo >= 0
            else ("non-positive" if hi <= 0 else "mixed"),
        }
        print(e, c, w_direction[e][c])

# COMMAND ----------

# DBTITLE 1,P against the increments of W -- alignment, scale and sign
pw_align = []
for metric in ("electricity", "heating", "cooling"):
    p, w = frames[f"{metric}_p"], frames[f"{metric}_w"]
    shared = [c for c in p.columns if c not in VALUE_EXCLUDE and c in w.columns]
    if not shared:
        continue
    for freq, secs in FREQ_SECONDS.items():
        scale = 3600.0 / secs
        pf = p.where(F.col("frequency") == freq).select(
            "datetime_utc", *[safe_num(c).alias(f"p_{c}") for c in shared]
        )
        wf = w.where(F.col("frequency") == freq).select(
            "datetime_utc", *[safe_num(c).alias(f"w_{c}") for c in shared]
        )
        win = Window.orderBy("datetime_utc")
        j = pf.join(wf, "datetime_utc")
        sel = [F.col(f"p_{c}") for c in shared]
        for c in shared:
            wc = F.col(f"w_{c}")
            sel += [
                ((wc - F.lag(wc).over(win)) * scale).alias(f"b_{c}"),
                ((F.lead(wc).over(win) - wc) * scale).alias(f"f_{c}"),
            ]
        jj = j.select(*sel)
        aggs = [F.count(F.lit(1)).alias("n")]
        for i, c in enumerate(shared):
            pc, b, f_ = F.col(f"p_{c}"), F.col(f"b_{c}"), F.col(f"f_{c}")
            aggs += [
                F.corr(pc, b).alias(f"cb{i}"),
                F.corr(pc, f_).alias(f"cf{i}"),
                (F.covar_pop(pc, b) / F.var_pop(b)).alias(f"sb{i}"),
                (F.covar_pop(pc, f_) / F.var_pop(f_)).alias(f"sf{i}"),
                F.avg(pc).alias(f"mp{i}"),
                F.avg(b).alias(f"mb{i}"),
            ]
        r = jj.agg(*aggs).first().asDict()
        for i, c in enumerate(shared):
            pw_align.append(
                {
                    "family": metric,
                    "column": c,
                    "frequency": freq,
                    "n": r["n"],
                    "corr_back": r[f"cb{i}"],
                    "corr_forward": r[f"cf{i}"],
                    "slope_back": r[f"sb{i}"],
                    "slope_forward": r[f"sf{i}"],
                    "mean_p": r[f"mp{i}"],
                    "mean_increment": r[f"mb{i}"],
                }
            )
            print(pw_align[-1])

# COMMAND ----------

# DBTITLE 1,P against the W increment after applying the measured scale (15min and 1h)
# The scale is read from the alignment test (nearest power of ten of the measured
# slope), never assumed; only combinations with |corr| >= 0.9 are re-tested.
pw_scaled = []
for x in pw_align:
    if x["frequency"] not in ("15min", "1h"):
        continue
    forward = abs(x["corr_forward"] or 0) >= abs(x["corr_back"] or 0)
    corr = x["corr_forward"] if forward else x["corr_back"]
    slope = x["slope_forward"] if forward else x["slope_back"]
    if corr is None or abs(corr) < 0.9 or not slope:
        continue
    scale = float(np.sign(slope) * 10 ** round(float(np.log10(abs(slope)))))
    secs = FREQ_SECONDS[x["frequency"]]
    fam, c = x["family"], x["column"]
    pf = (
        frames[f"{fam}_p"]
        .where(F.col("frequency") == x["frequency"])
        .select("datetime_utc", safe_num(c).alias("p_val"))
    )
    wf = (
        frames[f"{fam}_w"]
        .where(F.col("frequency") == x["frequency"])
        .select("datetime_utc", safe_num(c).alias("w_val"))
    )
    win = Window.orderBy("datetime_utc")
    wv = F.col("w_val")
    inc = (F.lead(wv).over(win) - wv) if forward else (wv - F.lag(wv).over(win))
    j = (
        pf.join(wf, "datetime_utc")
        .select("p_val", (inc * (3600.0 / secs) * scale).alias("implied"))
        .where(F.col("implied").isNotNull() & F.col("p_val").isNotNull())
    )
    res = additive_identity_check(
        j,
        "p_val",
        ["implied"],
        rel_tol=0.1,
        abs_floor=max(1.0, 0.02 * abs(x["mean_p"] or 0)),
    )
    pw_scaled.append(
        {
            "family": fam,
            "column": c,
            "frequency": x["frequency"],
            "direction": "forward" if forward else "back",
            "scale": scale,
            **res,
        }
    )
    print(pw_scaled[-1])

# COMMAND ----------

# DBTITLE 1,Energy profiles -- hour of day, weekday and month (1h frequency, UTC)
energy_profiles = {}
for e in ENERGY:
    if not e.endswith("_p"):
        continue
    d = (
        frames[e]
        .where(F.col("frequency") == "1h")
        .withColumn("ts", F.to_timestamp("datetime_utc"))
    )
    aggs = [F.avg(safe_num(c)).alias(c) for c in VCOLS[e]]
    energy_profiles[e] = {}
    for key, k in (
        ("hour", F.hour("ts")),
        ("weekday", F.dayofweek("ts")),
        ("month", F.month("ts")),
    ):
        rows = d.groupBy(k.alias("k")).agg(*aggs).orderBy("k").collect()
        energy_profiles[e][key] = {
            c: [(int(x["k"]), round(x[c], 1)) for x in rows if x[c] is not None]
            for c in VCOLS[e]
        }
    print(e, {k: len(v) for k, v in energy_profiles[e].items()})

# COMMAND ----------

# DBTITLE 1,Regime evidence -- first half vs second half of the series (recalibration / sensor swap)
spans = [
    (fc["min_ts"], fc["max_ts"])
    for e in ENERGY
    for fc in freq_cov[e].values()
    if fc.get("min_ts") and fc.get("max_ts")
]
regime = {}
# freq_cov min/max come from F.min/F.max on a Bronze STRING column -> they are
# strings, not datetimes; iso_midpoint parses them.
mid = (
    iso_midpoint(min(s[0] for s in spans), max(s[1] for s in spans)) if spans else None
)
if mid:
    for e in ENERGY:
        shift = regime_population_shift(frames[e], "datetime_utc", VCOLS[e], mid)
        regime[e] = {"cut": mid, **shift}
        print(
            f"{e}: split at {mid} -> pre={shift['pre_rows']} post={shift['post_rows']}; "
            f"flipped={shift['flipped']}"
        )

# COMMAND ----------

# DBTITLE 1,Samples for figures (sampled + first-N chronological)
value_pdf = {
    e: frames[e]
    .select(*[safe_num(c).alias(c) for c in VCOLS[e]])
    .sample(0.1, seed=42)
    .limit(150_000)
    .toPandas()
    for e in ENERGY
}
ts_pdf = {
    e: frames[e]
    .where(F.col("frequency") == "1h")
    .select("datetime_utc", *[safe_num(c).alias(c) for c in VCOLS[e]])
    .orderBy("datetime_utc")
    .limit(2000)
    .toPandas()
    for e in ENERGY
}

# COMMAND ----------

# DBTITLE 1,Figure -- rows per frequency + coverage % + duplicate groups
figs = []
if facet_bars(
    {e: freq_rows[e] for e in ENERGY},
    "Honda energy -- rows per frequency, by table",
    "honda_energy_rows_per_frequency.png",
    rot=0,
):
    figs.append(
        (
            "Honda energy -- rows per frequency, by table",
            "honda_energy_rows_per_frequency.png",
        )
    )
if facet_bars(
    {
        e: [
            (fq, continuity[e][fq]["coverage_pct"])
            for fq in FREQ_SECONDS
            if fq in continuity[e]
        ]
        for e in ENERGY
    },
    "Honda energy -- coverage % vs independent calendar, by frequency",
    "honda_energy_coverage_pct.png",
    rot=0,
):
    figs.append(
        ("Honda energy -- coverage % by frequency", "honda_energy_coverage_pct.png")
    )
if barplot(
    [(e, dup[e]["dup_groups"]) for e in ENERGY],
    "Honda energy -- duplicate (frequency, datetime_utc) groups",
    "table",
    "dup groups",
    rot=30,
    filename="honda_energy_duplicate_groups.png",
):
    figs.append(
        (
            "Honda energy -- duplicate (frequency, datetime_utc) groups",
            "honda_energy_duplicate_groups.png",
        )
    )

# COMMAND ----------

# DBTITLE 1,Figure -- longest gap (steps) per table x frequency
gap_ok = False
_gap_rows = [
    (fq, [continuity[e].get(fq, {}).get("longest_gap_steps", 0) or 0 for e in ENERGY])
    for fq in FREQ_SECONDS
]
if any(any(v) for _, v in _gap_rows):
    x = np.arange(len(ENERGY))
    fig, ax = plt.subplots(figsize=(11, 4))
    for i, (fq, vals) in enumerate(_gap_rows):
        ax.bar(x + i * 0.27, vals, width=0.27, label=fq)
    ax.set_xticks(x + 0.27)
    ax.set_xticklabels(ENERGY, rotation=30, ha="right")
    ax.legend()
    ax.set_title("Honda energy -- longest gap (missing steps) per table x frequency")
    ax.set_ylabel("steps")
    fig.tight_layout()
    _save_and_show(fig, "honda_energy_longest_gap_per_table.png")
    gap_ok = True
if gap_ok:
    figs.append(
        (
            "Honda energy -- longest gap (missing steps) per table x frequency",
            "honda_energy_longest_gap_per_table.png",
        )
    )
else:
    print("  no gaps in any series -- longest-gap figure not written")

# COMMAND ----------

# DBTITLE 1,Figure -- value distributions + hourly window + P vs W scatter
if facet_hists(
    {f"{e}.{c}": value_pdf[e][c].dropna().tolist() for e in ENERGY for c in VCOLS[e]},
    "Honda energy -- value distribution per table.column (sampled)",
    "honda_energy_value_distributions.png",
    ncols=4,
):
    figs.append(
        (
            "Honda energy -- value distribution per table.column",
            "honda_energy_value_distributions.png",
        )
    )


def _hourly_draw(tp, cols):
    def draw(ax):
        for c in cols:
            ax.plot(range(len(tp)), tp[c], label=c, linewidth=0.8)
        ax.legend(fontsize=6)

    return draw


if _facet_grid(
    [(e, _hourly_draw(ts_pdf[e], VCOLS[e])) for e in ENERGY if not ts_pdf[e].empty],
    "Honda energy -- first 2000 hourly points, by table",
    "honda_energy_first_hourly_points.png",
):
    figs.append(
        (
            "Honda energy -- first 2000 hourly points, by table",
            "honda_energy_first_hourly_points.png",
        )
    )


def _scatter_draw(col, pdf):
    def draw(ax):
        ax.scatter(pdf[f"p_{col}"], pdf[f"w_{col}"], s=6, alpha=0.3)
        ax.set_xlabel(f"P.{col}", fontsize=7)
        ax.set_ylabel(f"W.{col}", fontsize=7)

    return draw


if _facet_grid(
    [
        (metric, _scatter_draw(col, pdf))
        for metric, (col, pdf) in pw_scatter.items()
        if not pdf.empty
    ],
    "Honda energy -- P vs W per metric (sampled)",
    "honda_energy_p_vs_w_scatter.png",
):
    figs.append(
        ("Honda energy -- P vs W per metric", "honda_energy_p_vs_w_scatter.png")
    )

# COMMAND ----------

# DBTITLE 1,Findings
print("dup composition:", dup)
print("continuity:", continuity)
print("5-sigma outliers:", outliers)
print("mirror columns:", mirrors)
print("P<->W relationship:", pw_rel)

# COMMAND ----------


# DBTITLE 1,Number formatting helper


def fmt_c(x):
    return "-" if x is None else f"{x:.4g}"


# COMMAND ----------

# DBTITLE 1,Export profiling findings -> src/schemas/profiling/honda_iot.md
_profile = [
    "| table | rows | cols | frequencies | constant cols |",
    "|---|---|---|---|---|",
]
for e in ENERGY:
    _profile.append(
        f"| {e} | {totals[e]} | {len(prof[e]['cols'])} | "
        f"{ {fr: fc['rows'] for fr, fc in freq_cov[e].items()} } | "
        f"{', '.join(prof[e]['constant']) or '-'} |"
    )
_profile.append("")
_profile.append("Value columns per table: " + str({e: VCOLS[e] for e in ENERGY}))

_dq = ["| table | dup key groups | identical | conflicting |", "|---|---|---|---|"]
for e in ENERGY:
    b = dup[e]
    _dq.append(f"| {e} | {b['dup_groups']} | {b['identical']} | {b['conflicting']} |")
_dq.append("")
_dq.append(f"5-sigma outlier rows per value column: {outliers}")
_dq.append(f"Stuck-run rows (value == value 1 and 9 steps back, 1h): {stuck}")

_unit = [
    (
        "Value columns cast to double; P tables are assumed to be instantaneous power/flow and W tables a "
        "cumulative energy meter, with no unit in the data -- the direction and scale checks below test this:"
    ),
]
for e in ENERGY:
    for c in VCOLS[e]:
        vs = value_stats[e]
        _unit.append(
            f"- {e}.`{c}`: range {vs[c + '_min']}..{vs[c + '_max']}, mean {vs[c + '_avg']}, "
            f"sd {vs[c + '_sd']}, negative rows {vs[c + '_negative']}, zero {vs[c + '_zero']}, "
            f"non-numeric {vs[c + '_non_numeric']}"
        )
_unit.append("")
_unit.append("Exact-copy / sign-mirror column pairs (circular-feature risk):")
if mirrors:
    for a, b, why in mirrors:
        _unit.append(f"- `{a}` <-> `{b}`: {why}")
else:
    _unit.append("- none detected across the six tables.")

_temporal = [
    (
        f"datetime_utc is treated as {TS_TZ} per the dataset name. Coverage below is measured "
        "against an INDEPENDENT calendar (expected = span / step + 1), not the observed distinct "
        "count -- so 100% means genuinely gap-free, not tautological."
    ),
    "",
    "| table / frequency | observed | expected | coverage % | longest gap (steps) | on-step % |",
    "|---|---|---|---|---|---|",
]
for e in ENERGY:
    for fq in FREQ_SECONDS:
        r = continuity[e].get(fq)
        if not r:
            continue
        _temporal.append(
            f"| {e} / {fq} | {r['observed']} | {r['expected']} | {r['coverage_pct']} | "
            f"{r['longest_gap_steps']} | {r['on_step_pct']} |"
        )

_rel = [
    "P<->W value relationship per metric (inner join on (frequency, datetime_utc), Pearson corr):"
]
for metric, a in pw_rel.items():
    _rel.append(f"- {metric}: {a}")
_rel.append(f"P/W schema parity: {schema_parity}")

_patterns = [
    "Mean of each P column by UTC hour of day, weekday (1 = Sunday) and month, 1h frequency (period, mean):"
]
for e, kinds_ in energy_profiles.items():
    for key, cols in kinds_.items():
        for c, vs in cols.items():
            _patterns.append(f"- {e}.`{c}` by {key}: {vs}")

_coverage = [
    f"Rows per (table, frequency): { {e: {k: v['rows'] for k, v in freq_cov[e].items()} for e in ENERGY} }.",
    (
        "The 1min partition dominates every table (~50x the 1h partition). A model must not pool "
        "frequencies -- they are three resolutions of the same signal, and a random split would put "
        "near-duplicate 1min/15min/1h rows of the same hour on both sides."
    ),
    "Single building, single sensor set -- no entity dimension; the only split axis is time.",
]

_domain = ["`frequency` values vs the known resolution set (1min / 15min / 1h):"]
for e in ENERGY:
    d = freq_domain[e]
    _domain.append(
        f"- {e}: unexpected={d['unexpected'] or 'none'}, "
        f"unused={d['unused_allowed'] or 'none'}."
    )
if any(freq_domain[e]["unexpected_count"] for e in ENERGY):
    _domain.append(
        "-> an unexpected `frequency` label is an INGESTION defect (a bad partition value), "
        "not a source finding."
    )

_tcons = [
    para(
        "The W tables were assumed to be cumulative energy meters that never decrease. That assumption is",
        "tested here both literally (decreasing steps) and sign-aware (direction of change by sign of the",
        "column, and whether the magnitude keeps growing):",
    ),
    "",
]
for e, cols in w_monotonic.items():
    for c, res in cols.items():
        _tcons.append(
            f"- {e}.`{c}`: {res['decreasing_steps']} of {res['comparable_steps']} steps "
            f"decrease ({res['decreasing_pct']}%); largest drop {res['largest_drop']}."
        )
if not w_monotonic:
    _tcons.append("- No `*_w` cumulative-meter table in scope.")
_tcons.append(
    "Sign-aware direction per W column (steps up / down / flat, steps where the magnitude decreases, sign class):"
)
for e, cols in w_direction.items():
    for c, d in cols.items():
        _tcons.append(
            f"- {e}.`{c}`: up {d['up']}, down {d['down']}, flat {d['flat']}, magnitude decreasing {d['magnitude_decreasing']}; values {d['sign']}."
        )
_tcons.append(
    para(
        "A column whose values are all non-positive and whose steps mostly go down is a meter counted with a",
        "negative sign (a generation or export convention); it is monotone in magnitude, not in value.",
    )
)

_pcons = [
    para(
        "P is compared with the increment of the W meter converted to a rate. Two alignments are tested",
        "('back' = W[t] - W[t-1], 'forward' = W[t+1] - W[t]) and the scale between P and the increment is",
        "measured from the data, so no unit or timestamp convention is assumed in advance.",
    ),
    "",
]
_pcons.append(
    "Alignment, scale and sign test for every shared column and frequency (P against the W increment converted to a rate; 'back' = W[t] - W[t-1], 'forward' = W[t+1] - W[t]):"
)
for x in pw_align:
    _pcons.append(
        f"- {x['family']}.`{x['column']}` at {x['frequency']} (n {x['n']}): corr back {fmt_c(x['corr_back'])}, forward {fmt_c(x['corr_forward'])}; "
        f"slope of P on the increment back {fmt_c(x['slope_back'])}, forward {fmt_c(x['slope_forward'])}; mean P {fmt_c(x['mean_p'])}, mean increment {fmt_c(x['mean_increment'])}."
    )
_pcons.append(
    "Re-test after applying the measured scale (nearest power of ten of the slope) for the combinations with |corr| >= 0.9 at 15min and 1h; residual = P - scaled increment, tolerance 10%:"
)
for x in pw_scaled:
    _pcons.append(
        f"- {x['family']}.`{x['column']}` at {x['frequency']} ({x['direction']}, scale {x['scale']:g}): {x['violations']}/{x['comparable_rows']} rows outside 10% ({x['violation_pct']}%); "
        f"residual p01/p50/p99 {x['residual_p01_p50_p99']}."
    )
if not pw_scaled:
    _pcons.append("- No combination reached |corr| >= 0.9.")
_best = max(
    (
        x
        for x in pw_align
        if x["corr_back"] is not None or x["corr_forward"] is not None
    ),
    key=lambda x: max(abs(x["corr_back"] or 0), abs(x["corr_forward"] or 0)),
    default=None,
)
_strong = [
    (x["family"], x["column"], x["frequency"])
    for x in pw_align
    if max(abs(x["corr_back"] or 0), abs(x["corr_forward"] or 0)) >= 0.9
]
if _best:
    _pcons.append(
        f"Strongest alignment found: {_best['family']}.`{_best['column']}` at {_best['frequency']} with |corr| "
        f"{max(abs(_best['corr_back'] or 0), abs(_best['corr_forward'] or 0)):.3f}; combinations with |corr| >= 0.9: {_strong or 'none'}."
    )
    if not _strong:
        _pcons.append(
            para(
                "No alignment reproduces P from the W increments. What P and W measure, their units and their sign",
                "convention are therefore NOT established by this data; the relationship is recorded as unresolved",
                "and no interpretation is assumed.",
            )
        )

_regime = [
    para(
        "Rows split at the series midpoint -- a large null-rate or distinct-count",
        "move on one side points to a sensor swap / outage window rather than a",
        "physical change. This measures coverage, not signal level.",
    ),
    "",
]
for e in ENERGY:
    rg = regime.get(e)
    if not rg:
        _regime.append(f"- {e}: no datetime span -- not assessable.")
        continue
    _regime.append(
        f"- {e} (cut {rg['cut']}): pre={rg['pre_rows']}, post={rg['post_rows']}; "
        f"columns with a >=50pt null-rate move: {rg['flipped'] or 'none'}."
    )

_conflict = any(dup[e]["conflicting"] for e in ENERGY)
_neg = {
    f"{e}.{c}": value_stats[e][c + "_negative"]
    for e in ENERGY
    for c in VCOLS[e]
    if value_stats[e][c + "_negative"]
}

_silver = [
    "- Type conversion: datetime_utc -> timestamp (UTC); value columns -> double.",
    (
        "- `frequency` (1min / 15min / 1h) is a real physical resolution -> keep it in the grain; "
        "do not blend frequencies."
    ),
    "- Identical (frequency, datetime_utc) repeats can be de-duplicated.",
]
if _conflict:
    _silver.append(
        "- Conflicting (frequency, datetime_utc) duplicates exist -> a deterministic "
        "conflict-resolution rule is required before Silver."
    )
if mirrors:
    _silver.append(
        f"- Exact-copy / sign-mirror columns exist ({[(a, b) for a, b, _ in mirrors]}) -> keep "
        "ONE per pair, or an SCD/lineage note explaining the derivation; never expose both as "
        "independent features."
    )
if _neg:
    _silver.append(
        f"- Negative values in energy columns ({_neg}) -> confirm the sign convention "
        "(generation as negative? measurement error?) before Silver casting."
    )
_silver.append(
    "- Series are not necessarily dense (coverage / gaps above) -> observed points only; "
    "resampling is a downstream choice."
)
_silver.append("- Stuck-sensor runs and 5-sigma spikes -> data-quality flag, keep raw.")

_ml = ml_readiness_block(
    [
        (
            "Grain / grain drift",
            (
                f"One row per (frequency, datetime_utc) per table. Joining P+W per metric is 1:1 on that "
                f"key (schema parity {schema_parity}); pooling frequencies changes the grain."
            ),
        ),
        (
            "Join multiplication (1:N / M:N expansion)",
            (
                "P<->W join is 1:1 on (frequency, datetime_utc) -- no fan-out. A cross-metric wide join "
                "(electricity+heating+cooling at one timestamp) is also 1:1 on the intersection but drops "
                "the non-overlapping tail (see 03)."
            ),
        ),
        (
            "Target contamination",
            (
                "If a value column is the forecast target, the same column at the target timestamp (and "
                "the cumulative W meter, which encodes the future increment) must be excluded from features."
            ),
        ),
        (
            "Temporal / post-event leakage",
            (
                "If the W tables are running meters (assumed from the table name, not established -- see "
                "Temporal Consistency and Physical Consistency), W[t] already contains energy that flows after "
                "the prediction cutoff when the cutoff sits mid-interval; use first-differences, not the raw "
                "meter, and only points strictly before the cutoff."
            ),
        ),
        (
            "Proxy leakage",
            (
                "P and W of the same metric are near-redundant (corr above); a heat/cool total is close to "
                "the sum of its components -- a 'feature' that is an arithmetic function of the target leaks."
            ),
        ),
        (
            "Split / entity leakage",
            (
                "Single building, no entity id -- split by contiguous date range only, and never mix "
                "frequencies within one split (1min/15min/1h rows of the same hour are near-duplicates)."
            ),
        ),
        (
            "Historical-reference (point-in-time) leakage",
            (
                "No slowly-changing attributes here; a diurnal / seasonal profile used as a feature must be "
                "computed only from data before the prediction point, never over the full history."
            ),
        ),
        (
            "Survivorship / coverage bias",
            (
                "Continuity above shows the real gap profile. The 1min partition dominates; any statistic "
                "pooled across frequencies is really a 1min statistic."
            ),
        ),
        (
            "Missingness leakage",
            (
                "Whether a value is present may correlate with sensor downtime windows -- check before "
                "adding an 'is-missing' feature that a naive model could exploit."
            ),
        ),
        (
            "Duplicate-event leakage",
            (
                f"Duplicate key groups per table: { {e: dup[e]['dup_groups'] for e in ENERGY} } "
                f"(conflicting: { {e: dup[e]['conflicting'] for e in ENERGY} }) -- de-duplicate before "
                "counting observations or splitting."
            ),
        ),
        (
            "Target / feature temporal misalignment",
            (
                "P and W may be timestamped at different points of the interval (the alignment test in Physical "
                "Consistency compares W increments ending at, and starting at, the P timestamp) -- fix one "
                "convention before pairing."
            ),
        ),
        (
            "Unit / sign / circular-feature leakage",
            (
                f"Mirror/copy columns: {[(a, b) for a, b, _ in mirrors] or 'none'}. Negative energy: "
                f"{_neg or 'none'}. P<->W is a physical relationship, not independent signal -- using one "
                "to predict the other is circular."
            ),
        ),
        (
            "Data-generation-process leakage",
            (
                "The W meter reset/rollover behaviour and any gap-filling done upstream are part of the "
                "data-generation process -- a feature that spikes at a meter reset encodes the process, "
                "not the building's energy use."
            ),
        ),
        (
            "Class / label instability",
            "Not applicable -- all targets here are continuous.",
        ),
        (
            "Label availability lag",
            (
                "Meter readings are available at interval end; a nowcast at time t cannot use the interval "
                "[t, t+step] reading."
            ),
        ),
        (
            "Source / version / regime change",
            (
                "A single deployment; watch for sensor swaps or recalibration (a step change in level with "
                "no physical cause) when the series is extended."
            ),
        ),
        (
            "Sample-vs-full divergence",
            (
                "value_pdf is a 10% sample capped at 150k rows, the hourly figure uses the first 2000 "
                "chronological points, the P-vs-W scatter a 10% sample capped at 20k -- none are "
                "representative; use the full-table value_stats / outliers / continuity aggregates for "
                "any feature-quality decision."
            ),
        ),
    ]
)

_areas = {
    "Domain understanding": [
        "one building's electricity, heating and cooling series, each as a P and a W table, at 1min / 15min / 1h; total plus sub-channels (PV, CHP, cooling electricity)",
        f"sign conventions: { {f'{e}.{c}': d['sign'] for e, cols in w_direction.items() for c, d in cols.items()} }",
        f"P vs W-increment relationship: {'unresolved -- no alignment with |corr| >= 0.9' if not _strong else _strong}",
    ],
    "Structure and engineering": [
        f"6 Bronze tables with key (frequency, datetime_utc); rows {totals}",
        "the same signal is stored at three resolutions in one table; no unit column",
        f"P/W schema parity {schema_parity}; the CHP electricity column appears in two tables (mirror pairs: {[(a, b) for a, b, _ in mirrors] or 'none'})",
    ],
    "Temporal": [
        f"coverage/longest gap per table: { {e: {fq: (continuity[e][fq]['coverage_pct'], continuity[e][fq]['longest_gap_steps']) for fq in continuity[e]} for e in ENERGY} }",
        f"hour, weekday and month profiles for {list(energy_profiles)}",
    ],
    "Spatial": ["single site; no location column"],
    "Data quality": [
        f"duplicate key groups {[(e, dup[e]['dup_groups']) for e in ENERGY if dup[e]['dup_groups']] or 'none'}; 5-sigma outliers {outliers}",
        f"W meters counted with a negative sign or decreasing: { {e: [c for c, d in cols.items() if d['down'] > d['up']] for e, cols in w_direction.items()} }",
    ],
    "Statistical patterns": [
        "diurnal, weekday and seasonal profiles of each P channel (see Energy Profiles)",
        f"value ranges and negative-value counts per column: {_neg}",
    ],
    "Relationships": [
        f"P vs W (same timestamp) correlation: {pw_rel}",
        f"P vs W-increment best alignment: {(_best['family'], _best['column'], _best['frequency']) if _best else 'not computed'}",
    ],
    "Analytics use": [
        "load and generation channels for one site at minute resolution; supports profile and balance analysis once the P/W meaning is fixed"
    ],
    "ML use": [
        "candidate targets are the P channels; whether W leaks future energy relative to P depends on the unresolved P/W meaning (see ML-Readiness)"
    ],
    "AI / knowledge use": [
        "no text or label column; channel names carry the only semantics"
    ],
}

write_profiling(
    SOURCE,
    NB_KEY,
    SECTION_TITLE,
    blocks=[
        ("Profile", "\n".join(_profile)),
        ("Data Quality", "\n".join(_dq)),
        ("Unit & Semantic Validation", "\n".join(_unit)),
        ("Categorical / Domain Validation", "\n".join(_domain)),
        ("Temporal Semantics", "\n".join(_temporal)),
        ("Temporal Consistency", "\n".join(_tcons)),
        ("Physical Consistency", "\n".join(_pcons)),
        ("Regime / Version Evidence", "\n".join(_regime)),
        ("Coverage & Sampling Bias", "\n".join(_coverage)),
        ("Relationships", "\n".join(_rel)),
        ("Energy Profiles", "\n".join(_patterns)),
        (
            "EDA Findings",
            "\n".join(
                [
                    f"- dup composition: {dup}",
                    "- continuity (coverage % / longest gap steps): "
                    + str(
                        {
                            e: {
                                fq: (
                                    continuity[e][fq]["coverage_pct"],
                                    continuity[e][fq]["longest_gap_steps"],
                                )
                                for fq in continuity[e]
                            }
                            for e in ENERGY
                        }
                    ),
                    f"- 5-sigma outliers: {outliers}",
                    f"- mirror/duplicate columns: {[(a, b, w) for a, b, w in mirrors]}",
                    f"- P<->W relationship: {pw_rel}",
                ]
            ),
        ),
        ("ML-Readiness Evidence", _ml),
        ("Observations by Area", area_block(_areas)),
        ("Silver Implications", "\n".join(_silver)),
    ],
    figures=figs,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## EDA -- HONDA IOT WEATHER
# MAGIC
# MAGIC **Purpose:** Profile honda_iot_weather -- schema, missingness, constant
# MAGIC columns, per-frequency continuity against an independent calendar, value
# MAGIC plausibility (Ta range, Igm >= 0), stuck runs, duplicate keys (identical
# MAGIC vs conflicting), and the layered modelling-risk checklist -- as evidence
# MAGIC for Silver design and for the energy<->weather join in notebook 03.

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql import Window
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Configuration
CATALOG = "energy_commerce_retail_media"
BRONZE_SCHEMA = "bronze"
SOURCE = "honda_iot"
NB_KEY = "02_weather"
SECTION_TITLE = "Weather table (honda_iot_weather)"
TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.honda_iot_weather"
FREQ_SECONDS = {"1min": 60, "15min": 900, "1h": 3600}
# Ta = air temperature degC ; Igm = global irradiance W/m2.
PLAUSIBLE = {"Ta": (-40.0, 50.0), "Igm": (0.0, 1500.0)}
TS_TZ = "UTC"

# COMMAND ----------

# DBTITLE 1,Profile -- rows, missingness, approx distinct, value ranges (one agg)
df = spark.table(TABLE)
COLS = df.columns
VCOLS = [c for c in COLS if c not in ("frequency", "datetime_utc")]
ranges = {}
for c in VCOLS:
    v = safe_num(c)
    b = PLAUSIBLE.get(c.split("_")[-1])
    ranges |= {
        c + "_min": F.min(v),
        c + "_max": F.max(v),
        c + "_avg": F.avg(v),
        c + "_sd": F.stddev(v),
        c + "_p": F.percentile_approx(v, [0.01, 0.5, 0.99]),
        c + "_zero": F.sum((v == 0).cast("long")),
        c + "_negative": F.sum((v < 0).cast("long")),
        c + "_non_numeric": F.sum(
            (F.col(c).isNotNull() & (F.trim(F.col(c)) != "") & v.isNull()).cast("long")
        ),
    }
    if b:
        ranges[c + "_out_of_range"] = F.sum(((v < b[0]) | (v > b[1])).cast("long"))
P = profile_frame(df, extra=ranges)
S = P["extra"]
total = P["total"]
constant_cols = constant_column_names(df, P)
print(f"rows={total}  columns={len(COLS)}  ->  {COLS}")
for c in COLS:
    print(
        f"  {c:<32} missing={P['miss'][c]:>10} "
        f"rate={P['miss'][c] / total:.4f} approx_distinct={P['acd'][c]}"
    )
print("constant columns (exact):", constant_cols)
for c in VCOLS:
    print(f"  {c:<32}", {k[len(c) + 1 :]: S[k] for k in S if k.startswith(c + "_")})

# COMMAND ----------

# DBTITLE 1,Rows per frequency (one groupBy)
d = (
    df.groupBy("frequency")
    .agg(
        F.count(F.lit(1)).alias("rows"),
        F.min("datetime_utc").alias("min_ts"),
        F.max("datetime_utc").alias("max_ts"),
        F.countDistinct("datetime_utc").alias("distinct_ts"),
    )
    .orderBy("frequency")
    .collect()
)
freq_rows = [(x["frequency"], x["rows"]) for x in d]
freq_cov = {x["frequency"]: x.asDict() for x in d}
print([x.asDict() for x in d])

# COMMAND ----------

# DBTITLE 1,Duplicate (frequency, datetime_utc) key -- identical vs conflicting (one groupBy)
dq = dup_key_composition(df, ["frequency", "datetime_utc"])
print("duplicate key composition:", dq)

# COMMAND ----------

# DBTITLE 1,Temporal continuity -- coverage vs an INDEPENDENT per-frequency calendar
cg = continuity_grid(df, "datetime_utc", FREQ_SECONDS, entity_col="frequency")
continuity = cg["per_entity"]
for fq, r in continuity.items():
    print(fq, r)

# COMMAND ----------

# DBTITLE 1,Categorical domain (`frequency`) + regime split (first half vs second half)
freq_domain = categorical_domain(df, "frequency", FREQ_SECONDS.keys(), name="frequency")
if freq_domain["unexpected_count"]:
    print(
        f"UNEXPECTED frequency label(s): {freq_domain['unexpected']} -- ingestion defect"
    )

_span = [
    (v["min_ts"], v["max_ts"])
    for v in freq_cov.values()
    if v.get("min_ts") and v.get("max_ts")
]
regime = None
# min/max_ts are strings (F.min/F.max on a Bronze string column) -- parse them.
mid = (
    iso_midpoint(min(s[0] for s in _span), max(s[1] for s in _span)) if _span else None
)
if mid:
    regime = {"cut": mid, **regime_population_shift(df, "datetime_utc", VCOLS, mid)}
    print(
        f"regime split at {mid}: {regime['pre_rows']} / {regime['post_rows']}, flipped={regime['flipped']}"
    )

# COMMAND ----------

# DBTITLE 1,Stuck-run detection per value column (one windowed pass)
w2 = Window.partitionBy("frequency").orderBy("datetime_utc")
df_1h = df.where(F.col("frequency") == "1h")
for c in VCOLS:
    v = safe_num(c)
    df_1h = df_1h.withColumn(
        f"{c}_stuck",
        (
            v.isNotNull() & (v == F.lag(v, 1).over(w2)) & (v == F.lag(v, 11).over(w2))
        ).cast("long"),
    )
stuck = (
    df_1h.agg(*[F.sum(F.col(f"{c}_stuck")).alias(c) for c in VCOLS]).first().asDict()
)
print("stuck>=12-run (1h):", stuck)

# COMMAND ----------

# DBTITLE 1,Samples for figures + full-table diurnal profile
value_pdf = (
    df.select(*[safe_num(c).alias(c) for c in VCOLS])
    .sample(0.1, seed=42)
    .limit(150_000)
    .toPandas()
)
ts_pdf = (
    df.where(F.col("frequency") == "1h")
    .select("datetime_utc", *[safe_num(c).alias(c) for c in VCOLS])
    .orderBy("datetime_utc")
    .limit(3000)
    .toPandas()
)
hourly = (
    df.where(F.col("frequency") == "1h")
    .groupBy(F.hour(F.try_to_timestamp("datetime_utc")).alias("hod"))
    .agg(*[F.avg(safe_num(c)).alias(c) for c in VCOLS])
    .orderBy("hod")
    .collect()
)

# COMMAND ----------

# DBTITLE 1,Figure -- frequency overview (rows / coverage % / longest gap)
figs = []
if facet_bars(
    {
        "rows per frequency": freq_rows,
        "coverage % (independent calendar)": [
            (fq, continuity[fq]["coverage_pct"])
            for fq in FREQ_SECONDS
            if fq in continuity
        ],
        "longest gap (steps)": [
            (fq, continuity[fq]["longest_gap_steps"])
            for fq in FREQ_SECONDS
            if fq in continuity
        ],
    },
    "Honda weather -- frequency overview",
    "honda_weather_frequency.png",
    rot=0,
):
    figs.append(("Honda weather -- frequency overview", "honda_weather_frequency.png"))

# COMMAND ----------

# DBTITLE 1,Figure -- value distributions, hourly window, diurnal profile
if facet_hists(
    {c: value_pdf[c].dropna().tolist() for c in VCOLS},
    "Honda weather -- value distribution per column (sampled)",
    "honda_weather_value_distributions.png",
):
    figs.append(
        (
            "Honda weather -- value distribution per column",
            "honda_weather_value_distributions.png",
        )
    )
if not ts_pdf.empty:
    fig, axes = plt.subplots(len(VCOLS), 1, figsize=(12, 3 * len(VCOLS)), squeeze=False)
    for i, c in enumerate(VCOLS):
        axes[i][0].plot(range(len(ts_pdf)), ts_pdf[c], linewidth=0.8)
        axes[i][0].set_title(f"Honda weather -- {c} (first 3000 hourly points)")
    fig.tight_layout()
    _save_and_show(fig, "honda_weather_hourly_window.png")
    figs.append(
        (
            "Honda weather -- first 3000 hourly points per column",
            "honda_weather_hourly_window.png",
        )
    )


def _diurnal_draw(c):
    def draw_diurnal(ax):
        ax.plot([h["hod"] for h in hourly], [h[c] for h in hourly], marker="o")
        ax.set_xlabel("hour", fontsize=7)

    return draw_diurnal


if _facet_grid(
    [(c, _diurnal_draw(c)) for c in VCOLS],
    "Honda weather -- mean value by hour of day",
    "honda_weather_diurnal_profiles.png",
    ncols=2,
):
    figs.append(
        (
            "Honda weather -- mean value by hour of day",
            "honda_weather_diurnal_profiles.png",
        )
    )

# COMMAND ----------

# DBTITLE 1,Findings
print("constant columns:", constant_cols)
print("duplicate key composition:", dq)
print("continuity:", continuity)
print("stuck runs:", stuck)

# COMMAND ----------

# DBTITLE 1,Export profiling findings -> src/schemas/profiling/honda_iot.md
_prof = ["| column | missing | rate | approx_distinct |", "|---|---|---|---|"]
for c in COLS:
    _prof.append(
        f"| {c} | {P['miss'][c]} | {P['miss'][c] / total:.4f} | {P['acd'][c]} |"
    )
_prof.append("")
_prof.append(f"Rows: {total}. Constant columns (exact): {constant_cols or 'none'}.")

_dist = [
    "| column | min | p01 | p50 | p99 | max | mean | sd | zero | negative | non_numeric |",
    "|---|---|---|---|---|---|---|---|---|---|---|",
]
for c in VCOLS:
    p = S[c + "_p"] or [None, None, None]
    _dist.append(
        f"| {c} | {S[c + '_min']} | {p[0]} | {p[1]} | {p[2]} | {S[c + '_max']} | "
        f"{S[c + '_avg']} | {S[c + '_sd']} | {S[c + '_zero']} | {S[c + '_negative']} | "
        f"{S[c + '_non_numeric']} |"
    )

_oor = {c: S.get(c + "_out_of_range") for c in VCOLS if c + "_out_of_range" in S}

_unit = [
    para(
        "Value columns: Ta = air temperature (degC), Igm = global irradiance (W/m2).",
        f"Plausible bounds {PLAUSIBLE}.",
    ),
]
for c in VCOLS:
    _unit.append(
        f"- {c}: range {S[c + '_min']}..{S[c + '_max']}, mean {S[c + '_avg']}, "
        f"out-of-plausible-range rows {S.get(c + '_out_of_range')}, negative {S[c + '_negative']}, "
        f"non-numeric {S[c + '_non_numeric']}"
    )
_unit.append(
    para(
        "Igm at night is 0 by physics -- a large zero count is expected, not missingness;",
        "a negative Igm or an Igm far above ~1200 W/m2 in Germany is a sensor fault.",
    )
)

_temporal = [
    para(
        f"datetime_utc is treated as {TS_TZ} per the dataset name.",
        "Coverage below is measured against an INDEPENDENT calendar",
        "(expected = span / step + 1), so 100% means genuinely gap-free.",
    ),
    "",
    "| frequency | observed | expected | coverage % | longest gap (steps) | on-step % |",
    "|---|---|---|---|---|---|",
]
for fq in FREQ_SECONDS:
    r = continuity.get(fq)
    if r:
        _temporal.append(
            f"| {fq} | {r['observed']} | {r['expected']} | {r['coverage_pct']} | "
            f"{r['longest_gap_steps']} | {r['on_step_pct']} |"
        )

_dq = [f"Duplicate (frequency, datetime_utc) key composition: {dq}."]
if dq.get("conflicting", 0) > 0:
    _dq.append(
        para(
            "Conflicting duplicate keys exist (same key, differing non-key values) --",
            "Silver load needs a deterministic de-duplication rule for this table.",
        )
    )
elif dq.get("dup_groups", 0) > 0:
    _dq.append(
        "Duplicate keys are all fully identical rows -- a plain distinct suffices."
    )
else:
    _dq.append("(frequency, datetime_utc) is unique in Bronze.")
_dq.append(f"Stuck-run rows (value == value 1 and 11 steps back, 1h): {stuck}.")

_domain = [
    para(
        "`frequency` values vs the known resolution set (1min / 15min / 1h):",
        f"unexpected={freq_domain['unexpected'] or 'none'},",
        f"unused={freq_domain['unused_allowed'] or 'none'}.",
    ),
]
if freq_domain["unexpected_count"]:
    _domain.append(
        "-> an unexpected label is an INGESTION defect (a bad partition value), not a source finding."
    )

_regime = [
    para(
        "Rows split at the series midpoint. A large null-rate or distinct-count",
        "move on one side points to a sensor outage window, not a physical change",
        "(this measures coverage, not signal level).",
    ),
]
if regime:
    _regime.append(
        f"- cut {regime['cut']}: pre={regime['pre_rows']}, post={regime['post_rows']}; "
        f"columns with a >=50pt null-rate move: {regime['flipped'] or 'none'}."
    )
    for c in regime["flipped"]:
        v = regime["per_column"][c]
        _regime.append(
            f"  - `{c}`: null-rate pre={v['pre_null_rate']} post={v['post_null_rate']}."
        )
else:
    _regime.append("- no datetime span -- not assessable.")

_coverage = [
    f"Rows per frequency: { {k: v['rows'] for k, v in freq_cov.items()} }.",
    para(
        "The 1min partition dominates. Any statistic pooled across frequencies is really a",
        "1min statistic. Single weather source (no station id) -- the only split axis is time.",
    ),
    para(
        "This weather series is the feature side of the energy<->weather join (03); if it does",
        "not cover the full energy span, an inner join silently truncates the training window.",
    ),
]

_findings = []
for fq, r in continuity.items():
    if r["coverage_pct"] is not None and r["coverage_pct"] < 99.0:
        _findings.append(
            f"- {fq}: {r['coverage_pct']}% coverage, longest gap {r['longest_gap_steps']} steps."
        )
if constant_cols:
    _findings.append(f"- Constant columns carry no signal: {constant_cols}.")
if _oor and any(_oor.values()):
    _findings.append(f"- Out-of-plausible-range values present: {_oor}.")
if any(stuck.values()):
    _findings.append(f"- Sensor stuck-runs detected (1h): {stuck}.")
_findings_md = (
    "\n".join(_findings) if _findings else "No material data-quality issues found."
)

_silver = [
    "- Type conversion: datetime_utc -> timestamp (UTC); value columns -> double."
]
if dq.get("conflicting", 0) > 0:
    _silver.append(
        "- De-duplicate (frequency, datetime_utc) with a deterministic rule before Silver."
    )
if constant_cols:
    _silver.append(f"- Constant columns {constant_cols} can be dropped from Silver.")
if _oor and any(_oor.values()):
    _silver.append(
        "- Apply plausibility bounds; quarantine out-of-range weather readings rather than "
        "silently nulling them."
    )
_silver.append(
    "- Keep `frequency` in the grain; do not resample or blend resolutions at Silver."
)

_ml = ml_readiness_block(
    [
        (
            "Grain / grain drift",
            (
                "One row per (frequency, datetime_utc). Joining to the energy tables (03) must be on the "
                "same key at the same frequency; resampling drifts the grain."
            ),
        ),
        (
            "Join multiplication (1:N / M:N expansion)",
            (
                "Single weather source -> energy<->weather is at most 1:1 per (frequency, timestamp); see "
                "03 for the confirmed match rate."
            ),
        ),
        (
            "Target contamination",
            (
                "Weather is a feature, not a target here -- but if a weather variable becomes a target, its "
                "own future values and any smoothed/rolling version must be excluded from features."
            ),
        ),
        (
            "Temporal / post-event leakage",
            (
                "A weather reading timestamped at interval end already summarises that interval -- for a "
                "nowcast at time t use only readings strictly before t."
            ),
        ),
        (
            "Proxy leakage",
            (
                "Igm is a near-deterministic function of time-of-day and season -- a model given both Igm "
                "and a fine-grained clock feature is partly memorising the calendar."
            ),
        ),
        (
            "Split / entity leakage",
            "No entity dimension -- split by contiguous date range; never mix frequencies within a split.",
        ),
        (
            "Historical-reference (point-in-time) leakage",
            (
                "A diurnal / seasonal climatology used as a feature must be built only from data before the "
                "prediction point, not averaged over the full series."
            ),
        ),
        (
            "Survivorship / coverage bias",
            (
                "Continuity above shows the real gaps; a sensor-outage window is a real absence, not zero. "
                "If the weather span is shorter than the energy span, the joined training set is truncated."
            ),
        ),
        (
            "Missingness leakage",
            (
                "Ta/Igm missing together (2.4% each in the first profile) likely marks a station outage -- "
                "an 'is-missing' flag can leak the outage timing."
            ),
        ),
        (
            "Duplicate-event leakage",
            f"Duplicate key composition {dq} -- de-duplicate before treating a reading as one observation.",
        ),
        (
            "Target / feature temporal misalignment",
            (
                "Weather at interval end vs energy P at the instant vs energy W cumulative -- align all to "
                "one timestamp convention before joining."
            ),
        ),
        (
            "Unit / sign / circular-feature leakage",
            (
                "Ta in degC, Igm in W/m2 -- confirm before combining with any external weather source in a "
                "different unit. No mirror columns in this single-column-pair table."
            ),
        ),
        (
            "Data-generation-process leakage",
            (
                "Any upstream gap-filling / interpolation of the weather series is part of the "
                "data-generation process; an interpolated stretch will look unnaturally smooth."
            ),
        ),
        ("Class / label instability", "Not applicable -- continuous variables only."),
        (
            "Label availability lag",
            (
                "Not applicable to weather-as-feature; if forecast weather is used instead of observed, its "
                "publication lag and horizon must be respected."
            ),
        ),
        (
            "Source / version / regime change",
            (
                "A single sensor deployment; a recalibration or sensor swap would show as a step change "
                "with no physical cause."
            ),
        ),
        (
            "Sample-vs-full divergence",
            (
                "value_pdf is a 10% sample capped at 150k rows and the hourly-window figure uses the first "
                "3000 points -- the diurnal-profile figure IS a full-table groupBy average and is safe; use "
                "the full-table S aggregate for any threshold decision."
            ),
        ),
    ]
)

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
        ("Regime / Version Evidence", "\n".join(_regime)),
        ("Coverage & Sampling Bias", "\n".join(_coverage)),
        ("Distributions", "\n".join(_dist)),
        ("EDA Findings", _findings_md),
        ("ML-Readiness Evidence", _ml),
        ("Silver Implications", "\n".join(_silver)),
    ],
    figures=figs,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## EDA -- HONDA IOT RELATIONSHIPS AND FINDINGS
# MAGIC
# MAGIC **Purpose:** Cross-table checks across the 7 Honda IoT Bronze tables --
# MAGIC (frequency, datetime_utc) key uniqueness, pairwise overlap, full 7-way
# MAGIC join yield, the energy<->weather match rate and the shared time window,
# MAGIC and the layered modelling-risk checklist. All key-overlap analysis is
# MAGIC derived from one tagged union + one presence matrix rather than repeated
# MAGIC pairwise joins.

# COMMAND ----------

# DBTITLE 1,Imports
import numpy as np
import pandas as pd
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Configuration
CATALOG = "energy_commerce_retail_media"
BRONZE_SCHEMA = "bronze"
SOURCE = "honda_iot"
NB_KEY = "03_relationships_and_findings"
SECTION_TITLE = "Cross-table relationships (7 Honda IoT tables)"
DATASETS = [
    "electricity_p",
    "electricity_w",
    "heating_p",
    "heating_w",
    "cooling_p",
    "cooling_w",
    "weather",
]
ENERGY = [d for d in DATASETS if d != "weather"]
# P and W of a metric share one table; column measurement_type holds p or w.
TABLES = {
    d: f"{CATALOG}.{BRONZE_SCHEMA}.honda_iot_{d if d == 'weather' else d[:-2]}"
    for d in DATASETS
}


def honda_frame(dataset):
    df = bronze_frame(TABLES[dataset])
    if dataset == "weather":
        return df
    return df.where(F.col("measurement_type") == dataset[-1]).drop("measurement_type")


# COMMAND ----------

# DBTITLE 1,Tagged union of (frequency, datetime_utc, source) across all 7 tables
u = None
for d in TABLES:
    part = honda_frame(d).select("frequency", "datetime_utc", F.lit(d).alias("src"))
    u = part if u is None else u.unionByName(part)

# COMMAND ----------

# DBTITLE 1,Row counts + time span per (dataset, frequency)
grid = (
    u.groupBy("src", "frequency")
    .agg(
        F.min("datetime_utc").alias("min_ts"),
        F.max("datetime_utc").alias("max_ts"),
        F.countDistinct("datetime_utc").alias("distinct_ts"),
        F.count(F.lit(1)).alias("rows"),
    )
    .collect()
)
table_rows = {}
span = {}
for x in grid:
    table_rows[x["src"]] = table_rows.get(x["src"], 0) + x["rows"]
    lo, hi = span.get(x["src"], (x["min_ts"], x["max_ts"]))
    span[x["src"]] = (min(lo, x["min_ts"]), max(hi, x["max_ts"]))
    print(x.asDict())

# COMMAND ----------

# DBTITLE 1,Key-presence matrix -- one groupBy over the union
p = u.groupBy("frequency", "datetime_utc").agg(
    *[F.max((F.col("src") == d).cast("int")).alias(d) for d in DATASETS]
)

# COMMAND ----------

# DBTITLE 1,Overlap / referential stats -- one agg over the presence matrix
pairs = [(a, b) for i, a in enumerate(DATASETS) for b in DATASETS[i + 1 :]]
stats = (
    p.agg(
        F.count(F.lit(1)).alias("union_keys"),
        *[F.sum(d).alias("present__" + d) for d in DATASETS],
        F.sum(F.least(*[F.col(d) for d in DATASETS])).alias("all_seven"),
        *[F.sum(F.col(a) * F.col(b)).alias(f"pair__{a}__{b}") for a, b in pairs],
        *[F.sum(F.col(e) * F.col("weather")).alias(f"ew__{e}") for e in ENERGY],
    )
    .first()
    .asDict()
)

union_ct = stats["union_keys"]
present = {d: stats["present__" + d] for d in DATASETS}
grid_missing = {d: union_ct - present[d] for d in DATASETS}
overlap = {(a, b): stats[f"pair__{a}__{b}"] for a, b in pairs}
join_yield = {e: (present[e], stats[f"ew__{e}"]) for e in ENERGY}
key_unique = {d: table_rows.get(d, 0) == present[d] for d in DATASETS}
seven_ct = stats["all_seven"]

print(f"union keys={union_ct}  keys in all 7={seven_ct}")
for d in DATASETS:
    print(
        f"  {d:<16} present={present[d]:>10}  missing vs union={grid_missing[d]:>10}  "
        f"rows={table_rows.get(d)}  key_unique={key_unique[d]}"
    )
_ewr = {e: round(m / t * 100, 1) for e, (t, m) in join_yield.items() if t}
print("energy<->weather match rate:", _ewr)

# COMMAND ----------

# DBTITLE 1,Cross-source temporal overlap (energy vs weather span)
overlap_win = cross_source_overlap(
    {
        "energy": span.get("electricity_p", (None, None)),
        "weather": span.get("weather", (None, None)),
    }
)
print("shared window energy x weather:", overlap_win)

# COMMAND ----------

# DBTITLE 1,Figure -- pairwise overlap heatmap
figs = []
n = len(DATASETS)
m = np.zeros((n, n))
for i, a in enumerate(DATASETS):
    m[i, i] = present[a]
    for j, b in enumerate(DATASETS):
        if (a, b) in overlap:
            m[i, j] = m[j, i] = overlap[(a, b)]
fig, ax = plt.subplots(figsize=(8, 7))
im = ax.imshow(np.log10(m + 1), cmap="viridis")
fig.colorbar(im, ax=ax, label="log10(shared keys + 1)")
ax.set_xticks(range(n))
ax.set_xticklabels(DATASETS, rotation=45, ha="right")
ax.set_yticks(range(n))
ax.set_yticklabels(DATASETS)
ax.set_title("Honda -- pairwise (frequency, datetime_utc) overlap")
fig.tight_layout()
_save_and_show(fig, "honda_overlap_matrix.png")
figs.append(
    ("Honda pairwise (frequency, datetime_utc) overlap", "honda_overlap_matrix.png")
)

# COMMAND ----------

# DBTITLE 1,Figure -- missing keys vs union + energy<->weather join yield
if barplot(
    list(grid_missing.items()),
    "Honda -- (frequency, datetime_utc) keys missing vs union",
    "dataset",
    "missing keys",
    rot=30,
    filename="honda_keys_missing_vs_union.png",
):
    figs.append(
        (
            "Honda -- keys missing from each table vs the union",
            "honda_keys_missing_vs_union.png",
        )
    )
x = np.arange(len(ENERGY))
fig, ax = plt.subplots(figsize=(11, 4))
ax.bar(x - 0.2, [join_yield[e][0] for e in ENERGY], width=0.4, label="energy keys")
ax.bar(
    x + 0.2, [join_yield[e][1] for e in ENERGY], width=0.4, label="matched to weather"
)
ax.set_xticks(x)
ax.set_xticklabels(ENERGY, rotation=30, ha="right")
ax.legend()
ax.set_title("Honda -- energy<->weather join yield on (frequency, datetime_utc)")
ax.set_ylabel("keys")
fig.tight_layout()
_save_and_show(fig, "honda_energy_weather_join_yield.png")
figs.append(
    ("Honda -- energy <-> weather join yield", "honda_energy_weather_join_yield.png")
)

# COMMAND ----------

# DBTITLE 1,Hourly energy and weather frame (1h, inner join on datetime_utc)
WCOLS = [
    c
    for c in bronze_frame(TABLES["weather"]).columns
    if c not in ("frequency", "datetime_utc")
]
frame = (
    bronze_frame(TABLES["weather"])
    .where(F.col("frequency") == "1h")
    .select("datetime_utc", *[safe_num(c).alias(c) for c in WCOLS])
)
ENERGY_COLS = []
for e_ in [d_ for d_ in ENERGY if d_.endswith("_p")]:
    pt = honda_frame(e_).where(F.col("frequency") == "1h")
    vc = [c for c in pt.columns if c not in ("frequency", "datetime_utc")]
    fam = e_[: -len("_p")]
    frame = frame.join(
        pt.select("datetime_utc", *[safe_num(c).alias(f"{fam}_{c}") for c in vc]),
        "datetime_utc",
        "inner",
    )
    ENERGY_COLS += [f"{fam}_{c}" for c in vc]
hourly = frame.toPandas()
hourly["ts"] = pd.to_datetime(hourly["datetime_utc"])
hourly = hourly.sort_values("ts").set_index("ts").drop(columns=["datetime_utc"])
print(hourly.shape, ENERGY_COLS, WCOLS)

# COMMAND ----------

# DBTITLE 1,Energy against weather -- correlation at lag 0 and across +-6 hours
ew_corr = {"pearson": {}, "spearman": {}, "best_lag": {}}
for ec in ENERGY_COLS:
    for wc in WCOLS:
        pair = hourly[[ec, wc]].dropna()
        if len(pair) < 100:
            continue
        key = f"{ec} ~ {wc.split('_')[-1]}"
        ew_corr["pearson"][key] = round(float(pair[ec].corr(pair[wc])), 3)
        ew_corr["spearman"][key] = round(
            float(pair[ec].corr(pair[wc], method="spearman")), 3
        )
        lags = {k: hourly[ec].corr(hourly[wc].shift(k)) for k in range(-6, 7)}
        lags = {k: v for k, v in lags.items() if pd.notna(v)}
        if lags:
            k_best = max(lags, key=lambda k: abs(lags[k]))
            ew_corr["best_lag"][key] = (k_best, round(float(lags[k_best]), 3))
print(ew_corr["pearson"])

# COMMAND ----------

# DBTITLE 1,Energy against weather -- by season and by weather bin
season_of = {
    12: "winter",
    1: "winter",
    2: "winter",
    6: "summer",
    7: "summer",
    8: "summer",
}
ew_season, ew_bins = {}, {}
seasons = hourly.index.month.map(lambda m: season_of.get(m, "other"))
for ec in ENERGY_COLS:
    for wc in WCOLS:
        key = f"{ec} ~ {wc.split('_')[-1]}"
        for name in ("winter", "summer"):
            sub = hourly[seasons == name][[ec, wc]].dropna()
            if len(sub) > 100:
                ew_season.setdefault(key, {})[name] = round(
                    float(sub[ec].corr(sub[wc])), 3
                )
for ec in ENERGY_COLS:
    for wc in WCOLS:
        pair = hourly[[ec, wc]].dropna()
        if len(pair) < 100 or pair[wc].nunique() < 10:
            continue
        bins = pd.cut(pair[wc], 8)
        g = pair.groupby(bins, observed=True)[ec].mean()
        ew_bins[f"{ec} by {wc.split('_')[-1]}"] = [
            (str(k), round(float(v), 1)) for k, v in g.items()
        ]
print(list(ew_season)[:4], list(ew_bins)[:4])

# COMMAND ----------

# DBTITLE 1,Findings
print("key unique per table       :", key_unique)
print("keys missing vs union      :", grid_missing)
print("energy<->weather join yield :", join_yield)
print("keys shared by all 7        :", seven_ct, "of", union_ct)

# COMMAND ----------

# DBTITLE 1,Export profiling findings -> src/schemas/profiling/honda_iot.md
_ek = [
    "| table | rows | distinct keys present | missing vs union | key unique |",
    "|---|---|---|---|---|",
]
for d_ in DATASETS:
    _ek.append(
        f"| honda_iot_{d_} | {table_rows.get(d_)} | {present[d_]} | "
        f"{grid_missing[d_]} | {key_unique[d_]} |"
    )
_ek += [
    "",
    para(
        f"Union of (frequency, datetime_utc) keys: {union_ct}.",
        f"Keys shared by all 7 tables: {seven_ct} ({seven_ct / union_ct * 100:.1f}%).",
    ),
]

_rel = [
    "Energy<->weather match rate on (frequency, datetime_utc):",
    "",
    "| energy table | energy keys | matched to weather | % |",
    "|---|---|---|---|",
]
for e_ in ENERGY:
    tk, mk = join_yield[e_]
    _rel.append(
        f"| honda_iot_{e_} | {tk} | {mk} | {mk / tk * 100:.1f} |"
        if tk
        else f"| honda_iot_{e_} | 0 | 0 | |"
    )
_rel += ["", "Pairwise shared-key counts:", "", "| pair | shared keys |", "|---|---|"]
for (a_, b_), v_ in overlap.items():
    _rel.append(f"| {a_} + {b_} | {v_} |")
_rel += [
    "",
    "Relationship cardinality (energy stream -> weather, on (frequency, datetime_utc)):",
    "",
    "| energy table | child rows | distinct child keys | matched to weather | orphan keys | max fan-out |",
    "|---|---|---|---|---|---|",
]
for e_ in ENERGY:
    tk, mk = join_yield[e_]
    _rel.append(
        f"| honda_iot_{e_} | {table_rows.get(e_)} | {present[e_]} | {mk} | {present[e_] - mk} | "
        f"{'1 (key unique)' if key_unique[e_] else '>1 -- de-dup first'} |"
    )
_rel.append(
    para(
        "Because (frequency, datetime_utc) is unique in every table, every join here is 1:1 --",
        "the only cardinality risk is row LOSS on an inner join, quantified by the orphan-keys",
        "column, not row multiplication.",
    )
)

_rel.append("")
_rel.append(
    para(
        f"Energy (P tables, 1h) against weather on {hourly.shape[0]} hourly rows where every stream is present;",
        "Pearson and Spearman correlations at the same hour (energy column ~ weather column):",
    )
)
_rel.append(f"- Pearson: {ew_corr['pearson']}")
_rel.append(f"- Spearman: {ew_corr['spearman']}")
_rel.append(
    f"- Weather lead or lag (hours, corr) with the largest |corr| within +-6 h (positive = weather earlier): {ew_corr['best_lag']}"
)
_rel.append(
    f"- Correlation inside winter (Dec-Feb) and summer (Jun-Aug) only: {ew_season}"
)
_rel.append("- Mean of each energy column by weather bin (bin, mean):")
for k_, v_ in ew_bins.items():
    _rel.append(f"  - {k_}: {v_}")
_rel.append(
    para(
        "Correlations describe association in this one series; a weak or zero value is reported as found,",
        "and the sign of an energy column follows the source's sign convention (see 01).",
    )
)

_coverage = [
    para(
        "Time span per dataset:",
        str({d: (str(span[d][0]), str(span[d][1])) for d in DATASETS if d in span}),
    ),
    para(
        "Energy x weather shared window:",
        str(overlap_win["common_window"]),
        "-- an energy+weather model can only train inside this window; an inner join outside",
        "it silently drops rows.",
    ),
    para(
        f"{union_ct - seven_ct} of {union_ct} keys are absent from at least one table",
        f"(per-table gaps: {grid_missing}). This is a CROSS-table completeness gap -- a single",
        "table can still be internally dense (see 01/02 continuity) while differing from another",
        "by a handful of timestamps.",
    ),
]

_verdict = [
    f"(frequency, datetime_utc) is unique in every Honda table: {all(key_unique.values())}.",
    f"{seven_ct} of {union_ct} keys ({seven_ct / union_ct * 100:.1f}%) are in all 7 tables.",
    f"Energy<->weather match rate: {_ewr}.",
    para(
        "A shared 1:1 key exists. A wide 'all Honda metrics at (frequency, datetime_utc)' table",
        "is feasible on the intersection but drops the non-overlapping tail; the natural Silver",
        "grain is one fact per dataset (or per metric joining P+W), the wide table is Gold.",
    ),
]

_findings = []
if not all(key_unique.values()):
    _findings.append(
        f"- Duplicate (frequency, datetime_utc) keys in: "
        f"{[d_ for d_ in DATASETS if not key_unique[d_]]}."
    )
if seven_ct < union_ct:
    _findings.append(
        f"- {union_ct - seven_ct} keys missing from at least one table (gaps: {grid_missing})."
    )
if any(v < 100 for v in _ewr.values()):
    _findings.append(
        f"- Energy<->weather join is lossy for: {[e_ for e_, v in _ewr.items() if v < 100]}."
    )
_findings_md = (
    "\n".join(_findings)
    if _findings
    else "All 7 tables align 1:1 on the key with full coverage."
)

_silver = []
if not all(key_unique.values()):
    _silver.append(
        "- De-duplicate per-table on (frequency, datetime_utc) before any Silver join."
    )
_silver.append(
    "- Silver grain: one fact table per dataset keyed on (frequency, datetime_utc); "
    "join P+W per metric where a combined metric fact is needed."
)
if any(v < 100 for v in _ewr.values()):
    _silver.append(
        "- Use outer joins (not inner) when combining energy and weather; carry an explicit "
        "unmatched flag."
    )

_ml = ml_readiness_block(
    [
        (
            "Grain / grain drift",
            (
                f"(frequency, datetime_utc) is unique in every table ({all(key_unique.values())}); all "
                "pairwise and 7-way joins hold that grain. Resampling to a common frequency would change it."
            ),
        ),
        (
            "Join multiplication (1:N / M:N expansion)",
            (
                "1:1 on the shared key -- no fan-out. The risk is the opposite: an inner 7-way join keeps "
                f"only {seven_ct} of {union_ct} keys."
            ),
        ),
        (
            "Target contamination",
            (
                "No target across these tables -- see 01/02. A wide feature row must not include the target "
                "metric's own value at the target timestamp."
            ),
        ),
        (
            "Temporal / post-event leakage",
            (
                "Energy and weather share the timestamp grid -- same-timestamp weather is a legitimate "
                "feature for same-timestamp energy, but no later energy/weather value may feed an earlier "
                "prediction."
            ),
        ),
        (
            "Proxy leakage",
            (
                "P and W of one metric were expected to be related and totals to be sums of their components; "
                "01 measures how far that holds (the P/W relation is unresolved there). Where they are related, a "
                "wide join makes collinear features trivially available."
            ),
        ),
        (
            "Split / entity leakage",
            (
                "Split by contiguous date range across ALL tables at once, never per-table by row, so a "
                "timestamp's energy and weather stay on one side."
            ),
        ),
        (
            "Historical-reference (point-in-time) leakage",
            "No slowly-changing attributes; a climatology feature must be built only from pre-cutoff data.",
        ),
        (
            "Survivorship / coverage bias",
            (
                f"Energy x weather shared window {overlap_win['common_window']} -- training outside it is "
                "impossible; the join yield table shows where each energy stream loses weather coverage."
            ),
        ),
        (
            "Missingness leakage",
            (
                "A missing key in one table at a timestamp present in others marks a per-stream outage -- an "
                "'is-present' flag per stream can leak the outage timing."
            ),
        ),
        (
            "Duplicate-event leakage",
            (
                f"Key uniqueness per table: {key_unique} -- de-duplicate any table that is not unique before "
                "joining or splitting."
            ),
        ),
        (
            "Target / feature temporal misalignment",
            (
                "P, W and weather may use different timestamp conventions (not established by the data; 01 tests "
                "the P/W alignment) -- align to one before building a wide row."
            ),
        ),
        (
            "Unit / sign / circular-feature leakage",
            "See 01 -- P<->W and component<->total relationships are physical identities, not signal.",
        ),
        (
            "Data-generation-process leakage",
            (
                "Any upstream alignment / gap-filling that made the 7 grids match is part of the "
                "data-generation process; the residual mismatch here is what survived it."
            ),
        ),
        ("Class / label instability", "Not applicable -- continuous metrics only."),
        (
            "Label availability lag",
            (
                "Interval-end readings are not available until the interval closes -- a nowcast cannot use "
                "the interval it is predicting."
            ),
        ),
        (
            "Source / version / regime change",
            (
                "One deployment; a sensor swap on any stream would show as a step change and a shift in that "
                "stream's coverage window."
            ),
        ),
        (
            "Sample-vs-full divergence",
            (
                "Every number here (key presence, pairwise overlap, join yield, spans) is a full Spark "
                "aggregation over the tagged-union presence matrix -- no sampling."
            ),
        ),
    ]
)

_areas = {
    "Domain understanding": [
        f"energy streams (electricity, heating, cooling; total and sub-channels) and one weather station (columns {WCOLS}) sharing one timestamp key",
        f"energy against weather (Pearson, same hour): {ew_corr['pearson']}",
    ],
    "Structure and engineering": [
        f"7 tables, key (frequency, datetime_utc) unique in each: {all(key_unique.values())}; keys in all seven {seven_ct} of {union_ct}",
        f"energy to weather join yield {_ewr}",
    ],
    "Temporal": [
        f"shared window {overlap_win['common_window']}",
        f"best weather lead/lag against energy within +-6 h: {ew_corr['best_lag']}",
    ],
    "Spatial": ["one site and one weather station; no coordinates"],
    "Data quality": [f"keys missing per table vs the union: {grid_missing}"],
    "Statistical patterns": [
        f"season-specific correlations: {ew_season}",
        f"energy by weather bin: {list(ew_bins)}",
    ],
    "Relationships": [
        f"Spearman (same hour): {ew_corr['spearman']}",
        f"pairwise key overlap: {len(overlap)} pairs, all present in the relationships section",
    ],
    "Analytics use": [
        "weather-driven load and PV analysis is possible on the shared window; the weather has two variables only (temperature, global irradiance)"
    ],
    "ML use": [
        f"a wide hourly table of {len(ENERGY_COLS)} energy columns and {len(WCOLS)} weather columns exists on {hourly.shape[0]} common hours"
    ],
    "AI / knowledge use": ["no text; column names carry the semantics"],
}

write_profiling(
    SOURCE,
    NB_KEY,
    SECTION_TITLE,
    [
        ("Entities / Keys", "\n".join(_ek)),
        ("Relationships", "\n".join(_rel)),
        ("Coverage & Sampling Bias", "\n".join(_coverage)),
        ("EDA Findings", _findings_md + "\n\n" + "\n".join(_verdict)),
        ("ML-Readiness Evidence", _ml),
        ("Observations by Area", area_block(_areas)),
        ("Silver Implications", "\n".join(_silver)),
    ],
    figures=figs,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## EDA -- IOT DEVICE TELEMETRY (SAMPLES)
# MAGIC
# MAGIC **Purpose:** Profile the `iot` dataset read in place from the Databricks
# MAGIC Samples Volume (no Bronze table) -- file inventory and README provenance,
# MAGIC schema and nesting, missingness, constant columns, duplicates, device /
# MAGIC record keys and readings per device, timestamp unit and range, geography
# MAGIC validity and per-country coordinate spread, sensor value plausibility and
# MAGIC shape, categorical consistency, indicators of generated data, and the
# MAGIC layered modelling-risk checklist -- as evidence.

# COMMAND ----------

# DBTITLE 1,Imports
import itertools

from pyspark.sql import Window
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "iot"
NB_KEY = "01_iot_devices"
SECTION_TITLE = "IoT device telemetry (Samples: iot)"
ROOT = f"{SAMPLES_VOLUME_ROOT}/iot"
GLOBAL_BBOX = (-90.0, 90.0, -180.0, 180.0)
NAME_HINTS = {
    "device": ("device_id", "deviceid", "device"),
    "device_name": ("device_name", "devicename", "name"),
    "ip": ("ip",),
    "cca2": ("cca2",),
    "cca3": ("cca3",),
    "country": ("cn", "country"),
    "lat": ("latitude", "lat"),
    "lon": ("longitude", "lon", "lng"),
    "scale": ("scale", "unit"),
    "lcd": ("lcd",),
    "ts": ("timestamp", "ts", "event_time", "time"),
}
VALUE_HINTS = ("temp", "humidity", "battery", "c02", "co2")
# Physical plausibility bounds by column-name fragment (the unit is read from
# the `scale` column, so the temperature bound is deliberately wide).
BOUNDS = {"humidity": (0.0, 100.0), "temp": (-60.0, 150.0)}

# COMMAND ----------

# DBTITLE 1,Discover files
entries, kinds, frames = load_volume_frames(ROOT)
require_frames(frames, ROOT, kinds)
print(f"{ROOT}: {len(entries)} files")
for e in entries:
    print(f"  {file_kind(e['name']):<10} {e['size']:>12}  {e['path']}")
print({k: len(v) for k, v in kinds.items()})

# COMMAND ----------

# DBTITLE 1,Support files (README) -- provenance evidence
support = print_support_files(kinds)

# COMMAND ----------

# DBTITLE 1,Select the frame
FRAME, df, DATA_COLS = select_frame(frames, "json")

# COMMAND ----------

# DBTITLE 1,Resolve column roles
role = {}
for r, hints in NAME_HINTS.items():
    role[r] = next((c for h in hints for c in DATA_COLS if c.lower() == h), None)
value_cols = [c for c in DATA_COLS if any(h in c.lower() for h in VALUE_HINTS)]
print(role)
print("value columns:", value_cols)

# COMMAND ----------

# DBTITLE 1,Structural check -- corrupt records and nesting
nested, struct = structural_check(df, DATA_COLS)

# COMMAND ----------

# DBTITLE 1,Profile -- rows, missingness, approx distinct
prof = profile_frame(df.drop("__file"))
print(f"rows={prof['total']}  cols={len(prof['cols'])}")
for c in prof["cols"]:
    print(
        f"  {c[:40]:<40} missing={prof['miss'][c]:>8} "
        f"rate={prof['miss'][c] / prof['total'] if prof['total'] else 0:.4f} "
        f"approx_distinct={prof['acd'][c]}"
    )

# COMMAND ----------

# DBTITLE 1,Constant columns + full-row duplicates
d0 = df.drop("__file")
prof["constant"] = constant_column_names(d0, prof)
prof["dups"] = full_row_dup_count(d0, prof["total"])
print(f"constant={prof['constant']}  full-row duplicates={prof['dups']}")

# COMMAND ----------

# DBTITLE 1,Key candidates -- exact uniqueness
key_cands = [role[r] for r in ("device", "device_name", "ip") if role[r]]
uniq = exact_uniqueness(d0, key_cands)
for c, u in uniq.items():
    print(c, u)

# COMMAND ----------

# DBTITLE 1,Device / timestamp key duplicates
dev, tsc = role["device"], role["ts"]
dup_key = None
if dev and tsc:
    dup_key = dup_key_composition(d0, [dev, tsc])
    print(f"({dev}, {tsc}): {dup_key}")

# COMMAND ----------

# DBTITLE 1,Readings per device
per_device = None
if dev:
    pd_ = df.groupBy(qcol(dev)).count()
    per_device = (
        pd_.agg(
            F.count(F.lit(1)).alias("devices"),
            F.min("count").alias("min"),
            F.expr("percentile_approx(`count`, 0.5)").alias("p50"),
            F.expr("percentile_approx(`count`, 0.99)").alias("p99"),
            F.max("count").alias("max"),
        )
        .first()
        .asDict()
    )
    print(per_device)

# COMMAND ----------

# DBTITLE 1,Numeric parse yield + moments
num_cols = [c for c in DATA_COLS if c != "__file"]
num = numeric_scan(df, num_cols)
for c, s in num.items():
    if s["is_numeric"]:
        print(
            f"{c:<16} yield={s['yield']} min={s['min']} max={s['max']} "
            f"mean={fmt_num(s['mean'])} sd={fmt_num(s['sd'])} zero={s['zero']} negative={s['negative']}"
        )

# COMMAND ----------

# DBTITLE 1,Sensor plausibility
plaus = {}
for c in value_cols:
    if not num[c]["is_numeric"]:
        continue
    key = next((k for k in BOUNDS if k in c.lower()), None)
    lo, hi = BOUNDS.get(key, (None, None))
    plaus[c] = plausibility(df, c, lo=lo, hi=hi, sentinels=())
    print(
        c,
        {k: plaus[c].get(k) for k in ("min", "max", "mean", "below", "above", "zero")},
    )

# COMMAND ----------

# DBTITLE 1,Quantiles and distribution shape
quant, shape = {}, {}
for c in value_cols:
    if not num[c]["is_numeric"]:
        continue
    quant[c] = quantiles(df, c)
    v = to_double(c)
    shape[c] = (
        df.agg(F.skewness(v).alias("skew"), F.kurtosis(v).alias("kurt"))
        .first()
        .asDict()
    )
    k = num[c]["max"] - num[c]["min"] + 1
    is_int = dict(df.dtypes).get(c) in ("tinyint", "smallint", "int", "bigint")
    # Excess kurtosis of a uniform: -1.2 (continuous), -1.2*(k^2+1)/(k^2-1) (k integers).
    shape[c]["expected_uniform_kurt"] = (
        round(-1.2 * (k * k + 1) / (k * k - 1), 4) if is_int and k > 1 else -1.2
    )
    print(c, quant[c], shape[c])

# COMMAND ----------

# DBTITLE 1,Histograms (binned in Spark) + uniformity
hist, flat, flat_basis = {}, {}, {}
MAX_DISTINCT_VALUES = 200
for c in value_cols:
    s = num[c]
    if not s["is_numeric"]:
        continue
    hist[c] = hist_counts(df, c, s["min"], s["max"])
    is_int = dict(df.dtypes).get(c) in ("tinyint", "smallint", "int", "bigint")
    if is_int and prof["acd"][c] <= MAX_DISTINCT_VALUES:
        # Integer column: count per distinct value; 40 fixed bins would leave empty bins.
        counts = [r["count"] for r in df.groupBy(qcol(c)).count().collect()]
        flat_basis[c] = f"{len(counts)} distinct values"
    else:
        counts = [n for _, n in hist[c]]
        flat_basis[c] = "40 bins"
    mean_n = sum(counts) / len(counts) if counts else 0
    var = sum((n - mean_n) ** 2 for n in counts) / len(counts) if counts else 0
    flat[c] = round((var**0.5) / mean_n, 3) if mean_n else None
    print(c, "count coefficient of variation:", flat[c], f"({flat_basis[c]})")

# COMMAND ----------

# DBTITLE 1,Pairwise correlation of sensor columns
ncols = [c for c in value_cols if num[c]["is_numeric"]]
numdf = df.select(*[to_double(c).alias(c) for c in ncols])
corr = {(a, b): numdf.stat.corr(a, b) for a, b in itertools.combinations(ncols, 2)}
for pair, v in corr.items():
    print(pair, round(v, 4) if v is not None else None)
mirrors = mirror_columns({c: num[c] for c in ncols if num[c]["sd"]})
print("mirror pairs:", mirrors)

# COMMAND ----------

# DBTITLE 1,Categorical distributions
cat_cols = [c for c in prof["cols"] if 1 < prof["acd"][c] <= 60]
cat_dist = {}
for c in cat_cols:
    vc = df.groupBy(qcol(c)).count().orderBy(F.desc("count")).limit(30).collect()
    cat_dist[c] = [(r[0], r["count"]) for r in vc]
    print(c, cat_dist[c][:12])

# COMMAND ----------

# DBTITLE 1,Constant column values
const_vals = {}
for c in prof["constant"]:
    r = d0.select(as_str(c).alias("v")).where(F.col("v").isNotNull()).first()
    const_vals[c] = r["v"] if r else None
print(const_vals)

# COMMAND ----------

# DBTITLE 1,Colour label vs sensor values
lcd_profile = []
lcd_col = role["lcd"]
if lcd_col and lcd_col in prof["cols"] and 1 < prof["acd"][lcd_col] <= 12:
    aggs = [F.count(F.lit(1)).alias("rows")]
    for c in ncols:
        v = to_double(c)
        aggs += [
            F.min(v).alias(f"{c}__lo"),
            F.max(v).alias(f"{c}__hi"),
            F.avg(v).alias(f"{c}__mean"),
        ]
    for r in df.groupBy(qcol(lcd_col).alias("g")).agg(*aggs).orderBy("g").collect():
        lcd_profile.append(r.asDict())
        print(r.asDict())

# COMMAND ----------

# DBTITLE 1,Missing country name vs country code
cn_gap = None
cn_col, code_col = role["country"], role["cca3"] or role["cca2"]
if cn_col and code_col:
    miss_cn = is_missing(cn_col)
    present_code = ~is_missing(code_col)
    known = (
        d0.where(~miss_cn & present_code).select(qcol(code_col).alias("k")).distinct()
    )
    gap = d0.where(miss_cn & present_code).select(qcol(code_col).alias("k"))
    rows_missing = gap.count()
    codes_missing = gap.distinct().count()
    recoverable = gap.distinct().join(known, "k").count()
    cn_gap = {
        "rows_missing_name": rows_missing,
        "codes_involved": codes_missing,
        "codes_with_name_elsewhere": recoverable,
    }
    print(cn_gap)

# COMMAND ----------

# DBTITLE 1,Categorical consistency -- country name vs code
spread = []
pairs = [
    (role["cca3"], role["cca2"]),
    (role["cca3"], role["country"]),
    (role["cca2"], role["country"]),
]
for g, v in pairs:
    if g and v:
        spread.append(group_attribute_spread(d0, g, v))
        print(spread[-1])

# COMMAND ----------

# DBTITLE 1,Geography -- coordinate validity
geo = None
if role["lat"] and role["lon"]:
    geo = spatial_validity(
        d0, role["lat"], role["lon"], bbox=GLOBAL_BBOX, name="device"
    )
    print(geo)

# COMMAND ----------

# DBTITLE 1,Geography -- per-country coordinate spread
country_col = role["cca3"] or role["cca2"] or role["country"]
country_spread, span_stats = [], None
if country_col and role["lat"] and role["lon"]:
    la, lo = to_double(role["lat"]), to_double(role["lon"])
    rows = (
        df.groupBy(qcol(country_col).alias("c"))
        .agg(
            F.count(F.lit(1)).alias("n"),
            F.min(la).alias("lat_min"),
            F.max(la).alias("lat_max"),
            F.min(lo).alias("lon_min"),
            F.max(lo).alias("lon_max"),
        )
        .collect()
    )
    country_spread = [r.asDict() for r in rows]
    for r in country_spread:
        r["lat_span"] = r["lat_max"] - r["lat_min"]
        r["lon_span"] = r["lon_max"] - r["lon_min"]
    wide = [r for r in country_spread if r["lat_span"] > 60]
    lat_spans = sorted(r["lat_span"] for r in country_spread)
    lon_spans = sorted(r["lon_span"] for r in country_spread)
    span_stats = {
        "countries": len(country_spread),
        "lat_span_median": lat_spans[len(lat_spans) // 2],
        "lon_span_median": lon_spans[len(lon_spans) // 2],
        "lat_span_max": lat_spans[-1],
        "lon_span_max": lon_spans[-1],
        "wide_lat": [
            (str(r["c"]), r["n"], round(r["lat_span"], 1))
            for r in sorted(wide, key=lambda x: -x["lat_span"])[:10]
        ],
    }
    print(span_stats)

# COMMAND ----------

# DBTITLE 1,Device coordinate stability
stab = None
if dev and role["lat"] and role["lon"]:
    s = (
        df.groupBy(qcol(dev))
        .agg(
            F.approx_count_distinct(as_str(role["lat"])).alias("lat_variants"),
            F.approx_count_distinct(as_str(role["lon"])).alias("lon_variants"),
            F.count(F.lit(1)).alias("n"),
        )
        .where(F.col("n") > 1)
        .agg(
            F.count(F.lit(1)).alias("multi_reading_devices"),
            F.sum(
                ((F.col("lat_variants") > 1) | (F.col("lon_variants") > 1)).cast("long")
            ).alias("moving"),
        )
        .first()
        .asDict()
    )
    stab = s
    print(stab)

# COMMAND ----------

# DBTITLE 1,IP address format
ip_check = None
if role["ip"]:
    ipc = as_str(role["ip"])
    ip_check = (
        d0.agg(
            F.sum(ipc.rlike(r"^\d{1,3}(\.\d{1,3}){3}$").cast("long")).alias("ipv4"),
            F.sum(
                ipc.rlike(r"^(10\.|192\.168\.|172\.(1[6-9]|2[0-9]|3[01])\.)").cast(
                    "long"
                )
            ).alias("private"),
            F.count(F.lit(1)).alias("rows"),
        )
        .first()
        .asDict()
    )
    print(ip_check)

# COMMAND ----------

# DBTITLE 1,Timestamp unit and range
ts_info = None
if role["ts"]:
    tcol = role["ts"]
    if num[tcol]["is_numeric"]:
        ts_info = epoch_scan(d0, tcol)
        ts_info["kind"] = "epoch"
    else:
        ts_info = timestamp_semantics(
            d0, tcol, valid_from="1990-01-01", tz="unspecified"
        )
        ts_info["kind"] = "text"
    print(ts_info)

# COMMAND ----------

# DBTITLE 1,Within-device time series -- gaps and lag-1 autocorrelation
series = None
multi = bool(per_device and per_device["p50"] and per_device["p50"] > 1)
if multi and role["ts"] and dev and value_cols:
    tcol = role["ts"]
    tt = (
        to_double(tcol)
        if num[tcol]["is_numeric"]
        else F.unix_timestamp(parse_ts_multi(tcol))
    )
    w = Window.partitionBy(qcol(dev)).orderBy(tt)
    base = df.select(
        qcol(dev).alias("d"), tt.alias("t"), *[to_double(c).alias(c) for c in ncols]
    )
    lagged = base.withColumn("gap", F.col("t") - F.lag("t").over(w))
    gaps = (
        lagged.agg(
            F.expr("percentile_approx(gap, 0.5)").alias("gap_p50"),
            F.expr("percentile_approx(gap, 0.99)").alias("gap_p99"),
            F.max("gap").alias("gap_max"),
        )
        .first()
        .asDict()
    )
    ac = {}
    for c in ncols:
        lg = lagged.withColumn("prev", F.lag(c).over(w))
        ac[c] = lg.stat.corr(c, "prev")
    series = {"gaps": gaps, "autocorr": ac}
    print(series)
else:
    print(
        "one reading per device (or no device / time column): within-device series not assessable"
    )

# COMMAND ----------

# DBTITLE 1,Country concentration
country_conc = None
if country_col:
    top = (
        df.groupBy(qcol(country_col).alias("c"))
        .count()
        .orderBy(F.desc("count"))
        .limit(10)
        .collect()
    )
    country_conc = [
        (str(r["c"]), r["count"], round(r["count"] / prof["total"], 4)) for r in top
    ]
    print(country_conc)

# COMMAND ----------

# DBTITLE 1,Device name structure
name_struct = None
if role["device_name"]:
    n = as_str(role["device_name"])
    r = (
        d0.agg(
            F.approx_count_distinct(F.regexp_extract(n, r"^([^0-9]*)", 1)).alias(
                "prefixes"
            ),
            F.min(F.length(n)).alias("len_min"),
            F.max(F.length(n)).alias("len_max"),
            F.sum(n.rlike(r"[0-9]+$").cast("long")).alias("ends_with_digits"),
            F.sum(
                (F.regexp_extract(n, r"([0-9]+)$", 1) == as_str(dev)).cast("long")
                if dev
                else F.lit(0)
            ).alias("suffix_equals_id"),
        )
        .first()
        .asDict()
    )
    r["examples"] = [x[0] for x in d0.select(n).limit(3).collect()]
    name_struct = r
    print(name_struct)

# COMMAND ----------

# DBTITLE 1,IP address octet spread
ip_octets = None
if role["ip"]:
    parts = F.split(as_str(role["ip"]), r"\.")
    ip_octets = (
        d0.agg(
            *[
                F.approx_count_distinct(parts.getItem(i).cast("int")).alias(f"o{i}")
                for i in range(4)
            ],
            F.min(parts.getItem(0).cast("int")).alias("first_min"),
            F.max(parts.getItem(0).cast("int")).alias("first_max"),
        )
        .first()
        .asDict()
    )
    print(ip_octets)

# COMMAND ----------

# DBTITLE 1,Coordinate decimal places
coord_dec = {}
for c in (role["lat"], role["lon"]):
    if c:
        digits = F.length(F.regexp_extract(as_str(c), r"\.([0-9]+)$", 1))
        rows = (
            d0.groupBy(digits.alias("dp"))
            .count()
            .orderBy(F.desc("count"))
            .limit(6)
            .collect()
        )
        coord_dec[c] = [(r["dp"], r["count"]) for r in rows]
print(coord_dec)

# COMMAND ----------

# DBTITLE 1,Readings per timestamp instant
ts_dist = None
if role["ts"]:
    rows = (
        d0.groupBy(qcol(role["ts"]))
        .count()
        .orderBy(qcol(role["ts"]))
        .limit(20)
        .collect()
    )
    ts_dist = [(r[0], r["count"]) for r in rows]
    print(ts_dist)

# COMMAND ----------


# DBTITLE 1,Indicator helper


def disjoint_ranges(profile, col):
    ranges = sorted((r[f"{col}__lo"], r[f"{col}__hi"]) for r in profile)
    return len(ranges) > 1 and all(a[1] < b[0] for a, b in itertools.pairwise(ranges))


# COMMAND ----------

# DBTITLE 1,Generated-data indicators evaluated on the data
indicators = []
if shape:
    near = [
        c
        for c in shape
        if abs(shape[c]["kurt"] - shape[c]["expected_uniform_kurt"]) < 0.05
    ]
    indicators.append(
        (
            "value spread matches a uniform distribution (excess kurtosis within 0.05 of the uniform value)",
            f"{near} of {list(shape)}",
            len(near) == len(shape),
        )
    )
vals = [abs(v) for v in corr.values() if v is not None]
if vals:
    indicators.append(
        (
            "sensor correlations all below 0.02 in absolute value",
            f"largest {max(vals):.4f}",
            max(vals) < 0.02,
        )
    )
if ts_info and ts_info.get("kind") == "epoch" and ts_info.get("n"):
    span_s = (num[role["ts"]]["max"] - num[role["ts"]]["min"]) / ts_info["div"]
    indicators.append(
        (
            "all timestamps within one minute",
            f"span {span_s:.1f} s, {ts_info['distinct_ts']} distinct instants",
            span_s < 60,
        )
    )
if dev and num[dev]["is_numeric"] and uniq.get(dev):
    contiguous = int(num[dev]["max"] - num[dev]["min"] + 1) == uniq[dev]["distinct"]
    indicators.append(
        (
            "device id is a contiguous sequence",
            f"{num[dev]['min']:.0f}..{num[dev]['max']:.0f}",
            contiguous,
        )
    )
if span_stats:
    indicators.append(
        (
            "coordinates cluster by country label (median latitude span under 5 degrees)",
            f"median {span_stats['lat_span_median']:.1f} deg",
            span_stats["lat_span_median"] < 5,
        )
    )
if lcd_profile:
    driven = [c for c in ncols if disjoint_ranges(lcd_profile, c)]
    indicators.append(
        (
            f"the `{lcd_col}` label is a function of one sensor (disjoint ranges per label)",
            f"{driven or 'none'}",
            bool(driven),
        )
    )
for i in indicators:
    print(i)

# COMMAND ----------

# DBTITLE 1,Figures
figs = []
_panels = {c: hist[c] for c in hist}
if facet_bars(
    _panels,
    "IoT -- sensor value distributions",
    "iot_distributions.png",
    rot=60,
    ncols=2,
):
    figs.append(("IoT -- sensor value distributions", "iot_distributions.png"))
if cat_dist and facet_bars(
    {c: cat_dist[c] for c in list(cat_dist)[:4]},
    "IoT -- categorical columns",
    "iot_categorical.png",
    rot=45,
    ncols=2,
):
    figs.append(("IoT -- categorical columns", "iot_categorical.png"))

# COMMAND ----------

# DBTITLE 1,Findings
print(
    f"rows={prof['total']}, cols={len(prof['cols'])}, constant={prof['constant']}, "
    f"dups={prof['dups']}, devices={per_device}, geo={geo}, ts={ts_info}"
)

# COMMAND ----------

# DBTITLE 1,Export profiling findings -> src/schemas/profiling/iot.md
_profile = [
    "| frame | source files | rows | cols | constant columns | nested columns |",
    "|---|---|---|---|---|---|",
    para(
        f"| {FRAME} | {len(frames[FRAME]['paths'])} | {prof['total']} | {len(prof['cols'])} |",
        f"{', '.join(prof['constant']) or '-'} | {', '.join(nested) or '-'} |",
    ),
]

_prov = [
    para(
        "The dataset is read in place from the Databricks Samples Volume;",
        "there is no Bronze table and no ECRMAP acquisition step.",
    ),
    f"Files under `{ROOT}`: {len(entries)} ({ {k: len(v) for k, v in kinds.items()} }).",
]
for name, lines in support.items():
    _prov.append(f"README-type file `{name}` (first lines):")
    _prov += [f"  > {ln[:160]}" for ln in lines[:25] if ln.strip()]
if not support:
    _prov.append(
        "- No README / licence file in the directory: provenance, generator and licence are not documented here (LIMITATION)."
    )

_struct = [
    f"- schema: `{struct['schema']}`",
    f"- corrupt-record column present: {struct['corrupt_record']}; nested columns: {nested or 'none'}.",
    f"- column roles resolved by name: { {k: v for k, v in role.items() if v} }.",
]

_dq = [
    f"Full-row exact duplicates: {prof['dups']}.",
    "Missingness (rate per column): "
    + ", ".join(
        f"{c}={prof['miss'][c] / prof['total'] if prof['total'] else 0:.4f}"
        for c in prof["cols"]
    ),
]
if dup_key:
    _dq.append(
        f"Duplicates on (`{dev}`, `{tsc}`): {dup_key['dup_groups']} duplicated keys out of "
        f"{dup_key['distinct_keys']} ({dup_key['identical']} identical, {dup_key['conflicting']} conflicting)."
    )

_entities = ["Key candidates (column: distinct / ratio-to-rows / unique):"]
for c, u in uniq.items():
    _entities.append(f"- `{c}`: {u['distinct']} / {u['ratio']} / unique={u['unique']}")
if not uniq:
    _entities.append("- no device / name / ip column located by name.")
if dev and num[dev]["is_numeric"]:
    _span = int(num[dev]["max"] - num[dev]["min"] + 1)
    _entities.append(
        f"- `{dev}` runs {num[dev]['min']:.0f}..{num[dev]['max']:.0f}: "
        + (
            "contiguous with no gaps, so it is a sequence number, not an external device identity."
            if _span == uniq.get(dev, {}).get("distinct")
            else "not contiguous."
        )
    )
if per_device:
    _entities.append(
        f"- readings per device: {per_device['devices']} devices; min {per_device['min']}, "
        f"p50 {per_device['p50']}, p99 {per_device['p99']}, max {per_device['max']}."
    )

_unit = []
for c in DATA_COLS:
    s = num[c]
    if s["is_numeric"]:
        line = (
            f"- `{c}`: numeric yield {s['yield']:.1%}, range {s['min']}..{s['max']}, "
            f"mean {fmt_num(s['mean'])}, sd {fmt_num(s['sd'])}, zero {s['zero']}, negative {s['negative']}"
        )
        if c in plaus:
            line += f"; below-bound {plaus[c].get('below')}, above-bound {plaus[c].get('above')}"
        _unit.append(line + ".")
if role["scale"] and role["scale"] in const_vals:
    _unit.append(
        f"- `{role['scale']}` (temperature unit label) is constant: `{const_vals[role['scale']]}` on every row, so the unit label carries no information beyond that one value."
    )
elif role["scale"] and role["scale"] in cat_dist:
    _unit.append(
        f"- `{role['scale']}` (temperature unit label) values: {cat_dist[role['scale']][:6]} -- the temperature column is only comparable within one scale."
    )
_others = {c: v for c, v in const_vals.items() if c != role["scale"]}
if _others:
    _unit.append(f"- other constant columns and their value: {_others}.")
if ip_check:
    _unit.append(
        f"- `{role['ip']}`: IPv4-format {ip_check['ipv4']}/{ip_check['rows']}, private-range {ip_check['private']}."
    )

_domain = []
for c, pairs_ in cat_dist.items():
    _domain.append(
        f"- `{c}`: {len(pairs_)} values shown; "
        + fmt_pairs(pairs_, n=8).replace("\n", " ")
    )
for sp in spread:
    _domain.append(
        f"- `{sp['group_col']}` -> `{sp['value_col']}`: {sp['groups']} groups, "
        f"{sp['inconsistent_groups']} map to more than one value."
    )
if not _domain:
    _domain.append("- No low-cardinality categorical column located.")

_geo = []
if geo:
    _geo.append(
        f"- coordinates present {geo['present']}, missing {geo['missing']}, at 0/0 {geo['null_island']}, "
        f"outside the global box {geo['outside_bbox']}. (A lat/lon-swap test is not meaningful against a global box, so it is not reported.)"
    )
if span_stats:
    _geo.append(
        f"- per `{country_col}` group ({span_stats['countries']} groups): median latitude span {span_stats['lat_span_median']:.1f} deg, "
        f"median longitude span {span_stats['lon_span_median']:.1f} deg; largest {span_stats['lat_span_max']:.1f} / {span_stats['lon_span_max']:.1f} deg."
    )
    _geo.append(
        f"- groups spanning more than 60 degrees of latitude (group, rows, span): {span_stats['wide_lat'] or 'none'}. "
        "Coordinates drawn independently of the country label would give large spans in every group; small spans in nearly all groups mean the coordinates follow the label."
    )
if cn_gap:
    _geo.append(
        f"- country name missing on {cn_gap['rows_missing_name']} rows across {cn_gap['codes_involved']} country codes; "
        f"{cn_gap['codes_with_name_elsewhere']} of those codes have a name on other rows."
    )
if stab:
    _geo.append(
        f"- devices with more than one reading: {stab['multi_reading_devices']}; of those, "
        f"{stab['moving']} report more than one coordinate."
    )
if not _geo:
    _geo.append("- No latitude / longitude columns located.")

_temporal = []
if ts_info and ts_info.get("kind") == "epoch" and ts_info.get("n"):
    _temporal.append(
        f"- `{role['ts']}`: numeric epoch in {ts_info['unit']}; range {ts_info['min_ts']} .. {ts_info['max_ts']} (UTC); "
        f"{ts_info['distinct_days']} distinct days, {ts_info['distinct_ts']} distinct instants."
    )
elif ts_info and ts_info.get("kind") == "text":
    _temporal += [f"- {ln}" for ln in ts_info["lines"]]
else:
    _temporal.append("- No timestamp column located; temporal semantics not assessed.")
if series:
    _temporal.append(
        f"- within-device gaps (seconds): p50 {series['gaps']['gap_p50']}, p99 {series['gaps']['gap_p99']}, max {series['gaps']['gap_max']}."
    )

_tcons = []
if series:
    _tcons.append(
        "Lag-1 autocorrelation within device: "
        + ", ".join(
            f"{c}={v:.3f}" for c, v in series["autocorr"].items() if v is not None
        )
        + " -- values near zero mean successive readings behave as independent draws."
    )
else:
    _tcons.append(
        para(
            "Devices have (at most) one reading each in this file, so there is no within-device",
            "series and temporal consistency (gaps, ordering, autocorrelation) is not applicable.",
        )
    )

_value = ["Pairwise Pearson correlation between sensor columns:"]
_value += [f"- {a} vs {b}: {v:.3f}" for (a, b), v in corr.items() if v is not None]
_value.append(f"- mirror / duplicate column pairs: {mirrors or 'none'}")
_value.append(
    "Distribution shape (skewness / excess kurtosis vs the value expected for a uniform spread over the same range / count CV):"
)
for c in shape:
    _value.append(
        f"- `{c}`: skew {fmt_num(shape[c]['skew'])}, kurtosis {fmt_num(shape[c]['kurt'])} "
        f"(uniform: {shape[c]['expected_uniform_kurt']}), count CV {flat.get(c)} over {flat_basis.get(c)}"
    )
for r in lcd_profile:
    _value.append(
        f"- `{lcd_col}` = {r['g']} ({r['rows']} rows): "
        + "; ".join(
            f"{c} {fmt_num(r[f'{c}__lo'])}..{fmt_num(r[f'{c}__hi'])} (mean {fmt_num(r[f'{c}__mean'])})"
            for c in ncols
        )
    )
if lcd_profile:
    _value.append(
        para(
            f"If the ranges of one sensor column do not overlap between `{lcd_col}` values, the label is derived from that column;",
            "overlapping ranges mean the label is not a function of the sensors.",
        )
    )

_regime = [
    para(
        "Indicators that values are generated rather than measured, each evaluated on this",
        "data (evidence, not proof); 'holds' means the indicator is present:",
    ),
    *[
        f"- {name}: {evid} -> {'holds' if ok else 'does not hold'}"
        for name, evid, ok in indicators
    ],
    f"- count CV per sensor column (basis): { {c: (v, flat_basis[c]) for c, v in flat.items()} }",
    f"- excess kurtosis observed vs uniform: { {c: (round(shape[c]['kurt'], 3), shape[c]['expected_uniform_kurt']) for c in shape} }",
    f"- per-country coordinate spread: {span_stats}",
]

_entities.append(
    f"- device names: {name_struct}"
    if name_struct
    else "- no device name column located."
)
if ip_octets:
    _entities.append(
        f"- IP octets distinct values (approx): {[ip_octets[f'o{i}'] for i in range(4)]}; first octet {ip_octets['first_min']}..{ip_octets['first_max']}."
    )
if coord_dec:
    _geo.append(f"- decimal places in coordinates (places, rows): {coord_dec}.")
if country_conc:
    _geo.append(f"- rows by country, top 10 (code, rows, share): {country_conc}.")
if ts_dist:
    _temporal.append(f"- readings per timestamp instant (instant, rows): {ts_dist}.")

_areas = {
    "Domain understanding": [
        f"one record per device with battery, CO2, humidity and temperature sensors, an LCD colour label, a country label (name / 2-letter / 3-letter code), an IP address and coordinates; temperature unit `{const_vals.get(role['scale'], 'unlabelled')}`",
        f"the colour label is a function of: {[c for c in ncols if lcd_profile and disjoint_ranges(lcd_profile, c)] or 'no single sensor'}",
        f"device names: {name_struct['examples'] if name_struct else 'n/a'}",
    ],
    "Structure and engineering": [
        f"{len(entries)} file(s) ({ {k: len(v) for k, v in kinds.items()} }), read as {FRAME} with schema {struct['schema'][:120]}",
        f"device id is a row-number-like sequence: {any(n.startswith('device id is a contiguous') and ok for n, _, ok in indicators)}",
        f"timestamp stored as epoch {ts_info.get('unit') if ts_info else 'n/a'}; constant columns {prof['constant']}",
        f"cn missing on {cn_gap['rows_missing_name'] if cn_gap else 'n/a'} rows while the codes are complete",
    ],
    "Temporal": [
        f"{ts_info.get('min_ts')} .. {ts_info.get('max_ts')} ({ts_info.get('distinct_ts')} distinct instants) -- a snapshot, not a series"
        if ts_info and ts_info.get("n")
        else ""
    ],
    "Spatial": [
        f"{len(country_spread)} country groups; median coordinate span within a country {span_stats['lat_span_median']:.1f} x {span_stats['lon_span_median']:.1f} deg"
        if span_stats
        else "",
        f"concentration: {country_conc[:3] if country_conc else 'n/a'}",
        f"coordinates at 0/0: {geo['null_island'] if geo else 'n/a'}",
    ],
    "Data quality": [
        f"full-row duplicates {prof['dups']}; key duplicates {dup_key['dup_groups'] if dup_key else 'n/a'}",
        f"missing values only in: { {c: round(prof['miss'][c] / prof['total'], 4) for c in prof['cols'] if prof['miss'][c]} }",
        f"country-name / code inconsistency groups: {[(sp['group_col'], sp['value_col'], sp['inconsistent_groups']) for sp in spread if sp['inconsistent_groups']]}",
    ],
    "Statistical patterns": [
        f"excess kurtosis vs uniform: { {c: (round(shape[c]['kurt'], 3), shape[c]['expected_uniform_kurt']) for c in shape} }",
        f"indicators that hold: {[n for n, _, ok in indicators if ok] or 'none'}",
    ],
    "Relationships": [
        f"sensor-to-sensor correlations all within +/-{max(vals):.3f}" if vals else "",
        f"country name / 2-letter / 3-letter code mappings: {[(sp['group_col'], sp['value_col'], sp['groups'], sp['inconsistent_groups']) for sp in spread]}",
        f"label-to-sensor dependence: {[c for c in ncols if lcd_profile and disjoint_ranges(lcd_profile, c)] or 'none found'}",
    ],
    "Analytics use": [
        f"measures: {value_cols}; dimensions: {[c for c in (role['country'], role['cca3'], lcd_col) if c]}; the geography dimension has {len(country_spread)} members with concentration {country_conc[:2] if country_conc else 'n/a'}",
    ],
    "ML use": [
        f"no target column in the source; candidate label-like column `{lcd_col}` is fully determined by one sensor"
        if lcd_profile and any(disjoint_ranges(lcd_profile, c) for c in ncols)
        else "no target column in the source",
        "sensor columns are uncorrelated with each other, so they carry no information about one another"
        if vals and max(vals) < 0.02
        else "",
    ],
    "AI / knowledge use": [
        f"a country reference set can be read from the data: {len(country_spread)} codes with names (name missing for {cn_gap['codes_involved'] if cn_gap else 'n/a'} code)",
        "no free-text column; no README or licence in the directory, so provenance is undocumented",
    ],
}

_coverage = [
    para(
        "Coverage of the file cannot be checked against an external reference:",
        "the README (if any) is the only statement of origin.",
    ),
    f"- devices: {per_device['devices'] if per_device else 'unknown'}; countries: {len(country_spread) or 'unknown'}.",
]

_dist = []
for c, q in quant.items():
    _dist.append(
        f"- `{c}` quantiles (p1/p25/p50/p75/p99): "
        + ", ".join(fmt_num(v) for v in q.values())
    )
for c, pairs_ in cat_dist.items():
    _dist.append(f"- `{c}`: " + fmt_pairs(pairs_, n=10).replace("\n", " "))

_findings_md = (
    f"- rows={prof['total']}, cols={len(prof['cols'])}, constant={prof['constant']}, dups={prof['dups']}, "
    f"devices={per_device['devices'] if per_device else 'n/a'}"
)

_silver = [
    "- Read from the Samples Volume; no Bronze table exists for this dataset.",
    "- Timestamp needs an explicit unit rule (epoch unit inferred above); sensor columns need typed casts with a quarantine for failures.",
    "- The colour label `lcd` and the constant unit label need an explicit decision (keep as attribute vs derive) once the Value Consistency evidence is read.",
    "- Whether a device identifier is a stable entity key depends on the readings-per-device finding above.",
    "- Provenance and real/synthetic status are unresolved in the data itself; label the table accordingly.",
]

_dup_map = {"full_row": prof["dups"], "device_ts": dup_key}
_ml = ml_readiness_block(
    [
        (
            "Grain / grain drift",
            f"One row per device reading; rows={prof['total']}; readings per device={per_device}. If devices have one reading each, the grain is a device snapshot, not a time series.",
        ),
        (
            "Join multiplication (1:N / M:N expansion)",
            "The device identifier is the only join candidate; fan-out to another table depends on the readings-per-device finding.",
        ),
        (
            "Target contamination",
            "No target is defined by the source; humidity or temperature as a target leaves the other sensor columns as features whose independence is tested in Value Consistency.",
        ),
        (
            "Temporal / post-event leakage",
            f"Timestamp evidence: {ts_info}. A near-constant timestamp means there is no time ordering to leak from or to respect.",
        ),
        (
            "Proxy leakage",
            "Device name / id / IP / country code are near-identifiers; a model given them memorises the device.",
        ),
        (
            "Split / entity leakage",
            "Split by device; if each device has one reading, a random row split is already entity-disjoint.",
        ),
        (
            "Historical-reference (point-in-time) leakage",
            "Not applicable -- no reference table or validity windows.",
        ),
        (
            "Survivorship / coverage bias",
            f"Coverage is whatever the file contains; devices={per_device}, countries={len(country_spread)}.",
        ),
        (
            "Missingness leakage",
            "Missingness per column is in Data Quality; check that it is not correlated with device attributes before using is-missing features.",
        ),
        (
            "Duplicate-event leakage",
            f"Duplicates: {_dup_map}.",
        ),
        (
            "Target / feature temporal misalignment",
            "Sensor columns share one timestamp per row; no lag structure is represented.",
        ),
        (
            "Unit / sign / circular-feature leakage",
            f"Temperature unit is given by a label column (values: {const_vals.get(role['scale'], cat_dist.get(role['scale'], 'n/a'))}); mirror pairs: {mirrors or 'none'}.",
        ),
        (
            "Data-generation-process leakage",
            f"Indicators that hold (see Regime / Version Evidence): {[n for n, _, ok in indicators if ok] or 'none'}; where they hold, patterns learned here describe the generator, not devices. Indicators that do not hold: {[n for n, _, ok in indicators if not ok] or 'none'}.",
        ),
        (
            "Class / label instability",
            "No target label; categorical columns (colour / scale) are static attributes.",
        ),
        (
            "Label availability lag",
            "Not applicable -- no delayed label.",
        ),
        (
            "Source / version / regime change",
            "Single static file; no version indicator.",
        ),
        (
            "Sample-vs-full divergence",
            "Every statistic is a full Spark aggregation over the file read; whether the file is a sample of a larger set is not stated in the data.",
        ),
    ]
)

write_profiling(
    SOURCE,
    NB_KEY,
    SECTION_TITLE,
    blocks=[
        ("Profile", "\n".join(_profile)),
        ("Provenance / Source Evidence", "\n".join(_prov)),
        ("Structural Integrity", "\n".join(_struct)),
        ("Data Quality", "\n".join(_dq)),
        ("Entities / Keys", "\n".join(_entities)),
        ("Unit & Semantic Validation", "\n".join(_unit)),
        ("Categorical / Domain Validation", "\n".join(_domain)),
        ("Geography", "\n".join(_geo)),
        ("Temporal Semantics", "\n".join(_temporal)),
        ("Temporal Consistency", "\n".join(_tcons)),
        ("Value Consistency", "\n".join(_value)),
        ("Regime / Version Evidence", "\n".join(_regime)),
        ("Coverage & Sampling Bias", "\n".join(_coverage)),
        ("Distributions", "\n".join(_dist)),
        ("EDA Findings", _findings_md),
        ("ML-Readiness Evidence", _ml),
        ("Observations by Area", area_block(_areas)),
        ("Silver Implications", "\n".join(_silver)),
    ],
    figures=figs,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## EDA -- SMARD BALANCING ENERGY AND COMMERCIAL EXCHANGES
# MAGIC
# MAGIC **Purpose:** Profile the Bronze tables `smard_balancing_energy` and
# MAGIC `smard_commercial_exchanges`: nulls, constant columns, key, rates per
# MAGIC column kind and date-like columns -- as evidence.

# COMMAND ----------

# DBTITLE 1,Configuration
CATALOG = "energy_commerce_retail_media"
BRONZE_SCHEMA = "bronze"
SOURCE = "smard"
NB_KEY = "02_balancing_and_exchanges"
SECTION_TITLE = "Balancing energy and commercial exchange tables"
TABLES = ["smard_balancing_energy", "smard_commercial_exchanges"]

# COMMAND ----------

# DBTITLE 1,Read tables
FRAMES = {t: bronze_frame(f"{CATALOG}.{BRONZE_SCHEMA}.{t}") for t in TABLES}

# COMMAND ----------

# DBTITLE 1,Standard EDA -- profile, keys, rates, dates and findings export
standard_group_eda(
    SOURCE,
    NB_KEY,
    SECTION_TITLE,
    FRAMES,
    TABLES,
    "smard_02_rows.png",
    coverage=False,
    links=False,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## EDA -- ENERGY-CHARTS PHYSICAL FLOWS
# MAGIC
# MAGIC **Purpose:** Profile the Bronze table `energy_charts_physical_flows`: nulls, constant
# MAGIC columns, key, rates per column kind and date-like columns -- as evidence.

# COMMAND ----------

# DBTITLE 1,Configuration
CATALOG = "energy_commerce_retail_media"
BRONZE_SCHEMA = "bronze"
SOURCE = "energy_charts"
NB_KEY = "01_physical_flows"
SECTION_TITLE = "Physical cross-border flow table"
TABLES = ["energy_charts_physical_flows"]

# COMMAND ----------

# DBTITLE 1,Read tables
FRAMES = {t: bronze_frame(f"{CATALOG}.{BRONZE_SCHEMA}.{t}") for t in TABLES}

# COMMAND ----------

# DBTITLE 1,Standard EDA -- profile, keys, rates, dates and findings export
standard_group_eda(
    SOURCE,
    NB_KEY,
    SECTION_TITLE,
    FRAMES,
    TABLES,
    "energy_charts_01_rows.png",
    coverage=False,
    links=False,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## EDA -- VG250 MUNICIPALITY POINTS
# MAGIC
# MAGIC **Purpose:** Profile the Bronze table `vg250_municipality_point` (the point of each
# MAGIC municipality): nulls, constant columns, key, rates per column kind -- as
# MAGIC evidence.

# COMMAND ----------

# DBTITLE 1,Configuration
CATALOG = "energy_commerce_retail_media"
BRONZE_SCHEMA = "bronze"
SOURCE = "vg250"
NB_KEY = "01_municipality_point"
SECTION_TITLE = "Municipality point table"
TABLES = ["vg250_municipality_point"]

# COMMAND ----------

# DBTITLE 1,Read tables
FRAMES = {t: bronze_frame(f"{CATALOG}.{BRONZE_SCHEMA}.{t}") for t in TABLES}

# COMMAND ----------

# DBTITLE 1,Standard EDA -- profile, keys, rates, dates and findings export
standard_group_eda(
    SOURCE,
    NB_KEY,
    SECTION_TITLE,
    FRAMES,
    TABLES,
    "vg250_01_rows.png",
    coverage=False,
    links=False,
)
