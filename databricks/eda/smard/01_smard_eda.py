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
        F.to_timestamp(c),
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
by_year = {x["year"]: x["count"] for x in ym}
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


# DBTITLE 1,Physical consistency -- residual_load ~ load - wind - solar at the same (region, timestamp)
def _find_metric(*subs, exclude=()):
    for m in metrics:
        ml = str(m).lower()
        if all(s in ml for s in subs) and not any(x in ml for x in exclude):
            return m
    return None


m_resid = _find_metric("residual", "load")
m_load = _find_metric("load", exclude=("residual", "forecast")) or _find_metric(
    "consumption", exclude=("forecast",)
)
m_wind = _find_metric(
    "generation", "wind", exclude=("forecast", "offshore")
) or _find_metric("wind", exclude=("forecast",))
m_pv = _find_metric(
    "generation", "photovoltaic", exclude=("forecast",)
) or _find_metric("solar", exclude=("forecast",))
residual_identity = None
print(f"residual-load metrics: resid={m_resid} load={m_load} wind={m_wind} pv={m_pv}")
if all((m_resid, m_load, m_wind, m_pv)):
    piv = (
        df.where(F.col("metric").isin([m_resid, m_load, m_wind, m_pv]))
        .groupBy("region", "resolution", "timestamp_utc")
        .pivot("metric", [m_resid, m_load, m_wind, m_pv])
        .agg(F.first(safe_num("value")))
    )
    piv = piv.where(
        F.col(f"`{m_resid}`").isNotNull()
        & F.col(f"`{m_load}`").isNotNull()
        & F.col(f"`{m_wind}`").isNotNull()
        & F.col(f"`{m_pv}`").isNotNull()
    )
    residual_identity = additive_identity_check(
        piv,
        m_resid,
        [m_load, m_wind, m_pv],
        signs=[1, -1, -1],
        rel_tol=0.02,
        abs_floor=100.0,
    )
    print("residual-load identity:", residual_identity)

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
    metric_years.setdefault(x["metric"], {})[x["year"]] = (x["rows"], x["sum_value"])
metric_first_year = {m: min(y for y in yr) for m, yr in metric_years.items() if yr}
print(
    "metric first-seen year:",
    sorted(metric_first_year.items(), key=lambda p: p[1] or 0),
)

# COMMAND ----------

# DBTITLE 1,Samples -- value histogram + ONE representative series per metric (not mixed)
value_pdf = (
    df.select("metric", v.alias("value"))
    .where(v.isNotNull())
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
        "Physical identity: residual_load should equal load - wind - solar at the",
        "same (region, resolution, timestamp).",
    ),
]
if residual_identity:
    _pcons.append(
        f"- identity `{residual_identity['identity']}`: "
        f"{residual_identity['violations']}/{residual_identity['comparable_rows']} rows exceed "
        f"{int(residual_identity['rel_tol'] * 100)}% relative residual "
        f"({residual_identity['violation_pct']}%); residual p01/p50/p99 "
        f"{residual_identity['residual_p01_p50_p99']}, max abs {residual_identity['max_abs_residual']}."
    )
    _pcons.append(
        para(
            "A non-trivial violation share means these published series are not a clean additive",
            "set (rounding, different vintages, or an extra term such as pumped-storage load) --",
            "do not derive one from the others without reconciling.",
        )
    )
else:
    _pcons.append(
        "- LIMITATION: could not identify all of residual-load / load / wind / solar metrics by "
        f"name (resid={m_resid}, load={m_load}, wind={m_wind}, pv={m_pv}) -- identity not tested."
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
_late = [m for m, y in metric_first_year.items() if y and y > min(by_year)]
if _late:
    _regime2.append(
        f"- {len(_late)} metric(s) start after the table's first year {min(by_year)}: {_late}."
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
        ("Regime / Version Evidence", "\n".join(_regime2)),
        ("Entities / Keys", "\n".join(_entities)),
        ("Coverage & Sampling Bias", "\n".join(_coverage)),
        ("Distributions", "\n".join(_dist)),
        ("EDA Findings", _findings_md),
        ("ML-Readiness Evidence", _ml),
        ("Silver Implications", "\n".join(_silver)),
    ],
    figures=figs,
)
