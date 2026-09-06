# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # EDA -- DWD MISSING DATA
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** Profile dwd_missing_value_periods (the DWD-reported
# MAGIC missing-value windows) and reconcile it against the actual -999 /
# MAGIC blank values observed in the measurement tables -- reported vs
# MAGIC observed missingness, reported-period ordering, parameter-code domain,
# MAGIC missingness over time as regime evidence, a station x parameter
# MAGIC missingness matrix, and the layered modelling-risk checklist. The
# MAGIC reported-periods table is small and collected once; each measurement is
# MAGIC scanned twice (per-station rollup and per-year rollup).

# COMMAND ----------

# DBTITLE 1,Imports
import datetime as dt

import numpy as np
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %run ../_eda_common

# COMMAND ----------

# DBTITLE 1,Configuration
CATALOG = "energy_commerce_retail_media"
BRONZE_SCHEMA = "bronze"
SOURCE = "dwd"
NB_KEY = "03_missing_data"
SECTION_TITLE = "Missing data (reported periods vs observed -999/blank)"
MISSING_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.dwd_missing_value_periods"
MEASUREMENTS = [
    "air_temperature",
    "cloudiness",
    "moisture",
    "precipitation",
    "pressure",
    "sun",
    "wind",
]
MEASUREMENT_TABLES = {m: f"{CATALOG}.{BRONZE_SCHEMA}.dwd_{m}" for m in MEASUREMENTS}
NON_VALUE = {"STATIONS_ID", "CITY", "MESS_DATUM", "EOR"}
# DWD companion "Messverfahren-Index" columns -- not measured values, mostly
# blank; counting their blankness as "missing data" made cloudiness read as
# 100% missing and contradicted notebook 01.
INDICATOR_COLS = {"V_N_I"}


def value_cols(df):
    return [
        c
        for c in df.columns
        if c.upper() not in NON_VALUE
        and c.upper() not in INDICATOR_COLS
        and not c.upper().startswith("QN")
    ]


def to_dt(v):
    s = str(v or "").strip()
    s = s.removesuffix(".0")  # column inferred as double -> "2025021300.0"
    for fmt in (
        "%Y%m%d%H",
        "%Y%m%d",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d",
        "%d.%m.%Y %H:%M",
        "%d.%m.%Y",
    ):
        try:
            return dt.datetime.strptime(s, fmt).replace(tzinfo=dt.UTC)
        except ValueError:
            continue
    return None


# COMMAND ----------

# DBTITLE 1,Validate profiling export path
REPO_ROOT = _repo_root()
PROFILING_DIR = _profiling_dir()
print(f"OK  repo root: {REPO_ROOT}")
print(f"OK  profiling directory: {PROFILING_DIR}")

# COMMAND ----------

# DBTITLE 1,dwd_missing_value_periods -- collect once, analyse in Python
mv = spark.table(MISSING_TABLE)
MV_COLS = mv.columns
mv_recs = [x.asDict() for x in mv.collect()]
total = len(mv_recs)
sid_c = find_col(mv, "Stations_id", "STATIONS_ID", "stations_id")
param_c = find_col(mv, "Parameter", "parameter", "Kennung")
von_c = find_col(mv, "von_datum", "Von_Datum", "von")
bis_c = find_col(mv, "bis_datum", "Bis_Datum", "bis")
print(f"rows={total}  columns={MV_COLS}")
for c in MV_COLS:
    miss = sum(1 for d in mv_recs if d[c] is None or str(d[c]).strip() == "")
    print(f"  {c:<28} missing={miss:>6}  distinct={len({d[c] for d in mv_recs})}")
for d in mv_recs[:20]:
    print("  ", d)

mv_per_station = {}
mv_per_param = {}
for d in mv_recs:
    if sid_c:
        mv_per_station[d[sid_c]] = mv_per_station.get(d[sid_c], 0) + 1
    if param_c:
        mv_per_param[d[param_c]] = mv_per_param.get(d[param_c], 0) + 1
print("periods per station:", mv_per_station)
print("periods per parameter:", mv_per_param)

# COMMAND ----------

# DBTITLE 1,Reported period spans + von<=bis ordering (temporal consistency)
_TO_DT_UNPARSED: list = []
reported_gaps = {}
period_spans = []
inverted = 0
for d in mv_recs:
    if not (von_c and bis_c):
        break
    a, b = to_dt(d[von_c]), to_dt(d[bis_c])
    if not a or not b:
        if len(_TO_DT_UNPARSED) < 10:
            _TO_DT_UNPARSED.append((d[von_c], d[bis_c]))
        continue
    span_h = (b - a).total_seconds() / 3600
    if span_h < 0:
        inverted += 1
        continue
    period_spans.append(span_h)
    g = reported_gaps.setdefault(
        d[sid_c], {"reported_periods": 0, "longest": 0.0, "total": 0.0}
    )
    g["reported_periods"] += 1
    g["longest"] = max(g["longest"], span_h)
    g["total"] += span_h
print(f"inverted von>bis ranges: {inverted}  parsed spans: {len(period_spans)}")
if _TO_DT_UNPARSED:
    print(f"unparsed von/bis samples (first {len(_TO_DT_UNPARSED)}): {_TO_DT_UNPARSED}")
for sid, g in reported_gaps.items():
    print(f"station {sid}: {g}")

# COMMAND ----------

# DBTITLE 1,Parameter-code domain -- reported codes vs measurement value columns
observed_value_cols = {
    c for t in MEASUREMENT_TABLES.values() for c in value_cols(spark.table(t))
}
reported_codes = {str(d[param_c]) for d in mv_recs} if param_c else set()
codes_unknown = sorted(reported_codes - observed_value_cols)
codes_unused = sorted(observed_value_cols - reported_codes)
print("reported parameter codes not a measurement value column:", codes_unknown)
print("measurement value columns never in a reported period:", codes_unused)

# COMMAND ----------

# DBTITLE 1,Per-station rollup per measurement -- distinct hours + observed -999/blank (one scan each)
station_roll = {}
missing_rates = {}
for m, t in MEASUREMENT_TABLES.items():
    df = spark.table(t)
    s, dts = find_col(df, "STATIONS_ID"), find_col(df, "MESS_DATUM")
    vc = value_cols(df)
    exprs = [
        F.min(dts).alias("min_ts"),
        F.max(dts).alias("max_ts"),
        F.countDistinct(dts).alias("distinct_hours"),
        F.count(F.lit(1)).alias("rows"),
    ]
    for c in vc:
        v = safe_num(c)
        exprs += [
            F.sum((v == -999).cast("long")).alias(c + "__999"),
            F.sum((F.col(c).isNull() | (F.trim(F.col(c)) == "")).cast("long")).alias(
                c + "__blank"
            ),
        ]
    g = (
        df.groupBy(F.col(s).cast("string").alias("station"))
        .agg(*exprs)
        .orderBy("station")
        .collect()
    )
    station_roll[m] = [x.asDict() for x in g]
    total_m = sum(x["rows"] for x in g)
    missing_rates[m] = {}
    for c in vc:
        miss = sum(x[c + "__999"] + x[c + "__blank"] for x in g)
        missing_rates[m][c] = (miss, total_m)
        print(f"  {m}.{c:<24} missing_or_-999={miss:>12}  rate={miss / total_m:.4f}")

# COMMAND ----------

# DBTITLE 1,Regime evidence -- observed -999/blank rate by year per measurement (one scan each)
missing_over_time = {}
for m, t in MEASUREMENT_TABLES.items():
    df = spark.table(t)
    dts = find_col(df, "MESS_DATUM")
    any_missing = F.lit(False)
    for c in value_cols(df):
        v = safe_num(c)
        any_missing = (
            any_missing | (v == -999) | F.col(c).isNull() | (F.trim(F.col(c)) == "")
        )
    missing_over_time[m] = (
        df.select(
            F.substring(F.col(dts).cast("string"), 1, 4).alias("year"),
            any_missing.alias("miss"),
        )
        .groupBy("year")
        .agg(
            F.avg(F.col("miss").cast("double")).alias("missing_rate"),
            F.count(F.lit(1)).alias("rows"),
        )
        .orderBy("year")
        .collect()
    )
    print(m, [(x["year"], round(x["missing_rate"], 4)) for x in missing_over_time[m]])

# COMMAND ----------

# DBTITLE 1,Reconciliation -- reported periods vs observed -999/blank per station x parameter
reported_pairs = (
    {(str(d[sid_c]), d[param_c]) for d in mv_recs} if (sid_c and param_c) else set()
)
recon = []
for m in MEASUREMENTS:
    df = spark.table(MEASUREMENT_TABLES[m])
    vc = value_cols(df)
    for x in station_roll[m]:
        for c in vc:
            recon.append(
                {
                    "station": x["station"],
                    "measurement": m,
                    "parameter": c,
                    "observed_999": x[c + "__999"],
                    "observed_blank": x[c + "__blank"],
                    "rows": x["rows"],
                    "has_reported": (x["station"], c) in reported_pairs,
                }
            )
obs_no_report = sum(1 for r in recon if r["observed_999"] > 0 and not r["has_reported"])
report_no_obs = sum(
    1
    for r in recon
    if r["has_reported"] and r["observed_999"] == 0 and r["observed_blank"] == 0
)
print("station x parameter with -999 observed but NO reported period:", obs_no_report)
print(
    "station x parameter with a reported period but ZERO observed -999/blank:",
    report_no_obs,
)

# COMMAND ----------

# DBTITLE 1,Figure -- station x parameter observed missingness heatmap
figs = []
stations = sorted({r["station"] for r in recon})
params = sorted({r["parameter"] for r in recon})
rate = {}
for r in recon:
    rate[(r["station"], r["parameter"])] = rate.get(
        (r["station"], r["parameter"]), 0
    ) + (r["observed_999"] + r["observed_blank"]) / max(r["rows"], 1)
grid = np.array([[rate.get((st, p), 0.0) for p in params] for st in stations])
if grid.size:
    fig, ax = plt.subplots(
        figsize=(max(6, 0.8 * len(params)), max(3, 0.5 * len(stations)))
    )
    ax.imshow(grid, aspect="auto", cmap="magma")
    ax.set_xticks(range(len(params)))
    ax.set_xticklabels(params, rotation=45, ha="right")
    ax.set_yticks(range(len(stations)))
    ax.set_yticklabels(stations)
    ax.set_title("DWD -- observed missingness rate by station x parameter")
    fig.tight_layout()
    _save_and_show(fig, "dwd_missingness_heatmap.png")
    figs.append(
        (
            "DWD observed missingness rate by station x parameter",
            "dwd_missingness_heatmap.png",
        )
    )

# COMMAND ----------

# DBTITLE 1,Figure -- reported missing periods (one faceted figure)
if facet_bars(
    {
        "reported periods per station": sorted(mv_per_station.items()),
        "reported periods per parameter": sorted(mv_per_param.items()),
        "longest reported period (h) per station": [
            (sid, g["longest"]) for sid, g in reported_gaps.items()
        ],
    },
    "DWD missing_value_periods -- reported windows",
    "dwd_reported_missing_periods.png",
    ncols=2,
):
    figs.append(
        (
            "DWD missing_value_periods -- reported windows",
            "dwd_reported_missing_periods.png",
        )
    )

# COMMAND ----------

# DBTITLE 1,Figure -- observed -999/blank rate by year, per measurement (faceted)
if lines_grid(
    {
        m: [(x["year"], x["missing_rate"]) for x in rows]
        for m, rows in missing_over_time.items()
        if rows
    },
    "DWD -- observed -999/blank rate by year, per measurement",
    "dwd_missing_rate_by_year.png",
):
    figs.append(
        (
            "DWD observed -999/blank rate by year, per measurement",
            "dwd_missing_rate_by_year.png",
        )
    )

# COMMAND ----------

# DBTITLE 1,Figure -- missing rate per value column + distinct hours per station (faceted)
if facet_bars(
    {
        m: [(c, miss / tm) for c, (miss, tm) in cols.items()]
        for m, cols in missing_rates.items()
    },
    "DWD -- missing / -999 rate by value column, per measurement",
    "dwd_missing_rate_by_column.png",
    rot=30,
):
    figs.append(
        (
            "DWD missing / -999 rate by value column, per measurement",
            "dwd_missing_rate_by_column.png",
        )
    )
if facet_bars(
    {
        m: [(x["station"], x["distinct_hours"]) for x in station_roll[m]]
        for m in MEASUREMENTS
    },
    "DWD -- distinct observed hours per station, per measurement",
    "dwd_distinct_hours_per_station.png",
):
    figs.append(
        (
            "DWD distinct observed hours per station, per measurement",
            "dwd_distinct_hours_per_station.png",
        )
    )

# COMMAND ----------

# DBTITLE 1,Findings
_worst = {sid: g["longest"] for sid, g in reported_gaps.items()}
_obs_rate = {
    f"{m}.{c}": round(miss / tm, 4)
    for m, cs in missing_rates.items()
    for c, (miss, tm) in cs.items()
}
print("reported periods total:", total, " inverted ranges:", inverted)
print("reported longest gap per station:", _worst)
print("observed -999/blank rate per (measurement.column):", _obs_rate)
print(
    "reconciliation: -999-without-report =",
    obs_no_report,
    " report-without-observed =",
    report_no_obs,
)

# COMMAND ----------

# DBTITLE 1,Export profiling findings -> src/schemas/profiling/dwd.md
_profile = [
    f"- `dwd_missing_value_periods`: {total} rows, columns {MV_COLS}",
    f"- station-id column: `{sid_c}`  parameter column: `{param_c}`  von/bis: `{von_c}`/`{bis_c}`",
    f"- periods per station: {mv_per_station}",
    f"- periods per parameter: {mv_per_param}",
]

_dq = [
    "- REPORTED = a row in dwd_missing_value_periods; OBSERVED = a measurement value that is `-999` or blank. They are not guaranteed to line up.",
    f"- reconciliation: station x parameter with -999 observed but NO reported period: {obs_no_report}",
    f"- reconciliation: station x parameter with a reported period but ZERO observed -999/blank: {report_no_obs}",
]

_domain = [
    "Reported parameter codes vs the measurement value-column names (the DWD parameter code IS the column name):",
    f"- reported codes not a measurement value column: {codes_unknown}",
    f"- measurement value columns never in a reported period: {codes_unused}",
    para(
        "An unknown reported code points to a parameter outside the seven",
        "measurement tables profiled here (a deepening measurement, 05) or a code",
        "drift -- reconcile before treating REPORTED and OBSERVED as one signal.",
    ),
]

_tcons = [
    f"Reported-period span (hours): parsed {len(period_spans)} of {total} rows.",
]
if _TO_DT_UNPARSED:
    _tcons.append(f"- unparsed von/bis samples: {_TO_DT_UNPARSED}")
if period_spans:
    _sp = sorted(period_spans)
    _tcons.append(
        f"- span min={_sp[0]:.1f}, median={_sp[len(_sp) // 2]:.1f}, max={_sp[-1]:.1f} hours"
    )
_tcons.append(
    f"- `von_datum` > `bis_datum` (inverted) in {inverted} rows -- a validity check is needed at Silver."
)
_tcons.append(f"- longest reported missing period per station (hours): {_worst}")

_regime = [
    para(
        "Observed -999/blank rate by year per measurement -- missingness is",
        "time-varying, so no uniform-completeness assumption holds and any",
        "imputation is an explicit, evidenced choice per era.",
    ),
    "",
]
for m, rows in missing_over_time.items():
    _regime.append(f"- {m}: {[(x['year'], round(x['missing_rate'], 4)) for x in rows]}")

_dist = ["Observed -999/blank rate per (measurement.value-column):"]
for k, vrate in _obs_rate.items():
    _dist.append(f"- {k}: {vrate:.4f}")

_dom = ["Per-station observed distinct hours (measurement completeness proxy):"]
for m in MEASUREMENTS:
    _dom.append(
        f"- {m}: " + str({x["station"]: x["distinct_hours"] for x in station_roll[m]})
    )

_silver = [
    "- `-999` and blank are the DWD missing sentinels in the measurement fact -> convert to NULL in Silver.",
    "- Keep `dwd_missing_value_periods` as a reference table keyed by (station, parameter, from_ts, to_ts).",
]
if obs_no_report or report_no_obs:
    _silver.append(
        "- REPORTED and OBSERVED missingness disagree for some station x parameter (counts above) -> do NOT reconcile by deleting rows; flag with a data-quality column."
    )
if inverted:
    _silver.append(
        "- Inverted von>bis reported ranges -> a fix/exclusion rule is required (rule not yet established)."
    )
_silver.append(
    "- Missingness is time-varying (year plots) -> no uniform-completeness assumption; any imputation is an explicit, evidenced choice."
)

_target = para(
    "`dwd_missing_value_periods` is itself a natural label source for a",
    "missingness/outage-prediction use case; the observed -999/blank rate per",
    "(measurement.column) is a denser alternative target for the same question.",
)
_ml = ml_readiness_block(
    [
        (
            "Grain / grain drift",
            "Reported periods and per-station rollups are keyed by (station, parameter) -- split by station id, not by row.",
        ),
        (
            "Join multiplication (1:N / M:N expansion)",
            para(
                "The station x parameter reconciliation is 1:1 per pair by construction,",
                f"but the two sides disagree ({obs_no_report} observed-without-report,",
                f"{report_no_obs} reported-without-observed) -- treat REPORTED and OBSERVED",
                "as two signals, not one validated join.",
            ),
        ),
        (
            "Target contamination",
            para(
                _target,
                "The reported period and everything dated inside it must be excluded",
                "from features for predicting that same gap.",
            ),
        ),
        (
            "Temporal / post-event leakage",
            para(
                "A reported period's von/bis window is only known once DWD closes the",
                "gap -- a forecasting model may use only periods with bis_datum strictly",
                "before the prediction point.",
            ),
        ),
        (
            "Proxy leakage",
            "`DatumLetzteAktualisierung`-style fields and the parameter/station keys describe the gap itself -- circular for predicting it.",
        ),
        (
            "Split / entity leakage",
            "Split by station id so a station's reported periods do not leak across train/test.",
        ),
        (
            "Historical-reference (point-in-time) leakage",
            "Replay reported periods forward from a base date -- do not use the full set joined to a past date.",
        ),
        (
            "Survivorship / coverage bias",
            f"Reported periods are concentrated per station/parameter ({mv_per_station}) -- a station-level classifier sees a skewed positive rate.",
        ),
        (
            "Missingness leakage",
            "A missing von or bis value may itself mark the gap type -- check before an 'is-missing' feature.",
        ),
        (
            "Duplicate-event leakage",
            "De-duplicate the reported-periods table before counting gaps as independent observations.",
        ),
        (
            "Target / feature temporal misalignment",
            "von_datum (gap start) vs bis_datum (gap end) vs the DWD report date are distinct -- align target and features to one.",
        ),
        (
            "Unit / sign / circular-feature leakage",
            "Not applicable -- no numeric measures in the reported-periods table.",
        ),
        (
            "Data-generation-process leakage",
            "Whether a gap is REPORTED at all is a property of DWD's QC process, not the physical outage -- OBSERVED is the more direct signal.",
        ),
        (
            "Class / label instability",
            "Parameter codes and the reporting practice change across DWD archive versions -- pin the version.",
        ),
        (
            "Label availability lag",
            "Reported periods are back-loaded (a gap is logged after it closes) -- a real-time model cannot assume the row exists at the gap time.",
        ),
        (
            "Source / version / regime change",
            "Observed missingness rate by year (Regime / Version Evidence) shows the reporting regime shifting over the archive span.",
        ),
        (
            "Sample-vs-full divergence",
            "Every statistic (reported-period table, per-station rollups, yearly rates) is a full Spark scan or fully collected small table -- no sampling.",
        ),
    ]
)

write_profiling(
    SOURCE,
    NB_KEY,
    SECTION_TITLE,
    blocks=[
        ("Profile", "\n".join(_profile)),
        ("Data Quality", "\n".join(_dq)),
        ("Categorical / Domain Validation", "\n".join(_domain)),
        ("Temporal Consistency", "\n".join(_tcons)),
        ("Regime / Version Evidence", "\n".join(_regime)),
        ("Distributions", "\n".join(_dist)),
        ("Domain Findings", "\n".join(_dom)),
        ("ML-Readiness Evidence", _ml),
        (
            "EDA Findings",
            "\n".join(
                [
                    f"- reported periods total: {total}, inverted ranges: {inverted}",
                    f"- reported longest gap per station (h): {_worst}",
                    f"- observed -999/blank rate per (measurement.column): {_obs_rate}",
                    f"- reconciliation: -999-without-report={obs_no_report}, report-without-observed={report_no_obs}",
                ]
            ),
        ),
        ("Silver Implications", "\n".join(_silver)),
    ],
    figures=figs,
)
