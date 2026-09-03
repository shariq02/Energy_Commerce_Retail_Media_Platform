# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # EDA -- DWD MISSING DATA
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC **Author:** Sharique Mohammad
# MAGIC **Date:** August 2026
# MAGIC
# MAGIC **Purpose:** Profile dwd_missing_value_periods (the DWD-reported
# MAGIC missing-value windows) and reconcile it against the actual -999 /
# MAGIC blank values observed in the measurement tables -- reported vs
# MAGIC observed missingness, longest reported periods, missingness over time,
# MAGIC a station x parameter missingness matrix. Observed consecutive-gap
# MAGIC runs are in notebook 01. The reported-periods table is small and is
# MAGIC collected once; each measurement is scanned twice (per-station rollup
# MAGIC and per-year rollup).

# COMMAND ----------

# DBTITLE 1,Imports
import contextlib
import datetime as dt
import os as _os
import re as _re

import matplotlib.pyplot as plt
import numpy as np
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

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

# COMMAND ----------

# DBTITLE 1,Helpers


def find_col(df: DataFrame, *cands: str) -> str | None:
    low = {c.lower(): c for c in df.columns}
    for x in cands:
        if x.lower() in low:
            return low[x.lower()]
    return None


def value_cols(df):
    return [
        c
        for c in df.columns
        if c.upper() not in NON_VALUE
        and c.upper() not in INDICATOR_COLS
        and not c.upper().startswith("QN")
    ]


def barplot(pairs, title, xlabel, ylabel="rows", rot=0, figsize=(10, 4), filename=None):
    plt.figure(figsize=figsize)
    plt.bar([str(p[0]) for p in pairs], [p[1] for p in pairs])
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.xticks(rotation=rot, ha="right" if rot else "center")
    plt.tight_layout()
    if filename:
        plt.savefig(fig_path(filename), dpi=110, bbox_inches="tight")
    plt.show()


# COMMAND ----------

# DBTITLE 1,Profiling-export helper (writes src/schemas/profiling/<source>.md)


def _repo_root():
    p = _os.path.abspath(_os.getcwd())
    for _ in range(12):
        if _os.path.isdir(_os.path.join(p, "src", "schemas")) and _os.path.isdir(
            _os.path.join(p, "databricks", "eda")
        ):
            return p
        if _os.path.dirname(p) == p:
            break
        p = _os.path.dirname(p)
    with contextlib.suppress(Exception):
        wp = (
            dbutils.notebook.entry_point.getDbutils()
            .notebook()
            .getContext()
            .notebookPath()
            .get()
        )
        i = wp.rfind("/databricks/eda/")
        if i > 0:
            for cand in (wp[:i], "/Workspace" + wp[:i]):
                if _os.path.isdir(_os.path.join(cand, "src", "schemas")):
                    return cand
    raise RuntimeError(
        "repo root not found -- run from <repo>/databricks/eda/<source>/"
    )


def _profiling_dir():
    d = _os.path.join(_repo_root(), "src", "schemas", "profiling")
    _os.makedirs(_os.path.join(d, "figures"), exist_ok=True)
    return d


def fig_path(name):
    return _os.path.join(_profiling_dir(), "figures", name)


def fmt_pairs(pairs, n=25):
    # Render (label, value) pairs as markdown list lines, capped at n with a
    # "... (N more)" tail so the profiling .md never carries a 1000-row dump.
    items = list(pairs)
    out = [f"- {lbl}: {val}" for lbl, val in items[:n]]
    if len(items) > n:
        out.append(f"- ... ({len(items) - n} more)")
    return "\n".join(out)


def _facet_grid(items, suptitle, filename, ncols=3, panel=(4.6, 3.2)):
    items = [(str(k), draw) for k, draw in items if draw is not None]
    if not items:
        print(f"  _facet_grid: no data -> {filename}")
        return False
    ncols = min(ncols, len(items))
    nrows = -(-len(items) // ncols)
    fig, axes = plt.subplots(
        nrows, ncols, figsize=(panel[0] * ncols, panel[1] * nrows), squeeze=False
    )
    flat = list(axes.flatten())
    for ax, (title, draw) in zip(flat, items):
        draw(ax)
        ax.set_title(title, fontsize=9)
        ax.tick_params(labelsize=7)
    for ax in flat[len(items) :]:
        ax.set_visible(False)
    fig.suptitle(suptitle)
    fig.tight_layout()
    fig.savefig(fig_path(filename), dpi=110, bbox_inches="tight")
    plt.show()
    plt.close(fig)
    return True


def facet_bars(groups, suptitle, filename, rot=45, ncols=3, logy=False):
    def _mk(pairs):
        if not pairs:
            return None

        def draw(ax):
            ax.bar([str(p[0]) for p in pairs], [p[1] for p in pairs])
            if logy:
                ax.set_yscale("log")
            ax.tick_params(axis="x", labelrotation=rot)

        return draw

    src = groups.items() if hasattr(groups, "items") else groups
    return _facet_grid([(k, _mk(list(v))) for k, v in src], suptitle, filename, ncols)


def facet_hists(groups, suptitle, filename, bins=40, ncols=3, logy=True):
    def _mk(vals):
        if vals is None or not len(vals):
            return None

        def draw(ax):
            ax.hist(list(vals), bins=bins, log=logy)

        return draw

    src = groups.items() if hasattr(groups, "items") else groups
    return _facet_grid([(k, _mk(v)) for k, v in src], suptitle, filename, ncols)


def write_profiling(source, notebook_key, section_title, blocks, figures=None):
    d = _profiling_dir()
    md = _os.path.join(d, source + ".md")
    lines = [f"<!-- BEGIN {source}:{notebook_key} -->", f"## {section_title}", ""]
    for heading, body in blocks:
        if body is None or str(body).strip() == "":
            continue
        lines += [f"### {heading}", "", str(body).rstrip(), ""]
    for cap, name in figures or []:
        if not _os.path.exists(_os.path.join(d, "figures", name)):
            print(f"  profiling export: skipping absent figure {name}")
            continue
        lines += [f"### Figure -- {cap}", "", f"![{cap}](figures/{name})", ""]
    lines.append(f"<!-- END {source}:{notebook_key} -->")
    block = "\n".join(lines)
    existing = ""
    if _os.path.exists(md):
        with open(md, encoding="utf-8") as fh:
            existing = fh.read()
    pat = _re.compile(
        r"<!-- BEGIN "
        + _re.escape(source)
        + r":([\w.\-]+) -->.*?<!-- END "
        + _re.escape(source)
        + r":\1 -->",
        _re.DOTALL,
    )
    kept = {mm.group(1): mm.group(0) for mm in pat.finditer(existing)}
    kept[notebook_key] = block
    intro = f"_Auto-generated by the Phase 3 EDA notebooks (`databricks/eda/{source}/`). One `## ` section per notebook; re-running a notebook replaces its own section, other sections are preserved._"
    header = f"# {source.upper()} EDA PROFILE\n\n{intro}\n\n"
    body = "\n\n".join(kept[k] for k in sorted(kept))
    out = header + body + "\n"
    tmp = md + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        fh.write(out)
    _os.replace(tmp, md)
    print(f"profiling export -> {md}  ('{notebook_key}', {len(kept)} section(s))")


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

# DBTITLE 1,Reported period spans (von/bis -> hours) per station


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
print(f"inverted ranges: {inverted}  parsed spans: {len(period_spans)}")
if _TO_DT_UNPARSED:
    print(f"unparsed von/bis samples (first {len(_TO_DT_UNPARSED)}): {_TO_DT_UNPARSED}")
for sid, g in reported_gaps.items():
    print(f"station {sid}: {g}")

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
        # Validate numeric before casting to avoid CAST_INVALID_INPUT errors (e.g., 'I', 'P' in cloudiness)
        v = F.when(
            F.col(c).rlike("^-?[0-9]+(\\.[0-9]+)?$"), F.col(c).cast("double")
        ).otherwise(F.lit(None))
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

# DBTITLE 1,Observed missingness over time -- -999/blank rate by year per measurement (one scan each)
missing_over_time = {}
for m, t in MEASUREMENT_TABLES.items():
    df = spark.table(t)
    dts = find_col(df, "MESS_DATUM")
    any_missing = F.lit(False)
    for c in value_cols(df):
        # Validate numeric before casting to avoid CAST_INVALID_INPUT errors (e.g., 'I', 'P' in cloudiness)
        v = F.when(
            F.col(c).rlike("^-?[0-9]+(\\.[0-9]+)?$"), F.col(c).cast("double")
        ).otherwise(F.lit(None))
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
    print(
        f"{m}:",
        [(x["year"], round(x["missing_rate"], 4)) for x in missing_over_time[m]],
    )

# COMMAND ----------

# DBTITLE 1,Reconciliation -- reported periods vs observed -999/blank per station x parameter
# The measurement value-column name IS the DWD parameter code (TT_TU, RF_TU, ...).
reported_pairs = (
    {(str(d[sid_c]), d[param_c]) for d in mv_recs} if (sid_c and param_c) else set()
)
recon = []
for m in MEASUREMENTS:
    df = spark.table(MEASUREMENT_TABLES[m])
    vc = value_cols(df)
    for x in station_roll[m]:
        for c in vc:
            n999, nblank = x[c + "__999"], x[c + "__blank"]
            recon.append(
                {
                    "station": x["station"],
                    "measurement": m,
                    "parameter": c,
                    "observed_999": n999,
                    "observed_blank": nblank,
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
stations = sorted({r["station"] for r in recon})
params = sorted({r["parameter"] for r in recon})
rate = {}
for r in recon:
    rate[(r["station"], r["parameter"])] = rate.get(
        (r["station"], r["parameter"]), 0
    ) + (r["observed_999"] + r["observed_blank"]) / max(r["rows"], 1)
grid = np.array([[rate.get((st, p), 0.0) for p in params] for st in stations])
plt.figure(figsize=(max(6, 0.8 * len(params)), max(3, 0.5 * len(stations))))
plt.imshow(grid, aspect="auto", cmap="magma")
plt.colorbar(label="missing (-999/blank) rate")
plt.xticks(range(len(params)), params, rotation=45, ha="right")
plt.yticks(range(len(stations)), stations)
plt.title("DWD -- observed missingness rate by station x parameter")
plt.tight_layout()
plt.savefig(fig_path("dwd_missingness_heatmap.png"), dpi=110, bbox_inches="tight")
plt.show()

# COMMAND ----------

# DBTITLE 1,Figure -- reported missing periods (one faceted figure)
facet_bars(
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
)

# COMMAND ----------

# DBTITLE 1,Figure -- observed -999/blank rate by year, per measurement (faceted)


def _line_draw(rows):
    def draw(ax):
        ax.plot(
            [x["year"] for x in rows], [x["missing_rate"] for x in rows], marker="."
        )
        ax.tick_params(axis="x", labelrotation=90)

    return draw


_facet_grid(
    [(m, _line_draw(rows)) for m, rows in missing_over_time.items() if rows],
    "DWD -- observed -999/blank rate by year, per measurement",
    "dwd_missing_rate_by_year.png",
)

# COMMAND ----------

# DBTITLE 1,Figure -- missing rate per value column + distinct hours per station (faceted)
facet_bars(
    {
        m: [(c, miss / tm) for c, (miss, tm) in cols.items()]
        for m, cols in missing_rates.items()
    },
    "DWD -- missing / -999 rate by value column, per measurement",
    "dwd_missing_rate_by_column.png",
    rot=30,
)
facet_bars(
    {
        m: [(x["station"], x["distinct_hours"]) for x in station_roll[m]]
        for m in MEASUREMENTS
    },
    "DWD -- distinct observed hours per station, per measurement",
    "dwd_distinct_hours_per_station.png",
)

# COMMAND ----------

# DBTITLE 1,Findings
print("reported periods total:", total, " inverted ranges:", inverted)
print(
    "reported longest gap per station:",
    {sid: g["longest"] for sid, g in reported_gaps.items()},
)
print(
    "observed -999/blank rate per (measurement.column):",
    {
        f"{m}.{c}": round(miss / tm, 4)
        for m, cs in missing_rates.items()
        for c, (miss, tm) in cs.items()
    },
)
print(
    "reconciliation: -999-without-report =",
    obs_no_report,
    " report-without-observed =",
    report_no_obs,
)

# COMMAND ----------

# DBTITLE 1,Export profiling findings -> src/schemas/profiling/dwd.md
_obs_rate = {
    f"{m}.{c}": round(miss / tm, 4)
    for m, cs in missing_rates.items()
    for c, (miss, tm) in cs.items()
}
_worst = {sid: g["longest"] for sid, g in reported_gaps.items()}

_profile = [
    f"- `dwd_missing_value_periods`: {total} rows, columns {MV_COLS}",
    f"- station-id column: `{sid_c}`  parameter column: `{param_c}`  von/bis: `{von_c}`/`{bis_c}`",
    f"- periods per station: {mv_per_station}",
    f"- periods per parameter: {mv_per_param}",
]

_dq = [
    "- REPORTED = a row in dwd_missing_value_periods; OBSERVED = a measurement value that is `-999` or blank. They are not guaranteed to line up.",
    f"- inverted von>bis ranges in the reported table: {inverted}",
    f"- reconciliation: station x parameter with -999 observed but NO reported period: {obs_no_report}",
    f"- reconciliation: station x parameter with a reported period but ZERO observed -999/blank: {report_no_obs}",
]

_temporal = [
    f"reported-period span (hours): parsed {len(period_spans)} of {total} rows"
]
if _TO_DT_UNPARSED:
    _temporal.append(f"  unparsed von/bis samples: {_TO_DT_UNPARSED}")
if period_spans:
    _sp = sorted(period_spans)
    _temporal.append(
        f"  min={_sp[0]:.1f}, median={_sp[len(_sp) // 2]:.1f}, max={_sp[-1]:.1f}"
    )
_temporal.append(f"longest reported missing period per station (hours): {_worst}")
_temporal.append("observed -999/blank rate by year, per measurement:")
for m, rows in missing_over_time.items():
    _temporal.append(
        f"- {m}: {[(x['year'], round(x['missing_rate'], 4)) for x in rows]}"
    )

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

write_profiling(
    SOURCE,
    NB_KEY,
    SECTION_TITLE,
    blocks=[
        ("Profile", "\n".join(_profile)),
        ("Data Quality", "\n".join(_dq)),
        ("Temporal", "\n".join(_temporal)),
        ("Distributions", "\n".join(_dist)),
        ("Domain Findings", "\n".join(_dom)),
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
    figures=[
        (
            "DWD observed missingness rate by station x parameter",
            "dwd_missingness_heatmap.png",
        ),
        (
            "DWD missing_value_periods -- reported windows",
            "dwd_reported_missing_periods.png",
        ),
        (
            "DWD observed -999/blank rate by year, per measurement",
            "dwd_missing_rate_by_year.png",
        ),
        (
            "DWD missing / -999 rate by value column, per measurement",
            "dwd_missing_rate_by_column.png",
        ),
        (
            "DWD distinct observed hours per station, per measurement",
            "dwd_distinct_hours_per_station.png",
        ),
    ],
)
