# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # EDA -- DWD DEEPENING MEASUREMENTS & MISSING VALUE PERIODS
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** Profile the eight new DWD deepening measurement Bronze
# MAGIC tables (dew_point, soil_temperature, visibility, cloud_type,
# MAGIC wind_synop, extreme_wind, weather_phenomena, solar) and the new
# MAGIC missing_value_periods metadata table added alongside the original
# MAGIC seven measurements profiled in 01_weather_measurements_eda.py --
# MAGIC schema, missingness, constant columns, station coverage, hourly
# MAGIC frequency, per-station duplicates, value distributions -- as evidence
# MAGIC for Silver design. Column semantics (units, plausibility bounds) are
# MAGIC not assumed ahead of a live-schema read, unlike the original seven
# MAGIC measurements' notebook.

# COMMAND ----------

# DBTITLE 1,Imports
import contextlib
import os as _os
import re as _re

import matplotlib.pyplot as plt
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Configuration
CATALOG = "energy_commerce_retail_media"
BRONZE_SCHEMA = "bronze"
SOURCE = "dwd"
NB_KEY = "05_deepening"
SECTION_TITLE = "Deepening measurements (dew_point ... solar) & missing_value_periods"
MEASUREMENTS = [
    "dew_point",
    "soil_temperature",
    "visibility",
    "cloud_type",
    "wind_synop",
    "extreme_wind",
    "weather_phenomena",
    "solar",
]
TABLES = {m: f"{CATALOG}.{BRONZE_SCHEMA}.dwd_{m}" for m in MEASUREMENTS}
META_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.dwd_missing_value_periods"
NON_VALUE = {"STATIONS_ID", "CITY", "MESS_DATUM", "EOR"}
QN_CANDIDATES = ("QN_9", "QN_8", "QN_7", "QN_4", "QN_3", "QN")

# COMMAND ----------

# DBTITLE 1,Helpers


def find_col(df: DataFrame, *cands: str) -> str | None:
    low = {c.lower(): c for c in df.columns}
    for x in cands:
        if x.lower() in low:
            return low[x.lower()]
    return None


def as_ts(col: str):
    return F.to_timestamp(F.col(col).cast("string"), "yyyyMMddHH")


def value_columns(df: DataFrame) -> list:
    return [
        c
        for c in df.columns
        if c.upper() not in NON_VALUE and not c.upper().startswith("QN")
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
    intro = f"_Auto-generated by the EDA notebooks (`databricks/eda/{source}/`). One `## ` section per notebook; re-running a notebook replaces its own section, other sections are preserved._"
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

# DBTITLE 1,Profile -- rows, missingness, approx distinct, min/max ts, constant columns (one agg per table)
frames = {m: spark.table(t) for m, t in TABLES.items()}
prof = {}
for m in MEASUREMENTS:
    df = frames[m]
    cols = df.columns
    ts_col = find_col(df, "MESS_DATUM")
    exprs = [F.count(F.lit(1)).alias("__rows")]
    if ts_col:
        exprs += [
            F.min(ts_col).alias("__min_ts"),
            F.max(ts_col).alias("__max_ts"),
        ]
    for c in cols:
        miss = F.col(c).isNull() | (F.trim(F.col(c).cast("string")) == "")
        exprs += [
            F.sum(miss.cast("long")).alias(c + "__m"),
            F.approx_count_distinct(c).alias(c + "__d"),
        ]
    r = df.agg(*exprs).first().asDict()
    prof[m] = {
        "cols": cols,
        "total": r["__rows"],
        "min_ts": r.get("__min_ts"),
        "max_ts": r.get("__max_ts"),
        "miss": {c: r[c + "__m"] for c in cols},
        "acd": {c: r[c + "__d"] for c in cols},
    }
    sid = find_col(df, "STATIONS_ID")
    print(
        "=" * 90,
        f"\n{m}  rows={r['__rows']}  {r.get('__min_ts')}..{r.get('__max_ts')}  "
        f"stations~={r.get(sid + '__d') if sid else 'n/a'}\n",
        "=" * 90,
    )
    for c in cols:
        rate = prof[m]["miss"][c] / prof[m]["total"] if prof[m]["total"] else 0
        print(
            f"  {c:<26} missing={prof[m]['miss'][c]:>12} rate={rate:.4f} "
            f"approx_distinct={prof[m]['acd'][c]}"
        )
    print("constant columns:", [c for c in cols if prof[m]["acd"][c] <= 1])
totals = {m: prof[m]["total"] for m in MEASUREMENTS}
constant_cols = {
    m: [c for c in prof[m]["cols"] if prof[m]["acd"][c] <= 1] for m in MEASUREMENTS
}

# COMMAND ----------

# DBTITLE 1,Station coverage + per-station row counts (one groupBy per table)
station_counts = {}
for m in MEASUREMENTS:
    df = frames[m]
    sid = find_col(df, "STATIONS_ID")
    if sid is None:
        station_counts[m] = {}
        continue
    g = df.groupBy(sid).count().collect()
    station_counts[m] = {str(x[sid]): x["count"] for x in g}
    print(f"--- {m} ---", sorted(station_counts[m].items()))

# COMMAND ----------

# DBTITLE 1,QN quality-flag distribution (one groupBy per table, where present)
qn_dist = {}
for m in MEASUREMENTS:
    df = frames[m]
    qn = find_col(df, *QN_CANDIDATES)
    if qn is None:
        continue
    g = df.groupBy(qn).count().orderBy(F.desc("count")).collect()
    qn_dist[m] = [(x[qn], x["count"]) for x in g]
    print(f"--- {m} ({qn}) ---", qn_dist[m])

# COMMAND ----------

# DBTITLE 1,Duplicate (station, timestamp) -- identical vs conflicting (one groupBy per table)
dup_breakdown = {}
for m in MEASUREMENTS:
    df = frames[m]
    sid, dts = find_col(df, "STATIONS_ID"), find_col(df, "MESS_DATUM")
    if sid is None or dts is None:
        continue
    cols = df.columns
    dk = df.groupBy(sid, dts).agg(
        F.count(F.lit(1)).alias("n"),
        F.countDistinct(F.hash(*[F.col(c) for c in cols])).alias("row_variants"),
    )
    b = (
        dk.agg(
            F.sum((F.col("n") > 1).cast("long")).alias("dup_groups"),
            F.sum(((F.col("n") > 1) & (F.col("row_variants") == 1)).cast("long")).alias(
                "identical"
            ),
            F.sum(((F.col("n") > 1) & (F.col("row_variants") > 1)).cast("long")).alias(
                "conflicting"
            ),
        )
        .first()
        .asDict()
    )
    dup_breakdown[m] = b
    print(f"{m:<18} {b}")

# COMMAND ----------

# DBTITLE 1,Value columns -- range, percentiles, zero rows (one agg per table, no assumed plausibility bounds)
value_stats = {}
for m in MEASUREMENTS:
    df = frames[m]
    vcols = value_columns(df)
    exprs = []
    for c in vcols:
        v = F.when(F.col(c).rlike(r"^-?\d+(\.\d+)?$"), F.col(c).cast("double"))
        exprs += [
            F.min(v).alias(c + "_min"),
            F.max(v).alias(c + "_max"),
            F.avg(v).alias(c + "_mean"),
            F.stddev(v).alias(c + "_sd"),
            F.percentile_approx(v, [0.01, 0.5, 0.99]).alias(c + "_p"),
            F.sum((v == -999).cast("long")).alias(c + "_sentinel"),
            F.sum((v == 0).cast("long")).alias(c + "_zero"),
        ]
    value_stats[m] = df.agg(*exprs).first().asDict() if exprs else {}
    for c in vcols:
        print(
            f"{m}.{c:<14}",
            {
                k[len(c) + 1 :]: value_stats[m][k]
                for k in value_stats[m]
                if k.startswith(c + "_")
            },
        )

# COMMAND ----------

# DBTITLE 1,Hourly coverage % + longest gap per station (one windowed pass per table)
freq_cov = {}
for m in MEASUREMENTS:
    df = frames[m]
    sid, dts = find_col(df, "STATIONS_ID"), find_col(df, "MESS_DATUM")
    if sid is None or dts is None:
        continue
    w = Window.partitionBy("station").orderBy("ts")
    per_station = (
        df.select(
            F.col(sid).alias("station"),
            F.to_timestamp(F.substring(F.col(dts), 1, 10), "yyyyMMddHH").alias("ts")
        )
        .where(F.col("ts").isNotNull())
        .distinct()
        .withColumn(
            "gap_h",
            (F.col("ts").cast("long") - F.lag("ts").over(w).cast("long")) / 3600 - 1,
        )
        .groupBy("station")
        .agg(
            F.min("ts").alias("min_ts"),
            F.max("ts").alias("max_ts"),
            F.count(F.lit(1)).alias("observed_hours"),
            F.max(F.when(F.col("gap_h") > 0, F.col("gap_h"))).alias(
                "longest_gap_hours"
            ),
        )
        .withColumn(
            "expected_hours",
            (
                (F.col("max_ts").cast("long") - F.col("min_ts").cast("long")) / 3600 + 1
            ).cast("long"),
        )
        .withColumn(
            "coverage_pct",
            F.round(F.col("observed_hours") / F.col("expected_hours") * 100, 2),
        )
        .orderBy("station")
    )
    freq_cov[m] = [x.asDict() for x in per_station.collect()]
    print(f"--- {m} ---", freq_cov[m])

# COMMAND ----------

# DBTITLE 1,missing_value_periods -- small metadata table, collected in Python
mvp = spark.table(META_TABLE)
mvp_recs = [x.asDict() for x in mvp.collect()]
mvp_cols = mvp.columns
mvp_total = len(mvp_recs)
mvp_dups = mvp_total - len({tuple(sorted(d.items())) for d in mvp_recs})
mvp_consts = [c for c in mvp_cols if len({d[c] for d in mvp_recs}) <= 1]
print(
    f"missing_value_periods  rows={mvp_total}  cols={mvp_cols}  "
    f"full_row_duplicates={mvp_dups}  constant_columns={mvp_consts}"
)
for c in mvp_cols:
    missing = sum(1 for d in mvp_recs if d[c] is None or str(d[c]).strip() == "")
    print(f"  {c:<30} missing={missing:>6}  distinct={len({d[c] for d in mvp_recs})}")
for d in mvp_recs[:20]:
    print("  ", d)

# COMMAND ----------

# DBTITLE 1,Figure -- deepening measurement overview (rows / stations / year span)
facet_bars(
    {
        "rows per measurement": [(m, totals[m]) for m in MEASUREMENTS],
        "distinct stations": [(m, len(station_counts[m])) for m in MEASUREMENTS],
    },
    "DWD deepening -- measurement overview",
    "dwd_deepening_overview.png",
    rot=30,
    ncols=2,
)

# COMMAND ----------

# DBTITLE 1,Figure -- QN distribution + hourly coverage %
if qn_dist:
    facet_bars(
        qn_dist,
        "DWD deepening -- QN quality-flag distribution per measurement",
        "dwd_deepening_qn_distribution.png",
        rot=0,
    )
facet_bars(
    {
        m: [(r["station"], r["coverage_pct"]) for r in freq_cov.get(m, [])]
        for m in MEASUREMENTS
    },
    "DWD deepening -- hourly coverage % per station, by measurement",
    "dwd_deepening_hourly_coverage_pct.png",
)

# COMMAND ----------

# DBTITLE 1,Findings
findings_lines = []
for m in MEASUREMENTS:
    cov = [
        r["coverage_pct"] for r in freq_cov.get(m, []) if r["coverage_pct"] is not None
    ]
    worst_gap = max(
        (r["longest_gap_hours"] or 0 for r in freq_cov.get(m, [])), default=0
    )
    b = dup_breakdown.get(m, {})
    findings_lines.append(
        f"{m}: rows={totals[m]}, stations={len(station_counts[m])}, "
        f"hourly coverage {min(cov) if cov else 'n/a'}-{max(cov) if cov else 'n/a'}%, "
        f"longest gap {worst_gap}h, "
        f"dup identical={b.get('identical')}/conflicting={b.get('conflicting')}, "
        f"constant cols={constant_cols[m]}"
    )
print("\n".join(findings_lines))
print(
    f"missing_value_periods: rows={mvp_total}, duplicates={mvp_dups}, constant={mvp_consts}"
)

# COMMAND ----------

# DBTITLE 1,Export profiling findings -> src/schemas/profiling/dwd.md
_profile = [
    "| measurement | rows | cols | stations | first ts | last ts | constant cols |",
    "|---|---|---|---|---|---|---|",
]
for m in MEASUREMENTS:
    _profile.append(
        f"| {m} | {totals[m]} | {len(prof[m]['cols'])} | {len(station_counts[m])} | "
        f"{prof[m]['min_ts']} | {prof[m]['max_ts']} | "
        f"{', '.join(constant_cols[m]) or '-'} |"
    )
_profile.append(
    f"| missing_value_periods | {mvp_total} | {len(mvp_cols)} | - | - | - | "
    f"{', '.join(mvp_consts) or '-'} |"
)

_dq = [
    "| measurement | dup key groups | identical | conflicting |",
    "|---|---|---|---|",
]
for m in MEASUREMENTS:
    b = dup_breakdown.get(m, {})
    _dq.append(
        f"| {m} | {b.get('dup_groups')} | {b.get('identical')} | {b.get('conflicting')} |"
    )
_dq.append(f"\nmissing_value_periods full-row duplicates: {mvp_dups}.")

_temporal = [
    "Hourly grid (expected one row per station per hour); MESS_DATUM parsed yyyyMMddHH:"
]
for m in MEASUREMENTS:
    cov = [
        r["coverage_pct"] for r in freq_cov.get(m, []) if r["coverage_pct"] is not None
    ]
    worst = max((r["longest_gap_hours"] or 0 for r in freq_cov.get(m, [])), default=0)
    _temporal.append(
        f"- {m}: {prof[m]['min_ts']}..{prof[m]['max_ts']}, "
        f"per-station coverage {min(cov) if cov else 'n/a'}-{max(cov) if cov else 'n/a'}%, "
        f"longest gap {worst}h"
    )

_dist = []
for m in MEASUREMENTS:
    for c in value_columns(frames[m]):
        vs = value_stats[m]
        _dist.append(
            f"- {m}.`{c}`: min/max={vs.get(c + '_min')}/{vs.get(c + '_max')}, "
            f"p01/p50/p99={vs.get(c + '_p')}, mean={vs.get(c + '_mean')}, sd={vs.get(c + '_sd')}, "
            f"zero rows={vs.get(c + '_zero')}, -999 sentinel rows={vs.get(c + '_sentinel')}"
        )

_qn = ["QN quality flag distribution (where present):"]
for m, pairs in qn_dist.items():
    _qn.append(f"- {m}: {pairs}")

_findings_md = "\n".join(f"- {ln}" for ln in findings_lines)

_any_conflict = any(b.get("conflicting", 0) > 0 for b in dup_breakdown.values())
_any_sentinel = any(
    value_stats[m].get(c + "_sentinel", 0) > 0
    for m in MEASUREMENTS
    for c in value_columns(frames[m])
)
_silver = []
if _any_sentinel:
    _silver.append(
        "- `-999` (and blank) appears as a sentinel in at least one of these columns -> must "
        "become NULL before any stat, matching the original seven measurements."
    )
if _any_conflict:
    _silver.append(
        "- (STATIONS_ID, MESS_DATUM) has conflicting duplicate rows in at least one measurement "
        "-> a conflict-resolution rule is required (rule not yet established)."
    )
_silver.append("- Constant columns above carry no information.")
_silver.append(
    "- Hourly series are not necessarily continuous (coverage % / gaps above) -> no dense-grid "
    "assumption, same as the original seven measurements."
)
_silver.append(
    "- Value-column plausibility bounds were not assumed here (unlike 01's PLAUSIBLE table) -> "
    "define them from the distributions above before enabling an out-of-range quality flag."
)
_silver.append(
    "- missing_value_periods declares known gap windows per station/parameter -> reconcile against "
    "the observed hourly-coverage gaps above rather than assuming every gap is undeclared."
)

_ml_readiness = [
    (
        "No candidate ML target lives in these 8 deepening measurement tables -- like the original "
        "seven (01_weather_measurements_eda.py), they feed the shared `dim_weather_context` feature "
        "source, not a labelled table; `missing_value_periods` (profiled fully in "
        "03_missing_data_eda.py) is the nearest candidate target for a missingness/outage use case."
    ),
    (
        f"Grain and entity-grouped split: one row per (STATIONS_ID, MESS_DATUM) per measurement "
        f"({', '.join(MEASUREMENTS)}) -- split by STATIONS_ID or contiguous date range, never by row, "
        "matching the original seven measurements."
    ),
    (
        "Leakage: QN_* quality flags (where present) are assigned alongside the value, same caveat as "
        "the original seven -- any forecasting feature set may only use rows strictly before the "
        "prediction timestamp."
    ),
    (
        "Join cardinality: cross-table join cardinality between these 8 new tables and the "
        "original seven, and against station metadata, is NOT assessed in this notebook -- verify "
        "it (e.g. extend 04_dwd_relationships_and_findings.py) before using both groups together "
        "as joined features."
    ),
    (
        "Imbalance: not applicable -- no categorical target column; QN_* distributions (where present) "
        "are a quality flag, not a modelling target."
    ),
    (
        "Sample-vs-full divergence: not applicable in this notebook -- unlike 01, no value-column "
        "figure is drawn from a `.sample()` subset here; all reported stats (`value_stats`, "
        "`freq_cov`, `station_counts`) come from full-table Spark aggregations."
    ),
]
if _any_conflict:
    _ml_readiness.append(
        "Conflicting (STATIONS_ID, MESS_DATUM) duplicates exist in at least one deepening "
        "measurement (see Data Quality) and must be resolved deterministically before use as a "
        "feature source."
    )

write_profiling(
    SOURCE,
    NB_KEY,
    SECTION_TITLE,
    blocks=[
        ("Profile", "\n".join(_profile)),
        ("Data Quality", "\n".join(_dq) + "\n\n" + "\n".join(_qn)),
        ("Temporal", "\n".join(_temporal)),
        ("Distributions", "\n".join(_dist)),
        ("EDA Findings", _findings_md),
        ("ML-Readiness Evidence", "\n".join(f"- {ln}" for ln in _ml_readiness)),
        ("Silver Implications", "\n".join(_silver)),
    ],
    figures=[
        ("DWD deepening -- measurement overview", "dwd_deepening_overview.png"),
        (
            "DWD deepening -- QN quality-flag distribution",
            "dwd_deepening_qn_distribution.png",
        ),
        (
            "DWD deepening -- hourly coverage % per station",
            "dwd_deepening_hourly_coverage_pct.png",
        ),
    ],
)