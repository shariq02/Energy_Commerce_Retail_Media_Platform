# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # EDA -- DWD WEATHER MEASUREMENTS
# MAGIC
# MAGIC **Energy Commerce and Retail Media Analytics Platform**
# MAGIC **Author:** Sharique Mohammad
# MAGIC **Date:** August 2026
# MAGIC
# MAGIC **Purpose:** Profile the seven DWD weather-measurement Bronze tables
# MAGIC (air_temperature ... wind) -- schema, missingness, quality flags,
# MAGIC constant columns, temporal coverage & frequency (expected vs actual
# MAGIC hourly grid, gaps, longest gap per station), station x measurement
# MAGIC coverage matrix, per-station duplicates (identical vs conflicting),
# MAGIC value distributions & plausibility, QN-vs-missingness relationship --
# MAGIC as evidence for Silver design.

# COMMAND ----------

# DBTITLE 1,Imports
import contextlib
import os as _os
import re as _re

import matplotlib.pyplot as plt
import numpy as np
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Configuration
CATALOG = "energy_commerce_retail_media"
BRONZE_SCHEMA = "bronze"
SOURCE = "dwd"
NB_KEY = "01_weather_measurements"
SECTION_TITLE = "Weather measurements (air_temperature ... wind)"
MEASUREMENTS = [
    "air_temperature",
    "cloudiness",
    "moisture",
    "precipitation",
    "pressure",
    "sun",
    "wind",
]
TABLES = {m: f"{CATALOG}.{BRONZE_SCHEMA}.dwd_{m}" for m in MEASUREMENTS}
NON_VALUE = {"STATIONS_ID", "CITY", "MESS_DATUM", "EOR"}
# Any `QN_*` column is a DWD quality byte, never a measured value.
QN_CANDIDATES = ("QN_9", "QN_8", "QN_7", "QN_4", "QN_3", "QN")
# DWD companion "Messverfahren-Index" columns -- string indicators, not measured
# values; treating them as numeric produced all-None distribution rows.
INDICATOR_COLS = {"V_N_I"}

# Physical plausibility windows for the common DWD hourly parameters (values
# outside, and not the -999 sentinel, are "suspicious" not necessarily wrong).
PLAUSIBLE = {
    "TT_TU": (-40.0, 45.0),
    "RF_TU": (0.0, 100.0),
    "TF_STD": (-40.0, 45.0),
    "P": (900.0, 1080.0),
    "P0": (900.0, 1080.0),
    "N": (0.0, 8.0),
    "V_N": (0.0, 8.0),
    "R1": (0.0, 200.0),
    "RS_IND": (0.0, 1.0),
    "SD_SO": (0.0, 60.0),
    "F": (0.0, 60.0),
    "D": (0.0, 360.0),
}

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


def histplot(values, title, xlabel, bins=50, filename=None):
    plt.figure(figsize=(10, 4))
    plt.hist(values, bins=bins)
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel("count")
    plt.tight_layout()
    if filename:
        plt.savefig(fig_path(filename), dpi=110, bbox_inches="tight")
    plt.show()


# COMMAND ----------

# DBTITLE 1,Profiling-export helper (writes src/schemas/profiling/<source>.md)


def _repo_root():
    # Notebook CWD in a Databricks Git folder is <repo>/databricks/eda/<source>.
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
    # One <source>.md per source; each notebook owns one marker-delimited
    # `## ` section, re-run replaces its own, others preserved, order by key.
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

# DBTITLE 1,Profile -- rows, missingness, approx distinct, min/max ts, constant columns (one agg per table)
frames = {m: spark.table(t) for m, t in TABLES.items()}
prof = {}
for m in MEASUREMENTS:
    df = frames[m]
    cols = df.columns
    exprs = [
        F.count(F.lit(1)).alias("__rows"),
        F.min(find_col(df, "MESS_DATUM")).alias("__min_ts"),
        F.max(find_col(df, "MESS_DATUM")).alias("__max_ts"),
    ]
    for c in cols:
        miss = F.col(c).isNull() | (F.trim(F.col(c)) == "")
        exprs += [
            F.sum(miss.cast("long")).alias(c + "__m"),
            F.approx_count_distinct(c).alias(c + "__d"),
        ]
    r = df.agg(*exprs).first().asDict()
    prof[m] = {
        "cols": cols,
        "total": r["__rows"],
        "min_ts": r["__min_ts"],
        "max_ts": r["__max_ts"],
        "miss": {c: r[c + "__m"] for c in cols},
        "acd": {c: r[c + "__d"] for c in cols},
    }
    sid, city = find_col(df, "STATIONS_ID"), find_col(df, "city")
    print(
        "=" * 90,
        f"\n{m}  rows={r['__rows']}  {r['__min_ts']}..{r['__max_ts']}  "
        f"stations~={r[sid + '__d']}  cities~={r[city + '__d']}\n",
        "=" * 90,
    )
    for c in cols:
        print(
            f"  {c:<26} missing={r[c + '__m']:>12} rate={r[c + '__m'] / r['__rows']:.4f} approx_distinct={r[c + '__d']}"
        )
    print("constant columns:", [c for c in cols if r[c + "__d"] <= 1])
totals = {m: prof[m]["total"] for m in MEASUREMENTS}
coverage = {
    m: {
        "stations": prof[m]["acd"][find_col(frames[m], "STATIONS_ID")],
        "cities": prof[m]["acd"][find_col(frames[m], "city")],
        "min_ts": prof[m]["min_ts"],
        "max_ts": prof[m]["max_ts"],
    }
    for m in MEASUREMENTS
}
constant_cols = {
    m: [c for c in prof[m]["cols"] if prof[m]["acd"][c] <= 1] for m in MEASUREMENTS
}

# COMMAND ----------

# DBTITLE 1,Station / city breakdown + per-station row counts (one groupBy per table)
station_counts = {}
for m in MEASUREMENTS:
    df = frames[m]
    city, sid = find_col(df, "city"), find_col(df, "STATIONS_ID")
    g = df.groupBy(city, sid).count().collect()
    per_station = {}
    for x in g:
        per_station[str(x[sid])] = per_station.get(str(x[sid]), 0) + x["count"]
    station_counts[m] = per_station
    print(f"--- {m} ---", sorted((x[city], x[sid], x["count"]) for x in g))

# COMMAND ----------

# DBTITLE 1,QN quality-flag vs missingness / out-of-range (one groupBy per table)
qn_dist = {}
qn_quality = {}
for m in MEASUREMENTS:
    df = frames[m]
    qn = find_col(df, *QN_CANDIDATES)
    vcols = value_columns(df)
    if qn is None:
        continue
    any_sentinel = F.lit(False)
    any_oor = F.lit(False)
    for c in vcols:
        v = F.when(F.col(c).rlike(r"^-?\d+(\.\d+)?$"), F.col(c).cast("double"))
        any_sentinel = any_sentinel | (v == -999)
        b = PLAUSIBLE.get(c.upper())
        if b:
            any_oor = any_oor | ((v != -999) & ((v < b[0]) | (v > b[1])))
    g = (
        df.groupBy(qn)
        .agg(
            F.count(F.lit(1)).alias("rows"),
            F.sum(any_sentinel.cast("long")).alias("rows_with_-999"),
            F.sum(any_oor.cast("long")).alias("rows_out_of_range"),
        )
        .orderBy(F.desc("rows"))
        .collect()
    )
    qn_dist[m] = [(x[qn], x["rows"]) for x in g]
    qn_quality[m] = [x.asDict() for x in g]
    print(f"--- {m} ({qn}) ---", qn_quality[m])

# COMMAND ----------

# DBTITLE 1,Duplicate (station, timestamp) -- identical vs conflicting (one groupBy per table)
dup_breakdown = {}
for m in MEASUREMENTS:
    df = frames[m]
    cols = df.columns
    key = [find_col(df, "STATIONS_ID"), find_col(df, "MESS_DATUM")]
    dk = df.groupBy(*key).agg(
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

# DBTITLE 1,Value columns -- range, sentinel, percentiles, plausibility (one agg per table)
value_stats = {}
for m in MEASUREMENTS:
    df = frames[m]
    exprs = []
    for c in value_columns(df):
        v = F.when(F.col(c).rlike(r"^-?\d+(\.\d+)?$"), F.col(c).cast("double"))
        b = PLAUSIBLE.get(c.upper())
        exprs += [
            F.min(v).alias(c + "_min"),
            F.max(v).alias(c + "_max"),
            F.min(F.when(v != -999, v)).alias(c + "_min_ns"),
            F.max(F.when(v != -999, v)).alias(c + "_max_ns"),
            F.sum((v == -999).cast("long")).alias(c + "_sentinel"),
            F.avg(F.when(v != -999, v)).alias(c + "_mean"),
            F.stddev(F.when(v != -999, v)).alias(c + "_sd"),
            # Reuse the parsed, sentinel-excluded column expression `v` directly.
            # (An inline SQL regex string here is mangled by the Spark-SQL string
            # parser -- `\d` -> `d` -- so the CASE matched nothing and every
            # percentile came back NULL.)
            F.percentile_approx(F.when(v != -999, v), [0.01, 0.5, 0.99]).alias(
                c + "_p"
            ),
            F.sum((v == 0).cast("long")).alias(c + "_zero"),
            *(
                [
                    F.sum(((v != -999) & ((v < b[0]) | (v > b[1]))).cast("long")).alias(
                        c + "_oor"
                    )
                ]
                if b
                else []
            ),
        ]
    value_stats[m] = df.agg(*exprs).first().asDict()
    for c in value_columns(df):
        print(
            f"{m}.{c:<12}",
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
    w = Window.partitionBy("station").orderBy("ts")
    per_station = (
        df.select(F.col(sid).alias("station"), as_ts(dts).alias("ts"))
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
            F.sum(F.when(F.col("gap_h") > 0, F.col("gap_h")).otherwise(0)).alias(
                "total_missing_hours"
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

# DBTITLE 1,Station x measurement coverage matrix (reuses per-station row counts)
all_stations = sorted({s for m in MEASUREMENTS for s in station_counts[m]})
coverage_matrix = [
    {"station": s, **{m: station_counts[m].get(s, 0) for m in MEASUREMENTS}}
    for s in all_stations
]
for row in coverage_matrix:
    print(row)

# COMMAND ----------

# DBTITLE 1,Value-column sample for histograms / box plots (one sampled pass per table)
value_pdf = {}
for m in MEASUREMENTS:
    df = frames[m]
    vcols = value_columns(df)
    value_pdf[m] = (
        df.select(
            *[
                F.when(
                    F.col(c).rlike(r"^-?\d+(\.\d+)?$")
                    & (F.col(c).cast("double") != -999),
                    F.col(c).cast("double"),
                ).alias(c)
                for c in vcols
            ]
        )
        .sample(0.05, seed=42)
        .limit(150_000)
        .toPandas()
    )
    print(f"{m} value sample rows: {len(value_pdf[m])}")

# COMMAND ----------

# DBTITLE 1,Figure -- measurement overview (rows / stations / cities / year span)
facet_bars(
    {
        "rows per measurement": [(m, totals[m]) for m in MEASUREMENTS],
        "distinct stations": [(m, coverage[m]["stations"]) for m in MEASUREMENTS],
        "distinct cities": [(m, coverage[m]["cities"]) for m in MEASUREMENTS],
        "observation years spanned": [
            (
                m,
                int(str(coverage[m]["max_ts"])[:4])
                - int(str(coverage[m]["min_ts"])[:4])
                + 1,
            )
            for m in MEASUREMENTS
        ],
    },
    "DWD -- measurement overview",
    "dwd_measurement_overview.png",
    rot=30,
    ncols=2,
)

# COMMAND ----------

# DBTITLE 1,Figure -- QN distribution, dup composition, coverage %, longest gap
facet_bars(
    qn_dist,
    "DWD -- QN quality-flag distribution per measurement",
    "dwd_qn_distribution.png",
    rot=0,
)
x = np.arange(len(MEASUREMENTS))
plt.figure(figsize=(11, 4))
plt.bar(
    x - 0.2,
    [dup_breakdown[m]["identical"] for m in MEASUREMENTS],
    width=0.4,
    label="fully-identical repeat groups",
)
plt.bar(
    x + 0.2,
    [dup_breakdown[m]["conflicting"] for m in MEASUREMENTS],
    width=0.4,
    label="keys with conflicting rows",
)
plt.xticks(x, MEASUREMENTS, rotation=30, ha="right")
plt.legend()
plt.title("DWD -- duplicate key composition")
plt.ylabel("key groups")
plt.tight_layout()
plt.savefig(fig_path("dwd_duplicate_key_composition.png"), dpi=110, bbox_inches="tight")
plt.show()
facet_bars(
    {m: [(r["station"], r["coverage_pct"]) for r in freq_cov[m]] for m in MEASUREMENTS},
    "DWD -- hourly coverage % per station, by measurement",
    "dwd_hourly_coverage_pct.png",
)
facet_bars(
    {
        m: [(r["station"], r["longest_gap_hours"] or 0) for r in freq_cov[m]]
        for m in MEASUREMENTS
    },
    "DWD -- longest missing-hours gap per station, by measurement",
    "dwd_longest_gap_hours.png",
)

# COMMAND ----------

# DBTITLE 1,Figure -- station x measurement coverage heatmap
grid = np.array(
    [[row[m] for m in MEASUREMENTS] for row in coverage_matrix], dtype=float
)
plt.figure(figsize=(9, max(3, 0.4 * len(all_stations))))
plt.imshow(
    np.where(grid > 0, np.log10(grid + 1), np.nan), aspect="auto", cmap="viridis"
)
plt.colorbar(label="log10(row count)")
plt.xticks(range(len(MEASUREMENTS)), MEASUREMENTS, rotation=45, ha="right")
plt.yticks(range(len(all_stations)), all_stations)
plt.title("DWD -- station x measurement coverage (row count)")
plt.tight_layout()
plt.savefig(
    fig_path("dwd_station_x_measurement_coverage.png"), dpi=110, bbox_inches="tight"
)
plt.show()

# COMMAND ----------


# DBTITLE 1,Figure -- value column spread per measurement (sampled, sentinel excluded)
def _box_draw(pdf, cols):
    def draw(ax):
        ax.boxplot(
            [pdf[c].dropna().tolist() for c in cols], labels=cols, showfliers=True
        )
        ax.tick_params(axis="x", labelrotation=30)

    return draw


_box_items = []
for m in MEASUREMENTS:
    pdf = value_pdf[m]
    cols = [c for c in pdf.columns if pdf[c].notna().any()]
    if cols:
        _box_items.append((m, _box_draw(pdf, cols)))
_facet_grid(
    _box_items,
    "DWD -- value column spread per measurement (sampled)",
    "dwd_value_column_spread.png",
)

# COMMAND ----------

# DBTITLE 1,Findings
findings_lines = []
for m in MEASUREMENTS:
    cov = [r["coverage_pct"] for r in freq_cov[m] if r["coverage_pct"] is not None]
    worst_gap = max((r["longest_gap_hours"] or 0 for r in freq_cov[m]), default=0)
    b = dup_breakdown[m]
    findings_lines.append(
        f"{m}: rows={totals[m]}, stations={coverage[m]['stations']}, "
        f"hourly coverage {min(cov) if cov else 'n/a'}-{max(cov) if cov else 'n/a'}%, "
        f"longest gap {worst_gap}h, dup identical={b['identical']}/conflicting={b['conflicting']}, "
        f"constant cols={constant_cols[m]}"
    )
print("\n".join(findings_lines))

# COMMAND ----------

# DBTITLE 1,Export profiling findings -> src/schemas/profiling/dwd.md
_profile = [
    "| measurement | rows | cols | stations~ | cities~ | first ts | last ts | constant cols |",
    "|---|---|---|---|---|---|---|---|",
]
for m in MEASUREMENTS:
    _profile.append(
        f"| {m} | {totals[m]} | {len(prof[m]['cols'])} | {coverage[m]['stations']} | "
        f"{coverage[m]['cities']} | {coverage[m]['min_ts']} | {coverage[m]['max_ts']} | "
        f"{', '.join(constant_cols[m]) or '-'} |"
    )
_miss = ["Highest-missingness column per measurement (missing includes blank):"]
for m in MEASUREMENTS:
    mi = max(prof[m]["miss"].items(), key=lambda kv: kv[1])
    _miss.append(f"- {m}: `{mi[0]}` {mi[1]} ({mi[1] / totals[m]:.2%})")

_dq = [
    "| measurement | dup key groups | identical | conflicting | -999 sentinel (worst col) | out-of-range (worst col) |",
    "|---|---|---|---|---|---|",
]
for m in MEASUREMENTS:
    b = dup_breakdown[m]
    vs = value_stats[m]
    sent = max(
        ((c, vs.get(c + "_sentinel", 0)) for c in value_columns(frames[m])),
        key=lambda t: t[1],
        default=("-", 0),
    )
    oor = max(
        (
            (c, vs.get(c + "_oor", 0))
            for c in value_columns(frames[m])
            if c + "_oor" in vs
        ),
        key=lambda t: t[1],
        default=("-", 0),
    )
    _dq.append(
        f"| {m} | {b['dup_groups']} | {b['identical']} | {b['conflicting']} | {sent[0]}={sent[1]} | {oor[0]}={oor[1]} |"
    )

_temporal = [
    "Hourly grid (expected one row per station per hour); MESS_DATUM parsed yyyyMMddHH:"
]
for m in MEASUREMENTS:
    cov = [r["coverage_pct"] for r in freq_cov[m] if r["coverage_pct"] is not None]
    worst = max((r["longest_gap_hours"] or 0 for r in freq_cov[m]), default=0)
    _temporal.append(
        f"- {m}: {coverage[m]['min_ts']}..{coverage[m]['max_ts']}, "
        f"per-station coverage {min(cov) if cov else 'n/a'}-{max(cov) if cov else 'n/a'}%, longest gap {worst}h"
    )

_coverage = [
    f"{len(all_stations)} distinct station ids across the 7 measurements: {all_stations}.",
    "Per-measurement station presence (row count per cell) — see the exported heatmap figure.",
]
for row in coverage_matrix:
    _coverage.append(
        f"- station {row['station']}: "
        + ", ".join(f"{m}={row[m]}" for m in MEASUREMENTS)
    )

_dist = []
for m in MEASUREMENTS:
    for c in value_columns(frames[m]):
        vs = value_stats[m]
        _dist.append(
            f"- {m}.`{c}`: min/max(no sentinel)={vs.get(c + '_min_ns')}/{vs.get(c + '_max_ns')}, "
            f"p01/p50/p99={vs.get(c + '_p')}, mean={vs.get(c + '_mean')}, sd={vs.get(c + '_sd')}, "
            f"zero rows={vs.get(c + '_zero')}, -999={vs.get(c + '_sentinel')}"
            + (f", out-of-range={vs.get(c + '_oor')}" if c + "_oor" in vs else "")
        )

_qn = ["QN quality flag vs -999 sentinel / out-of-range rows:"]
for m in MEASUREMENTS:
    if m in qn_quality:
        _qn.append(f"- {m}: " + "; ".join(str(d) for d in qn_quality[m]))

_any_conflict = any(dup_breakdown[m]["conflicting"] > 0 for m in MEASUREMENTS)
_any_sentinel = any(
    value_stats[m].get(c + "_sentinel", 0) > 0
    for m in MEASUREMENTS
    for c in value_columns(frames[m])
)
_silver = []
if _any_sentinel:
    _silver.append(
        "- `-999` (and blank) is the DWD missing sentinel and is present -> must become NULL before any stat."
    )
if _any_conflict:
    _silver.append(
        "- (STATIONS_ID, MESS_DATUM) has conflicting duplicate rows in at least one measurement -> a conflict-resolution rule is required (rule not yet established)."
    )
_silver.append(
    "- Identical (STATIONS_ID, MESS_DATUM) repeats can be de-duplicated safely."
)
_silver.append("- Constant columns above carry no information.")
_silver.append(
    "- Hourly series are not continuous (coverage % / gaps above) -> no dense-grid assumption."
)
_silver.append(
    "- Out-of-range non-sentinel values are flagged as suspicious, not proven wrong -> keep raw + a quality flag."
)

write_profiling(
    SOURCE,
    NB_KEY,
    SECTION_TITLE,
    blocks=[
        ("Profile", "\n".join(_profile) + "\n\n" + "\n".join(_miss)),
        ("Data Quality", "\n".join(_dq) + "\n\n" + "\n".join(_qn)),
        ("Temporal", "\n".join(_temporal)),
        ("Coverage", "\n".join(_coverage)),
        ("Distributions", "\n".join(_dist)),
        ("EDA Findings", "\n".join(f"- {ln}" for ln in findings_lines)),
        ("Silver Implications", "\n".join(_silver)),
    ],
    figures=[
        ("DWD measurement overview", "dwd_measurement_overview.png"),
        ("DWD QN quality-flag distribution", "dwd_qn_distribution.png"),
        ("DWD duplicate key composition", "dwd_duplicate_key_composition.png"),
        ("DWD hourly coverage % per station", "dwd_hourly_coverage_pct.png"),
        ("DWD longest missing-hours gap per station", "dwd_longest_gap_hours.png"),
        (
            "DWD station x measurement coverage",
            "dwd_station_x_measurement_coverage.png",
        ),
        ("DWD value column spread per measurement", "dwd_value_column_spread.png"),
    ],
)
