# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # EDA -- HONDA IOT ENERGY
# MAGIC
# MAGIC **Energy Commerce and Retail Media Analytics Platform**
# MAGIC **Author:** Sharique Mohammad
# MAGIC **Date:** August 2026
# MAGIC
# MAGIC **Purpose:** Profile the six Honda IoT energy Bronze tables
# MAGIC (electricity / heating / cooling, each P and W) -- schema, missingness,
# MAGIC constant columns, frequency partitions, sensor/time continuity
# MAGIC (expected vs actual, interval consistency, gaps), value ranges,
# MAGIC distributions & suspicious readings (negatives, spikes, stuck runs),
# MAGIC per-timestamp duplicates (identical vs conflicting), and the P<->W
# MAGIC value relationship -- as evidence for Silver design.

# COMMAND ----------

# DBTITLE 1,Imports
import contextlib
import os as _os
import re as _re

import matplotlib.pyplot as plt
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
TABLES = {e: f"{CATALOG}.{BRONZE_SCHEMA}.honda_iot_{e}" for e in ENERGY}
KEY_COLS = ["frequency", "datetime_utc"]
VALUE_EXCLUDE = {"frequency", "datetime_utc"}
FREQ_SECONDS = {"1min": 60, "15min": 900, "1h": 3600}

# COMMAND ----------


# DBTITLE 1,Helpers
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

# DBTITLE 1,Profile -- rows, missingness, approx distinct, constant columns (one agg per table)
frames = {e: spark.table(t) for e, t in TABLES.items()}
prof = {}
for e in ENERGY:
    df = frames[e]
    cols = df.columns
    exprs = [F.count(F.lit(1)).alias("__rows")]
    for c in cols:
        miss = F.col(c).isNull() | (F.trim(F.col(c)) == "")
        exprs += [
            F.sum(miss.cast("long")).alias(c + "__m"),
            F.approx_count_distinct(c).alias(c + "__d"),
        ]
    r = df.agg(*exprs).first().asDict()
    prof[e] = {
        "cols": cols,
        "total": r["__rows"],
        "acd": {c: r[c + "__d"] for c in cols},
    }
    print("=" * 88, f"\n{e}  rows={r['__rows']}  ->  {cols}")
    for c in cols:
        print(
            f"  {c:<28} missing={r[c + '__m']:>12} rate={r[c + '__m'] / r['__rows']:.4f} approx_distinct={r[c + '__d']}"
        )
    print("constant columns:", [c for c in cols if r[c + "__d"] <= 1])
    df.show(5, truncate=False)
totals = {e: prof[e]["total"] for e in ENERGY}
VCOLS = {e: [c for c in prof[e]["cols"] if c not in VALUE_EXCLUDE] for e in ENERGY}

# COMMAND ----------

# DBTITLE 1,Rows per frequency + timestamp coverage (one groupBy per table)
freq_rows = {}
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
    freq_rows[e] = [(x["frequency"], x["rows"]) for x in d]
    freq_cov[e] = {x["frequency"]: x.asDict() for x in d}
    print(f"{e}:", [x.asDict() for x in d])

# COMMAND ----------

# DBTITLE 1,Duplicate (frequency, datetime_utc) key -- identical vs conflicting (one groupBy per table)
dup = {}
for e in ENERGY:
    df = frames[e]
    dk = df.groupBy(*KEY_COLS).agg(
        F.count(F.lit(1)).alias("n"),
        F.countDistinct(F.hash(*[F.col(c) for c in df.columns])).alias("row_variants"),
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
    dup[e] = b
    print(f"{e:<16} {b}")

# COMMAND ----------

# DBTITLE 1,Value columns -- range, percentiles, zero/negative/non-numeric (one agg per table)
value_stats = {}
for e in ENERGY:
    df = frames[e]
    exprs = []
    for c in VCOLS[e]:
        v = F.col(c).cast("double")
        exprs += [
            F.min(v).alias(c + "_min"),
            F.max(v).alias(c + "_max"),
            F.avg(v).alias(c + "_avg"),
            F.stddev(v).alias(c + "_sd"),
            F.expr(
                f"percentile_approx(cast(`{c}` as double), array(0.01,0.25,0.5,0.75,0.99))"
            ).alias(c + "_p"),
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

# DBTITLE 1,Suspicious readings -- 5-sigma outliers + stuck runs (two passes per table, all cols)
outliers = {}
for e in ENERGY:
    df = frames[e]
    vs = value_stats[e]
    oor_exprs = []
    for c in VCOLS[e]:
        v = F.col(c).cast("double")
        m, sd = vs[c + "_avg"], vs[c + "_sd"]
        oor_exprs.append(
            F.sum((F.abs(v - F.lit(m)) > 5 * F.lit(sd)).cast("long")).alias(c)
            if sd
            else F.lit(0).alias(c)
        )
    outliers[e] = df.agg(*oor_exprs).first().asDict()
    # stuck sensor: a value equal to the value 1 and 9 rows earlier within the 1h series
    w = Window.partitionBy("frequency").orderBy("datetime_utc")
    df_1h = df.where(F.col("frequency") == "1h")
    # First pass: compute window columns
    for c in VCOLS[e]:
        v = F.col(c).cast("double")
        df_1h = df_1h.withColumn(
            f"{c}_stuck",
            (
                v.isNotNull() & (v == F.lag(v, 1).over(w)) & (v == F.lag(v, 9).over(w))
            ).cast("long"),
        )
    # Second pass: aggregate the flags
    stuck_exprs = [F.sum(F.col(f"{c}_stuck")).alias(c) for c in VCOLS[e]]
    stuck = df_1h.agg(*stuck_exprs).first().asDict()
    print(f"{e}: 5sigma_outliers={outliers[e]}  stuck>=10run(1h)={stuck}")

# COMMAND ----------

# DBTITLE 1,Sensor/time continuity + interval consistency (one windowed pass per table)
continuity = {}
for e in ENERGY:
    df = frames[e]
    w = Window.partitionBy("frequency").orderBy("ts")
    deltas = (
        df.select("frequency", F.to_timestamp("datetime_utc").alias("ts"))
        .where(F.col("ts").isNotNull())
        .distinct()
        .withColumn(
            "delta_s", F.col("ts").cast("long") - F.lag("ts").over(w).cast("long")
        )
    )
    g = deltas.groupBy("frequency", "delta_s").count().collect()
    rows = []
    for freq, step in FREQ_SECONDS.items():
        fg = [x for x in g if x["frequency"] == freq]
        if not fg:
            continue
        cov = freq_cov[e].get(freq)
        span = None
        if cov and cov["min_ts"] and cov["max_ts"]:
            # datetime_utc is an ISO string; distinct_ts vs implied span
            observed = cov["distinct_ts"]
        else:
            observed = sum(x["count"] for x in fg) + 1
        total_intervals = sum(x["count"] for x in fg if x["delta_s"] is not None)
        on_step = sum(x["count"] for x in fg if x["delta_s"] == step)
        longest_gap = max(
            (
                (x["delta_s"] / step - 1)
                for x in fg
                if x["delta_s"] and x["delta_s"] > step
            ),
            default=0,
        )
        missing_steps = sum(
            (x["delta_s"] / step - 1) * x["count"]
            for x in fg
            if x["delta_s"] and x["delta_s"] > step
        )
        expected = observed + missing_steps
        rows.append(
            (
                freq,
                observed,
                round(observed / expected * 100, 2) if expected else None,
                round(on_step / total_intervals * 100, 2) if total_intervals else None,
                round(longest_gap, 1),
                round(missing_steps, 1),
            )
        )
    continuity[e] = rows
    print(
        f"{e}: (freq, observed, coverage%, on_step%, longest_gap_steps, missing_steps)"
    )
    for x in rows:
        print("  ", x)

# COMMAND ----------

# DBTITLE 1,P <-> W value relationship per metric (one join + one agg per metric)
pw_rel = {}
pw_scatter = {}
for metric in ("electricity", "heating", "cooling"):
    p, w = frames[f"{metric}_p"], frames[f"{metric}_w"]
    shared = [c for c in p.columns if c not in VALUE_EXCLUDE and c in w.columns]
    if not shared:
        continue
    j = p.select(
        *KEY_COLS, *[F.col(c).cast("double").alias(f"p_{c}") for c in shared]
    ).join(
        w.select(*KEY_COLS, *[F.col(c).cast("double").alias(f"w_{c}") for c in shared]),
        on=KEY_COLS,
        how="inner",
    )
    a = (
        j.agg(
            F.count(F.lit(1)).alias("matched"),
            *[F.corr(f"p_{c}", f"w_{c}").alias(f"corr_{c}") for c in shared],
        )
        .first()
        .asDict()
    )
    pw_rel[metric] = a
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
    print(f"{metric}: {a}")

# COMMAND ----------

# DBTITLE 1,P vs W schema parity
for metric in ("electricity", "heating", "cooling"):
    p, w = frames[f"{metric}_p"].columns, frames[f"{metric}_w"].columns
    print(f"{metric}:  identical={p == w}  P={p}  W={w}")

# COMMAND ----------

# DBTITLE 1,Value-column sample for histograms (one sampled pass per table)
value_pdf = {}
for e in ENERGY:
    df = frames[e]
    value_pdf[e] = (
        df.select(*[F.col(c).cast("double").alias(c) for c in VCOLS[e]])
        .sample(0.1, seed=42)
        .limit(150_000)
        .toPandas()
    )
    print(f"{e} value sample rows: {len(value_pdf[e])}")

# COMMAND ----------

# DBTITLE 1,Hourly time-series window (first ~2000 points per table)
ts_pdf = {}
for e in ENERGY:
    ts_pdf[e] = (
        frames[e]
        .where(F.col("frequency") == "1h")
        .select("datetime_utc", *[F.col(c).cast("double").alias(c) for c in VCOLS[e]])
        .orderBy("datetime_utc")
        .limit(2000)
        .toPandas()
    )

# COMMAND ----------

# DBTITLE 1,Figure -- rows per frequency, coverage %, duplicate groups
facet_bars(
    {e: freq_rows[e] for e in ENERGY},
    "Honda energy -- rows per frequency, by table",
    "honda_energy_rows_per_frequency.png",
    rot=0,
)
facet_bars(
    {e: [(x[0], x[2]) for x in continuity[e]] for e in ENERGY},
    "Honda energy -- coverage % by frequency, by table",
    "honda_energy_coverage_pct.png",
    rot=0,
)
barplot(
    [(e, dup[e]["dup_groups"]) for e in ENERGY],
    "Honda energy -- duplicate (frequency, datetime_utc) groups",
    "table",
    "dup groups",
    rot=30,
    filename="honda_energy_duplicate_groups.png",
)

# COMMAND ----------

# DBTITLE 1,Figure -- longest gap (missing steps) per table x frequency
freqs = list(FREQ_SECONDS)
x = np.arange(len(ENERGY))
plt.figure(figsize=(11, 4))
for i, freq in enumerate(freqs):
    vals = [next((r[4] for r in continuity[e] if r[0] == freq), 0) for e in ENERGY]
    plt.bar(x + i * 0.27, vals, width=0.27, label=freq)
plt.xticks(x + 0.27, ENERGY, rotation=30, ha="right")
plt.legend()
plt.title("Honda energy -- longest gap (missing steps) per table x frequency")
plt.ylabel("steps")
plt.tight_layout()
plt.savefig(
    fig_path("honda_energy_longest_gap_per_table.png"), dpi=110, bbox_inches="tight"
)
plt.show()

# COMMAND ----------

# DBTITLE 1,Figure -- value distributions and hourly windows (faceted)
facet_hists(
    {f"{e}.{c}": value_pdf[e][c].dropna().tolist() for e in ENERGY for c in VCOLS[e]},
    "Honda energy -- value distribution per table.column (sampled)",
    "honda_energy_value_distributions.png",
    ncols=4,
)


def _hourly_draw(tp, cols):
    def draw(ax):
        for c in cols:
            ax.plot(range(len(tp)), tp[c], label=c, linewidth=0.8)
        ax.legend(fontsize=6)

    return draw


_facet_grid(
    [(e, _hourly_draw(ts_pdf[e], VCOLS[e])) for e in ENERGY if not ts_pdf[e].empty],
    "Honda energy -- first 2000 hourly points, by table",
    "honda_energy_first_hourly_points.png",
)

# COMMAND ----------


# DBTITLE 1,Figure -- P vs W scatter per metric (faceted)
def _scatter_draw(col, pdf):
    def draw(ax):
        ax.scatter(pdf[f"p_{col}"], pdf[f"w_{col}"], s=6, alpha=0.3)
        ax.set_xlabel(f"P.{col}", fontsize=7)
        ax.set_ylabel(f"W.{col}", fontsize=7)

    return draw


_facet_grid(
    [
        (metric, _scatter_draw(col, pdf))
        for metric, (col, pdf) in pw_scatter.items()
        if not pdf.empty
    ],
    "Honda energy -- P vs W per metric (sampled)",
    "honda_energy_p_vs_w_scatter.png",
)

# COMMAND ----------

# DBTITLE 1,Findings
print("dup composition:", dup)
print(
    "coverage % / on-step % per table/frequency:",
    {e: [(x[0], x[2], x[3]) for x in continuity[e]] for e in ENERGY},
)
print("5-sigma outliers:", outliers)
print("P<->W relationship:", pw_rel)

# COMMAND ----------

# DBTITLE 1,Export profiling findings -> src/schemas/profiling/honda_iot.md
_profile = [
    "| table | rows | cols | frequencies | constant cols |",
    "|---|---|---|---|---|",
]
for e in ENERGY:
    consts = [c for c in prof[e]["cols"] if prof[e]["acd"][c] <= 1]
    _profile.append(
        f"| {e} | {totals[e]} | {len(prof[e]['cols'])} | "
        f"{ {fr: fc['rows'] for fr, fc in freq_cov[e].items()} } | {', '.join(consts) or '-'} |"
    )
_profile.append("")
_profile.append("Value columns per table: " + str({e: VCOLS[e] for e in ENERGY}))

_dq = ["| table | dup key groups | identical | conflicting |", "|---|---|---|---|"]
for e in ENERGY:
    b = dup[e]
    _dq.append(f"| {e} | {b['dup_groups']} | {b['identical']} | {b['conflicting']} |")
_dq.append("")
_dq.append("5-sigma outlier rows per value column: " + str(outliers))

_temporal = [
    "Per (table, frequency): observed, coverage %, on-step %, longest gap (steps), missing steps:"
]
for e in ENERGY:
    for row in continuity[e]:
        _temporal.append(
            f"- {e} / {row[0]}: observed={row[1]}, coverage={row[2]}%, on-step={row[3]}%, longest gap={row[4]}, missing steps={row[5]}"
        )

_dist = []
for e in ENERGY:
    for c in VCOLS[e]:
        vs = value_stats[e]
        _dist.append(
            f"- {e}.`{c}`: min/max={vs.get(c + '_min')}/{vs.get(c + '_max')}, "
            f"p01/25/50/75/99={vs.get(c + '_p')}, mean={vs.get(c + '_avg')}, sd={vs.get(c + '_sd')}, "
            f"zero rows={vs.get(c + '_zero')}, negative rows={vs.get(c + '_negative')}, non-numeric={vs.get(c + '_non_numeric')}"
        )

_rel = [
    "P<->W value relationship per metric (matched rows on (frequency, datetime_utc) + per-column Pearson corr):"
]
for metric, a in pw_rel.items():
    _rel.append(f"- {metric}: {a}")
_rel.append("")
_rel.append(
    "P/W schema parity: "
    + str(
        {
            metric: frames[f"{metric}_p"].columns == frames[f"{metric}_w"].columns
            for metric in ("electricity", "heating", "cooling")
        }
    )
)

_any_conflict = any(dup[e]["conflicting"] > 0 for e in ENERGY)
_silver = [
    "- Type conversion: datetime_utc -> timestamp; value columns -> double.",
    "- `frequency` (1min / 15min / 1h) is a real physical resolution -> keep it in the grain; do not blend frequencies.",
]
if _any_conflict:
    _silver.append(
        "- (frequency, datetime_utc) has conflicting duplicate rows in at least one table -> a conflict-resolution rule is required (rule not yet established)."
    )
_silver.append("- Identical (frequency, datetime_utc) repeats can be de-duplicated.")
_silver.append(
    "- Series are not dense (coverage % / gaps above) -> observed points only; resampling is a downstream choice."
)
_silver.append("- Stuck-sensor runs and 5-sigma spikes -> data-quality flag, keep raw.")
_silver.append(
    "- P and W tables of a metric are schema-identical, join 1:1 on (frequency, datetime_utc), and are highly correlated (corr above) -> may be modelled as one fact per metric (a Silver modelling choice)."
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
        ("Relationships", "\n".join(_rel)),
        (
            "EDA Findings",
            "\n".join(
                [
                    f"- dup composition: {dup}",
                    "- coverage % / on-step % per table/frequency: "
                    + str(
                        {e: [(x[0], x[2], x[3]) for x in continuity[e]] for e in ENERGY}
                    ),
                    f"- 5-sigma outliers: {outliers}",
                    f"- P<->W relationship: {pw_rel}",
                ]
            ),
        ),
        ("Silver Implications", "\n".join(_silver)),
    ],
    figures=[
        (
            "Honda energy -- rows per frequency, by table",
            "honda_energy_rows_per_frequency.png",
        ),
        ("Honda energy -- coverage % by frequency", "honda_energy_coverage_pct.png"),
        (
            "Honda energy -- duplicate (frequency, datetime_utc) groups",
            "honda_energy_duplicate_groups.png",
        ),
        (
            "Honda energy -- longest gap (missing steps) per table x frequency",
            "honda_energy_longest_gap_per_table.png",
        ),
        (
            "Honda energy -- value distribution per table.column",
            "honda_energy_value_distributions.png",
        ),
        (
            "Honda energy -- first 2000 hourly points, by table",
            "honda_energy_first_hourly_points.png",
        ),
        (
            "Honda energy -- P vs W per metric",
            "honda_energy_p_vs_w_scatter.png",
        ),
    ],
)
