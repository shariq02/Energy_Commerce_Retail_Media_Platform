# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # EDA -- HONDA IOT WEATHER
# MAGIC
# MAGIC **Energy Commerce and Retail Media Analytics Platform**
# MAGIC **Author:** Sharique Mohammad
# MAGIC **Date:** August 2026
# MAGIC
# MAGIC **Purpose:** Profile honda_iot_weather -- schema (the sanitized
# MAGIC WeatherStation.Weather.* column names), missingness, constant columns,
# MAGIC frequency partitions, time continuity (expected vs actual, interval
# MAGIC consistency, gaps), value ranges, distributions & plausibility
# MAGIC (Ta range, Igm >= 0, stuck runs), duplicates (identical vs
# MAGIC conflicting) -- as evidence for Silver design and for the
# MAGIC energy<->weather join in notebook 03.

# COMMAND ----------

# DBTITLE 1,Imports
import contextlib
import os as _os
import re as _re

import matplotlib.pyplot as plt
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

# COMMAND ----------


# DBTITLE 1,Helpers
def barplot(pairs, title, xlabel, ylabel="rows", rot=0, filename=None):
    plt.figure(figsize=(10, 4))
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

# DBTITLE 1,Profile -- rows, missingness, approx distinct, constant columns, value ranges (one agg)
df = spark.table(TABLE)
COLS = df.columns
VCOLS = [c for c in COLS if c not in ("frequency", "datetime_utc")]
exprs = [F.count(F.lit(1)).alias("__rows")]
for c in COLS:
    miss = F.col(c).isNull() | (F.trim(F.col(c)) == "")
    exprs += [
        F.sum(miss.cast("long")).alias(c + "__m"),
        F.approx_count_distinct(c).alias(c + "__d"),
    ]
for c in VCOLS:
    v = F.col(c).cast("double")
    b = PLAUSIBLE.get(c.split("_")[-1])
    exprs += [
        F.min(v).alias(c + "_min"),
        F.max(v).alias(c + "_max"),
        F.avg(v).alias(c + "_avg"),
        F.stddev(v).alias(c + "_sd"),
        F.expr(f"percentile_approx(cast(`{c}` as double), array(0.01,0.5,0.99))").alias(
            c + "_p"
        ),
        F.sum((v == 0).cast("long")).alias(c + "_zero"),
        F.sum(
            (F.col(c).isNotNull() & (F.trim(F.col(c)) != "") & v.isNull()).cast("long")
        ).alias(c + "_non_numeric"),
        *(
            [F.sum(((v < b[0]) | (v > b[1])).cast("long")).alias(c + "_out_of_range")]
            if b
            else []
        ),
    ]
S = df.agg(*exprs).first().asDict()
total = S["__rows"]
constant_cols = [c for c in COLS if S[c + "__d"] <= 1]
print(f"rows={total}  columns={len(COLS)}  ->  {COLS}")
for c in COLS:
    print(
        f"  {c:<32} missing={S[c + '__m']:>10} rate={S[c + '__m'] / total:.4f} approx_distinct={S[c + '__d']}"
    )
print("constant columns:", constant_cols)
for c in VCOLS:
    print(f"  {c:<32}", {k[len(c) + 1 :]: S[k] for k in S if k.startswith(c + "_")})
df.show(10, truncate=False)

# COMMAND ----------

# DBTITLE 1,Rows per frequency + timestamp coverage (one groupBy)
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
print([x.asDict() for x in d])

# COMMAND ----------

# DBTITLE 1,Duplicate (frequency, datetime_utc) key -- identical vs conflicting (one groupBy)
dk = df.groupBy("frequency", "datetime_utc").agg(
    F.count(F.lit(1)).alias("n"),
    F.countDistinct(F.hash(*[F.col(c) for c in COLS])).alias("row_variants"),
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
print("duplicate key composition:", b)

# COMMAND ----------

# DBTITLE 1,Time continuity + interval consistency (one windowed pass)
w = Window.partitionBy("frequency").orderBy("ts")
deltas = (
    df.select("frequency", F.to_timestamp("datetime_utc").alias("ts"))
    .where(F.col("ts").isNotNull())
    .distinct()
    .withColumn("delta_s", F.col("ts").cast("long") - F.lag("ts").over(w).cast("long"))
)
g = deltas.groupBy("frequency", "delta_s").count().collect()
continuity = []
for freq, step in FREQ_SECONDS.items():
    fg = [x for x in g if x["frequency"] == freq]
    if not fg:
        continue
    observed = sum(x["count"] for x in fg)
    intervals = sum(x["count"] for x in fg if x["delta_s"] is not None)
    on_step = sum(x["count"] for x in fg if x["delta_s"] == step)
    longest_gap = max(
        ((x["delta_s"] / step - 1) for x in fg if x["delta_s"] and x["delta_s"] > step),
        default=0,
    )
    missing = sum(
        (x["delta_s"] / step - 1) * x["count"]
        for x in fg
        if x["delta_s"] and x["delta_s"] > step
    )
    expected = observed + missing
    continuity.append(
        (
            freq,
            observed,
            round(observed / expected * 100, 2) if expected else None,
            round(on_step / intervals * 100, 2) if intervals else None,
            round(longest_gap, 1),
        )
    )
    print(continuity[-1])
delta_dist = sorted(
    ((x["frequency"], x["delta_s"], x["count"]) for x in g), key=lambda t: -t[2]
)[:20]
print("top interval sizes (frequency, delta_s, count):", delta_dist)

# COMMAND ----------

# DBTITLE 1,Stuck-run detection per value column (one windowed pass)
w2 = Window.partitionBy("frequency").orderBy("datetime_utc")
df_1h = df.where(F.col("frequency") == "1h")
# First pass: compute window columns
for c in VCOLS:
    v = F.col(c).cast("double")
    df_1h = df_1h.withColumn(
        f"{c}_stuck",
        (
            v.isNotNull() & (v == F.lag(v, 1).over(w2)) & (v == F.lag(v, 11).over(w2))
        ).cast("long"),
    )
# Second pass: aggregate the flags
stuck_exprs = [F.sum(F.col(f"{c}_stuck")).alias(c) for c in VCOLS]
stuck = df_1h.agg(*stuck_exprs).first().asDict()
print("stuck>=12-run (1h):", stuck)

# COMMAND ----------

# DBTITLE 1,Value sample + hourly window + diurnal profile
value_pdf = (
    df.select(*[F.col(c).cast("double").alias(c) for c in VCOLS])
    .sample(0.1, seed=42)
    .limit(150_000)
    .toPandas()
)
ts_pdf = (
    df.where(F.col("frequency") == "1h")
    .select("datetime_utc", *[F.col(c).cast("double").alias(c) for c in VCOLS])
    .orderBy("datetime_utc")
    .limit(3000)
    .toPandas()
)
hourly = (
    df.where(F.col("frequency") == "1h")
    .groupBy(F.hour(F.to_timestamp("datetime_utc")).alias("hod"))
    .agg(*[F.avg(F.col(c).cast("double")).alias(c) for c in VCOLS])
    .orderBy("hod")
    .collect()
)
print("value sample rows:", len(value_pdf))

# COMMAND ----------

# DBTITLE 1,Figure -- frequency overview (rows / coverage % / longest gap)
facet_bars(
    {
        "rows per frequency": freq_rows,
        "coverage % by frequency": [(r[0], r[2]) for r in continuity],
        "longest gap (steps) by frequency": [(r[0], r[4]) for r in continuity],
    },
    "Honda weather -- frequency overview",
    "honda_weather_frequency.png",
    rot=0,
)

# COMMAND ----------

# DBTITLE 1,Figure -- value distributions, hourly window, diurnal profile
facet_hists(
    {c: value_pdf[c].dropna().tolist() for c in VCOLS},
    "Honda weather -- value distribution per column (sampled)",
    "honda_weather_value_distributions.png",
)
if not ts_pdf.empty:
    fig, axes = plt.subplots(len(VCOLS), 1, figsize=(12, 3 * len(VCOLS)), squeeze=False)
    for i, c in enumerate(VCOLS):
        axes[i][0].plot(range(len(ts_pdf)), ts_pdf[c], linewidth=0.8)
        axes[i][0].set_title(f"Honda weather -- {c} (first 3000 hourly points)")
    plt.tight_layout()
    fig.savefig(
        fig_path("honda_weather_hourly_window.png"), dpi=110, bbox_inches="tight"
    )
    plt.show()


def _diurnal_draw(c):
    def draw(ax):
        ax.plot([h["hod"] for h in hourly], [h[c] for h in hourly], marker="o")
        ax.set_xlabel("hour", fontsize=7)

    return draw


_facet_grid(
    [(c, _diurnal_draw(c)) for c in VCOLS],
    "Honda weather -- mean value by hour of day",
    "honda_weather_diurnal_profiles.png",
    ncols=2,
)

# COMMAND ----------

# DBTITLE 1,Findings
print("constant columns:", constant_cols)
print("duplicate key composition:", b)
print("coverage % / on-step % per frequency:", [(r[0], r[2], r[3]) for r in continuity])
print(
    "value plausibility (range / out_of_range / zero):",
    {
        c: {
            k[len(c) + 1 :]: S[k]
            for k in S
            if k.startswith(c + "_")
            and ("out_of_range" in k or "zero" in k or "min" in k or "max" in k)
        }
        for c in VCOLS
    },
)
print("stuck runs:", stuck)

# COMMAND ----------

# DBTITLE 1,Export profiling findings -> src/schemas/profiling/honda_iot.md
_prof = ["| column | missing | rate | approx_distinct |", "|---|---|---|---|"]
for c in COLS:
    _prof.append(
        f"| {c} | {S[c + '__m']} | {S[c + '__m'] / total:.4f} | {S[c + '__d']} |"
    )
_prof.append("")
_prof.append(f"Rows: {total}. Constant columns: {constant_cols or 'none'}.")

_dist = [
    "| column | min | p01 | p50 | p99 | max | mean | sd | zero | non_numeric |",
    "|---|---|---|---|---|---|---|---|---|---|",
]
for c in VCOLS:
    p = S[c + "_p"] or [None, None, None]
    _dist.append(
        f"| {c} | {S[c + '_min']} | {p[0]} | {p[1]} | {p[2]} | {S[c + '_max']} | "
        f"{S[c + '_avg']} | {S[c + '_sd']} | {S[c + '_zero']} | {S[c + '_non_numeric']} |"
    )
_oor = {c: S.get(c + "_out_of_range") for c in VCOLS if c + "_out_of_range" in S}
if _oor:
    _dist += ["", f"Out-of-plausible-range counts (PLAUSIBLE={PLAUSIBLE}): {_oor}"]
_dist += ["", f"Stuck runs (>=12 identical consecutive values, 1h partition): {stuck}"]

_temporal = [
    "| frequency | rows | coverage % | on-step % | longest gap (steps) |",
    "|---|---|---|---|---|",
]
_cont = {r[0]: r for r in continuity}
for f_, n_ in freq_rows:
    r = _cont.get(f_)
    if r:
        _temporal.append(f"| {f_} | {n_} | {r[2]} | {r[3]} | {r[4]} |")
    else:
        _temporal.append(f"| {f_} | {n_} | | | |")
_temporal += ["", f"Top interval sizes (frequency, delta_s, count): {delta_dist[:8]}"]

_dq = [f"Duplicate (frequency, datetime_utc) key composition: {b}."]
if b.get("conflicting", 0) > 0:
    _dq.append(
        "Conflicting duplicate keys exist (same key, differing non-key values) -- "
        "Silver load needs a deterministic de-duplication rule for this table."
    )
elif b.get("dup_groups", 0) > 0:
    _dq.append(
        "Duplicate keys are all fully identical rows -- a plain distinct suffices."
    )
else:
    _dq.append("(frequency, datetime_utc) is unique in Bronze.")
_nn = {c: S[c + "_non_numeric"] for c in VCOLS if S[c + "_non_numeric"]}
if _nn:
    _dq.append(f"Non-numeric values in numeric columns: {_nn}.")

_findings = []
for f_, obs, cov, onstep, gap in continuity:
    if cov is not None and cov < 99.0:
        _findings.append(f"- {f_}: ~{cov}% temporal coverage, longest gap {gap} steps.")
    if onstep is not None and onstep < 99.0:
        _findings.append(f"- {f_}: only {onstep}% of intervals are exactly one step.")
if constant_cols:
    _findings.append(
        f"- Constant/near-constant columns carry no signal: {constant_cols}."
    )
if _oor and any(_oor.values()):
    _findings.append(f"- Out-of-range physical values present: {_oor}.")
if any(stuck.values()):
    _findings.append(f"- Sensor stuck-runs detected (1h): {stuck}.")
_findings_md = (
    "\n".join(_findings) if _findings else "No material data-quality issues found."
)

_silver = []
if b.get("conflicting", 0) > 0:
    _silver.append(
        "- De-duplicate (frequency, datetime_utc) with a deterministic rule before Silver."
    )
if constant_cols:
    _silver.append(f"- Constant columns {constant_cols} can be dropped from Silver.")
if _nn:
    _silver.append(
        f"- Cast/validate numeric columns; quarantine non-numeric values ({_nn})."
    )
if _oor and any(_oor.values()):
    _silver.append("- Apply plausibility bounds / flag out-of-range weather readings.")
_silver_md = "\n".join(_silver) if _silver else ""

write_profiling(
    SOURCE,
    NB_KEY,
    SECTION_TITLE,
    [
        ("Profile", "\n".join(_prof)),
        ("Data Quality", "\n".join(_dq)),
        ("Temporal", "\n".join(_temporal)),
        ("Distributions", "\n".join(_dist)),
        ("EDA Findings", _findings_md),
        ("Silver Implications", _silver_md),
    ],
    figures=[
        ("Honda weather -- frequency overview", "honda_weather_frequency.png"),
        (
            "Honda weather -- value distribution per column",
            "honda_weather_value_distributions.png",
        ),
        (
            "Honda weather -- first 3000 hourly points per column",
            "honda_weather_hourly_window.png",
        ),
        (
            "Honda weather -- mean value by hour of day",
            "honda_weather_diurnal_profiles.png",
        ),
    ],
)
