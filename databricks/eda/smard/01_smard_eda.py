# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # EDA -- SMARD ENERGY TIME SERIES
# MAGIC
# MAGIC **Energy Commerce and Retail Media Analytics Platform**   
# MAGIC **Author:** Sharique Mohammad    
# MAGIC **Date:** August 2026
# MAGIC
# MAGIC **Purpose:** Profile smard_energy_timeseries (one long-format Bronze
# MAGIC table: metric / filter_id / region / resolution / timestamp_utc /
# MAGIC value) -- schema, missingness, constant columns, metric & region
# MAGIC cardinality, metric x region x resolution coverage, per-series temporal
# MAGIC continuity (expected vs actual points, gaps), temporal activity, value
# MAGIC ranges & distributions & suspicious values, per-series duplicates
# MAGIC (identical vs conflicting) -- as evidence for Silver design.

# COMMAND ----------

# DBTITLE 1,Imports
import matplotlib.pyplot as plt
import numpy as np
from pyspark.sql import Window
from pyspark.sql import functions as F

import contextlib
import os as _os
import re as _re

# COMMAND ----------

# DBTITLE 1,Configuration
CATALOG = "energy_commerce_retail_media"
BRONZE_SCHEMA = "bronze"
SOURCE = "smard"
NB_KEY = "01_smard"
SECTION_TITLE = "SMARD energy time series (smard_energy_timeseries)"
TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.smard_energy_timeseries"
SERIES_KEY = ["metric", "filter_id", "region", "resolution"]

# Nominal seconds between points per SMARD resolution label (month/year are
# variable-length and skipped from the expected-vs-actual check).
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

# DBTITLE 1,Helpers
def as_ts(col: str):
    c = F.col(col).cast("string")
    return F.coalesce(
        F.to_timestamp(c),
        (c.cast("double") / 1000).cast("timestamp"),
        c.cast("double").cast("timestamp"),
    )


def step_col():
    e = F.lit(None).cast("long")
    for label, secs in RESOLUTION_SECONDS.items():
        e = F.when(F.lower(F.col("resolution")) == label, F.lit(secs)).otherwise(e)
    return e


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


def write_profiling(source, notebook_key, section_title, blocks, figures=None):
    d = _profiling_dir()
    md = _os.path.join(d, source + ".md")
    lines = [f"<!-- BEGIN {source}:{notebook_key} -->", f"## {section_title}", ""]
    for heading, body in blocks:
        if body is None or str(body).strip() == "":
            continue
        lines += [f"### {heading}", "", str(body).rstrip(), ""]
    for cap, name in figures or []:
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

# DBTITLE 1,Profile -- rows, missingness, approx distinct, constant columns (one agg)
df = spark.table(TABLE)
COLS = df.columns
exprs = [F.count(F.lit(1)).alias("__rows")]
for c in COLS:
    miss = F.col(c).isNull() | (F.trim(F.col(c)) == "")
    exprs += [
        F.sum(miss.cast("long")).alias(c + "__m"),
        F.approx_count_distinct(c).alias(c + "__d"),
    ]
r = df.agg(*exprs).first().asDict()
total = r["__rows"]
acd = {c: r[c + "__d"] for c in COLS}
constant_cols = [c for c in COLS if acd[c] <= 1]
print(f"rows={total}  columns={len(COLS)}  ->  {COLS}")
for c in COLS:
    print(
        f"  {c:<14} missing={r[c + '__m']:>12} rate={r[c + '__m'] / total:.4f} approx_distinct={acd[c]}"
    )
print("constant columns:", constant_cols)
distinct_rows = df.distinct().count()
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
print("absent combos:", missing_combos[:50])
print("regions per metric:", metric_regions)
print("resolutions per metric:", metric_res)

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
print(f"distinct series = {len(series)}")
print(
    "(series, ts) dup groups:",
    db["dup_groups"],
    " conflicting values:",
    db["conflicting"],
)

# COMMAND ----------

# DBTITLE 1,Value stats + filter_id cardinality per metric (one groupBy)
v = F.col("value").cast("double")
bm = (
    df.groupBy("metric")
    .agg(
        F.min(v).alias("min"),
        F.max(v).alias("max"),
        F.avg(v).alias("mean"),
        F.stddev(v).alias("sd"),
        F.expr(
            "percentile_approx(cast(value as double), array(0.01,0.25,0.5,0.75,0.99))"
        ).alias("p01_25_50_75_99"),
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

# DBTITLE 1,5-sigma outliers per metric (one agg using collected mean/sd)
oe = []
for m, x in by_metric.items():
    if x["sd"] and x["sd"] > 0:
        oe.append(
            F.sum(
                ((F.col("metric") == m) & (F.abs(v - x["mean"]) > 5 * x["sd"])).cast(
                    "long"
                )
            ).alias(m)
        )
outliers = df.agg(*oe).first().asDict() if oe else {}
print("rows beyond 5 sigma per metric:", outliers)

# COMMAND ----------

# DBTITLE 1,Per-series temporal continuity -- one windowed pass
w = Window.partitionBy(*SERIES_KEY).orderBy("ts")
sd = (
    df.select(*SERIES_KEY, as_ts("timestamp_utc").alias("ts"), step_col().alias("step"))
    .where(F.col("ts").isNotNull() & F.col("step").isNotNull())
    .distinct()
    .withColumn(
        "gap_steps",
        (F.col("ts").cast("long") - F.lag("ts").over(w).cast("long")) / F.col("step")
        - 1,
    )
    .groupBy(*SERIES_KEY, "step")
    .agg(
        F.min("ts").alias("min_ts"),
        F.max("ts").alias("max_ts"),
        F.count(F.lit(1)).alias("observed"),
        F.max(F.when(F.col("gap_steps") > 0, F.col("gap_steps"))).alias("longest_gap"),
        F.sum(F.when(F.col("gap_steps") > 0, F.col("gap_steps")).otherwise(0)).alias(
            "missing_steps"
        ),
    )
).collect()
continuity = []
for x in sd:
    exp = int((x["max_ts"].timestamp() - x["min_ts"].timestamp()) / x["step"]) + 1
    continuity.append(
        {
            "series": "|".join(str(x[k]) for k in SERIES_KEY),
            "resolution": x["resolution"],
            "observed": x["observed"],
            "expected": exp,
            "coverage_pct": round(x["observed"] / exp * 100, 2) if exp else None,
            "longest_gap": x["longest_gap"],
            "missing_steps": x["missing_steps"],
        }
    )
    print(continuity[-1])

# COMMAND ----------

# DBTITLE 1,Temporal activity -- rows per year and per month (one groupBy)
ym = (
    df.groupBy(
        F.year(as_ts("timestamp_utc")).alias("year"),
        F.date_format(as_ts("timestamp_utc"), "yyyy-MM").alias("month"),
    )
    .count()
    .collect()
)
by_year = {}
by_month = {}
for x in ym:
    by_year[x["year"]] = by_year.get(x["year"], 0) + x["count"]
    by_month[x["month"]] = by_month.get(x["month"], 0) + x["count"]
print("by year:", sorted(by_year.items()))

# COMMAND ----------

# DBTITLE 1,Value sample + per-metric time-series windows
value_pdf = (
    df.select("metric", v.alias("value"))
    .where(v.isNotNull())
    .sample(0.1, seed=42)
    .limit(200_000)
    .toPandas()
)
ts_pdf = {}
for m in metrics:
    ts_pdf[m] = (
        df.where(F.col("metric") == m)
        .select("timestamp_utc", v.alias("value"))
        .orderBy("timestamp_utc")
        .limit(3000)
        .toPandas()
    )
print("value sample rows:", len(value_pdf))

# COMMAND ----------

# DBTITLE 1,Figure -- distributions, series volume, coverage, activity
for c, pairs in dist.items():
    barplot(
        pairs,
        f"SMARD -- rows per {c}",
        c,
        "rows",
        rot=45,
        filename=f"smard_rows_per_{c}.png",
    )
barplot(
    series_rows,
    "SMARD -- rows per series",
    "series",
    "rows",
    rot=90,
    figsize=(12, 5),
    filename="smard_rows_per_series.png",
)
barplot(
    sorted(by_year.items()),
    "SMARD -- rows per year",
    "year",
    "rows",
    rot=45,
    filename="smard_rows_per_year.png",
)
if continuity:
    barplot(
        [
            (c["series"], c["coverage_pct"])
            for c in continuity
            if c["coverage_pct"] is not None
        ],
        "SMARD -- per-series temporal coverage %",
        "series",
        "%",
        rot=90,
        figsize=(12, 5),
        filename="smard_per_series_coverage_pct.png",
    )
    barplot(
        [(c["series"], c["longest_gap"]) for c in continuity if c["longest_gap"]],
        "SMARD -- per-series longest gap (missing steps)",
        "series",
        "steps",
        rot=90,
        figsize=(12, 5),
        filename="smard_per_series_longest_gap.png",
    )

# COMMAND ----------

# DBTITLE 1,Figure -- value distribution and time series per metric
for m in metrics:
    s = value_pdf.loc[value_pdf["metric"] == m, "value"]
    if len(s):
        histplot(
            s.tolist(),
            f"SMARD {m} -- value distribution (n={len(s)} sample)",
            "value",
            filename=f"smard_{m}_value_distribution.png",
        )
    pdf = ts_pdf[m]
    if not pdf.empty:
        plt.figure(figsize=(12, 3))
        plt.plot(range(len(pdf)), pdf["value"], linewidth=0.7)
        plt.title(f"SMARD {m} -- first 3000 points")
        plt.xlabel("time index")
        plt.ylabel("value")
        plt.tight_layout()
        plt.savefig(
            fig_path(f"smard_{m}_time_series.png"), dpi=110, bbox_inches="tight"
        )
        plt.show()

# COMMAND ----------

# DBTITLE 1,Figure -- metric|region x resolution coverage heatmap
labels_mr = [f"{m}|{rg}" for m in metrics for rg in regions]
grid = np.array(
    [
        [1 if (m, rg, rs) in present else 0 for rs in resolutions]
        for m in metrics
        for rg in regions
    ],
    dtype=float,
)
plt.figure(figsize=(max(5, 1.2 * len(resolutions)), max(3, 0.35 * len(labels_mr))))
plt.imshow(grid, aspect="auto", cmap="Greens")
plt.xticks(range(len(resolutions)), resolutions, rotation=45, ha="right")
plt.yticks(range(len(labels_mr)), labels_mr, fontsize=7)
plt.title("SMARD -- metric|region x resolution presence")
plt.tight_layout()
plt.savefig(fig_path("smard_coverage_matrix.png"), dpi=110, bbox_inches="tight")
plt.show()

# COMMAND ----------

# DBTITLE 1,Findings
print("constant columns:", constant_cols)
print(
    "exact duplicates:",
    total - distinct_rows,
    " conflicting (series,ts) keys:",
    db["conflicting"],
)
print(
    "distinct series:",
    len(series),
    " absent (metric,region,resolution) combos:",
    len(missing_combos),
)
print(
    "per-series coverage % (sample):",
    [c["coverage_pct"] for c in continuity if c["coverage_pct"] is not None][:20],
)
print("5-sigma outliers per metric:", outliers)
print(
    "value ranges / zero / negative per metric:",
    {
        m: {k: x[k] for k in ("min", "max", "zero_rows", "negative_rows")}
        for m, x in by_metric.items()
    },
)

# COMMAND ----------

# DBTITLE 1,Export profiling findings -> src/schemas/profiling/smard.md
_prof = ["| column | missing | rate | approx_distinct |", "|---|---|---|---|"]
for c in COLS:
    _prof.append(f"| {c} | {r[c + '__m']} | {r[c + '__m'] / total:.4f} | {acd[c]} |")
_prof += [
    "",
    (
        f"Rows: {total}. Long format, series key = {SERIES_KEY}. "
        f"Constant columns: {constant_cols or 'none'}. "
        f"Distinct series: {len(series)}."
    ),
]

_dq = [
    f"Exact full-row duplicates: {total - distinct_rows}.",
    (
        f"(series, timestamp_utc) duplicate groups: {db['dup_groups']}, "
        f"of which conflicting (differing value): {db['conflicting']}."
    ),
]
if db["conflicting"]:
    _dq.append(
        "Conflicting duplicate series points exist -- Silver needs a deterministic "
        "value-selection rule per (series, timestamp)."
    )
_nn = {m: x["non_numeric"] for m, x in by_metric.items() if x["non_numeric"]}
if _nn:
    _dq.append(f"Non-numeric values in `value` per metric: {_nn}.")

_temporal = [
    "Per-series continuity (fixed-step resolutions only):",
    "",
    "| series | resolution | observed | expected | coverage % | longest gap | missing steps |",
    "|---|---|---|---|---|---|---|",
]
for c in continuity:
    _temporal.append(
        f"| {c['series']} | {c['resolution']} | {c['observed']} | {c['expected']} | "
        f"{c['coverage_pct']} | {c['longest_gap']} | {c['missing_steps']} |"
    )
_temporal += ["", f"Rows per year: {sorted(by_year.items())}."]

_entities = [
    f"Distinct series (metric|filter_id|region|resolution): {len(series)}.",
    f"Metrics ({len(metrics)}): {metrics}",
    f"Regions ({len(regions)}): {regions}",
    f"Resolutions ({len(resolutions)}): {resolutions}",
]

_coverage = [
    (
        f"metric x region x resolution: {len(present)} present, {len(missing_combos)} absent "
        f"of {len(metrics) * len(regions) * len(resolutions)} possible."
    ),
    "",
    f"Regions per metric: {metric_regions}",
    "",
    f"Resolutions per metric: {metric_res}",
    "",
    f"Absent combos (first 30): {missing_combos[:30]}",
]

_dist = [
    "| metric | min | max | mean | sd | p01/25/50/75/99 | zero | negative | 5-sigma outliers |",
    "|---|---|---|---|---|---|---|---|---|",
]
for m, x in by_metric.items():
    _dist.append(
        f"| {m} | {x['min']} | {x['max']} | {x['mean']} | {x['sd']} | "
        f"{x['p01_25_50_75_99']} | {x['zero_rows']} | {x['negative_rows']} | {outliers.get(m)} |"
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
_low_cov = [
    c["series"]
    for c in continuity
    if c["coverage_pct"] is not None and c["coverage_pct"] < 99
]
if _low_cov:
    _findings.append(f"- Series with <99% temporal coverage: {_low_cov[:20]}.")
_neg = {m: x["negative_rows"] for m, x in by_metric.items() if x["negative_rows"]}
if _neg:
    _findings.append(f"- Negative values present per metric: {_neg}.")
if any(outliers.values()):
    _findings.append(
        f"- 5-sigma value outliers per metric: { {m: v for m, v in outliers.items() if v} }."
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
        "- De-duplicate (series key, timestamp_utc) with a deterministic rule."
    )
elif total - distinct_rows:
    _silver.append("- Apply distinct on load to drop exact duplicate rows.")
if _nn:
    _silver.append("- Cast `value` to double; quarantine non-numeric values.")
_silver.append(
    "- Silver grain: one row per (metric, filter_id, region, resolution, timestamp_utc)."
)
if _low_cov:
    _silver.append(
        "- Expect and preserve gaps in series; do not forward-fill without a stated rule."
    )

write_profiling(
    SOURCE,
    NB_KEY,
    SECTION_TITLE,
    [
        ("Profile", "\n".join(_prof)),
        ("Data Quality", "\n".join(_dq)),
        ("Temporal", "\n".join(_temporal)),
        ("Entities / Keys", "\n".join(_entities)),
        ("Coverage", "\n".join(_coverage)),
        ("Distributions", "\n".join(_dist)),
        ("EDA Findings", _findings_md),
        ("Silver Implications", "\n".join(_silver)),
    ],
    figures=[
        ("SMARD metric|region x resolution presence", "smard_coverage_matrix.png"),
        ("SMARD rows per series", "smard_rows_per_series.png"),
        ("SMARD rows per year", "smard_rows_per_year.png"),
        ("SMARD per-series temporal coverage %", "smard_per_series_coverage_pct.png"),
        (
            "SMARD per-series longest gap (missing steps)",
            "smard_per_series_longest_gap.png",
        ),
    ],
)