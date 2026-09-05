# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # EDA -- REDISPATCH MEASURES
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** Profile the redispatch_measures Bronze table -- schema,
# MAGIC missingness, constant columns, full-row duplicates, low-cardinality
# MAGIC categorical distributions (reason, fuel type, grid operator),
# MAGIC temporal activity of measure start/end events, and numeric-looking
# MAGIC value-column distributions -- as evidence for Silver design.

# COMMAND ----------

# DBTITLE 1,Imports
import contextlib
import os as _os
import re as _re

import matplotlib.pyplot as plt
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Configuration
CATALOG = "energy_commerce_retail_media"
BRONZE_SCHEMA = "bronze"
SOURCE = "redispatch"
NB_KEY = "01_redispatch"
SECTION_TITLE = "Redispatch measures (redispatch_measures)"
TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.redispatch_measures"
# Any column containing one of these substrings is treated as an event
# date/timestamp for the temporal-activity check (dynamic, since the exact
# German field name is not assumed ahead of a live-schema read).
DATE_COL_HINTS = ("datum", "beginn", "ende", "date", "start", "end", "zeit")

# COMMAND ----------

# DBTITLE 1,Helpers


def date_like_cols(cols):
    return [c for c in cols if any(h in c.lower() for h in DATE_COL_HINTS)]


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
miss = {c: r[c + "__m"] for c in COLS}
constant_cols = [c for c in COLS if acd[c] <= 1]
print(f"rows={total}  columns={len(COLS)}  ->  {COLS}")
for c in COLS:
    rate = miss[c] / total if total else 0
    print(f"  {c:<40} missing={miss[c]:>10} rate={rate:.4f} approx_distinct={acd[c]}")
print("constant columns:", constant_cols)
distinct_rows = df.distinct().count()
print("exact full-row duplicates:", total - distinct_rows)

# COMMAND ----------

# DBTITLE 1,Low-cardinality categorical distributions (one groupBy per flagged column)
cat_cols = [c for c in COLS if 1 < acd[c] <= 50]
categorical_dist = {}
for c in cat_cols:
    vc = df.groupBy(c).count().orderBy(F.desc("count")).limit(50).collect()
    categorical_dist[c] = [(x[c], x["count"]) for x in vc]
print("categorical columns:", cat_cols)
for c, pairs in categorical_dist.items():
    print(f"  {c}: {pairs}")

# COMMAND ----------

# DBTITLE 1,Numeric-looking value-column distributions (one agg, comma-decimal aware)
num_candidates = [
    c
    for c in COLS
    if c not in cat_cols
    and acd[c] > 50
    and not any(h in c.lower() for h in DATE_COL_HINTS)
]
numeric_stats = {}
if num_candidates:
    exprs = []
    for c in num_candidates:
        v = F.when(
            F.col(c).rlike(r"^-?\d+([.,]\d+)?$"),
            F.regexp_replace(F.col(c), ",", ".").cast("double"),
        )
        exprs += [
            F.min(v).alias(c + "_min"),
            F.max(v).alias(c + "_max"),
            F.avg(v).alias(c + "_mean"),
            F.stddev(v).alias(c + "_sd"),
            F.sum(v.isNotNull().cast("long")).alias(c + "_parsed"),
        ]
    numeric_stats = df.agg(*exprs).first().asDict()
print("numeric-looking columns profiled:", num_candidates)
for c in num_candidates:
    print(
        f"  {c}: min={numeric_stats.get(c + '_min')} max={numeric_stats.get(c + '_max')} "
        f"mean={numeric_stats.get(c + '_mean')} sd={numeric_stats.get(c + '_sd')} "
        f"parsed_rows={numeric_stats.get(c + '_parsed')}"
    )

# COMMAND ----------

# DBTITLE 1,Temporal activity -- rows per year per detected date-like column (one groupBy per column)
dcols = date_like_cols(COLS)
temporal = {}
for c in dcols:
    parsed = F.coalesce(
        F.try_to_date(F.col(c).cast("string"), F.lit("dd.MM.yyyy")),
        F.try_to_date(F.col(c).cast("string"), F.lit("yyyyMMdd")),
        F.try_to_date(F.col(c).cast("string")),
    )
    g = (
        df.select(F.year(parsed).alias("yr"))
        .where(F.col("yr").isNotNull())
        .groupBy("yr")
        .count()
        .orderBy("yr")
        .collect()
    )
    temporal[c] = [(x["yr"], x["count"]) for x in g]
print("date-like columns:", dcols)
for c, pairs in temporal.items():
    print(f"  {c}: {pairs}")

# COMMAND ----------

# DBTITLE 1,Figure -- profile overview
facet_bars(
    {
        "rows per categorical value (first flagged column)": (
            next(iter(categorical_dist.values())) if categorical_dist else []
        ),
        "rows per year (first date-like column)": (
            next(iter(temporal.values())) if temporal else []
        ),
    },
    "Redispatch measures -- overview",
    "redispatch_overview.png",
    rot=45,
    ncols=2,
)

# COMMAND ----------

# DBTITLE 1,Findings
print("constant columns:", constant_cols)
print("exact duplicates:", total - distinct_rows)
print("categorical columns:", cat_cols)
print("numeric-looking columns:", num_candidates)
print("date-like columns:", dcols)

# COMMAND ----------

# DBTITLE 1,Export profiling findings -> src/schemas/profiling/redispatch.md
_prof = ["| column | missing | rate | approx_distinct |", "|---|---|---|---|"]
for c in COLS:
    rate = miss[c] / total if total else 0
    _prof.append(f"| {c} | {miss[c]} | {rate:.4f} | {acd[c]} |")
_prof += [
    "",
    f"Rows: {total}. Constant columns: {constant_cols or 'none'}.",
]

_dq = [f"Exact full-row duplicates: {total - distinct_rows}."]

_temporal = ["Rows per year, by detected date-like column:"]
for c, pairs in temporal.items():
    _temporal.append(f"- `{c}`: {pairs}")

_dist = []
for c, pairs in categorical_dist.items():
    _dist.append(f"- `{c}`: " + fmt_pairs(pairs, n=15))
for c in num_candidates:
    _dist.append(
        f"- `{c}`: min={numeric_stats.get(c + '_min')}, max={numeric_stats.get(c + '_max')}, "
        f"mean={numeric_stats.get(c + '_mean')}, sd={numeric_stats.get(c + '_sd')}, "
        f"parsed_rows={numeric_stats.get(c + '_parsed')} of {total}"
    )

_findings = []
if constant_cols:
    _findings.append(f"- Constant columns: {constant_cols}.")
if total - distinct_rows:
    _findings.append(f"- {total - distinct_rows} exact duplicate rows.")
_unparsed = {
    c: total - numeric_stats.get(c + "_parsed", total)
    for c in num_candidates
    if numeric_stats.get(c + "_parsed", total) < total
}
if _unparsed:
    _findings.append(
        f"- Numeric-looking columns with unparsed values (non-numeric text present): {_unparsed}."
    )
_findings_md = (
    "\n".join(_findings) if _findings else "No material data-quality issues found."
)

_silver = []
if constant_cols:
    _silver.append(f"- Drop constant columns {constant_cols} from Silver.")
if total - distinct_rows:
    _silver.append("- Apply distinct on load to drop exact duplicate rows.")
if _unparsed:
    _silver.append(
        "- Cast numeric-looking columns to double with an explicit decimal-separator rule; "
        "quarantine values that fail to parse."
    )
_silver.append(
    "- redispatch_measures is one row per redispatch event/measure -> Silver grain is the "
    "event itself; join to power_plant_list / MaStR generation units via whichever plant/unit "
    "identifier column is present, confirmed against those sources' key columns."
)

_ml_readiness = [
    (
        "Candidate target signals: a redispatch `reason`/cause categorical column (if present among "
        f"{cat_cols}) is a plausible classification target; the numeric-looking measure-volume/"
        f"duration columns ({num_candidates}) are candidate regression targets for a redispatch-"
        "volume-forecasting use case."
    ),
    (
        "Leakage: the detected date-like columns "
        f"({dcols}) likely include both a measure start and end time -- using the END time, the "
        "realised duration, or the realised volume to predict WHETHER/WHEN a redispatch event "
        "starts is leakage; a forecasting model may only use information available at or before "
        "the measure's start time."
    ),
    (
        "Grain and entity-grouped split: grain is one row per redispatch event/measure, with no "
        "explicit entity id in this table beyond a grid-operator/plant identifier -- split by that "
        "identifier (once confirmed against power_plant_list/MaStR) or by contiguous date range, not "
        "by row, since events from the same operator/plant are temporally correlated."
    ),
    (
        "Join cardinality: the join to power_plant_list / MaStR generation units via a plant/unit "
        "identifier column is NOT confirmed in this notebook (flagged as an open item in Silver "
        "Implications) -- verify 1:1 vs 1:N before using plant attributes as joined features, since "
        "a plant with multiple redispatch events joined 1:N against a single plant-attribute row is "
        "expected and safe, but the reverse (multiple plant rows matching one event) would be a "
        "cartesian-explosion risk."
    ),
    (
        "Imbalance: redispatch `reason`/cause and grid-operator categorical columns (Distributions) "
        "are likely dominated by one or two common causes -- check the actual distribution before "
        "using reason as a classification target, since a naive model could win by always predicting "
        "the majority cause."
    ),
    (
        "Sample-vs-full divergence: not applicable -- every statistic here is computed from a full "
        "Spark aggregation or `.distinct().count()`, no `.sample()`/`.limit()` subset feeds any "
        "reported number."
    ),
]
if total - distinct_rows:
    _ml_readiness.append(
        f"{total - distinct_rows} exact full-row duplicates exist (see Data Quality) -- "
        "de-duplicate before counting redispatch events as independent observations."
    )

write_profiling(
    SOURCE,
    NB_KEY,
    SECTION_TITLE,
    blocks=[
        ("Profile", "\n".join(_prof)),
        ("Data Quality", "\n".join(_dq)),
        ("Temporal", "\n".join(_temporal)),
        ("Distributions", "\n".join(_dist)),
        ("EDA Findings", _findings_md),
        ("ML-Readiness Evidence", "\n".join(f"- {ln}" for ln in _ml_readiness)),
        ("Silver Implications", "\n".join(_silver)),
    ],
    figures=[
        ("Redispatch measures -- overview", "redispatch_overview.png"),
    ],
)
