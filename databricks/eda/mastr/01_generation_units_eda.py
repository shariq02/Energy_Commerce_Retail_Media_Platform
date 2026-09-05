# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # EDA -- MASTR GENERATION UNITS
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** Profile the six MaStR generation-unit Bronze tables
# MAGIC (einheiten_wind, einheiten_biomasse, einheiten_wasser,
# MAGIC einheiten_verbrennung, einheiten_kernkraft,
# MAGIC einheiten_geothermie_gsgk) -- schema, missingness, constant columns,
# MAGIC MastrNummer key cardinality, full-row duplicates, low-cardinality
# MAGIC categorical distributions -- as evidence for Silver design.

# COMMAND ----------

# DBTITLE 1,Imports
import contextlib
import os as _os
import re as _re

import matplotlib.pyplot as plt
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Configuration
CATALOG = "energy_commerce_retail_media"
BRONZE_SCHEMA = "bronze"
SOURCE = "mastr"
NB_KEY = "01_generation_units"
SECTION_TITLE = "Generation units (einheiten_wind ... einheiten_geothermie_gsgk)"
DATASETS = [
    "einheiten_wind",
    "einheiten_biomasse",
    "einheiten_wasser",
    "einheiten_verbrennung",
    "einheiten_kernkraft",
    "einheiten_geothermie_gsgk",
]
TABLES = {d: f"{CATALOG}.{BRONZE_SCHEMA}.mastr_{d}" for d in DATASETS}

# COMMAND ----------

# DBTITLE 1,Helpers


def find_col(df: DataFrame, *cands: str) -> str | None:
    low = {c.lower(): c for c in df.columns}
    for x in cands:
        if x.lower() in low:
            return low[x.lower()]
    return None


def key_like_cols(cols, suffix="mastrnummer"):
    return [c for c in cols if c.lower().endswith(suffix)]


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

# DBTITLE 1,Profile each table -- rows, missingness, approx distinct, constant columns (one agg per table)
frames = {d: spark.table(t) for d, t in TABLES.items()}
prof = {}
for name, df in frames.items():
    cols = df.columns
    exprs = [F.count(F.lit(1)).alias("__rows")]
    for c in cols:
        miss = F.col(c).isNull() | (F.trim(F.col(c).cast("string")) == "")
        exprs += [
            F.sum(miss.cast("long")).alias(c + "__m"),
            F.approx_count_distinct(c).alias(c + "__d"),
        ]
    r = df.agg(*exprs).first().asDict()
    total = r["__rows"]
    acd = {c: r[c + "__d"] for c in cols}
    miss = {c: r[c + "__m"] for c in cols}
    constant = [c for c in cols if acd[c] <= 1]
    prof[name] = {
        "cols": cols,
        "total": total,
        "acd": acd,
        "miss": miss,
        "constant": constant,
    }
    print("=" * 90, f"\n{name}  rows={total}  cols={len(cols)}  -> {cols}")
    for c in cols:
        rate = miss[c] / total if total else 0
        print(
            f"  {c:<40} missing={miss[c]:>10} rate={rate:.4f} approx_distinct={acd[c]}"
        )
    print("constant columns:", constant)

# COMMAND ----------

# DBTITLE 1,Key columns (*MastrNummer) -- cardinality + likely primary key (reuses profile)
key_report = {}
for name in DATASETS:
    total = prof[name]["total"]
    picks = []
    for k in key_like_cols(prof[name]["cols"]):
        ratio = prof[name]["acd"][k] / total if total else 0
        picks.append((k, prof[name]["acd"][k], round(ratio, 4)))
    key_report[name] = picks
    print(f"{name}: {picks}")

# COMMAND ----------

# DBTITLE 1,Full-row duplicates per table
dup_counts = {}
for name, df in frames.items():
    total = prof[name]["total"]
    distinct_rows = df.distinct().count()
    dup_counts[name] = total - distinct_rows
    print(f"{name}: exact full-row duplicates = {dup_counts[name]}")

# COMMAND ----------

# DBTITLE 1,Low-cardinality (categorical) column value counts (one groupBy per flagged column)
categorical_dist = {}
for name, df in frames.items():
    cat_cols = [c for c in prof[name]["cols"] if 1 < prof[name]["acd"][c] <= 50]
    dists = {}
    for c in cat_cols:
        vc = df.groupBy(c).count().orderBy(F.desc("count")).limit(50).collect()
        dists[c] = [(x[c], x["count"]) for x in vc]
    categorical_dist[name] = dists
    print(f"{name} categorical columns: {cat_cols}")

# COMMAND ----------

# DBTITLE 1,Figure -- rows per table, key cardinality ratio, duplicates
facet_bars(
    {
        "rows per table": [(d, prof[d]["total"]) for d in DATASETS],
        "columns per table": [(d, len(prof[d]["cols"])) for d in DATASETS],
        "full-row duplicates": [(d, dup_counts[d]) for d in DATASETS],
        "primary-key uniqueness ratio (best MastrNummer col)": [
            (d, max((p[2] for p in key_report[d]), default=0)) for d in DATASETS
        ],
    },
    "MaStR generation units -- overview",
    "mastr_generation_units_overview.png",
    rot=30,
    ncols=2,
)

# COMMAND ----------

# DBTITLE 1,Figure -- top categorical distributions (faceted, one panel per table's first flagged column)
facet_bars(
    {
        d: (next(iter(categorical_dist[d].values())) if categorical_dist[d] else [])
        for d in DATASETS
    },
    "MaStR generation units -- first categorical column per table",
    "mastr_generation_units_categorical.png",
    rot=45,
    ncols=3,
)

# COMMAND ----------

# DBTITLE 1,Findings
findings_lines = []
for d in DATASETS:
    findings_lines.append(
        f"{d}: rows={prof[d]['total']}, cols={len(prof[d]['cols'])}, "
        f"constant={prof[d]['constant']}, duplicates={dup_counts[d]}, "
        f"key candidates={key_report[d]}"
    )
print("\n".join(findings_lines))

# COMMAND ----------

# DBTITLE 1,Export profiling findings -> src/schemas/profiling/mastr.md
_profile = ["| table | rows | cols | constant columns |", "|---|---|---|---|"]
for d in DATASETS:
    _profile.append(
        f"| {d} | {prof[d]['total']} | {len(prof[d]['cols'])} | "
        f"{', '.join(prof[d]['constant']) or '-'} |"
    )

_dq = ["Full-row exact duplicates per table:"]
for d in DATASETS:
    _dq.append(f"- {d}: {dup_counts[d]}")

_entities = [
    "MastrNummer-suffixed key column cardinality (column, approx_distinct, ratio-to-rows):"
]
for d in DATASETS:
    _entities.append(f"- {d}: {key_report[d]}")

_dist = []
for d in DATASETS:
    for c, pairs in categorical_dist[d].items():
        _dist.append(f"- {d}.`{c}`: " + fmt_pairs(pairs, n=15))

_findings_md = "\n".join(f"- {ln}" for ln in findings_lines)

_silver = [
    "- Each table's near-unique `*MastrNummer` column (ratio close to 1.0 above) is the natural Bronze->Silver grain key.",
    "- Constant columns above carry no information and can be dropped at Silver.",
]
if any(dup_counts.values()):
    _silver.append(
        "- Exact duplicate rows exist in at least one table -> de-duplicate on load."
    )
_silver.append(
    "- Generation-unit tables are one physical unit per row; a unit can appear in more than one energy-carrier table only if it changed carrier type -> reconcile at Gold, not assumed absent."
)

_ml_readiness = [
    (
        "No candidate ML target lives in these master-data tables directly -- each row describes a "
        "generation unit's static/slowly-changing attributes; a decommission/status-change target "
        "would come from the change-history tables (04_change_history_eda.py), not from here."
    ),
    (
        "Grain and entity-grouped split: the near-unique `*MastrNummer` column per table (ratio "
        "close to 1.0 in key_report above) is the entity id -- split any model using generation-"
        "unit attributes by MastrNummer, not by row, since a unit's attributes are static within "
        "this table and a row-level split provides no independence benefit while masking leakage "
        "from duplicate rows."
    ),
    (
        "Join cardinality: whether the same physical unit ever appears in more than one carrier "
        "table (a carrier-type change) is NOT assessed in this notebook -- see "
        "06_mastr_relationships_and_findings.py for the confirmed cross-table key overlap before "
        "assuming these 6 tables are mutually exclusive partitions of one unit population."
    ),
    (
        "Leakage: not applicable in this notebook's scope -- these are current-state attribute tables "
        "with no temporal ordering to leak across; see 04_change_history_eda.py for time-ordered "
        "leakage risks."
    ),
    (
        "Imbalance: the low-cardinality categorical columns profiled above (Distributions) may be "
        "skewed toward one dominant category per table -- check the specific column's distribution "
        "before using it as a stratification or target variable."
    ),
    (
        "Sample-vs-full divergence: not applicable -- every statistic here (profile, key cardinality, "
        "duplicate counts, categorical distributions) is computed from a full Spark aggregation or "
        "`.distinct().count()`, no `.sample()`/`.limit()` subset feeds any reported number."
    ),
]
if any(dup_counts.values()):
    _ml_readiness.append(
        "Exact full-row duplicates exist in at least one table (see Data Quality) -- de-duplicate "
        "before treating MastrNummer as a unique entity key for a split."
    )

write_profiling(
    SOURCE,
    NB_KEY,
    SECTION_TITLE,
    blocks=[
        ("Profile", "\n".join(_profile)),
        ("Data Quality", "\n".join(_dq)),
        ("Entities / Keys", "\n".join(_entities)),
        ("Distributions", "\n".join(_dist)),
        ("EDA Findings", _findings_md),
        ("ML-Readiness Evidence", "\n".join(f"- {ln}" for ln in _ml_readiness)),
        ("Silver Implications", "\n".join(_silver)),
    ],
    figures=[
        (
            "MaStR generation units -- overview",
            "mastr_generation_units_overview.png",
        ),
        (
            "MaStR generation units -- first categorical column per table",
            "mastr_generation_units_categorical.png",
        ),
    ],
)