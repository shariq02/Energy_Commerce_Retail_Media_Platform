# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # EDA -- POWER PLANT LIST (BNETZA KRAFTWERKSLISTE)
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** Profile the two power plant list Bronze tables
# MAGIC (power_plant_list, power_plant_capacity_additions) -- schema,
# MAGIC missingness, constant columns, plant-identifier key cardinality,
# MAGIC full-row duplicates, low-cardinality categorical distributions
# MAGIC (fuel type, state, status), capacity-value distributions, and
# MAGIC cross-table plant-identifier coverage -- as evidence for Silver
# MAGIC design.

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
SOURCE = "power_plant_list"
NB_KEY = "01_power_plant_list"
SECTION_TITLE = "Power plant list (power_plant_list, power_plant_capacity_additions)"
DATASETS = ["power_plant_list", "power_plant_capacity_additions"]
TABLES = {d: f"{CATALOG}.{BRONZE_SCHEMA}.{d}" for d in DATASETS}
# Any column containing one of these substrings is treated as a plant/block
# identifier for the cross-table coverage check (dynamic, since the exact
# BNetzA German field name is not assumed ahead of a live-schema read).
ID_COL_HINTS = ("kraftwerksnummer", "blocknummer", "anlagenkennziffer", "mastrnummer")

# COMMAND ----------

# DBTITLE 1,Helpers


def find_col(df: DataFrame, *cands: str) -> str | None:
    low = {c.lower(): c for c in df.columns}
    for x in cands:
        if x.lower() in low:
            return low[x.lower()]
    return None


def id_like_cols(cols):
    return [c for c in cols if any(h in c.lower() for h in ID_COL_HINTS)]


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

# DBTITLE 1,Full-row duplicates + plant-identifier key candidates per table
dup_counts = {}
id_key_report = {}
for name, df in frames.items():
    total = prof[name]["total"]
    distinct_rows = df.distinct().count()
    dup_counts[name] = total - distinct_rows
    picks = []
    for k in id_like_cols(prof[name]["cols"]):
        ratio = prof[name]["acd"][k] / total if total else 0
        picks.append((k, prof[name]["acd"][k], round(ratio, 4)))
    id_key_report[name] = picks
    print(f"{name}: duplicates={dup_counts[name]}  id-like columns={picks}")

# COMMAND ----------

# DBTITLE 1,Low-cardinality categorical + numeric-looking value-column distributions (one pass per table)
categorical_dist = {}
numeric_stats = {}
for name, df in frames.items():
    cols = prof[name]["cols"]
    cat_cols = [c for c in cols if 1 < prof[name]["acd"][c] <= 50]
    dists = {}
    for c in cat_cols:
        vc = df.groupBy(c).count().orderBy(F.desc("count")).limit(50).collect()
        dists[c] = [(x[c], x["count"]) for x in vc]
    categorical_dist[name] = dists

    num_candidates = [
        c
        for c in cols
        if c not in cat_cols
        and c not in id_like_cols(cols)
        and prof[name]["acd"][c] > 50
    ]
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
            F.sum(v.isNotNull().cast("long")).alias(c + "_parsed"),
        ]
    numeric_stats[name] = df.agg(*exprs).first().asDict() if exprs else {}
    print(f"{name} categorical columns: {cat_cols}")
    print(f"{name} numeric-looking columns profiled: {num_candidates}")

# COMMAND ----------

# DBTITLE 1,Figure -- overview + top categorical distribution per table
facet_bars(
    {
        "rows per table": [(d, prof[d]["total"]) for d in DATASETS],
        "columns per table": [(d, len(prof[d]["cols"])) for d in DATASETS],
        "full-row duplicates": [(d, dup_counts[d]) for d in DATASETS],
    },
    "Power plant list -- overview",
    "power_plant_list_overview.png",
    rot=30,
    ncols=2,
)
facet_bars(
    {
        d: (next(iter(categorical_dist[d].values())) if categorical_dist[d] else [])
        for d in DATASETS
    },
    "Power plant list -- first categorical column per table",
    "power_plant_list_categorical.png",
    rot=45,
    ncols=2,
)

# COMMAND ----------

# DBTITLE 1,Findings
findings_lines = []
for d in DATASETS:
    findings_lines.append(
        f"{d}: rows={prof[d]['total']}, cols={len(prof[d]['cols'])}, "
        f"constant={prof[d]['constant']}, duplicates={dup_counts[d]}, "
        f"id-like key candidates={id_key_report[d]}"
    )
print("\n".join(findings_lines))

# COMMAND ----------

# DBTITLE 1,Export profiling findings -> src/schemas/profiling/power_plant_list.md
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
    "Plant/block-identifier-like key column cardinality (column, approx_distinct, ratio-to-rows):"
]
for d in DATASETS:
    _entities.append(f"- {d}: {id_key_report[d]}")

_dist = []
for d in DATASETS:
    for c, pairs in categorical_dist[d].items():
        _dist.append(f"- {d}.`{c}`: " + fmt_pairs(pairs, n=15))
    ns = numeric_stats[d]
    for k in ns:
        if k.endswith("_min"):
            base = k[: -len("_min")]
            _dist.append(
                f"- {d}.`{base}`: min={ns.get(base + '_min')}, max={ns.get(base + '_max')}, "
                f"mean={ns.get(base + '_mean')}, parsed_rows={ns.get(base + '_parsed')}"
            )

_findings_md = "\n".join(f"- {ln}" for ln in findings_lines)

_silver = [
    "- Constant columns above carry no information and can be dropped at Silver.",
]
if any(dup_counts.values()):
    _silver.append(
        "- Exact duplicate rows exist in at least one table -> de-duplicate on load."
    )
_silver.append(
    "- power_plant_capacity_additions records capacity change events against a plant/block "
    "identifier -> model as a Silver change fact joined to power_plant_list, not merged into "
    "current-state plant attributes."
)
_silver.append(
    "- Numeric-looking value columns above were parsed heuristically (comma-as-decimal handled); "
    "confirm actual units and decimal convention against the source layout before Silver casting."
)

_ml_readiness = [
    (
        "Candidate target signals: `power_plant_capacity_additions` is a capacity-change event log "
        "-- a natural target for a capacity-growth/plant-retirement-prediction use case; fuel-type/"
        "state/status categorical columns in `power_plant_list` (Distributions) are candidate "
        "classification targets."
    ),
    (
        "Leakage: capacity-addition rows are change EVENTS -- using an addition dated after a "
        "prediction cutoff to build a feature for that same cutoff's prediction is leakage, "
        "matching the append-only-log caveat found in MaStR's change-history tables; only additions "
        "strictly before the prediction point may be used as features."
    ),
    (
        "Grain and entity-grouped split: the plant/block-identifier-like column(s) in `id_key_report` "
        "above are the entity id -- split by that identifier, not by row, since a plant can have "
        "multiple capacity-addition events over time."
    ),
    (
        "Join cardinality: power_plant_list <-> power_plant_capacity_additions is NOT confirmed "
        "1:1 or 1:N in this notebook -- a plant with multiple addition events would make this 1:N, "
        "and joining as if 1:1 either loses history or duplicates the plant's static attributes per "
        "event; verify the join cardinality before combining the two tables as features."
    ),
    (
        "Imbalance: the low-cardinality categorical columns profiled above (Distributions) may be "
        "skewed toward one dominant category -- check before using fuel type/state/status as a "
        "stratification or target variable."
    ),
    (
        "Sample-vs-full divergence: not applicable -- every statistic here is computed from a full "
        "Spark aggregation or `.distinct().count()`, no `.sample()`/`.limit()` subset feeds any "
        "reported number; note separately that the numeric-looking value columns were parsed "
        "heuristically (comma-as-decimal) -- an unparsed value becomes NULL, not a wrong number, "
        "but any regression target built from these columns should check `_parsed` counts against "
        "total rows first."
    ),
]
if any(dup_counts.values()):
    _ml_readiness.append(
        "Exact full-row duplicates exist in at least one table (see Data Quality) -- de-duplicate "
        "before treating the plant identifier as a unique entity key for a split."
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
        ("Power plant list -- overview", "power_plant_list_overview.png"),
        (
            "Power plant list -- first categorical column per table",
            "power_plant_list_categorical.png",
        ),
    ],
)
