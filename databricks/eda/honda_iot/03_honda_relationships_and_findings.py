# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # EDA -- HONDA IOT RELATIONSHIPS AND FINDINGS
# MAGIC
# MAGIC **Energy Commerce and Retail Media Analytics Platform**  
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** August 2026
# MAGIC
# MAGIC **Purpose:** Cross-table checks across the 7 Honda IoT Bronze tables --
# MAGIC timestamp-grid alignment per frequency, join cardinality on
# MAGIC (frequency, datetime_utc), pairwise overlap matrix, full 7-way join
# MAGIC yield, and an evidence-based verdict on combining the datasets. All
# MAGIC key-overlap analysis is derived from one tagged union + one presence
# MAGIC matrix rather than repeated pairwise joins.

# COMMAND ----------

# DBTITLE 1,Imports
import contextlib
import os as _os
import re as _re

import matplotlib.pyplot as plt
import numpy as np
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Configuration
CATALOG = "energy_commerce_retail_media"
BRONZE_SCHEMA = "bronze"
SOURCE = "honda_iot"
NB_KEY = "03_relationships_and_findings"
SECTION_TITLE = "Cross-table relationships (7 Honda IoT tables)"
DATASETS = [
    "electricity_p",
    "electricity_w",
    "heating_p",
    "heating_w",
    "cooling_p",
    "cooling_w",
    "weather",
]
ENERGY = [d for d in DATASETS if d != "weather"]
TABLES = {d: f"{CATALOG}.{BRONZE_SCHEMA}.honda_iot_{d}" for d in DATASETS}

# COMMAND ----------

# DBTITLE 1,Helper

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

# DBTITLE 1,Tagged union of (frequency, datetime_utc, source) across all 7 tables
u = None
for d, t in TABLES.items():
    part = spark.table(t).select("frequency", "datetime_utc", F.lit(d).alias("src"))
    u = part if u is None else u.union(part)

# COMMAND ----------

# DBTITLE 1,Timestamp grid + row counts per (dataset, frequency) -- one groupBy
grid = (
    u.groupBy("src", "frequency")
    .agg(
        F.min("datetime_utc").alias("min_ts"),
        F.max("datetime_utc").alias("max_ts"),
        F.countDistinct("datetime_utc").alias("distinct_ts"),
        F.count(F.lit(1)).alias("rows"),
    )
    .collect()
)
table_rows = {}
for x in grid:
    table_rows[x["src"]] = table_rows.get(x["src"], 0) + x["rows"]
    print(x.asDict())
for d, t in TABLES.items():
    print(f"{d:<16} columns -> {spark.table(t).columns}")

# COMMAND ----------

# DBTITLE 1,Key-presence matrix -- one groupBy over the union
p = u.groupBy("frequency", "datetime_utc").agg(
    *[F.max((F.col("src") == d).cast("int")).alias(d) for d in DATASETS]
)

# COMMAND ----------

# DBTITLE 1,Overlap / referential stats -- one agg over the presence matrix
pairs = [(a, b) for i, a in enumerate(DATASETS) for b in DATASETS[i + 1 :]]
stats = (
    p.agg(
        F.count(F.lit(1)).alias("union_keys"),
        *[F.sum(d).alias("present__" + d) for d in DATASETS],
        F.sum(F.least(*[F.col(d) for d in DATASETS])).alias("all_seven"),
        *[F.sum(F.col(a) * F.col(b)).alias(f"pair__{a}__{b}") for a, b in pairs],
        *[F.sum(F.col(e) * F.col("weather")).alias(f"ew__{e}") for e in ENERGY],
    )
    .first()
    .asDict()
)

union_ct = stats["union_keys"]
present = {d: stats["present__" + d] for d in DATASETS}
grid_missing = {d: union_ct - present[d] for d in DATASETS}
overlap = {(a, b): stats[f"pair__{a}__{b}"] for a, b in pairs}
join_yield = {e: (present[e], stats[f"ew__{e}"]) for e in ENERGY}
key_unique = {d: table_rows.get(d, 0) == present[d] for d in DATASETS}
seven_ct = stats["all_seven"]

print(f"union keys={union_ct}  keys in all 7={seven_ct}")
for d in DATASETS:
    print(
        f"  {d:<16} present={present[d]:>10}  missing vs union={grid_missing[d]:>10}  "
        f"rows={table_rows.get(d)}  key_unique={key_unique[d]}"
    )
for e in ENERGY:
    total_keys, matched_keys = join_yield[e]
    print(
        f"  {e:<16} energy keys={total_keys}  matched to weather={matched_keys}  ({matched_keys / total_keys * 100:.1f}%)"
        if total_keys
        else f"  {e}: no keys"
    )

# COMMAND ----------

# DBTITLE 1,Verdict -- can the Honda datasets be combined?
print(f"(frequency, datetime_utc) unique in every table : {all(key_unique.values())}")
print(
    f"keys shared by all 7                            : {seven_ct} of {union_ct} "
    f"({seven_ct / union_ct * 100:.1f}%))"
)
print(
    f"energy<->weather match rate                     : "
    f"{ {e: round(matched_keys / total_keys * 100, 1) for e, (total_keys, matched_keys) in join_yield.items() if total_keys} }"
)
print(
    "=> shared 1:1 key exists; a wide 'all Honda metrics at (freq, ts)' table is "
    "feasible on the intersection but loses the non-overlapping tail; one fact per "
    "dataset (or per metric joining P+W), a wide table is Gold."
)

# COMMAND ----------

# DBTITLE 1,Figure -- pairwise overlap heatmap
n = len(DATASETS)
m = np.zeros((n, n))
for i, a in enumerate(DATASETS):
    m[i, i] = present[a]
    for j, b in enumerate(DATASETS):
        if (a, b) in overlap:
            m[i, j] = m[j, i] = overlap[(a, b)]
plt.figure(figsize=(8, 7))
plt.imshow(np.log10(m + 1), cmap="viridis")
plt.colorbar(label="log10(shared keys + 1)")
plt.xticks(range(n), DATASETS, rotation=45, ha="right")
plt.yticks(range(n), DATASETS)
plt.title("Honda -- pairwise (frequency, datetime_utc) overlap")
plt.tight_layout()
plt.savefig(fig_path("honda_overlap_matrix.png"), dpi=110, bbox_inches="tight")
plt.show()

# COMMAND ----------

# DBTITLE 1,Figure -- missing keys vs union, and energy<->weather join yield
barplot(
    list(grid_missing.items()),
    "Honda -- (frequency, datetime_utc) keys missing vs union",
    "dataset",
    "missing keys",
    rot=30,
    filename="honda_keys_missing_vs_union.png",
)
x = np.arange(len(ENERGY))
plt.figure(figsize=(11, 4))
plt.bar(x - 0.2, [join_yield[e][0] for e in ENERGY], width=0.4, label="energy keys")
plt.bar(
    x + 0.2, [join_yield[e][1] for e in ENERGY], width=0.4, label="matched to weather"
)
plt.xticks(x, ENERGY, rotation=30, ha="right")
plt.legend()
plt.title("Honda -- energy<->weather join yield on (frequency, datetime_utc)")
plt.ylabel("keys")
plt.tight_layout()
plt.savefig(
    fig_path("honda_energy_weather_join_yield.png"), dpi=110, bbox_inches="tight"
)
plt.show()

# COMMAND ----------

# DBTITLE 1,Findings
print("key unique per table       :", key_unique)
print("keys missing vs union      :", grid_missing)
print("energy<->weather join yield :", join_yield)
print("keys shared by all 7        :", seven_ct, "of", union_ct)
print("pairwise overlap            :", overlap)

# COMMAND ----------

# DBTITLE 1,Export profiling findings -> src/schemas/profiling/honda_iot.md
_ek = [
    "| table | rows | distinct keys present | missing vs union | key unique |",
    "|---|---|---|---|---|",
]
for d_ in DATASETS:
    _ek.append(
        f"| honda_iot_{d_} | {table_rows.get(d_)} | {present[d_]} | {grid_missing[d_]} | {key_unique[d_]} |"
    )
_ek += [
    "",
    (
        f"Union of (frequency, datetime_utc) keys: {union_ct}. "
        f"Keys shared by all 7 tables: {seven_ct} ({seven_ct / union_ct * 100:.1f}%)."
    ),
]

_rel = [
    "Energy<->weather match rate on (frequency, datetime_utc):",
    "",
    "| energy table | energy keys | matched to weather | % |",
    "|---|---|---|---|",
]
for e_ in ENERGY:
    tk, mk = join_yield[e_]
    _rel.append(
        f"| honda_iot_{e_} | {tk} | {mk} | {mk / tk * 100:.1f} |"
        if tk
        else f"| honda_iot_{e_} | 0 | 0 | |"
    )
_rel += ["", "Pairwise shared-key counts:", ""]
_rel += ["| pair | shared keys |", "|---|---|"]
for (a_, b_), v_ in overlap.items():
    _rel.append(f"| {a_} + {b_} | {v_} |")

_verdict = []
_verdict.append(
    f"(frequency, datetime_utc) is unique in every Honda table: {all(key_unique.values())}."
)
_verdict.append(
    f"{seven_ct} of {union_ct} keys ({seven_ct / union_ct * 100:.1f}%) are present in all 7 tables."
)
_ewr = {e_: round(mk / tk * 100, 1) for e_, (tk, mk) in join_yield.items() if tk}
_verdict.append(f"Energy<->weather match rate: {_ewr}.")
_verdict.append(
    "A shared 1:1 key exists. A wide 'all Honda metrics at (frequency, datetime_utc)' table "
    "is feasible on the intersection but drops the non-overlapping tail; the natural Silver grain "
    "is one fact per dataset (or per metric joining P+W), with the wide table left to Gold."
)

_findings = []
if not all(key_unique.values()):
    _findings.append(
        f"- Some tables have duplicate (frequency, datetime_utc) keys: "
        f"{[d_ for d_ in DATASETS if not key_unique[d_]]}."
    )
if seven_ct < union_ct:
    _findings.append(
        f"- {union_ct - seven_ct} keys are missing from at least one table "
        f"(per-table gaps: {grid_missing})."
    )
if any(v < 100 for v in _ewr.values()):
    _findings.append(
        f"- Energy<->weather join is lossy for: {[e_ for e_, v in _ewr.items() if v < 100]}."
    )
_findings_md = (
    "\n".join(_findings)
    if _findings
    else "All 7 tables align 1:1 on the key with full coverage."
)

_silver = []
if not all(key_unique.values()):
    _silver.append(
        "- De-duplicate per-table on (frequency, datetime_utc) before any Silver join."
    )
_silver.append(
    "- Silver grain: one fact table per dataset keyed on (frequency, datetime_utc); "
    "join P+W per metric where a combined metric fact is needed."
)
if any(v < 100 for v in _ewr.values()):
    _silver.append(
        "- Use outer joins (not inner) when combining energy and weather to retain unmatched rows."
    )

write_profiling(
    SOURCE,
    NB_KEY,
    SECTION_TITLE,
    [
        ("Entities / Keys", "\n".join(_ek)),
        ("Relationships", "\n".join(_rel)),
        ("EDA Findings", _findings_md + "\n\n" + "\n".join(_verdict)),
        ("Silver Implications", "\n".join(_silver)),
    ],
    figures=[
        (
            "Honda pairwise (frequency, datetime_utc) overlap",
            "honda_overlap_matrix.png",
        ),
        (
            "Honda -- keys missing from each table vs the union",
            "honda_keys_missing_vs_union.png",
        ),
        (
            "Honda -- energy <-> weather join yield",
            "honda_energy_weather_join_yield.png",
        ),
    ],
)