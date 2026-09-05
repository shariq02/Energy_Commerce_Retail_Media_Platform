# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # EDA -- MASTR CHANGE HISTORY
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** Profile the three MaStR deletion / change-log Bronze
# MAGIC tables (geloeschte_deaktivierte_einheiten,
# MAGIC geloeschte_deaktivierte_marktakteure,
# MAGIC einheiten_aenderung_netzbetreiberzuordnungen) -- schema, missingness,
# MAGIC constant columns, MastrNummer key cardinality, full-row duplicates,
# MAGIC temporal activity of the change events -- as evidence for Silver
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
SOURCE = "mastr"
NB_KEY = "04_change_history"
SECTION_TITLE = (
    "Change history (geloeschte_deaktivierte_* / "
    "einheiten_aenderung_netzbetreiberzuordnungen)"
)
DATASETS = [
    "geloeschte_deaktivierte_einheiten",
    "geloeschte_deaktivierte_marktakteure",
    "einheiten_aenderung_netzbetreiberzuordnungen",
]
TABLES = {d: f"{CATALOG}.{BRONZE_SCHEMA}.mastr_{d}" for d in DATASETS}
# Any column containing one of these substrings is treated as a change-event
# date/timestamp for the temporal-activity check (dynamic, not hardcoded per
# table since the exact German field name varies by table).
DATE_COL_HINTS = ("datum", "date", "zeitpunkt")

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

# DBTITLE 1,Key columns (*MastrNummer) -- cardinality (reuses profile)
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

# DBTITLE 1,Temporal activity of change events -- rows per year (one groupBy per detected date column)
temporal = {}
for name, df in frames.items():
    dcols = date_like_cols(prof[name]["cols"])
    per_col = {}
    for c in dcols:
        parsed = F.coalesce(
            F.to_date(F.col(c).cast("string"), "yyyyMMdd"),
            F.to_date(F.col(c).cast("string")),
        )
        g = (
            df.select(F.year(parsed).alias("yr"))
            .where(F.col("yr").isNotNull())
            .groupBy("yr")
            .count()
            .orderBy("yr")
            .collect()
        )
        per_col[c] = [(x["yr"], x["count"]) for x in g]
    temporal[name] = per_col
    print(f"{name} date-like columns -> rows per year:", per_col)

# COMMAND ----------

# DBTITLE 1,Figure -- rows per table, duplicates, and rows-per-year for the first detected date column
facet_bars(
    {
        "rows per table": [(d, prof[d]["total"]) for d in DATASETS],
        "full-row duplicates": [(d, dup_counts[d]) for d in DATASETS],
    },
    "MaStR change history -- overview",
    "mastr_change_history_overview.png",
    rot=30,
    ncols=2,
)
facet_bars(
    {d: (next(iter(temporal[d].values())) if temporal[d] else []) for d in DATASETS},
    "MaStR change history -- rows per year (first date-like column per table)",
    "mastr_change_history_temporal.png",
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
        f"key candidates={key_report[d]}, date-like columns={list(temporal[d].keys())}"
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

_temporal = ["Rows per year, by detected date-like column:"]
for d in DATASETS:
    for c, pairs in temporal[d].items():
        _temporal.append(f"- {d}.`{c}`: {pairs}")

_findings_md = "\n".join(f"- {ln}" for ln in findings_lines)

_silver = [
    (
        "- These are append-only change-event logs, not current-state entities -> model as a Silver "
        "change/event fact, never overwritten by the current-state generation-unit/market-actor tables."
    ),
]
if any(dup_counts.values()):
    _silver.append(
        "- Exact duplicate rows exist in at least one table -> de-duplicate on load."
    )
_silver.append(
    "- einheiten_aenderung_netzbetreiberzuordnungen records network-operator reassignment events; "
    "join to the generation-unit MastrNummer to build a time-varying operator-assignment history, "
    "not a static attribute."
)

_ml_readiness = [
    (
        "Candidate target signals: `geloeschte_deaktivierte_einheiten`/`geloeschte_deaktivierte_"
        "marktakteure` are literal decommission/deactivation-event labels for a unit- or actor-"
        "level survival/churn use case; `einheiten_aenderung_netzbetreiberzuordnungen` is an "
        "operator-reassignment event label."
    ),
    (
        "Leakage: these are append-only change-event logs -- a decommission-prediction model may "
        "only use change events with a date/timestamp strictly before the prediction cutoff; using "
        "any row from AFTER the cutoff (including the deactivation event itself) to build a feature "
        "for predicting that same event is definitional leakage."
    ),
    (
        "Grain and entity-grouped split: grain is one change EVENT per row, keyed by the referenced "
        "unit/actor's MastrNummer, not one row per entity -- split by that MastrNummer so an "
        "entity's full change history stays on one side of a split; a unit can have multiple change "
        "rows over time (see key_report ratios below 1.0 where present)."
    ),
    (
        "Join cardinality: joining these change-event tables to the current-state generation-unit "
        "tables (01_generation_units_eda.py) is 1:N per unit if a unit has multiple historical "
        "change events -- confirm the exact foreign-key coverage in "
        "06_mastr_relationships_and_findings.py before joining as if 1:1; treating a 1:N join as "
        "1:1 either loses history or silently duplicates the unit's static attributes per event."
    ),
    (
        "Imbalance: deletion/deactivation/reassignment events are almost certainly rare relative to "
        "the full generation-unit or market-actor population -- a decommission or reassignment "
        "classifier built against the full current-state population as the negative class will "
        "face severe class imbalance."
    ),
    (
        "Sample-vs-full divergence: not applicable -- every statistic here (profile, key cardinality, "
        "duplicate counts, rows-per-year) is computed from a full Spark aggregation, no "
        "`.sample()`/`.limit()` subset feeds any reported number."
    ),
]
if any(dup_counts.values()):
    _ml_readiness.append(
        "Exact full-row duplicates exist in at least one table (see Data Quality) -- de-duplicate "
        "before counting change events as independent observations."
    )

write_profiling(
    SOURCE,
    NB_KEY,
    SECTION_TITLE,
    blocks=[
        ("Profile", "\n".join(_profile)),
        ("Data Quality", "\n".join(_dq)),
        ("Entities / Keys", "\n".join(_entities)),
        ("Temporal", "\n".join(_temporal)),
        ("EDA Findings", _findings_md),
        ("ML-Readiness Evidence", "\n".join(f"- {ln}" for ln in _ml_readiness)),
        ("Silver Implications", "\n".join(_silver)),
    ],
    figures=[
        ("MaStR change history -- overview", "mastr_change_history_overview.png"),
        (
            "MaStR change history -- rows per year (first date-like column per table)",
            "mastr_change_history_temporal.png",
        ),
    ],
)
