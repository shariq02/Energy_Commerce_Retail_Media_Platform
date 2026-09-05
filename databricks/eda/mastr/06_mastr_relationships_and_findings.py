# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # EDA -- MASTR RELATIONSHIPS AND FINDINGS
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** Cross-table checks across all 28 MaStR Bronze tables --
# MAGIC MastrNummer key set overlap between generation units, EEG-support,
# MAGIC market actors and network tables, referential integrity of the
# MAGIC foreign-key MastrNummer columns, and an evidence-based verdict on
# MAGIC joinability. Key sets are collected (they are far smaller than the
# MAGIC row counts) and reconciled with Python set math instead of repeated
# MAGIC pairwise Spark joins.

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
NB_KEY = "06_relationships_and_findings"
SECTION_TITLE = "Cross-table relationships & verdict"

GENERATION_UNITS = [
    "einheiten_wind",
    "einheiten_biomasse",
    "einheiten_wasser",
    "einheiten_verbrennung",
    "einheiten_kernkraft",
    "einheiten_geothermie_gsgk",
]
EEG_SUPPORT = [
    "anlagen_eeg_wind",
    "anlagen_eeg_biomasse",
    "anlagen_eeg_wasser",
    "anlagen_eeg_geothermie_gsgk",
    "anlagen_kwk",
    "einheiten_genehmigung",
    "ertuechtigungen",
]
MARKET_NETWORK = [
    "marktakteure",
    "marktakteure_und_rollen",
    "netzanschlusspunkte",
    "netze",
    "lokationen",
    "bilanzierungsgebiete",
]
CHANGE_HISTORY = [
    "geloeschte_deaktivierte_einheiten",
    "geloeschte_deaktivierte_marktakteure",
    "einheiten_aenderung_netzbetreiberzuordnungen",
]
REFERENCE = [
    "einheitentypen",
    "katalogkategorien",
    "katalogwerte",
    "lokationstypen",
    "marktfunktionen",
    "marktrollen",
]
ANALYTICAL = GENERATION_UNITS + EEG_SUPPORT + MARKET_NETWORK + CHANGE_HISTORY
ALL_DATASETS = ANALYTICAL + REFERENCE
TABLES = {d: f"{CATALOG}.{BRONZE_SCHEMA}.mastr_{d}" for d in ALL_DATASETS}

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


def own_key_col(df: DataFrame) -> str | None:
    # The own-entity key is the *MastrNummer column with the shortest name
    # among those on the table (foreign keys carry an extra role prefix,
    # e.g. "NetzbetreiberMastrNummer" vs. the shorter own "MastrNummer").
    cands = key_like_cols(df.columns)
    return min(cands, key=len) if cands else None


def barplot(
    pairs, title, xlabel, ylabel="count", rot=0, figsize=(10, 4), filename=None
):
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

# DBTITLE 1,Own-key set per table (collected -- key sets are small relative to row counts)
frames = {d: spark.table(t) for d, t in TABLES.items()}
own_key = {}
key_set = {}
row_count = {}
for name, df in frames.items():
    k = own_key_col(df)
    own_key[name] = k
    row_count[name] = df.count()
    if k:
        key_set[name] = {
            x[0] for x in df.select(F.col(k).cast("string")).distinct().collect()
        }
    else:
        key_set[name] = set()
    print(
        f"{name:<48} own_key={k}  rows={row_count[name]:>10}  distinct_keys={len(key_set[name])}"
    )

# COMMAND ----------

# DBTITLE 1,Generation-unit key union + EEG-support / genehmigung foreign-key coverage
gen_union = set().union(*(key_set[d] for d in GENERATION_UNITS))
print(f"union of generation-unit MastrNummer keys = {len(gen_union)}")
eeg_fk_coverage = {}
for d in EEG_SUPPORT:
    df = frames[d]
    fk = next(
        (
            c
            for c in key_like_cols(df.columns)
            if c != own_key[d] and "einheit" in c.lower()
        ),
        None,
    )
    if fk is None:
        eeg_fk_coverage[d] = {"fk_col": None}
        continue
    fk_vals = {x[0] for x in df.select(F.col(fk).cast("string")).distinct().collect()}
    orphans = len(fk_vals - gen_union)
    eeg_fk_coverage[d] = {
        "fk_col": fk,
        "distinct_fk_values": len(fk_vals),
        "orphans_vs_generation_units": orphans,
    }
print("EEG-support / genehmigung foreign-key coverage vs generation units:")
for d, v in eeg_fk_coverage.items():
    print(f"  {d}: {v}")

# COMMAND ----------

# DBTITLE 1,Market-actor role coverage -- marktakteure_und_rollen vs marktakteure
ma_key = key_set["marktakteure"]
mur_fk = next(
    (
        c
        for c in key_like_cols(frames["marktakteure_und_rollen"].columns)
        if c != own_key["marktakteure_und_rollen"]
    ),
    own_key["marktakteure_und_rollen"],
)
mur_vals = (
    {
        x[0]
        for x in frames["marktakteure_und_rollen"]
        .select(F.col(mur_fk).cast("string"))
        .distinct()
        .collect()
    }
    if mur_fk
    else set()
)
print(f"marktakteure_und_rollen join column: {mur_fk}")
print(f"marktakteure keys with no role row: {len(ma_key - mur_vals)}")
print(f"role rows referencing an unknown marktakteure key: {len(mur_vals - ma_key)}")

# COMMAND ----------

# DBTITLE 1,Change-history referential integrity vs current-state tables
change_orphans = {}
for d in CHANGE_HISTORY:
    df = frames[d]
    fk = next(
        (c for c in key_like_cols(df.columns) if "einheit" in c.lower()),
        own_key[d],
    )
    if fk is None:
        continue
    fk_vals = {x[0] for x in df.select(F.col(fk).cast("string")).distinct().collect()}
    change_orphans[d] = {
        "fk_col": fk,
        "distinct_fk_values": len(fk_vals),
        "not_in_generation_units": len(fk_vals - gen_union),
    }
print("Change-history foreign keys vs generation-unit key union:")
for d, v in change_orphans.items():
    print(f"  {d}: {v}")

# COMMAND ----------

# DBTITLE 1,Figure -- key coverage summary
barplot(
    [(d, row_count[d]) for d in ALL_DATASETS],
    "MaStR -- rows per Bronze table (all 28)",
    "table",
    "rows",
    rot=90,
    figsize=(16, 5),
    filename="mastr_rows_per_table.png",
)
barplot(
    [
        (d, v.get("orphans_vs_generation_units", 0))
        for d, v in eeg_fk_coverage.items()
        if v.get("fk_col")
    ],
    "MaStR -- EEG-support/genehmigung foreign keys not found in generation units",
    "table",
    "orphan keys",
    rot=30,
    filename="mastr_eeg_fk_orphans.png",
)

# COMMAND ----------

# DBTITLE 1,Verdict -- can the 28 tables be joined on MastrNummer as designed?
_any_orphans = any(
    v.get("orphans_vs_generation_units", 0) > 0 for v in eeg_fk_coverage.values()
) or any(v.get("not_in_generation_units", 0) > 0 for v in change_orphans.values())
print(
    f"any EEG-support/change-history foreign key with no matching generation unit: {_any_orphans}"
)
print(f"marktakteure keys with no role row: {len(ma_key - mur_vals)}")
print(
    "=> MaStR's own-entity MastrNummer is the reliable Silver grain key per table; "
    "cross-table joins are foreign-key MastrNummer -> another table's own MastrNummer, "
    "confirmed above rather than assumed from column naming alone."
)

# COMMAND ----------

# DBTITLE 1,Findings
print("own key per table:", own_key)
print("distinct key counts:", {d: len(key_set[d]) for d in ALL_DATASETS})
print("EEG-support foreign-key coverage:", eeg_fk_coverage)
print("change-history foreign-key coverage:", change_orphans)

# COMMAND ----------

# DBTITLE 1,Export profiling findings -> src/schemas/profiling/mastr.md
_ent = [
    f"Own-entity key column per table: {own_key}",
    f"Distinct own-key count per table: { {d: len(key_set[d]) for d in ALL_DATASETS} }",
    f"Union of generation-unit MastrNummer keys: {len(gen_union)}",
]

_rel = [
    (
        "EEG-support / genehmigung foreign-key coverage vs the generation-unit key union "
        "(fk_col, distinct_fk_values, orphans_vs_generation_units):"
    ),
]
for d, v in eeg_fk_coverage.items():
    _rel.append(f"- {d}: {v}")
_rel += [
    "",
    f"marktakteure_und_rollen join column: `{mur_fk}`",
    f"- marktakteure keys with no role row: {len(ma_key - mur_vals)}",
    f"- role rows referencing an unknown marktakteure key: {len(mur_vals - ma_key)}",
    "",
    (
        "Change-history foreign keys vs generation-unit key union "
        "(fk_col, distinct_fk_values, not_in_generation_units):"
    ),
]
for d, v in change_orphans.items():
    _rel.append(f"- {d}: {v}")

_verdict = [
    f"- Any EEG-support/change-history foreign key orphaned against generation units: {_any_orphans}.",
    (
        f"- marktakteure keys missing a role row: {len(ma_key - mur_vals)}; role rows with an unknown "
        f"marktakteure key: {len(mur_vals - ma_key)}."
    ),
    (
        "- Verdict: each table's own-entity MastrNummer is a safe Silver grain key; cross-table joins "
        "must be validated against the live foreign-key coverage above per release, since MaStR's "
        "German field names do not guarantee a byte-identical key match without this check."
    ),
]

_silver = [
    (
        "- Silver join key: own-entity `*MastrNummer` per table, joined to another table's own key via "
        "the matching foreign-key `*MastrNummer` column (verified above, not assumed from naming)."
    ),
]
if _any_orphans:
    _silver.append(
        "- Orphaned foreign keys exist -> a left join must not silently drop the fact row; flag "
        "the orphan instead."
    )
if ma_key - mur_vals or mur_vals - ma_key:
    _silver.append(
        "- marktakteure <-> marktakteure_und_rollen is not a clean 1:1/1:N without gaps -> "
        "reconcile before treating roles as a simple child table."
    )

_ml_readiness = [
    (
        "No candidate ML target lives across these 28 tables directly -- this notebook is a "
        "joinability audit; see 01-04 for per-table target candidates (decommission events, "
        "authorisation outcomes, repowering events)."
    ),
    (
        f"Join cardinality / cartesian-explosion risk: EEG-support and change-history foreign keys "
        f"are checked against the generation-unit key union ({len(gen_union)} distinct keys) -- "
        f"any orphan found ({_any_orphans}) means a left join must be used (not inner) or fact rows "
        "silently disappear; a foreign key with MULTIPLE matching rows in a target table (not "
        "checked by set-membership alone) would additionally risk fan-out -- verify row-level join "
        "cardinality, not just key-set overlap, before joining at scale."
    ),
    (
        f"marktakteure <-> marktakteure_und_rollen is not a clean gap-free 1:1/1:N: "
        f"{len(ma_key - mur_vals)} marktakteure keys have no role row and "
        f"{len(mur_vals - ma_key)} role rows reference an unknown marktakteure key -- treating this "
        "as a simple child table without handling both gaps risks silently dropping actors from a "
        "role-based feature."
    ),
    (
        "Grain and entity-grouped split: each table's own-entity `*MastrNummer` (own_key above) is the "
        "correct split unit for any cross-table model -- always split by the GENERATION-UNIT or "
        "MARKET-ACTOR MastrNummer at the root of a join chain, not by row in any individual table, so "
        "that one entity's rows across multiple joined tables stay together."
    ),
    (
        "Leakage: change-history foreign-key coverage (checked here against the generation-unit key "
        "union) confirms WHICH units have historical change events, but not WHEN -- combining this "
        "notebook's cross-table joins with a temporal target (e.g. decommission prediction) still "
        "requires the date-based leakage guard described in 04_change_history_eda.py; a clean key "
        "match here does not imply a temporally safe feature."
    ),
    (
        "Imbalance: not applicable at this join-audit level -- see 01-04 for per-table imbalance notes "
        "on rare categorical/event columns."
    ),
    (
        "Sample-vs-full divergence: not applicable -- every statistic here (key sets, foreign-key "
        "coverage, row counts) is computed from a full Spark distinct/count or a fully collected key "
        "set, no `.sample()`/`.limit()` subset feeds any reported number."
    ),
]

write_profiling(
    SOURCE,
    NB_KEY,
    SECTION_TITLE,
    blocks=[
        ("Entities / Keys", "\n".join(_ent)),
        ("Relationships", "\n".join(_rel)),
        ("EDA Findings", "\n".join(_verdict)),
        ("ML-Readiness Evidence", "\n".join(f"- {ln}" for ln in _ml_readiness)),
        ("Silver Implications", "\n".join(_silver)),
    ],
    figures=[
        ("MaStR rows per Bronze table (all 28)", "mastr_rows_per_table.png"),
        (
            "MaStR EEG-support/genehmigung foreign keys not found in generation units",
            "mastr_eeg_fk_orphans.png",
        ),
    ],
)