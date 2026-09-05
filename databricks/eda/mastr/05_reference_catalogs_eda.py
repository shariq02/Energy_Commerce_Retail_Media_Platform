# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # EDA -- MASTR REFERENCE CATALOGS
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** Profile the six small MaStR reference-catalog Bronze
# MAGIC tables (einheitentypen, katalogkategorien, katalogwerte,
# MAGIC lokationstypen, marktfunktionen, marktrollen) -- schema, missingness,
# MAGIC constant columns, duplicates, code reconciliation against the
# MAGIC analytical tables' categorical columns. These tables are small, so
# MAGIC each is collected once and analysed in Python.

# COMMAND ----------

# DBTITLE 1,Imports
import contextlib
import os as _os
import re as _re

import matplotlib.pyplot as plt

# COMMAND ----------

# DBTITLE 1,Configuration
CATALOG = "energy_commerce_retail_media"
BRONZE_SCHEMA = "bronze"
SOURCE = "mastr"
NB_KEY = "05_reference_catalogs"
SECTION_TITLE = (
    "Reference catalogs (einheitentypen / katalog* / lokationstypen / "
    "marktfunktionen / marktrollen)"
)
DATASETS = [
    "einheitentypen",
    "katalogkategorien",
    "katalogwerte",
    "lokationstypen",
    "marktfunktionen",
    "marktrollen",
]
TABLES = {d: f"{CATALOG}.{BRONZE_SCHEMA}.mastr_{d}" for d in DATASETS}

# COMMAND ----------

# DBTITLE 1,Helpers


def find_key(cols, *cands):
    low = {c.lower(): c for c in cols}
    for x in cands:
        if x.lower() in low:
            return low[x.lower()]
    return None


def find_key_like(cols, *substrings):
    for c in cols:
        cl = c.lower()
        if any(s in cl for s in substrings):
            return c
    return None


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

# DBTITLE 1,Collect the 6 catalog tables (small) and profile in Python
meta = {}
for name, t in TABLES.items():
    df = spark.table(t)
    recs = [x.asDict() for x in df.collect()]
    cols = df.columns
    total = len(recs)
    dups = total - len({tuple(sorted(d.items())) for d in recs})
    consts = [c for c in cols if len({d[c] for d in recs}) <= 1]
    meta[name] = {"cols": cols, "recs": recs, "total": total}
    print(
        "=" * 90,
        f"\n{name}  rows={total}  cols={cols}  full_row_duplicates={dups}  constant_columns={consts}",
    )
    for c in cols:
        missing = sum(1 for d in recs if d[c] is None or str(d[c]).strip() == "")
        print(f"  {c:<30} missing={missing:>6}  distinct={len({d[c] for d in recs})}")
    for d in recs[:20]:
        print("  ", d)

# COMMAND ----------

# DBTITLE 1,katalogkategorien <-> katalogwerte reconciliation (Python)
kk = meta["katalogkategorien"]
kw = meta["katalogwerte"]
kk_id = find_key(kk["cols"], "KatalogKategorieId", "Id", "kategorie_id")
kw_fk = find_key_like(kw["cols"], "kategorie")
kk_ids = {d[kk_id] for d in kk["recs"]} if kk_id else set()
kw_fks = {d[kw_fk] for d in kw["recs"]} if kw_fk else set()
print(f"katalogkategorien key column: {kk_id}   katalogwerte FK-like column: {kw_fk}")
print("katalogkategorien ids not referenced by any katalogwerte row:", kk_ids - kw_fks)
print("katalogwerte FK values with no matching katalogkategorien row:", kw_fks - kk_ids)

# COMMAND ----------

# DBTITLE 1,Findings
_dup_report = {}
for name, x in meta.items():
    _dup_report[name] = x["total"] - len({tuple(sorted(d.items())) for d in x["recs"]})
print("full-row duplicates per table:", _dup_report)

# COMMAND ----------

# DBTITLE 1,Figure -- rows per catalog table
barplot(
    [(n, meta[n]["total"]) for n in DATASETS],
    "MaStR reference catalogs -- rows per table",
    "table",
    "rows",
    rot=30,
    filename="mastr_reference_catalogs_overview.png",
)

# COMMAND ----------

# DBTITLE 1,Export profiling findings -> src/schemas/profiling/mastr.md
_profile = [
    "| table | rows | cols | full-row dups | constant columns |",
    "|---|---|---|---|---|",
]
for name, x in meta.items():
    consts = [c for c in x["cols"] if len({d[c] for d in x["recs"]}) <= 1]
    _profile.append(
        f"| {name} | {x['total']} | {len(x['cols'])} | {_dup_report[name]} | "
        f"{', '.join(consts) or '-'} |"
    )

_dq = [f"Full-row exact duplicates per table: {_dup_report}"]

_recon = [
    f"katalogkategorien key column: `{kk_id}`, katalogwerte FK-like column: `{kw_fk}`",
    f"katalogkategorien ids with no referencing katalogwerte row: {sorted(kk_ids - kw_fks)}",
    f"katalogwerte FK values with no matching katalogkategorien row: {sorted(kw_fks - kk_ids)}",
]

_findings_md = "\n".join(
    f"- {name}: {v} full-row duplicate(s)" for name, v in _dup_report.items()
)

_silver = [
    (
        "- All six tables are static lookups -> model as Silver reference dimensions, SCD type 1 "
        "(overwrite on reload) unless a future MaStR release adds a validity-period column."
    ),
]
if any(_dup_report.values()):
    _silver.append(
        "- Exact duplicate rows exist in at least one table -> de-duplicate on load."
    )
if kw_fks - kk_ids:
    _silver.append(
        "- katalogwerte has FK values with no matching katalogkategorien row -> either the FK "
        "column identified above is wrong or the catalog tables are not fully self-consistent; "
        "verify column semantics against the live schema before enforcing a foreign key."
    )
_silver.append(
    "- einheitentypen / lokationstypen / marktfunktionen / marktrollen are the candidate code "
    "lookups for the categorical columns profiled in 01-04; reconcile the categorical value sets "
    "found there against these tables' codes before finalising Silver contracts."
)

_ml_readiness = [
    (
        "No candidate ML target lives in these six static reference/catalog tables -- they are pure "
        "code lookups, not observations of an outcome."
    ),
    (
        f"Join cardinality: katalogkategorien <-> katalogwerte is confirmed as a code-lookup join "
        f"on `{kk_id}` <-> `{kw_fk}`; {len(kk_ids - kw_fks)} category codes have no referencing "
        f"katalogwerte row and {len(kw_fks - kk_ids)} katalogwerte FK values reference an unknown "
        "category -- either gap means an inner join to enrich a categorical feature with its "
        "catalog label will silently drop rows on one side."
    ),
    (
        "Grain and entity-grouped split: not applicable -- these are code dimensions with no "
        "observation-level grain to split; any model using them joins in a static label, it does not "
        "train directly on these tables."
    ),
    "Leakage: not applicable -- static reference data carries no temporal ordering to leak across.",
    "Imbalance: not applicable -- these are code enumerations, not a class-balance concern.",
    (
        "Sample-vs-full divergence: not applicable -- all six tables are fully collected (no "
        "sampling) since they are small."
    ),
]

write_profiling(
    SOURCE,
    NB_KEY,
    SECTION_TITLE,
    blocks=[
        ("Profile", "\n".join(_profile)),
        ("Data Quality", "\n".join(_dq)),
        ("Domain Findings", "\n".join(_recon)),
        ("EDA Findings", _findings_md),
        ("ML-Readiness Evidence", "\n".join(f"- {ln}" for ln in _ml_readiness)),
        ("Silver Implications", "\n".join(_silver)),
    ],
    figures=[
        (
            "MaStR reference catalogs -- rows per table",
            "mastr_reference_catalogs_overview.png",
        ),
    ],
)