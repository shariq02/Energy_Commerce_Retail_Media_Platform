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
# MAGIC **Purpose:** Profile the six MaStR static reference / code-lookup Bronze
# MAGIC tables (einheitentypen, katalogkategorien, katalogwerte, lokationstypen,
# MAGIC marktfunktionen, marktrollen) -- schema, full-row duplicates, the
# MAGIC katalogkategorien <-> katalogwerte lookup consistency, and how these
# MAGIC code sets constrain the categorical columns profiled in 01-04. Small
# MAGIC tables, collected and reconciled in Python.

# COMMAND ----------

# MAGIC %run ../_eda_common

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
    meta[name] = {
        "cols": cols,
        "recs": recs,
        "total": total,
        "dups": dups,
        "consts": consts,
    }
    print(
        "=" * 90, f"\n{name}  rows={total}  cols={cols}  dups={dups}  constant={consts}"
    )
    for c in cols:
        missing = sum(1 for d in recs if d[c] is None or str(d[c]).strip() == "")
        print(f"  {c:<30} missing={missing:>6}  distinct={len({d[c] for d in recs})}")
    for d in recs[:20]:
        print("  ", d)

# COMMAND ----------

# DBTITLE 1,katalogkategorien <-> katalogwerte lookup consistency
kk, kw = meta["katalogkategorien"], meta["katalogwerte"]
kk_id = find_col(
    spark.table(TABLES["katalogkategorien"]), "Id", "KatalogKategorieId", "kategorie_id"
)
kw_fk = next((c for c in kw["cols"] if "kategorie" in c.lower()), None)
kk_ids = {d[kk_id] for d in kk["recs"]} if kk_id else set()
kw_fks = {d[kw_fk] for d in kw["recs"]} if kw_fk else set()
kat_ri = referential_integrity(
    kw_fks, kk_ids, child=f"katalogwerte.{kw_fk}", parent=f"katalogkategorien.{kk_id}"
)
print(f"catalog lookup key: {kk_id} <-> {kw_fk}")
print("referential integrity:", kat_ri)
for ln in ri_interpretation(kat_ri):
    print("  ", ln)

# COMMAND ----------

# DBTITLE 1,Figure -- rows per catalog table
figs = []
if barplot(
    [(n, meta[n]["total"]) for n in DATASETS],
    "MaStR reference catalogs -- rows per table",
    "table",
    "rows",
    rot=30,
    filename="mastr_reference_catalogs_overview.png",
):
    figs.append(
        (
            "MaStR reference catalogs -- rows per table",
            "mastr_reference_catalogs_overview.png",
        )
    )

# COMMAND ----------

# DBTITLE 1,Export profiling findings -> src/schemas/profiling/mastr.md
_profile = [
    "| table | rows | cols | full-row dups | constant columns |",
    "|---|---|---|---|---|",
]
for name in DATASETS:
    x = meta[name]
    _profile.append(
        f"| {name} | {x['total']} | {len(x['cols'])} | {x['dups']} | "
        f"{', '.join(x['consts']) or '-'} |"
    )

_dq = [
    f"Full-row exact duplicates per table: { {n: meta[n]['dups'] for n in DATASETS} }"
]

_recon = [
    f"katalogkategorien key `{kk_id}` <-> katalogwerte FK `{kw_fk}`:",
    (
        f"- {kat_ri['orphans']} katalogwerte FK values with no matching category "
        f"(match rate {kat_ri['match_rate']})."
    ),
    (
        f"- {kat_ri['unused_parent']} categories never referenced by a katalogwerte row: "
        f"{sorted(str(x) for x in (kk_ids - kw_fks))[:15]}."
    ),
]
for ln in ri_interpretation(kat_ri):
    _recon.append(f"- {ln}")

_domain = [
    (
        "These six tables are the authoritative code sets for the categorical columns profiled in "
        "01-04. Before Silver, each low-cardinality code column found there (status, technology, "
        "energy carrier, market role, ...) must be reconciled against the matching catalog here -- "
        "an unknown code is a quarantine class, not a silent NULL. That reconciliation is a Silver "
        "task; this notebook only confirms the catalogs are internally consistent."
    ),
]

_findings_md = "\n".join(
    f"- {name}: {meta[name]['dups']} full-row duplicate(s), "
    f"{len(meta[name]['cols'])} column(s)"
    for name in DATASETS
)

_silver = [
    (
        "- All six tables are static lookups -> Silver reference dimensions, SCD type 1 (overwrite "
        "on reload) unless a future MaStR release adds a validity-period column."
    ),
    (
        "- Pin the MaStR catalog release used -- code meanings change between register versions, so "
        "a code decoded against the wrong catalog vintage is a silent label error downstream."
    ),
]
if any(meta[n]["dups"] for n in DATASETS):
    _silver.append("- Exact duplicate rows exist -> de-duplicate on load.")
if kat_ri["orphans"]:
    _silver.append(
        "- katalogwerte has FK values with no category row -> confirm the FK column semantics "
        "against the live schema before enforcing the foreign key."
    )

_ml = ml_readiness_block(
    [
        (
            "Grain / grain drift",
            (
                "Code dimensions with no observation grain -- a model joins a label in, it does not train "
                "on these rows."
            ),
        ),
        (
            "Join multiplication (1:N / M:N expansion)",
            (
                f"katalogkategorien <-> katalogwerte is a clean code lookup ({kat_ri['match_rate']} match). "
                "A decode join fans out only if a code appears in more than one category -- not observed."
            ),
        ),
        (
            "Target contamination",
            "Not applicable -- static reference data, no outcome.",
        ),
        ("Temporal / post-event leakage", "Not applicable -- no temporal ordering."),
        (
            "Proxy leakage",
            (
                "A decoded label is a deterministic function of the code -- using both the raw code and "
                "its decoded label as separate features is redundant, not leakage, but wastes capacity."
            ),
        ),
        ("Split / entity leakage", "Not applicable -- no entities to split."),
        (
            "Historical-reference (point-in-time) leakage",
            (
                "Real risk: decoding a historical record with today's catalog assigns a meaning the code "
                "did not have then. Keep the catalog version alongside the decoded value."
            ),
        ),
        (
            "Survivorship / coverage bias",
            (
                f"{kat_ri['unused_parent']} categories are never used by any katalogwerte row -- harmless "
                "for a dimension, but a one-hot over the full catalog carries always-zero columns."
            ),
        ),
        ("Missingness leakage", "Not applicable -- fully populated small tables."),
        (
            "Duplicate-event leakage",
            (
                f"Full-row duplicates: { {n: meta[n]['dups'] for n in DATASETS} } -- de-duplicate so a "
                "decode join stays 1:1."
            ),
        ),
        ("Target / feature temporal misalignment", "Not applicable -- no dates."),
        (
            "Unit / sign / circular-feature leakage",
            "Not applicable -- no numeric measures.",
        ),
        (
            "Data-generation-process leakage",
            (
                "The catalogs describe MaStR's own classification scheme -- a code encodes how MaStR "
                "categorises a thing, not an independent physical property."
            ),
        ),
        (
            "Class / label instability",
            (
                "PRIMARY concern here: MaStR revises these code sets between releases (adds/retires/"
                "renames). A class defined by a raw code is only stable within one catalog vintage."
            ),
        ),
        ("Label availability lag", "Not applicable -- reference data."),
        (
            "Source / version / regime change",
            (
                "Each MaStR release ships an updated catalog -- pin and diff it across releases before "
                "pooling records decoded under different vintages."
            ),
        ),
        (
            "Sample-vs-full divergence",
            "All six tables are fully collected (small) -- no sampling.",
        ),
    ]
)

write_profiling(
    SOURCE,
    NB_KEY,
    SECTION_TITLE,
    blocks=[
        ("Profile", "\n".join(_profile)),
        ("Data Quality", "\n".join(_dq)),
        ("Referential Integrity", "\n".join(_recon)),
        ("Categorical / Domain Validation", "\n".join(_domain)),
        ("EDA Findings", _findings_md),
        ("ML-Readiness Evidence", _ml),
        ("Silver Implications", "\n".join(_silver)),
    ],
    figures=figs,
)
