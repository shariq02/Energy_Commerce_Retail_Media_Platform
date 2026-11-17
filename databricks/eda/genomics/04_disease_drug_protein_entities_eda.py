# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # EDA -- GENOMICS DISEASE, DRUG, PROTEIN AND OTHER ENTITY TABLES
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** November 2026
# MAGIC
# MAGIC **Purpose:** Profile the entity tables of genomics Gold (`disease_profile`,
# MAGIC `drug_profile`, `protein_profile`, `transcript_profile`, `epitope_profile`,
# MAGIC `genetic_test_profile`, `clinical_trial_profile`): nulls, constant columns,
# MAGIC key, rates per column kind, coverage by source, the fill of the link columns,
# MAGIC date-like columns and the shape of the text columns -- as evidence.

# COMMAND ----------

# MAGIC %run ../_eda_common

# COMMAND ----------

# DBTITLE 1,Configuration
CATALOG = "energy_commerce_retail_media"
SCHEMA = "genomics_gold"
SOURCE = "genomics"
NB_KEY = "04_disease_drug_protein_entities"
SECTION_TITLE = "Disease, drug, protein, transcript, epitope, test and trial tables"
TABLES = [
    "disease_profile",
    "drug_profile",
    "protein_profile",
    "transcript_profile",
    "epitope_profile",
    "genetic_test_profile",
    "clinical_trial_profile",
]

# COMMAND ----------

# DBTITLE 1,Validate profiling export path
REPO_ROOT = _repo_root()
PROFILING_DIR = _profiling_dir()
print(f"OK  repo root: {REPO_ROOT}")
print(f"OK  profiling directory: {PROFILING_DIR}")

# COMMAND ----------

# DBTITLE 1,Read tables
FRAMES = read_tables(CATALOG, SCHEMA, TABLES)
KINDS = {name: column_kinds(frame) for name, frame in FRAMES.items()}

# COMMAND ----------

# DBTITLE 1,Profile -- rows, nulls, constant and empty columns
PROFILES = {}
for name, frame in FRAMES.items():
    PROFILES[name] = table_profile(frame)
    p = PROFILES[name]
    print(
        f"{name}: rows={p['rows']:,} columns={p['columns']} "
        f"empty={len(p['empty'])} constant={len(p['constant'])}"
    )

# COMMAND ----------

# DBTITLE 1,Key and duplicates
KEYS = {name: table_key_report(frame) for name, frame in FRAMES.items()}
for name, key in KEYS.items():
    print(name, key)

# COMMAND ----------

# DBTITLE 1,Boolean and numeric rates
BOOLEANS = {
    name: boolean_rates(frame, KINDS[name]["boolean"]) for name, frame in FRAMES.items()
}
NUMERICS = {
    name: numeric_scan(frame, KINDS[name]["numeric"]) for name, frame in FRAMES.items()
}
for name in TABLES:
    print(
        name, "boolean columns:", len(BOOLEANS[name]), "numeric:", len(NUMERICS[name])
    )

# COMMAND ----------

# DBTITLE 1,Coverage by source
COVERAGE = {
    name: coverage_by_source(frame, PROFILES[name]) for name, frame in FRAMES.items()
}
for name, groups in COVERAGE.items():
    print(
        name, "source groups:", len(groups), {g: v["rows"] for g, v in groups.items()}
    )

# COMMAND ----------

# DBTITLE 1,Link columns -- null rate of every column ending in _key
LINK_FILL = link_fill(PROFILES)
for name, fill in LINK_FILL.items():
    print(name, fill)

# COMMAND ----------

# DBTITLE 1,Date-like columns -- timestamp type or a date in the name
DATE_COLUMNS = {}
for name, p in PROFILES.items():
    names = KINDS[name]["timestamp"] + [
        c for c in KINDS[name]["text"] if "date" in c.lower()
    ]
    names = [c for c in names if c not in PROVENANCE_COLUMNS]
    DATE_COLUMNS[name] = {c: p["null_rate"][c] for c in names}
    print(name, len(names), "date-like columns")

# COMMAND ----------

# DBTITLE 1,Figure -- rows per table
drawn = barplot(
    [(n, p["rows"]) for n, p in PROFILES.items()],
    "Rows per table",
    "table",
    ylabel="rows",
    rot=20,
    logy=True,
    filename="genomics_04_rows.png",
)
FIGURES = [("Rows per table", "genomics_04_rows.png")] if drawn else []

# COMMAND ----------

# DBTITLE 1,Export profiling findings
READINESS = [
    (
        "Grain / grain drift",
        "; ".join(
            f"`{n}` key `{k['key']}` unique: {k.get('unique')}" for n, k in KEYS.items()
        ),
    ),
    (
        "Duplicate-event leakage",
        "; ".join(
            para(
                f"{n} dup groups {(k.get('duplicates') or {}).get('dup_groups', 0)},",
                f"conflicting {(k.get('duplicates') or {}).get('conflicting', 0)}",
            )
            for n, k in KEYS.items()
        ),
    ),
    (
        "Survivorship / coverage bias",
        "; ".join(
            para(
                f"{n}: {sum((r or 0) >= HIGH_NULL_RATE for r in p['null_rate'].values())}",
                f"columns with a null rate of {pct(HIGH_NULL_RATE)} or more",
            )
            for n, p in PROFILES.items()
        ),
    ),
    (
        "Sample-vs-full divergence",
        "every count and rate is read from the whole table; no sample is used.",
    ),
]
EVIDENCE = {
    "Data quality": [
        f"{n}: {len(p['empty'])} empty, {len(p['constant'])} constant columns"
        for n, p in PROFILES.items()
    ],
    "Structure and engineering": [
        f"{n}: {len(shape_lists(p, KINDS[n])['near_unique'])} near-unique text columns"
        for n, p in PROFILES.items()
    ],
    "Temporal": [f"{n}: {len(c)} date-like columns" for n, c in DATE_COLUMNS.items()],
    "Relationships": [f"{n}: {len(f)} link columns" for n, f in LINK_FILL.items()],
}
blocks = [
    ("Profile", profile_block(PROFILES)),
    ("Data Quality", quality_block(PROFILES)),
    ("Entities / Keys", key_block(KEYS)),
    ("Coverage & Sampling Bias", coverage_block(COVERAGE)),
    ("Relationships", fill_lines(LINK_FILL)),
    ("Temporal Semantics", fill_lines(DATE_COLUMNS)),
    ("Distributions", distribution_block(PROFILES, KINDS, BOOLEANS, NUMERICS)),
    ("EDA Findings", table_findings_lines(PROFILES, KEYS)),
    ("ML-Readiness Evidence", ml_readiness_block(READINESS)),
    ("Observations by Area", area_block(EVIDENCE)),
]
write_profiling(SOURCE, NB_KEY, SECTION_TITLE, blocks, FIGURES)
