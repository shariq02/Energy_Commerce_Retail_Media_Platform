# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # EDA -- GENOMICS VARIANT FEATURE TABLES
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** November 2026
# MAGIC
# MAGIC **Purpose:** Profile the variant feature tables of genomics Gold
# MAGIC (`variant_profile`, `structural_variant_profile`): nulls, constant
# MAGIC columns, key, rates per column kind, coverage by source, and which flags and
# MAGIC scores carry the pathogenic or benign class of `variant_profile` -- as
# MAGIC evidence.

# COMMAND ----------

# MAGIC %run ../_eda_common

# COMMAND ----------

# DBTITLE 1,Configuration
CATALOG = "energy_commerce_retail_media"
SCHEMA = "genomics_gold"
SOURCE = "genomics"
NB_KEY = "02_variant_features"
SECTION_TITLE = "Variant feature tables and the class they carry"
TABLES = ["variant_profile", "structural_variant_profile"]
CLINICAL = "variant_profile"
POSITIVE = "is_pathogenic"
NEGATIVE = "is_benign"
NOT_FEATURES = {"position", "end_position"}

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

# DBTITLE 1,Columns by null-rate band
BANDS = {}
for name, p in PROFILES.items():
    rates = [r for r in p["null_rate"].values() if r is not None]
    BANDS[name] = {
        "under 1%": sum(r < 0.01 for r in rates),
        "1% to 50%": sum(0.01 <= r < 0.5 for r in rates),
        "50% to under 100%": sum(0.5 <= r < 1.0 for r in rates),
        "100%": sum(r >= 1.0 for r in rates),
    }
    print(name, BANDS[name])

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

# DBTITLE 1,Class slice of variant_profile
CLINICAL_FRAME = spark.table(f"{CATALOG}.{SCHEMA}.{CLINICAL}")
LABEL = with_target(CLINICAL_FRAME, POSITIVE, NEGATIVE).select("variant_key", "__pos")
LABEL_ROWS = LABEL.count()
LABEL_KEYS = LABEL.select("variant_key").distinct().count()
if LABEL_ROWS != LABEL_KEYS:
    raise ValueError(f"variant_key repeats in the class slice: {LABEL_ROWS} rows")
print(f"labelled variants: {LABEL_ROWS:,}")

# COMMAND ----------

# DBTITLE 1,Association with the class -- every table with variant_key as its key
ASSOC = {}
for name, frame in FRAMES.items():
    key = KEYS[name]
    if key["key"] != "variant_key" or not key["unique"]:
        print(f"{name}: variant_key is not its unique key, no association")
        continue
    joined = frame.join(LABEL, "variant_key")
    flags = KINDS[name]["boolean"]
    scores = [c for c in KINDS[name]["numeric"] if c not in NOT_FEATURES]
    flag_assoc = flag_vs_label(joined, flags)
    score_assoc = score_vs_label(joined, scores)
    ranked = [c for c, _ in top_by(score_assoc, "correlation", PAIR_COLUMNS)]
    ASSOC[name] = {
        "rows": joined.count(),
        "flags": flag_assoc,
        "scores": score_assoc,
        "pairs": pair_correlations(joined, ranked),
    }
    print(
        name, "labelled rows:", ASSOC[name]["rows"], "pairs:", len(ASSOC[name]["pairs"])
    )

# COMMAND ----------

# DBTITLE 1,Columns that separate the class almost fully
LEAKING = {}
for name, a in ASSOC.items():
    LEAKING[name] = {
        "flags": [
            c for c, v in top_by(a["flags"], "separation") if v >= FLAG_SEPARATION
        ],
        "scores": [
            c
            for c, v in top_by(a["scores"], "correlation")
            if abs(v) >= LEAK_CORRELATION
        ],
        "gap": [c for c, v in top_by(a["scores"], "missing_gap") if v >= MISSING_GAP],
    }
    print(name, {k: len(v) for k, v in LEAKING[name].items()})

# COMMAND ----------

# DBTITLE 1,Figure -- empty and constant columns per table
drawn = barplot(
    [(n, len(p["empty"]) + len(p["constant"])) for n, p in PROFILES.items()],
    "Empty and constant columns per table",
    "table",
    ylabel="columns",
    rot=20,
    filename="genomics_02_empty_constant_columns.png",
)
FIGURES = (
    [("Empty and constant columns per table", "genomics_02_empty_constant_columns.png")]
    if drawn
    else []
)

# COMMAND ----------

# DBTITLE 1,Export profiling findings
band_lines = [
    f"- {name}: " + ", ".join(f"{label} {count}" for label, count in bands.items())
    for name, bands in BANDS.items()
]
association = "\n".join(
    association_lines(f"{name} ({a['rows']:,} labelled rows)", a["flags"], a["scores"])
    + "\n- numeric pairs at or above the limit: "
    + f"{len(a['pairs'])}"
    + "\n"
    + fmt_pairs([(f"{x} / {y}", r) for x, y, r in a["pairs"]], LIST_LIMIT)
    for name, a in ASSOC.items()
)
labelled_share = {
    name: (
        round(a["rows"] / PROFILES[name]["rows"], 4) if PROFILES[name]["rows"] else None
    )
    for name, a in ASSOC.items()
}
READINESS = [
    (
        "Grain / grain drift",
        "; ".join(
            f"`{n}` key `{k['key']}` unique: {k.get('unique')}" for n, k in KEYS.items()
        ),
    ),
    (
        "Join multiplication (1:N / M:N expansion)",
        para(
            f"the class slice has one row per variant_key ({LABEL_ROWS:,} rows);",
            "tables joined to it: " + ", ".join(ASSOC) + ".",
        ),
    ),
    (
        "Target contamination",
        "; ".join(
            f"{n}: {len(v['flags'])} flags, {len(v['scores'])} numeric columns"
            for n, v in LEAKING.items()
        ),
    ),
    (
        "Proxy leakage",
        "; ".join(
            f"{n}: " + ", ".join((v["flags"] + v["scores"])[:10])
            for n, v in LEAKING.items()
            if v["flags"] or v["scores"]
        )
        or "no flag or numeric column reaches the limits.",
    ),
    (
        "Survivorship / coverage bias",
        "labelled share of each table: "
        + ", ".join(f"{n} {pct(s)}" for n, s in labelled_share.items()),
    ),
    (
        "Missingness leakage",
        "; ".join(f"{n}: {len(v['gap'])} columns" for n, v in LEAKING.items()),
    ),
    (
        "Duplicate-event leakage",
        "; ".join(
            f"{n} dup groups {(k.get('duplicates') or {}).get('dup_groups', 0)}"
            for n, k in KEYS.items()
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
    "Statistical patterns": [
        f"{n}: {len(a['pairs'])} numeric pairs above {LEAK_CORRELATION}"
        for n, a in ASSOC.items()
    ],
    "ML use": [
        f"{n}: {len(v['flags']) + len(v['scores'])} columns separate the class"
        for n, v in LEAKING.items()
    ],
}
blocks = [
    ("Profile", profile_block(PROFILES)),
    (
        "Data Quality",
        quality_block(PROFILES)
        + "\n\n**Columns by null-rate band**\n"
        + "\n".join(band_lines),
    ),
    ("Entities / Keys", key_block(KEYS)),
    ("Coverage & Sampling Bias", coverage_block(COVERAGE)),
    ("Distributions", distribution_block(PROFILES, KINDS, BOOLEANS, NUMERICS)),
    ("Association with the class", association),
    ("EDA Findings", table_findings_lines(PROFILES, KEYS)),
    ("ML-Readiness Evidence", ml_readiness_block(READINESS)),
    ("Observations by Area", area_block(EVIDENCE)),
]
write_profiling(SOURCE, NB_KEY, SECTION_TITLE, blocks, FIGURES)
