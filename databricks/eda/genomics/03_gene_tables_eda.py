# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # EDA -- GENOMICS GENE TABLES
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** November 2026
# MAGIC
# MAGIC **Purpose:** Profile the gene tables of genomics Gold (`gene_profile`,
# MAGIC `gene_clinical_profile`, `gene_cancer_profile`, `gene_drug_profile`,
# MAGIC `gene_expression_profile`, `gene_disease_profile`): nulls, constant columns,
# MAGIC key, rates per column kind, coverage by source, the genes each table covers,
# MAGIC the diseases per gene, and which gene columns carry the pathogenic or benign
# MAGIC class of the variants of the gene -- as evidence.

# COMMAND ----------

# MAGIC %run ../_eda_common

# COMMAND ----------

# DBTITLE 1,Configuration
CATALOG = "energy_commerce_retail_media"
SCHEMA = "genomics_gold"
SOURCE = "genomics"
NB_KEY = "03_gene_tables"
SECTION_TITLE = "Gene tables and the variant class they carry"
GENE = "gene_profile"
LINKS = "gene_disease_profile"
TABLES = [
    GENE,
    "gene_clinical_profile",
    "gene_cancer_profile",
    "gene_drug_profile",
    "gene_expression_profile",
    LINKS,
]
CLINICAL = "variant_clinical_profile"
POSITIVE = "is_pathogenic"
NEGATIVE = "is_benign"

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

# DBTITLE 1,Genes of gene_profile that a table does not cover
GENE_COVERAGE = {}
for name in TABLES:
    if name == GENE:
        continue
    GENE_COVERAGE[name] = key_overlap(
        FRAMES[GENE], "gene_key", FRAMES[name], "gene_key", normalise=False
    )
    print(name, GENE_COVERAGE[name])

# COMMAND ----------

# DBTITLE 1,Diseases per gene and repeated gene-disease pairs
PER_GENE = group_size_shares(FRAMES[LINKS], ["gene_key"])
PER_PAIR = group_size_shares(FRAMES[LINKS], ["gene_key", "disease_key"])
print("rows per gene:", PER_GENE)
print("rows per gene and disease:", PER_PAIR)

# COMMAND ----------

# DBTITLE 1,Class slice of the variants, with the gene key
CLINICAL_FRAME = spark.table(f"{CATALOG}.{SCHEMA}.{CLINICAL}")
LABEL = with_target(CLINICAL_FRAME, POSITIVE, NEGATIVE).select("gene_key", "__pos")
print(f"labelled variants: {LABEL.count():,}")

# COMMAND ----------

# DBTITLE 1,Association with the variant class -- tables with gene_key as their key
ASSOC = {}
for name in TABLES:
    key = KEYS[name]
    if key["key"] != "gene_key" or not key["unique"]:
        print(f"{name}: gene_key is not its unique key, no association")
        continue
    joined = FRAMES[name].join(LABEL, "gene_key")
    flags = KINDS[name]["boolean"]
    scores = KINDS[name]["numeric"]
    flag_assoc = flag_vs_label(joined, flags)
    score_assoc = score_vs_label(joined, scores)
    ranked = [c for c, _ in top_by(score_assoc, "correlation", PAIR_COLUMNS)]
    ASSOC[name] = {
        "rows": joined.count(),
        "flags": flag_assoc,
        "scores": score_assoc,
        "pairs": pair_correlations(joined, ranked),
    }
    print(name, "labelled variants:", ASSOC[name]["rows"])

# COMMAND ----------

# DBTITLE 1,Gene columns that separate the variant class almost fully
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

# DBTITLE 1,Figure -- genes of gene_profile missing from each table
drawn = barplot(
    [(n, m["a_not_in_b"]) for n, m in GENE_COVERAGE.items()],
    "Genes of gene_profile without a row in the table",
    "table",
    ylabel="genes",
    rot=20,
    filename="genomics_03_gene_coverage.png",
)
FIGURES = (
    [
        (
            "Genes of gene_profile without a row in the table",
            "genomics_03_gene_coverage.png",
        )
    ]
    if drawn
    else []
)

# COMMAND ----------

# DBTITLE 1,Export profiling findings
coverage_lines = [
    para(
        f"- {name}: genes of `{GENE}` ({m['a_distinct']:,}) without a row here:",
        f"{m['a_not_in_b']:,} ({pct(1 - m['a_share_in_b'] if m['a_share_in_b'] is not None else None)}).",
    )
    for name, m in GENE_COVERAGE.items()
]
link_lines = [
    "- rows per gene, share of genes: "
    + ", ".join(f"{k} {v}" for k, v in PER_GENE.items() if k != "groups"),
    f"- genes in `{LINKS}`: {PER_GENE['groups']:,}",
    "- rows per gene and disease, share of pairs: "
    + ", ".join(f"{k} {v}" for k, v in PER_PAIR.items() if k != "groups"),
    f"- gene and disease pairs: {PER_PAIR['groups']:,}",
]
association = "\n".join(
    association_lines(
        f"{name} ({a['rows']:,} labelled variants)", a["flags"], a["scores"]
    )
    + "\n- numeric pairs at or above the limit: "
    + f"{len(a['pairs'])}"
    for name, a in ASSOC.items()
)
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
            f"`{LINKS}` has {PER_GENE['groups']:,} genes and {PER_PAIR['groups']:,}",
            "gene and disease pairs; a join on gene_key repeats a gene by its rows.",
        ),
    ),
    (
        "Target contamination",
        para(
            "gene columns computed from the labels of the variants of the gene:",
            "; ".join(
                f"{n}: {len(v['flags'])} flags, {len(v['scores'])} numeric columns"
                for n, v in LEAKING.items()
            ),
        ),
    ),
    (
        "Proxy leakage",
        "; ".join(
            f"{n}: " + ", ".join((v["flags"] + v["scores"])[:10])
            for n, v in LEAKING.items()
            if v["flags"] or v["scores"]
        )
        or "no gene flag or numeric column reaches the limits.",
    ),
    (
        "Split / entity leakage",
        para(
            "one gene has many variants; a gene column joined to a variant model",
            "repeats the gene in every split unless the split is by gene.",
        ),
    ),
    (
        "Survivorship / coverage bias",
        "; ".join(
            f"{n}: {m['a_not_in_b']:,} genes of `{GENE}` not covered"
            for n, m in GENE_COVERAGE.items()
        ),
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
    "Relationships": [
        f"{n}: {m['a_not_in_b']:,} genes of `{GENE}` without a row"
        for n, m in GENE_COVERAGE.items()
    ],
    "ML use": [
        f"{n}: {len(v['flags']) + len(v['scores'])} gene columns separate the class"
        for n, v in LEAKING.items()
    ],
}
blocks = [
    ("Profile", profile_block(PROFILES)),
    ("Data Quality", quality_block(PROFILES)),
    ("Entities / Keys", key_block(KEYS)),
    (
        "Coverage & Sampling Bias",
        coverage_block(COVERAGE) + "\n" + "\n".join(coverage_lines),
    ),
    ("Relationships", "\n".join(link_lines)),
    ("Distributions", distribution_block(PROFILES, KINDS, BOOLEANS, NUMERICS)),
    ("Association with the variant class", association),
    ("EDA Findings", table_findings_lines(PROFILES, KEYS)),
    ("ML-Readiness Evidence", ml_readiness_block(READINESS)),
    ("Observations by Area", area_block(EVIDENCE)),
]
write_profiling(SOURCE, NB_KEY, SECTION_TITLE, blocks, FIGURES)
