# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # EDA -- GENOMICS (GOLD TABLES)
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** November 2026
# MAGIC
# MAGIC **Purpose:** exploratory analysis of the genomics data in one notebook. Each section profiles one group of
# MAGIC tables and writes its findings to `src/schemas/profiling/<source>.md` (one section key per group).

# COMMAND ----------

# MAGIC %run ./_eda_common

# COMMAND ----------

# MAGIC %md
# MAGIC ## EDA -- GENOMICS VARIANT LABEL AND BALANCE
# MAGIC
# MAGIC **Purpose:** Profile `variant_profile` and
# MAGIC `clinvar_classification_change` in genomics Gold: nulls, constant columns, key,
# MAGIC the label text and the class flags, class balance, review quality against the
# MAGIC class, flag and score association with the class, snapshot history and the
# MAGIC variants whose term changed -- as evidence.

# COMMAND ----------

# DBTITLE 1,Configuration, tables and standard profile
from pyspark.sql import functions as F

CATALOG = "energy_commerce_retail_media"
SCHEMA = "genomics_gold"
SOURCE = "genomics"
NB_KEY = "01_variant_label_and_balance"
SECTION_TITLE = "Variant label and balance (variant_profile)"
CLINICAL = "variant_profile"
CHANGES = "clinvar_classification_change"
TABLES = [CLINICAL, CHANGES]
LABEL_TEXT = "clinical_significance_label"
POSITIVE = "is_pathogenic"
NEGATIVE = "is_benign"
CLASS_FLAGS = (POSITIVE, NEGATIVE, "is_vus", "has_no_review_stars")
OWN_LABEL_COLUMNS = {LABEL_TEXT, *CLASS_FLAGS}
print(f"OK  repo root: {_repo_root()}")
print(f"OK  profiling directory: {_profiling_dir()}")
FRAMES, *STD = open_group(CATALOG, SCHEMA, TABLES, coverage=False)
KINDS, PROFILES, KEYS, BOOLEANS, NUMERICS, COVERAGE = STD

# COMMAND ----------

# DBTITLE 1,Label text -- rows per value
LABEL_COUNTS = value_counts(FRAMES[CLINICAL], LABEL_TEXT)
for value, rows, share in LABEL_COUNTS:
    print(f"  {value:<45} {rows:>12,}  {share:.4f}")

# COMMAND ----------

# DBTITLE 1,Class flags against the label text
flag_sums = [F.sum(F.when(qcol(f), 1).otherwise(0)).alias(f) for f in CLASS_FLAGS]
CROSSTAB = (
    FRAMES[CLINICAL]
    .groupBy(qcol(LABEL_TEXT).cast("string").alias("label"))
    .agg(F.count(F.lit(1)).alias("rows"), *flag_sums)
    .orderBy(F.desc("rows"))
    .collect()
)
flags_on = sum(F.when(qcol(f), 1).otherwise(0) for f in CLASS_FLAGS)
OVERLAP = (
    FRAMES[CLINICAL]
    .agg(
        F.count(F.lit(1)).alias("rows"),
        F.sum(F.when(flags_on == 0, 1).otherwise(0)).alias("no_flag"),
        F.sum(F.when(flags_on > 1, 1).otherwise(0)).alias("several_flags"),
    )
    .first()
    .asDict()
)
print("rows with no class flag:", OVERLAP["no_flag"])
print("rows with several class flags:", OVERLAP["several_flags"])
for row in CROSSTAB:
    print(row.asDict())

# COMMAND ----------

# DBTITLE 1,Class balance -- pathogenic against benign
TARGET = with_target(FRAMES[CLINICAL], POSITIVE, NEGATIVE)
balance = TARGET.agg(
    F.count(F.lit(1)).alias("known"), F.sum("__pos").alias("positive")
).first()
KNOWN = balance["known"] or 0
POSITIVE_ROWS = int(balance["positive"] or 0)
NEGATIVE_ROWS = KNOWN - POSITIVE_ROWS
IMBALANCE = (
    round(max(POSITIVE_ROWS, NEGATIVE_ROWS) / min(POSITIVE_ROWS, NEGATIVE_ROWS), 3)
    if POSITIVE_ROWS and NEGATIVE_ROWS
    else None
)
print(f"rows with a pathogenic or benign class: {KNOWN:,}")
print(f"pathogenic {POSITIVE_ROWS:,}  benign {NEGATIVE_ROWS:,}  ratio {IMBALANCE}")

# COMMAND ----------

# DBTITLE 1,Review quality -- all rows and the rate of pathogenic per score
QUALITY_ALL = value_counts(FRAMES[CLINICAL], "review_quality_score")
QUALITY_RATE = (
    TARGET.groupBy(qcol("review_quality_score").alias("score"))
    .agg(F.count(F.lit(1)).alias("rows"), F.avg("__pos").alias("pathogenic_rate"))
    .orderBy("score")
    .collect()
)
for value, rows, share in QUALITY_ALL:
    print(f"  score {value:<10} {rows:>12,}  {share:.4f}")
for row in QUALITY_RATE:
    print(row.asDict())

# COMMAND ----------

# DBTITLE 1,Association with the class -- flags, numeric columns and pairs
FLAGS = [c for c in KINDS[CLINICAL]["boolean"] if c not in OWN_LABEL_COLUMNS]
SCORES = [c for c in KINDS[CLINICAL]["numeric"] if c != "position"]
FLAG_ASSOC = flag_vs_label(TARGET, FLAGS)
SCORE_ASSOC = score_vs_label(TARGET, SCORES)
LEAK = leaking_columns(FLAG_ASSOC, SCORE_ASSOC)
print("flags above the separation limit:", LEAK["flags"])
print("numeric columns above the correlation limit:", LEAK["scores"])
RANKED = [c for c, _ in top_by(SCORE_ASSOC, "correlation", PAIR_COLUMNS)]
PAIRS = pair_correlations(TARGET, RANKED)
print(len(PAIRS), "pairs; first 10:", PAIRS[:10])

# COMMAND ----------

# DBTITLE 1,Snapshot history and variants whose term changed
SNAPSHOTS = (
    FRAMES[CLINICAL]
    .agg(
        F.count(F.lit(1)).alias("rows"),
        F.sum(
            F.when(
                qcol("clinvar_first_snapshot") == qcol("clinvar_last_snapshot"), 1
            ).otherwise(0)
        ).alias("single_snapshot"),
        F.sum(F.when(qcol("clinvar_snapshot_count") > 1, 1).otherwise(0)).alias(
            "several_snapshots"
        ),
        F.sum(F.when(qcol("in_current_clinvar"), 1).otherwise(0)).alias("current"),
    )
    .first()
    .asDict()
)
print(SNAPSHOTS)
CHANGED_KEYS = FRAMES[CHANGES].select("variant_key").distinct()
CHANGED_CLASS = (
    CHANGED_KEYS.join(TARGET.select("variant_key", "__pos"), "variant_key")
    .agg(F.count(F.lit(1)).alias("labelled"), F.sum("__pos").alias("pathogenic"))
    .first()
)
CHANGED = {
    "variants": CHANGED_KEYS.count(),
    "labelled": CHANGED_CLASS["labelled"] or 0,
    "pathogenic": int(CHANGED_CLASS["pathogenic"] or 0),
}
TRANSITIONS = (
    FRAMES[CHANGES]
    .groupBy(
        qcol("previous_clinical_significance_simple").alias("previous"),
        qcol("clinical_significance_simple").alias("current"),
    )
    .count()
    .orderBy(F.desc("count"))
    .limit(LIST_LIMIT)
    .collect()
)
print(CHANGED)
for row in TRANSITIONS:
    print(row.asDict())

# COMMAND ----------

# DBTITLE 1,Export profiling findings
labels_drawn = barplot(
    [(v, n) for v, n, _ in LABEL_COUNTS],
    "Rows per clinical significance label",
    "label",
    rot=45,
    logy=True,
    filename="genomics_01_label_counts.png",
)
FIGURES = (
    [("Rows per clinical significance label", "genomics_01_label_counts.png")]
    if labels_drawn
    else []
)
rows_clinical = PROFILES[CLINICAL]["rows"]
changed_share = round(CHANGED["variants"] / rows_clinical, 4) if rows_clinical else None
label_lines = [
    f"- rows per label text value (top {len(LABEL_COUNTS)}):",
    fmt_pairs([(v, f"{n:,} ({pct(s)})") for v, n, s in LABEL_COUNTS]),
    para(
        f"- rows with no class flag: {OVERLAP['no_flag']:,};",
        f"rows with several class flags: {OVERLAP['several_flags']:,}.",
    ),
    "- flag counts per label text value (label, rows, " + ", ".join(CLASS_FLAGS) + "):",
    fmt_pairs([(r["label"], [r[c] for c in ("rows", *CLASS_FLAGS)]) for r in CROSSTAB]),
    para(
        f"- rows with a pathogenic or benign class: {KNOWN:,}",
        f"({pct(KNOWN / rows_clinical if rows_clinical else None)});",
        f"pathogenic {POSITIVE_ROWS:,}, benign {NEGATIVE_ROWS:,},",
        f"ratio {IMBALANCE}.",
    ),
    "- review quality score, all rows:",
    fmt_pairs([(v, f"{n:,} ({pct(s)})") for v, n, s in QUALITY_ALL]),
    "- review quality score, pathogenic rate among labelled rows:",
    fmt_pairs(
        [
            (r["score"], f"{r['rows']:,} rows, {pct(r['pathogenic_rate'])}")
            for r in QUALITY_RATE
        ]
    ),
]
history_lines = [
    para(
        f"- rows with one ClinVar snapshot only: {SNAPSHOTS['single_snapshot']:,};",
        f"with several snapshots: {SNAPSHOTS['several_snapshots']:,};",
        f"in the current ClinVar file: {SNAPSHOTS['current']:,}.",
    ),
    para(
        f"- variants with a term change: {CHANGED['variants']:,}",
        f"({pct(changed_share)} of rows); labelled among them: {CHANGED['labelled']:,},",
        f"pathogenic: {CHANGED['pathogenic']:,}.",
    ),
    "- most frequent changes (previous, current, rows):",
    fmt_pairs(
        [((r["previous"], r["current"]), f"{r['count']:,}") for r in TRANSITIONS]
    ),
]
export_group(
    SOURCE,
    NB_KEY,
    SECTION_TITLE,
    STD,
    extra_blocks=[
        ("Label and Balance", "\n".join(label_lines)),
        (
            "Association with the class",
            association_lines(CLINICAL, FLAG_ASSOC, SCORE_ASSOC),
        ),
        ("Snapshot History and Term Changes", "\n".join(history_lines)),
    ],
    readiness=[
        (
            "Target contamination",
            para(
                f"flags with a separation of {FLAG_SEPARATION} or more:",
                f"{len(LEAK['flags'])} ({', '.join(LEAK['flags'][:10])}); the class",
                "flags and the label text are in the same table; rows with several",
                f"class flags: {OVERLAP['several_flags']:,}.",
            ),
        ),
        (
            "Proxy leakage",
            para(
                f"numeric columns with a correlation of {LEAK_CORRELATION} or more",
                f"with the class: {len(LEAK['scores'])}",
                f"({', '.join(LEAK['scores'][:10])}); numeric pairs above the limit",
                f"among the top columns: {len(PAIRS)}.",
            ),
        ),
        (
            "Temporal / post-event leakage",
            para(
                f"{SNAPSHOTS['single_snapshot']:,} rows have one snapshot only;",
                f"{SNAPSHOTS['several_snapshots']:,} rows span several snapshots.",
            ),
        ),
        (
            "Historical-reference (point-in-time) leakage",
            para(
                "the table carries the first and last snapshot of each variant;",
                "the label has no date of its own; rows in the current file:",
                f"{SNAPSHOTS['current']:,}.",
            ),
        ),
        (
            "Missingness leakage",
            para(
                f"numeric columns whose missing rate differs by {MISSING_GAP} or",
                f"more between the classes: {len(LEAK['gap'])}",
                f"({', '.join(LEAK['gap'][:10])}).",
            ),
        ),
        (
            "Class / label instability",
            para(
                f"{CHANGED['variants']:,} variants ({pct(changed_share)} of rows)",
                "have a term change between snapshots.",
            ),
        ),
        (
            "Source / version / regime change",
            para(
                f"{SNAPSHOTS['several_snapshots']:,} rows carry more than one",
                "ClinVar snapshot.",
            ),
        ),
    ],
    evidence={
        "Statistical patterns": [
            f"pathogenic to benign ratio {IMBALANCE}",
            f"{len(PAIRS)} numeric pairs above {LEAK_CORRELATION}",
        ],
        "Temporal": [f"{SNAPSHOTS['several_snapshots']:,} rows span several snapshots"],
        "ML use": [
            para(
                f"{KNOWN:,} labelled rows; {len(LEAK['flags'])} flags and",
                f"{len(LEAK['scores'])} numeric columns separate the class almost fully",
            )
        ],
    },
    figures=FIGURES,
    links=False,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## EDA -- GENOMICS VARIANT FEATURE TABLES
# MAGIC
# MAGIC **Purpose:** Profile the variant feature tables of genomics Gold
# MAGIC (`variant_profile`, `structural_variant_profile`): nulls, constant
# MAGIC columns, key, rates per column kind, coverage by source, and which flags and
# MAGIC scores carry the pathogenic or benign class of `variant_profile` -- as
# MAGIC evidence.

# COMMAND ----------

# DBTITLE 1,Configuration, tables, standard profile and null-rate bands
CATALOG = "energy_commerce_retail_media"
SCHEMA = "genomics_gold"
SOURCE = "genomics"
NB_KEY = "02_variant_features"
SECTION_TITLE = "Variant feature tables and the class they carry"
TABLES = ["variant_profile", "structural_variant_profile"]
CLINICAL = "variant_profile"
POSITIVE = "is_pathogenic"
NEGATIVE = "is_benign"
OWN_COLUMNS = {
    "position",
    "end_position",
    "clinical_significance_label",
    "is_pathogenic",
    "is_benign",
    "is_vus",
    "has_no_review_stars",
}
FRAMES, *STD = open_group(CATALOG, SCHEMA, TABLES)
KINDS, PROFILES, KEYS, BOOLEANS, NUMERICS, COVERAGE = STD
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

# DBTITLE 1,Class slice and association with the class
LABEL = with_target(spark.table(f"{CATALOG}.{SCHEMA}.{CLINICAL}"), POSITIVE, NEGATIVE)
LABEL = LABEL.select("variant_key", "__pos")
LABEL_ROWS = LABEL.count()
if LABEL_ROWS != LABEL.select("variant_key").distinct().count():
    raise ValueError(f"variant_key repeats in the class slice: {LABEL_ROWS} rows")
print(f"labelled variants: {LABEL_ROWS:,}")
ASSOC, LEAKING = class_association(
    FRAMES, TABLES, KINDS, "variant_key", LABEL, skip=OWN_COLUMNS
)

# COMMAND ----------

# DBTITLE 1,Export profiling findings
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
band_lines = [
    f"- {name}: " + ", ".join(f"{label} {count}" for label, count in bands.items())
    for name, bands in BANDS.items()
]
labelled_share = {
    name: (
        round(a["rows"] / PROFILES[name]["rows"], 4) if PROFILES[name]["rows"] else None
    )
    for name, a in ASSOC.items()
}
export_group(
    SOURCE,
    NB_KEY,
    SECTION_TITLE,
    STD,
    extra_blocks=[
        ("Association with the class", association_block(ASSOC, "rows", True))
    ],
    quality_extra="**Columns by null-rate band**\n" + "\n".join(band_lines),
    readiness=[
        (
            "Join multiplication (1:N / M:N expansion)",
            para(
                f"the class slice has one row per variant_key ({LABEL_ROWS:,} rows);",
                "tables joined to it: " + ", ".join(ASSOC) + ".",
            ),
        ),
        *leak_entries(LEAKING),
        (
            "Survivorship / coverage bias",
            "labelled share of each table: "
            + ", ".join(f"{n} {pct(s)}" for n, s in labelled_share.items()),
        ),
    ],
    evidence={
        "Statistical patterns": [
            f"{n}: {len(a['pairs'])} numeric pairs above {LEAK_CORRELATION}"
            for n, a in ASSOC.items()
        ],
        "ML use": [
            f"{n}: {len(v['flags']) + len(v['scores'])} columns separate the class"
            for n, v in LEAKING.items()
        ],
    },
    figures=FIGURES,
    links=False,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## EDA -- GENOMICS GENE TABLES
# MAGIC
# MAGIC **Purpose:** Profile the gene tables of genomics Gold (`gene_profile`,
# MAGIC `gene_disease_profile`, `relation_record`): nulls, constant columns,
# MAGIC key, rates per column kind, coverage by source, the genes each table covers,
# MAGIC the diseases per gene, and which gene columns carry the pathogenic or benign
# MAGIC class of the variants of the gene -- as evidence.

# COMMAND ----------

# DBTITLE 1,Configuration, tables, standard profile and gene coverage
CATALOG = "energy_commerce_retail_media"
SCHEMA = "genomics_gold"
SOURCE = "genomics"
NB_KEY = "03_gene_tables"
SECTION_TITLE = "Gene tables and the variant class they carry"
GENE = "gene_profile"
LINKS = "relation_record"
TABLES = [GENE, "gene_disease_profile", LINKS]
CLINICAL = "variant_profile"
POSITIVE = "is_pathogenic"
NEGATIVE = "is_benign"
FRAMES, *STD = open_group(CATALOG, SCHEMA, TABLES)
KINDS, PROFILES, KEYS, BOOLEANS, NUMERICS, COVERAGE = STD
GENE_COVERAGE = {}
for name in TABLES:
    if name == GENE:
        continue
    GENE_COVERAGE[name] = key_overlap(
        FRAMES[GENE], "gene_key", FRAMES[name], "gene_key", normalise=False
    )
    print(name, GENE_COVERAGE[name])
PER_GENE = group_size_shares(FRAMES[LINKS], ["gene_key"])
PER_PAIR = group_size_shares(FRAMES[LINKS], ["gene_key", "disease_key"])
print("rows per gene:", PER_GENE)
print("rows per gene and disease:", PER_PAIR)

# COMMAND ----------

# DBTITLE 1,Class slice of the variants and association with the variant class
LABEL = with_target(spark.table(f"{CATALOG}.{SCHEMA}.{CLINICAL}"), POSITIVE, NEGATIVE)
LABEL = LABEL.select("gene_key", "__pos")
print(f"labelled variants: {LABEL.count():,}")
ASSOC, LEAKING = class_association(FRAMES, TABLES, KINDS, "gene_key", LABEL)

# COMMAND ----------

# DBTITLE 1,Export profiling findings
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
export_group(
    SOURCE,
    NB_KEY,
    SECTION_TITLE,
    STD,
    coverage_extra="\n".join(coverage_lines),
    extra_blocks=[
        ("Genes and Diseases", "\n".join(link_lines)),
        ("Association with the variant class", association_block(ASSOC, "variants")),
    ],
    readiness=[
        (
            "Join multiplication (1:N / M:N expansion)",
            para(
                f"`{LINKS}` has {PER_GENE['groups']:,} genes and {PER_PAIR['groups']:,}",
                "gene and disease pairs; a join on gene_key repeats a gene by its rows.",
            ),
        ),
        *leak_entries(
            LEAKING,
            prefix="gene columns computed from the labels of the variants of the gene:",
            noun="gene ",
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
    ],
    evidence={
        "Relationships": [
            f"{n}: {m['a_not_in_b']:,} genes of `{GENE}` without a row"
            for n, m in GENE_COVERAGE.items()
        ],
        "ML use": [
            f"{n}: {len(v['flags']) + len(v['scores'])} gene columns separate the class"
            for n, v in LEAKING.items()
        ],
    },
    figures=FIGURES,
    links=False,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## EDA -- GENOMICS DISEASE, DRUG, PROTEIN AND OTHER ENTITY TABLES
# MAGIC
# MAGIC **Purpose:** Profile the entity tables of genomics Gold (`disease_profile`,
# MAGIC `drug_profile`, `protein_profile`, `transcript_profile`, `epitope_profile`,
# MAGIC `genetic_test_profile`, `clinical_trial_profile`): nulls, constant columns,
# MAGIC key, rates per column kind, coverage by source, the fill of the link columns,
# MAGIC date-like columns and the shape of the text columns -- as evidence.

# COMMAND ----------

# DBTITLE 1,Standard EDA of the entity tables
run_groups(
    "energy_commerce_retail_media",
    "genomics",
    [
        {
            "schema": "genomics_gold",
            "nb_key": "04_disease_drug_protein_entities",
            "title": "Disease, drug, protein, transcript, epitope, test and trial tables",
            "tables": [
                "disease_profile",
                "drug_profile",
                "protein_profile",
                "transcript_profile",
                "epitope_profile",
                "genetic_test_profile",
                "clinical_trial_profile",
            ],
        }
    ],
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## EDA -- GENOMICS CANCER AND SOMATIC TABLES
# MAGIC
# MAGIC **Purpose:** Profile the cancer tables of genomics Gold (`cell_line_profile`,
# MAGIC `tumor_sample_profile`, `somatic_mutation_profile`, `somatic_site_profile`):
# MAGIC nulls, constant columns, key, rates per column kind, coverage by source, the
# MAGIC fill of the link columns, mutations per sample and per gene, and consistency
# MAGIC checks inside the somatic mutation rows -- as evidence.

# COMMAND ----------

# DBTITLE 1,Configuration, tables, standard profile and mutations per sample and gene
CATALOG = "energy_commerce_retail_media"
SCHEMA = "genomics_gold"
SOURCE = "genomics"
NB_KEY = "05_cancer_and_somatic"
SECTION_TITLE = "Cell line, tumor sample and somatic mutation tables"
MUTATION = "somatic_mutation_profile"
TABLES = [
    "cell_line_profile",
    "tumor_sample_profile",
    MUTATION,
    "somatic_site_profile",
]
FRAMES, *STD = open_group(CATALOG, SCHEMA, TABLES)
KINDS, PROFILES, KEYS, BOOLEANS, NUMERICS, COVERAGE = STD
LINK_FILL = link_fill(PROFILES)
PER_SAMPLE = group_size_shares(FRAMES[MUTATION], ["sample_barcode"])
PER_GENE = group_size_shares(FRAMES[MUTATION], ["gene_key"])
print("rows per sample:", PER_SAMPLE)
print("rows per gene key:", PER_GENE)

# COMMAND ----------

# DBTITLE 1,Consistency inside the somatic mutation rows
CHECKS = (
    FRAMES[MUTATION]
    .agg(
        F.count(F.lit(1)).alias("rows"),
        F.sum(F.when(qcol("is_snv") & qcol("is_indel"), 1).otherwise(0)).alias(
            "snv_and_indel"
        ),
        F.sum(F.when(qcol("is_missense") & qcol("is_silent"), 1).otherwise(0)).alias(
            "missense_and_silent"
        ),
        F.sum(
            F.when(qcol("tumor_alt_count") > qcol("tumor_depth"), 1).otherwise(0)
        ).alias("alt_count_above_depth"),
        F.sum(
            F.when(
                qcol("tumor_ref_count") + qcol("tumor_alt_count") > qcol("tumor_depth"),
                1,
            ).otherwise(0)
        ).alias("counts_above_depth"),
        F.sum(
            F.when(qcol("end_position") < qcol("start_position"), 1).otherwise(0)
        ).alias("end_before_start"),
    )
    .first()
    .asDict()
)
print(CHECKS)

# COMMAND ----------

# DBTITLE 1,Export profiling findings
rows_mutation = CHECKS["rows"] or 0
check_lines = [
    f"- {label}: {CHECKS[field]:,} ({pct(CHECKS[field] / rows_mutation if rows_mutation else None)})"
    for label, field in (
        ("rows with both the single-nucleotide and the indel flag", "snv_and_indel"),
        ("rows with both the missense and the silent flag", "missense_and_silent"),
        ("rows with an alternate count above the depth", "alt_count_above_depth"),
        (
            "rows with reference plus alternate count above the depth",
            "counts_above_depth",
        ),
        ("rows with an end position before the start position", "end_before_start"),
    )
]
size_lines = [
    "- rows per sample, share of samples: "
    + ", ".join(f"{k} {v}" for k, v in PER_SAMPLE.items() if k != "groups"),
    f"- samples with a mutation: {PER_SAMPLE['groups']:,}",
    "- rows per gene key, share of genes: "
    + ", ".join(f"{k} {v}" for k, v in PER_GENE.items() if k != "groups"),
    f"- gene keys with a mutation: {PER_GENE['groups']:,}",
]
export_group(
    SOURCE,
    NB_KEY,
    SECTION_TITLE,
    STD,
    quality_extra="\n".join(check_lines),
    extra_blocks=[("Mutations per Sample and per Gene", "\n".join(size_lines))],
    readiness=[
        (
            "Split / entity leakage",
            para(
                f"{PER_SAMPLE['groups']:,} samples and {PER_GENE['groups']:,} gene keys",
                f"share {rows_mutation:,} mutation rows; a split by row puts one sample",
                "in several splits.",
            ),
        ),
        (
            "Survivorship / coverage bias",
            para(
                "link columns of the mutation rows:",
                "; ".join(f"{c} null {pct(r)}" for c, r in LINK_FILL[MUTATION].items()),
            ),
        ),
        (
            "Data-generation-process leakage",
            para(
                "rows with reference plus alternate count above the depth:",
                f"{CHECKS['counts_above_depth']:,}; alternate count above the depth:",
                f"{CHECKS['alt_count_above_depth']:,}.",
            ),
        ),
    ],
    evidence={
        "Statistical patterns": [
            f"{PER_SAMPLE['groups']:,} samples carry {rows_mutation:,} mutation rows"
        ]
    },
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## EDA -- GENOMICS BENCHMARK TABLES
# MAGIC
# MAGIC **Purpose:** Profile the benchmark tables of genomics Gold
# MAGIC (`benchmark_variant`, `dms_assay`, `dms_assay_mutant`,
# MAGIC `embedding_split`): nulls, constant columns, key, rates per column kind,
# MAGIC coverage by source, label balance per split, keys that sit in more than one
# MAGIC split, overlap with the labelled ClinVar variants, and the assay rows against
# MAGIC their mutant rows -- as evidence.

# COMMAND ----------

# DBTITLE 1,Configuration, tables, standard profile and label balance per split
CATALOG = "energy_commerce_retail_media"
SCHEMA = "genomics_gold"
SOURCE = "genomics"
NB_KEY = "06_benchmarks"
SECTION_TITLE = "Benchmark, assay and embedding split tables"
BENCHMARKS = ["benchmark_variant"]
ASSAY = "dms_assay"
MUTANT = "dms_assay_mutant"
TABLES = [*BENCHMARKS, ASSAY, MUTANT, "embedding_split"]
CLINICAL = "variant_profile"
SPLIT_KEYS = ("variant_key", "protein_id", "gene")
FRAMES, *STD = open_group(CATALOG, SCHEMA, TABLES)
KINDS, PROFILES, KEYS, BOOLEANS, NUMERICS, COVERAGE = STD
LABELS = {}
for name in BENCHMARKS:
    rows = (
        FRAMES[name]
        .groupBy(
            qcol("split").cast("string").alias("split"),
            qcol("label").cast("string").alias("label"),
        )
        .count()
        .orderBy("split", "label")
        .collect()
    )
    LABELS[name] = [(r["split"], r["label"], r["count"]) for r in rows]
    print(name, LABELS[name])

# COMMAND ----------

# DBTITLE 1,Keys in more than one split and overlap with the variants of variant_profile
SPLIT_OVERLAP = {}
for name in BENCHMARKS:
    SPLIT_OVERLAP[name] = {}
    for column in SPLIT_KEYS:
        if column in FRAMES[name].columns:
            SPLIT_OVERLAP[name][column] = share_in_several_groups(
                FRAMES[name], column, "split"
            )
    print(name, SPLIT_OVERLAP[name])
CLINICAL_FRAME = spark.table(f"{CATALOG}.{SCHEMA}.{CLINICAL}")
CLINICAL_OVERLAP = {
    name: key_overlap(
        FRAMES[name], "variant_key", CLINICAL_FRAME, "variant_key", normalise=False
    )
    for name in BENCHMARKS
}
for name, m in CLINICAL_OVERLAP.items():
    print(name, m)

# COMMAND ----------

# DBTITLE 1,Assay rows against mutant rows and binarized scan score balance
ASSAY_KEY = exact_uniqueness(FRAMES[ASSAY], ["assay_id"])["assay_id"]
ASSAY_LINK = key_overlap(
    FRAMES[MUTANT], "assay_id", FRAMES[ASSAY], "assay_id", normalise=False
)
MUTANT_ROWS = (
    FRAMES[MUTANT].groupBy("assay_id").count().withColumnRenamed("count", "mutant_rows")
)
CONSISTENCY = (
    FRAMES[ASSAY]
    .select("assay_id", "total_mutants")
    .join(MUTANT_ROWS, "assay_id", "left")
    .agg(
        F.count(F.lit(1)).alias("assays"),
        F.sum(F.when(F.col("mutant_rows").isNull(), 1).otherwise(0)).alias(
            "without_mutant_rows"
        ),
        F.sum(
            F.when(F.col("total_mutants") == F.col("mutant_rows"), 1).otherwise(0)
        ).alias("count_matches"),
    )
    .first()
    .asDict()
)
print(ASSAY_KEY, ASSAY_LINK, CONSISTENCY)
SCAN_BALANCE = value_counts(FRAMES[MUTANT], "scan_score_binarized")
print(SCAN_BALANCE)

# COMMAND ----------

# DBTITLE 1,Export profiling findings
label_lines = []
for name, rows in LABELS.items():
    label_lines.append(f"**{name}** (split, label, rows)")
    label_lines.append(fmt_pairs([((s, lab), f"{n:,}") for s, lab, n in rows]))
split_lines = []
for name, columns in SPLIT_OVERLAP.items():
    for column, m in columns.items():
        split_lines.append(
            para(
                f"- {name}, `{column}`: {m['keys']:,} distinct values,",
                f"{m['in_several']:,} in more than one split ({pct(m['share'])}).",
            )
        )
overlap_lines = [
    para(
        f"- {name}: {m['a_distinct']:,} distinct variant keys,",
        f"{m['a_not_in_b']:,} not in `{CLINICAL}`",
        f"(match rate {m['a_share_in_b']}).",
    )
    for name, m in CLINICAL_OVERLAP.items()
]
assay_lines = [
    para(
        f"- assays: {CONSISTENCY['assays']:,}; assay_id rows {ASSAY_KEY['rows']:,},",
        f"distinct {ASSAY_KEY['distinct']:,}.",
    ),
    para(
        f"- mutant rows with an assay_id not in `{ASSAY}`:",
        f"{ASSAY_LINK['a_not_in_b']:,} of {ASSAY_LINK['a_distinct']:,} assay ids.",
    ),
    para(
        f"- assays without mutant rows: {CONSISTENCY['without_mutant_rows']:,};",
        "assays whose total_mutants equals the mutant rows:",
        f"{CONSISTENCY['count_matches']:,}.",
    ),
    "- binarized scan score, rows: "
    + ", ".join(f"{v}: {n:,}" for v, n, _ in SCAN_BALANCE),
]
split_total = sum(m["in_several"] for c in SPLIT_OVERLAP.values() for m in c.values())
export_group(
    SOURCE,
    NB_KEY,
    SECTION_TITLE,
    STD,
    extra_blocks=[
        ("Label Balance per Split", "\n".join(label_lines)),
        ("Split Overlap", "\n".join(split_lines)),
        ("Overlap and Assays", "\n".join(overlap_lines + assay_lines)),
    ],
    readiness=[
        (
            "Split / entity leakage",
            para(
                f"{split_total:,} key values sit in more than one split (see the split",
                "lines); variants that are also in the labelled ClinVar table:",
                "; ".join(
                    f"{n} {m['a_distinct'] - m['a_not_in_b']:,}"
                    for n, m in CLINICAL_OVERLAP.items()
                ),
            ),
        ),
        (
            "Data-generation-process leakage",
            para(
                "label rows per split are listed per benchmark table; the balance of",
                "each set follows from how the set was built.",
            ),
        ),
    ],
    evidence={
        "Relationships": [
            f"{n}: {m['a_distinct'] - m['a_not_in_b']:,} variant keys in `{CLINICAL}`"
            for n, m in CLINICAL_OVERLAP.items()
        ],
        "ML use": [f"{split_total:,} key values sit in more than one split"],
    },
    links=False,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## EDA -- GENOMICS RELATIONSHIPS AND FINDINGS
# MAGIC
# MAGIC **Purpose:** Cross-table checks over the genomics Gold tables. It checks that
# MAGIC the schema holds exactly the tables the EDA notebooks cover, matches every key
# MAGIC column to its parent table (orphans, repeated parent keys), profiles the two
# MAGIC multi-value tables and measures how many cells hold more than one value --
# MAGIC as evidence.

# COMMAND ----------

# DBTITLE 1,Configuration, schema check, tables and standard profile
CATALOG = "energy_commerce_retail_media"
SCHEMA = "genomics_gold"
SOURCE = "genomics"
NB_KEY = "07_relationships_and_findings"
SECTION_TITLE = "Cross-table relationships and multi-value tables"
ALL_TABLES = [
    "benchmark_variant",
    "case_assay_record",
    "cell_line_profile",
    "clinical_trial_profile",
    "clinvar_classification_change",
    "disease_profile",
    "dms_assay",
    "dms_assay_mutant",
    "drug_profile",
    "embedding_split",
    "entity_identity_record",
    "epitope_profile",
    "relation_record",
    "gene_disease_profile",
    "gene_measurement",
    "gene_profile",
    "genetic_test_profile",
    "multi_value_attribute",
    "multi_value_score",
    "protein_profile",
    "protein_variant_record",
    "somatic_mutation_profile",
    "somatic_site_profile",
    "structural_variant_profile",
    "transcript_profile",
    "tumor_sample_profile",
    "variant_profile",
]
MULTI_ATTRIBUTE = "multi_value_attribute"
MULTI_SCORE = "multi_value_score"
TABLES = [MULTI_ATTRIBUTE, MULTI_SCORE]
# Key columns whose parent is not named after the column.
EXTRA_LINKS = [
    ("structural_variant_profile", "overlapping_gene_key", "gene_profile", "gene_key"),
    ("dms_assay_mutant", "assay_id", "dms_assay", "assay_id"),
]
IN_SCHEMA = sorted(
    r["tableName"] for r in spark.sql(f"SHOW TABLES IN {CATALOG}.{SCHEMA}").collect()
)
NOT_COVERED = sorted(set(IN_SCHEMA) - set(ALL_TABLES))
NOT_FOUND = sorted(set(ALL_TABLES) - set(IN_SCHEMA))
print(f"tables in the schema: {len(IN_SCHEMA)}; listed here: {len(ALL_TABLES)}")
print("in the schema, not covered:", NOT_COVERED)
print("covered, not in the schema:", NOT_FOUND)
if NOT_COVERED or NOT_FOUND:
    raise ValueError(f"table list differs: {NOT_COVERED} {NOT_FOUND}")
FRAMES_ALL = read_tables(CATALOG, SCHEMA, ALL_TABLES)
ROWS = {name: frame.count() for name, frame in FRAMES_ALL.items()}
for name, rows in ROWS.items():
    print(f"  {name:<32} rows={rows:>12,} columns={len(FRAMES_ALL[name].columns)}")
FRAMES, *STD = open_group(CATALOG, SCHEMA, TABLES)
KINDS, PROFILES, KEYS, BOOLEANS, NUMERICS, COVERAGE = STD

# COMMAND ----------

# DBTITLE 1,Key columns, their parent tables and the match of every key column
LINKS = []
for name, frame in FRAMES_ALL.items():
    for column in frame.columns:
        parent = column[: -len("_key")] + "_profile"
        if (
            column.endswith("_key")
            and parent in ALL_TABLES
            and parent != name
            and column in FRAMES_ALL[parent].columns
        ):
            LINKS.append((name, column, parent, column))
LINKS += EXTRA_LINKS
print(len(LINKS), "links")
MATCHES = {}
PARENT_KEYS = {}
for child, child_col, parent, parent_col in LINKS:
    if (parent, parent_col) not in PARENT_KEYS:
        stats = exact_uniqueness(FRAMES_ALL[parent], [parent_col])[parent_col]
        PARENT_KEYS[(parent, parent_col)] = {
            **stats,
            "repeated_rows": stats["rows"] - stats["nulls"] - stats["distinct"],
        }
    MATCHES[(child, child_col, parent, parent_col)] = key_overlap(
        FRAMES_ALL[child],
        child_col,
        FRAMES_ALL[parent],
        parent_col,
        normalise=False,
    )
    print(
        child, child_col, "->", parent, MATCHES[(child, child_col, parent, parent_col)]
    )

# COMMAND ----------

# DBTITLE 1,Entity keys of multi_value_attribute and cells with more than one value
ENTITY_ROWS = value_counts(FRAMES[MULTI_ATTRIBUTE], "entity_type")
ENTITY_MATCHES = {}
for entity, rows, _share in ENTITY_ROWS:
    parent = f"{entity}_profile"
    parent_col = f"{entity}_key"
    if parent in ALL_TABLES and parent_col in FRAMES_ALL[parent].columns:
        child = FRAMES[MULTI_ATTRIBUTE].filter(qcol("entity_type") == entity)
        ENTITY_MATCHES[entity] = key_overlap(
            child, "entity_key", FRAMES_ALL[parent], parent_col, normalise=False
        )
    else:
        ENTITY_MATCHES[entity] = None
    print(entity, rows, ENTITY_MATCHES[entity])
VALUES_PER_ATTRIBUTE = group_size_shares(
    FRAMES[MULTI_ATTRIBUTE], ["entity_type", "entity_key", "attribute"]
)
VALUES_PER_SCORE = group_size_shares(FRAMES[MULTI_SCORE], ["variant_key", "score_name"])
SCORE_LINK = MATCHES[(MULTI_SCORE, "variant_key", "variant_profile", "variant_key")]
VARIANT_ROWS = ROWS["variant_profile"]
print("values per attribute cell:", VALUES_PER_ATTRIBUTE)
print("values per score cell:", VALUES_PER_SCORE)
print("variants with a multi-value score:", SCORE_LINK["a_distinct"], VARIANT_ROWS)

# COMMAND ----------

# DBTITLE 1,Export profiling findings
ranked_links = sorted(MATCHES.items(), key=lambda t: -t[1]["a_not_in_b"])
drawn = barplot(
    [(f"{k[0]}.{k[1]}", m["a_not_in_b"]) for k, m in ranked_links],
    "Key values without a parent row, per link",
    "child table and column",
    ylabel="distinct key values",
    rot=60,
    figsize=(12, 5),
    filename="genomics_07_orphans.png",
)
FIGURES = (
    [("Key values without a parent row, per link", "genomics_07_orphans.png")]
    if drawn
    else []
)
inventory = md_table(
    ("table", "rows", "columns"),
    [(n, f"{ROWS[n]:,}", len(FRAMES_ALL[n].columns)) for n in ALL_TABLES],
)
link_rows = [
    (
        f"{k[0]}.{k[1]}",
        f"{k[2]}.{k[3]}",
        f"{m['a_distinct']:,}",
        f"{m['a_not_in_b']:,}",
        m["a_share_in_b"],
        PARENT_KEYS[(k[2], k[3])]["repeated_rows"],
    )
    for k, m in MATCHES.items()
]
link_table = md_table(
    ("child", "parent", "distinct keys", "orphans", "match rate", "parent repeats"),
    link_rows,
)
entity_lines = [
    f"- {entity}: {rows:,} rows"
    + (
        f"; {m['a_not_in_b']:,} of {m['a_distinct']:,} entity keys without a parent"
        if m
        else "; no parent table with a matching key column"
    )
    for (entity, rows, _share), m in zip(
        ENTITY_ROWS, ENTITY_MATCHES.values(), strict=True
    )
]
multi_lines = [
    "- values per attribute cell, share of cells: "
    + ", ".join(f"{k} {v}" for k, v in VALUES_PER_ATTRIBUTE.items() if k != "groups"),
    f"- attribute cells with several values: {VALUES_PER_ATTRIBUTE['groups']:,}",
    "- values per score cell, share of cells: "
    + ", ".join(f"{k} {v}" for k, v in VALUES_PER_SCORE.items() if k != "groups"),
    f"- score cells with several values: {VALUES_PER_SCORE['groups']:,}",
    para(
        f"- variants with a multi-value score: {SCORE_LINK['a_distinct']:,}",
        f"({pct(SCORE_LINK['a_distinct'] / VARIANT_ROWS if VARIANT_ROWS else None)}",
        "of `variant_profile`).",
    ),
]
with_orphans = [(k, m) for k, m in MATCHES.items() if m["a_not_in_b"]]
repeated_parents = {
    f"{p}.{c}": s["repeated_rows"]
    for (p, c), s in PARENT_KEYS.items()
    if s["repeated_rows"]
}
findings = [
    f"- {len(ALL_TABLES)} tables in the schema, all covered; {sum(ROWS.values()):,} rows.",
    f"- key columns matched to a parent: {len(MATCHES)}; with orphan keys: {len(with_orphans)}.",
    "- parent key columns with repeated values: "
    + (", ".join(f"{k} ({v:,})" for k, v in repeated_parents.items()) or "none")
    + ".",
    table_findings_lines(PROFILES, KEYS),
]
export_group(
    SOURCE,
    NB_KEY,
    SECTION_TITLE,
    STD,
    extra_blocks=[
        ("Table Inventory", inventory),
        ("Referential Integrity", link_table),
        ("Entity Keys of multi_value_attribute", "\n".join(entity_lines)),
        ("Multi-Value Cells", "\n".join(multi_lines)),
    ],
    findings="\n".join(findings),
    readiness=[
        (
            "Join multiplication (1:N / M:N expansion)",
            "parent key columns with repeated values: "
            + (
                ", ".join(f"{k} ({v:,})" for k, v in repeated_parents.items()) or "none"
            ),
        ),
        (
            "Survivorship / coverage bias",
            para(
                f"{len(with_orphans)} of {len(MATCHES)} links have key values without a",
                "parent row; the list is in the relationship table.",
            ),
        ),
    ],
    evidence={
        "Structure and engineering": [
            f"{len(ALL_TABLES)} Gold tables, {len(LINKS)} key links checked"
        ],
        "Relationships": [
            f"{len(with_orphans)} links with orphan keys",
            f"{len(repeated_parents)} parent key columns with repeated values",
        ],
        "Data quality": [
            para(
                f"{VALUES_PER_ATTRIBUTE['groups']:,} attribute cells and",
                f"{VALUES_PER_SCORE['groups']:,} score cells hold several values",
            )
        ],
    },
    figures=FIGURES,
    links=False,
)
