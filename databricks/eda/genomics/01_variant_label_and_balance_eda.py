# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # EDA -- GENOMICS VARIANT LABEL AND BALANCE
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** November 2026
# MAGIC
# MAGIC **Purpose:** Profile `variant_clinical_profile` and
# MAGIC `clinvar_classification_change` in genomics Gold: nulls, constant columns, key,
# MAGIC the label text and the class flags, class balance, review quality against the
# MAGIC class, flag and score association with the class, snapshot history and the
# MAGIC variants whose term changed -- as evidence.

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %run ../_eda_common

# COMMAND ----------

# DBTITLE 1,Configuration
CATALOG = "energy_commerce_retail_media"
SCHEMA = "genomics_gold"
SOURCE = "genomics"
NB_KEY = "01_variant_label_and_balance"
SECTION_TITLE = "Variant label and balance (variant_clinical_profile)"
CLINICAL = "variant_clinical_profile"
CHANGES = "clinvar_classification_change"
TABLES = [CLINICAL, CHANGES]
LABEL_TEXT = "clinical_significance_label"
POSITIVE = "is_pathogenic"
NEGATIVE = "is_benign"
CLASS_FLAGS = (POSITIVE, NEGATIVE, "is_vus", "clinical_significance_is_uncertain")
OWN_LABEL_COLUMNS = {LABEL_TEXT, *CLASS_FLAGS}

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

# DBTITLE 1,Association with the class -- flags and numeric columns
FLAGS = [c for c in KINDS[CLINICAL]["boolean"] if c not in OWN_LABEL_COLUMNS]
SCORES = [c for c in KINDS[CLINICAL]["numeric"] if c != "position"]
FLAG_ASSOC = flag_vs_label(TARGET, FLAGS)
SCORE_ASSOC = score_vs_label(TARGET, SCORES)
print("flags above the separation limit:")
print([c for c, v in top_by(FLAG_ASSOC, "separation") if v >= FLAG_SEPARATION])
print("numeric columns above the correlation limit:")
print([c for c, v in top_by(SCORE_ASSOC, "correlation") if abs(v) >= LEAK_CORRELATION])

# COMMAND ----------

# DBTITLE 1,Pairs of numeric columns above the limit
RANKED = [c for c, _ in top_by(SCORE_ASSOC, "correlation", PAIR_COLUMNS)]
PAIRS = pair_correlations(TARGET, RANKED)
print(len(PAIRS), "pairs; first 10:", PAIRS[:10])

# COMMAND ----------

# DBTITLE 1,Snapshot history
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

# COMMAND ----------

# DBTITLE 1,Variants whose term changed
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

# DBTITLE 1,Figure -- rows per label text
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

# COMMAND ----------

# DBTITLE 1,Export profiling findings
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
leaking_flags = [c for c, v in top_by(FLAG_ASSOC, "separation") if v >= FLAG_SEPARATION]
leaking_scores = [
    c for c, v in top_by(SCORE_ASSOC, "correlation") if abs(v) >= LEAK_CORRELATION
]
gap_columns = [c for c, v in top_by(SCORE_ASSOC, "missing_gap") if v >= MISSING_GAP]
key = KEYS[CLINICAL]
READINESS = [
    (
        "Grain / grain drift",
        para(
            f"`{CLINICAL}` key `{key['key']}`, unique: {key.get('unique')};",
            f"`{CHANGES}` key `{KEYS[CHANGES]['key']}`,",
            f"unique: {KEYS[CHANGES].get('unique')}.",
        ),
    ),
    (
        "Target contamination",
        para(
            f"flags with a separation of {FLAG_SEPARATION} or more: {len(leaking_flags)}",
            f"({', '.join(leaking_flags[:10])}); the class flags and the label text",
            "are in the same table; rows with several class flags:",
            f"{OVERLAP['several_flags']:,}.",
        ),
    ),
    (
        "Proxy leakage",
        para(
            f"numeric columns with a correlation of {LEAK_CORRELATION} or more with",
            f"the class: {len(leaking_scores)} ({', '.join(leaking_scores[:10])});",
            f"numeric pairs above the limit among the top columns: {len(PAIRS)}.",
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
            f"numeric columns whose missing rate differs by {MISSING_GAP} or more",
            f"between the classes: {len(gap_columns)} ({', '.join(gap_columns[:10])}).",
        ),
    ),
    (
        "Duplicate-event leakage",
        para(
            f"`{CLINICAL}` duplicate groups:",
            f"{(key.get('duplicates') or {}).get('dup_groups', 0)}.",
        ),
    ),
    (
        "Class / label instability",
        para(
            f"{CHANGED['variants']:,} variants ({pct(changed_share)} of rows) have a",
            "term change between snapshots.",
        ),
    ),
    (
        "Source / version / regime change",
        para(
            f"{SNAPSHOTS['several_snapshots']:,} rows carry more than one ClinVar",
            "snapshot.",
        ),
    ),
    (
        "Sample-vs-full divergence",
        "every count and rate is read from the whole table; no sample is used.",
    ),
]
EVIDENCE = {
    "Data quality": [
        para(
            f"{len(PROFILES[CLINICAL]['empty'])} empty and",
            f"{len(PROFILES[CLINICAL]['constant'])} constant columns in `{CLINICAL}`",
        )
    ],
    "Statistical patterns": [
        f"pathogenic to benign ratio {IMBALANCE}",
        f"{len(PAIRS)} numeric pairs above {LEAK_CORRELATION}",
    ],
    "Temporal": [f"{SNAPSHOTS['several_snapshots']:,} rows span several snapshots"],
    "ML use": [
        para(
            f"{KNOWN:,} labelled rows; {len(leaking_flags)} flags and",
            f"{len(leaking_scores)} numeric columns separate the class almost fully",
        )
    ],
}
blocks = [
    ("Profile", profile_block(PROFILES)),
    ("Data Quality", quality_block(PROFILES)),
    ("Entities / Keys", key_block(KEYS)),
    ("Distributions", distribution_block(PROFILES, KINDS, BOOLEANS, NUMERICS)),
    ("Label and Balance", "\n".join(label_lines)),
    (
        "Association with the class",
        association_lines(CLINICAL, FLAG_ASSOC, SCORE_ASSOC),
    ),
    ("Snapshot History and Term Changes", "\n".join(history_lines)),
    ("EDA Findings", table_findings_lines(PROFILES, KEYS)),
    ("ML-Readiness Evidence", ml_readiness_block(READINESS)),
    ("Observations by Area", area_block(EVIDENCE)),
]
write_profiling(SOURCE, NB_KEY, SECTION_TITLE, blocks, FIGURES)
