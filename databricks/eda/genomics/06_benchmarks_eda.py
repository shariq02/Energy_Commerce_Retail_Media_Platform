# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # EDA -- GENOMICS BENCHMARK TABLES
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** November 2026
# MAGIC
# MAGIC **Purpose:** Profile the benchmark tables of genomics Gold
# MAGIC (`benchmark_coding_variant`, `benchmark_population_variant`,
# MAGIC `benchmark_regulatory_variant`, `dms_assay`, `dms_assay_mutant`,
# MAGIC `embedding_split`): nulls, constant columns, key, rates per column kind,
# MAGIC coverage by source, label balance per split, keys that sit in more than one
# MAGIC split, overlap with the labelled ClinVar variants, and the assay rows against
# MAGIC their mutant rows -- as evidence.

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
NB_KEY = "06_benchmarks"
SECTION_TITLE = "Benchmark, assay and embedding split tables"
BENCHMARKS = [
    "benchmark_coding_variant",
    "benchmark_population_variant",
    "benchmark_regulatory_variant",
]
ASSAY = "dms_assay"
MUTANT = "dms_assay_mutant"
TABLES = [*BENCHMARKS, ASSAY, MUTANT, "embedding_split"]
CLINICAL = "variant_clinical_profile"
SPLIT_KEYS = ("variant_key", "protein_id", "gene")

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

# DBTITLE 1,Label balance per split
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

# DBTITLE 1,Keys that sit in more than one split
SPLIT_OVERLAP = {}
for name in BENCHMARKS:
    SPLIT_OVERLAP[name] = {}
    for column in SPLIT_KEYS:
        if column in FRAMES[name].columns:
            SPLIT_OVERLAP[name][column] = share_in_several_groups(
                FRAMES[name], column, "split"
            )
    print(name, SPLIT_OVERLAP[name])

# COMMAND ----------

# DBTITLE 1,Overlap with the variants of variant_clinical_profile
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

# DBTITLE 1,Assay rows against mutant rows
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

# COMMAND ----------

# DBTITLE 1,Binarized scan score balance
SCAN_BALANCE = value_counts(FRAMES[MUTANT], "scan_score_binarized")
print(SCAN_BALANCE)

# COMMAND ----------

# DBTITLE 1,Figure -- rows per table
drawn = barplot(
    [(n, p["rows"]) for n, p in PROFILES.items()],
    "Rows per table",
    "table",
    ylabel="rows",
    rot=20,
    logy=True,
    filename="genomics_06_rows.png",
)
FIGURES = [("Rows per table", "genomics_06_rows.png")] if drawn else []

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
READINESS = [
    (
        "Grain / grain drift",
        "; ".join(
            f"`{n}` key `{k['key']}` unique: {k.get('unique')}" for n, k in KEYS.items()
        ),
    ),
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
        "Data-generation-process leakage",
        para(
            "label rows per split are listed per benchmark table; the balance of each",
            "set follows from how the set was built.",
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
        f"{n}: {m['a_distinct'] - m['a_not_in_b']:,} variant keys in `{CLINICAL}`"
        for n, m in CLINICAL_OVERLAP.items()
    ],
    "ML use": [f"{split_total:,} key values sit in more than one split"],
}
blocks = [
    ("Profile", profile_block(PROFILES)),
    ("Data Quality", quality_block(PROFILES)),
    ("Entities / Keys", key_block(KEYS)),
    ("Coverage & Sampling Bias", coverage_block(COVERAGE)),
    ("Label Balance per Split", "\n".join(label_lines)),
    ("Split Overlap", "\n".join(split_lines)),
    ("Relationships", "\n".join(overlap_lines + assay_lines)),
    ("Distributions", distribution_block(PROFILES, KINDS, BOOLEANS, NUMERICS)),
    ("EDA Findings", table_findings_lines(PROFILES, KEYS)),
    ("ML-Readiness Evidence", ml_readiness_block(READINESS)),
    ("Observations by Area", area_block(EVIDENCE)),
]
write_profiling(SOURCE, NB_KEY, SECTION_TITLE, blocks, FIGURES)
