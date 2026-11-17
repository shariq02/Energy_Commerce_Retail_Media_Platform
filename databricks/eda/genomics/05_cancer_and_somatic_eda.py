# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # EDA -- GENOMICS CANCER AND SOMATIC TABLES
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** November 2026
# MAGIC
# MAGIC **Purpose:** Profile the cancer tables of genomics Gold (`cell_line_profile`,
# MAGIC `tumor_sample_profile`, `somatic_mutation_profile`, `somatic_site_profile`):
# MAGIC nulls, constant columns, key, rates per column kind, coverage by source, the
# MAGIC fill of the link columns, mutations per sample and per gene, and consistency
# MAGIC checks inside the somatic mutation rows -- as evidence.

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
NB_KEY = "05_cancer_and_somatic"
SECTION_TITLE = "Cell line, tumor sample and somatic mutation tables"
MUTATION = "somatic_mutation_profile"
TABLES = [
    "cell_line_profile",
    "tumor_sample_profile",
    MUTATION,
    "somatic_site_profile",
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

# DBTITLE 1,Mutations per sample and per gene
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

# DBTITLE 1,Figure -- rows per table
drawn = barplot(
    [(n, p["rows"]) for n, p in PROFILES.items()],
    "Rows per table",
    "table",
    ylabel="rows",
    rot=20,
    logy=True,
    filename="genomics_05_rows.png",
)
FIGURES = [("Rows per table", "genomics_05_rows.png")] if drawn else []

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
    "Relationships": [f"{n}: {len(f)} link columns" for n, f in LINK_FILL.items()],
    "Statistical patterns": [
        f"{PER_SAMPLE['groups']:,} samples carry {rows_mutation:,} mutation rows"
    ],
}
blocks = [
    ("Profile", profile_block(PROFILES)),
    ("Data Quality", quality_block(PROFILES) + "\n\n" + "\n".join(check_lines)),
    ("Entities / Keys", key_block(KEYS)),
    ("Coverage & Sampling Bias", coverage_block(COVERAGE)),
    ("Relationships", fill_lines(LINK_FILL) + "\n\n" + "\n".join(size_lines)),
    ("Distributions", distribution_block(PROFILES, KINDS, BOOLEANS, NUMERICS)),
    ("EDA Findings", table_findings_lines(PROFILES, KEYS)),
    ("ML-Readiness Evidence", ml_readiness_block(READINESS)),
    ("Observations by Area", area_block(EVIDENCE)),
]
write_profiling(SOURCE, NB_KEY, SECTION_TITLE, blocks, FIGURES)
