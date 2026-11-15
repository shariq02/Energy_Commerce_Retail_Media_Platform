# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # GOLD GENOMICS -- BENCHMARKS AND COVERAGE
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** November 2026
# MAGIC
# MAGIC **Purpose:** the three benchmark tables (sets with the same columns together),
# MAGIC the DMS assays, mutants and embedding splits. Runs last of the genomics Gold
# MAGIC notebooks: the last cell checks that every Silver attribute is a column of a
# MAGIC Gold table.

# COMMAND ----------

# DBTITLE 1,Gold library
# MAGIC %run ../_gold_common

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "genomics"
COMPONENT = "gold/genomics/03_benchmarks"
RID = gold_run_id()
GATE = {"source": SOURCE, "component": COMPONENT, "rid": RID}
COMMON = {**GATE, "schema": GENOMICS_GOLD_SCHEMA}
BENCHMARK_TABLES = {
    "benchmark_population_variant": ("gnomad_balanced", "maps"),
    "benchmark_regulatory_variant": ("pathogenic_non_coding", "causal_eqtl"),
    "benchmark_coding_variant": (
        "pathogenic_coding",
        "clinvar_vs_benign",
        "proteingym_clinical",
        "spectralbio",
    ),
}
COPIES = ("dms_assay", "dms_assay_mutant", "embedding_split")
# entity type of entity_derived_attribute -> the Gold tables that hold its columns
ATTRIBUTE_TABLES = {
    "gene": ("gene_profile", *GENE_PROFILE_RULES),
    "variant": ("variant_profile", *VARIANT_PROFILE_RULES),
    "structural_variant_gene": ("structural_variant_profile",),
    "transcript": ("transcript_profile",),
    "protein": ("protein_profile",),
    "disease": ("disease_profile",),
    "drug": ("drug_profile",),
    "cell_line": ("cell_line_profile",),
    "genetic_test": ("genetic_test_profile",),
    "clinical_trial": ("clinical_trial_profile",),
    "epitope": ("epitope_profile",),
    "tumor_sample": ("tumor_sample_profile",),
    "somatic_site": ("somatic_site_profile",),
    "somatic_mutation": ("somatic_mutation_profile",),
}
TABLES = {}

# COMMAND ----------

# DBTITLE 1,Read Silver
benchmark = from_silver(read_silver("benchmark_variant"))
copies = {table: from_silver(read_silver(table)) for table in COPIES}

# COMMAND ----------

# DBTITLE 1,Transform -- the benchmark tables (only the columns their sets fill)
for table, sets in BENCHMARK_TABLES.items():
    rows = benchmark.filter(F.col("benchmark_set").isin(list(sets)))
    TABLES[table] = (
        add_provenance(drop_empty_columns(rows), SOURCE, RID, ecosystem="genomics"),
        ["source_record_id"],
    )

# COMMAND ----------

# DBTITLE 1,Transform -- dms_assay, dms_assay_mutant, embedding_split
for table, frame in copies.items():
    TABLES[table] = (
        add_provenance(frame, SOURCE, RID, ecosystem="genomics"),
        ["source_record_id"],
    )

# COMMAND ----------

# DBTITLE 1,Grain gate -- every table
gate_tables(TABLES, **GATE)

# COMMAND ----------

# DBTITLE 1,Write -- every table
write_tables(TABLES, **COMMON)

# COMMAND ----------

# DBTITLE 1,Inspect and export -- every table
inspect_tables(TABLES, findings="genomics", **COMMON)

# COMMAND ----------

# DBTITLE 1,Validation -- tables have rows and hold every benchmark row
validate_tables(TABLES, **COMMON)
benchmark_rows = sum(
    read_gold(t, source=SOURCE, schema=GENOMICS_GOLD_SCHEMA).count()
    for t in BENCHMARK_TABLES
)
check(
    COMPONENT,
    SOURCE,
    "every_benchmark_row_in_one_gold_table",
    benchmark_rows == benchmark.count(),
    detail=f"gold={benchmark_rows}",
    metric_value=float(benchmark_rows),
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Validation -- every Silver attribute is a column of a Gold table
names = spark.createDataFrame(
    read_silver("entity_derived_attribute")
    .select("entity_type", "rule_name", "attribute", "value_type")
    .distinct()
    .collect()
)
missing = []
for entity_type in [r[0] for r in names.select("entity_type").distinct().collect()]:
    gold_columns = {
        c.removesuffix("__attribute")
        for table in ATTRIBUTE_TABLES.get(entity_type, ())
        for c in read_gold(table, source=SOURCE, schema=GENOMICS_GOLD_SCHEMA).columns
    }
    expected = set(attribute_columns(names, entity_type).values())
    missing += [f"{entity_type}.{c}" for c in sorted(expected - gold_columns)]
check(
    COMPONENT,
    SOURCE,
    "every_silver_attribute_is_a_gold_column",
    not missing,
    detail=", ".join(missing[:50]),
    metric_value=float(len(missing)),
    rid=RID,
)
