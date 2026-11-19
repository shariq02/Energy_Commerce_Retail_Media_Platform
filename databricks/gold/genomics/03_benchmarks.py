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
# MAGIC **Purpose:** `benchmark_variant` (every benchmark set, the columns that a set
# MAGIC fills), the DMS assays, mutants and embedding splits. Runs last of the
# MAGIC genomics Gold notebooks: its last two cells check that every Silver attribute
# MAGIC is a column of a Gold table and that every Silver table feeds a Gold table.

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
COPIES = ("dms_assay", "dms_assay_mutant", "embedding_split")
# entity type of entity_derived_attribute -> the Gold tables that hold its columns
ATTRIBUTE_TABLES = {
    "gene": ("gene_profile",),
    "variant": ("variant_profile", "structural_variant_profile"),
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
# Gold table -> the Silver tables it is built from (gold/genomics/01, 02, 03)
GOLD_FEEDS = {
    "gene_profile": ("entity_master", "entity_derived_attribute"),
    "variant_profile": (
        "variant",
        "entity_derived_attribute",
        "locus_score",
        "variant_population_frequency",
    ),
    "multi_value_attribute": ("entity_derived_attribute",),
    "multi_value_score": ("locus_score",),
    "transcript_profile": ("entity_master", "entity_derived_attribute"),
    "protein_profile": ("entity_master", "entity_derived_attribute"),
    "disease_profile": ("entity_master", "entity_derived_attribute"),
    "drug_profile": ("entity_master", "entity_derived_attribute"),
    "cell_line_profile": ("cell_line", "entity_derived_attribute"),
    "genetic_test_profile": ("genetic_test", "entity_derived_attribute"),
    "clinical_trial_profile": ("clinical_trial", "entity_derived_attribute"),
    "epitope_profile": ("epitope", "entity_derived_attribute"),
    "tumor_sample_profile": ("tumor_sample", "entity_derived_attribute"),
    "somatic_mutation_profile": ("somatic_mutation", "entity_derived_attribute"),
    "somatic_site_profile": ("somatic_mutation", "entity_derived_attribute"),
    "gene_disease_profile": ("entity_relation", "entity_master"),
    "structural_variant_profile": ("variant", "entity_derived_attribute"),
    "clinvar_classification_change": ("variant_classification",),
    "benchmark_variant": ("benchmark_variant",),
    "dms_assay": ("dms_assay",),
    "dms_assay_mutant": ("dms_assay_mutant",),
    "embedding_split": ("embedding_split",),
    "gene_measurement": (
        "gene_dependency",
        "gene_expression_median",
        "perturbation_expression",
        "perturbation_sample",
        "gene_disease_score_year",
        "regulatory_element_gene_link",
    ),
    "protein_variant_record": (
        "protein_residue_score",
        "protein_feature",
        "protein_gene_link",
        "variant_hgvs",
        "variant_condition",
        "variant_feature_overlap",
    ),
    "case_assay_record": (
        "faers_case",
        "faers_case_drug",
        "faers_case_item",
        "epitope_mhc_assay",
    ),
    "entity_identity_record": (
        "entity_id_crosswalk",
        "entity_name",
        "entity_claim",
        "entity_coordinate",
        "ontology_relation",
    ),
}
TABLES = {}

# COMMAND ----------

# DBTITLE 1,Read Silver
benchmark = from_silver(read_silver("benchmark_variant"))
copies = {table: from_silver(read_silver(table)) for table in COPIES}

# COMMAND ----------

# DBTITLE 1,Transform -- benchmark_variant (the columns that a set fills)
TABLES["benchmark_variant"] = (
    add_provenance(drop_empty_columns(benchmark), SOURCE, RID, ecosystem="genomics"),
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
benchmark_rows = read_gold(
    "benchmark_variant", source=SOURCE, schema=GENOMICS_GOLD_SCHEMA
).count()
check(
    COMPONENT,
    SOURCE,
    "every_benchmark_row_in_the_gold_table",
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

# COMMAND ----------

# DBTITLE 1,Validation -- every Silver table feeds a Gold table
unfed = unfed_tables(GOLD_FEEDS, silver_tables(GENOMICS_SILVER_SCHEMAS))
check(
    COMPONENT,
    SOURCE,
    "every_silver_table_feeds_a_gold_table",
    not unfed,
    detail=", ".join(unfed),
    metric_value=float(len(unfed)),
    rid=RID,
)
