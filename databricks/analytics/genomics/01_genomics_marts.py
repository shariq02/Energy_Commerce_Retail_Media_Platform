# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # ANALYTICS GENOMICS -- MARTS
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** November 2026
# MAGIC
# MAGIC **Purpose:** the 6 `genomics_analytics` tables from Gold: genes with their
# MAGIC Silver scores, drug and somatic values and ranks, one overview row per disease,
# MAGIC the genes of each genetic test, the review queue of uncertain and conflicting
# MAGIC variants, population frequencies of the pathogenic variants, and ClinVar class
# MAGIC changes per snapshot pair. No new score: ranks and counts are computed here.

# COMMAND ----------

# DBTITLE 1,Gold library
# MAGIC %run ../../gold/_gold_common

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "genomics"
COMPONENT = "analytics/genomics/01_genomics_marts"
RID = analytics_run_id()
RULES = load_yaml("src/schemas/mappings/analytics_rules.yml")
GATE = {"source": SOURCE, "component": COMPONENT, "rid": RID}
COMMON = {**GATE, "schema": ANALYTICS_SCHEMAS["genomics"]}
GOLD = {"source": SOURCE, "schema": GENOMICS_GOLD_SCHEMA}
# Silver attribute -> its rule (the pivot name when two rules share it)
GENE_VALUES = {
    "gene_importance_score": "gene_importance",
    "gene_annotation_quality": "gene_annotation_quality",
    "primary_gene_category": "primary_gene_category",
    "primary_function": "primary_function",
    "druggability_score": "druggability",
    "pathogenic_variant_count": "pathogenic_variant_count",
    "total_disease_count": "gene_disease_count",
    "is_pharmacogene": "pharmacogene",
    "pharmacogene_category": "pharmacogene",
    "pharmacogene_evidence_level": "pharmacogene",
    "related_drug_count": "related_drug_count",
    "gene_test_count": "gene_test_count",
    "somatic_mutation_count": "somatic_mutation_count",
    "is_frequently_mutated": "somatic_mutation_count",
    "tumor_sample_count": "tumor_sample_count",
}
# gene rank column -> the value it orders, largest first
GENE_RANKS = {
    "importance_rank": "gene_importance_score",
    "disease_link_rank": "linked_disease_count",
    "druggability_rank": "druggability_score",
    "somatic_mutation_rank": "somatic_mutation_count",
}
VARIANT_LABELS = [
    "variant_key",
    "assembly",
    "contig",
    "position",
    "reference_allele",
    "alternate_allele",
    "gene_key",
]
LABEL = ("clinical_significance_label", "clinical_significance_label")
DRUG_VARIANT = ("is_pharmgkb_annotated_variant", "drug_response_features")
# Silver attribute -> its rule
REVIEW_VALUES = {
    "quality_tier": "review_quality",
    "review_quality_score": "review_quality",
    "predicted_pathogenicity": "protein_impact",
    "pathogenicity_score": "protein_impact",
    "mutation_severity_score": "protein_impact",
    "protein_impact_category": "protein_impact",
    "is_loss_of_function": "protein_impact",
    "conservation_level": "conservation",
    "frequency_band": "frequency_bands",
    "max_population_frequency": "frequency_bands",
    "days_since_evaluation": "review_recency",
    "is_recently_evaluated": "review_recency",
}
FREQUENCY_VALUES = {
    "allele_frequency": "double",
    "allele_count": "bigint",
    "allele_number": "bigint",
    "homozygote_count": "bigint",
    "filter_status": "string",
}
CHANGE_GRAIN = [
    "classification_type",
    "previous_clinical_significance_simple",
    "clinical_significance_simple",
    "previous_last_source_snapshot",
    "first_source_snapshot",
]
TABLES = {}

# COMMAND ----------

# DBTITLE 1,Read Gold
genes = from_gold(read_gold("gene_profile", **GOLD))
clinical = read_gold("variant_profile", **GOLD)
disease_profile = from_gold(read_gold("disease_profile", **GOLD))
gene_disease = read_gold("relation_record", **GOLD).filter(
    (F.col("relation_kind") == "gene_disease")
    & (F.col("gene_key").isNotNull() | F.col("source_gene_id").isNotNull())
    & (F.col("disease_key").isNotNull() | F.col("source_disease_id").isNotNull())
)
genetic_test = read_gold("genetic_test_profile", **GOLD)
classification_change = read_gold("clinvar_classification_change", **GOLD)
label = pick_column(clinical, *LABEL)
gene_symbols = genes.select("gene_key", F.col("symbol").alias("gene_symbol"))

# COMMAND ----------

# DBTITLE 1,Transform -- gene_prioritization (Silver values, drug variants, ranks)
disease_links = (
    gene_disease.filter(F.col("gene_key").isNotNull())
    .groupBy("gene_key")
    .agg(
        F.countDistinct("disease_key").alias("linked_disease_count"),
        F.max("gene_disease_score").alias("max_gene_disease_score"),
    )
)
drug_variant = pick_column(clinical, *DRUG_VARIANT)
drug_variants = (
    clinical.filter(F.col(drug_variant) & F.col("gene_key").isNotNull())
    .groupBy("gene_key")
    .agg(F.countDistinct("variant_key").alias("drug_variant_count"))
)
prioritization = (
    genes.select(
        "gene_key",
        "symbol",
        "full_name",
        *[
            F.col(pick_column(genes, name, rule)).alias(name)
            for name, rule in GENE_VALUES.items()
        ],
    )
    .join(disease_links, "gene_key", "left")
    .join(drug_variants, "gene_key", "left")
)
for rank, value in GENE_RANKS.items():
    order = Window.orderBy(F.col(value).desc_nulls_last())
    prioritization = prioritization.withColumn(rank, F.dense_rank().over(order))
TABLES["gene_prioritization"] = (
    add_provenance(prioritization, SOURCE, RID, ecosystem="genomics"),
    ["gene_key"],
)

# COMMAND ----------

# DBTITLE 1,Transform -- disease_overview (disease profile and linked genes)
linked_genes = (
    gene_disease.filter(F.col("disease_key").isNotNull())
    .groupBy("disease_key")
    .agg(
        F.countDistinct("gene_key").alias("linked_gene_count"),
        F.count(F.lit(1)).alias("relation_count"),
        sorted_values("relation_kind").alias("relation_kinds"),
        F.max("gene_disease_score").alias("max_gene_disease_score"),
        F.max_by("gene_symbol", "gene_disease_score").alias("top_scored_gene_symbol"),
    )
)
# `genetic_test_count` is a Silver attribute of disease_profile (distinct tests per
# disease), so it is not counted again here.
TABLES["disease_overview"] = (
    add_provenance(
        disease_profile.join(linked_genes, "disease_key", "left"),
        SOURCE,
        RID,
        ecosystem="genomics",
    ),
    ["disease_key"],
)

# COMMAND ----------

# DBTITLE 1,Transform -- genetic_test_panel (one row per test and gene)
panel_grain = ["test_key", "gene_key"]
TABLES["genetic_test_panel"] = (
    add_provenance(
        genetic_test.filter(F.col("gene_key").isNotNull())
        .groupBy(*panel_grain)
        .agg(
            F.min("gene_symbol").alias("gene_symbol"),
            F.min("test_type").alias("test_type"),
            F.min("latest_version").alias("latest_version"),
            F.countDistinct("condition_name").alias("condition_count"),
            sorted_values("condition_name").alias("condition_names"),
            sorted_values("disease_key").alias("disease_keys"),
        )
        .withColumn(
            "panel_gene_count", F.count(F.lit(1)).over(Window.partitionBy("test_key"))
        ),
        SOURCE,
        RID,
        ecosystem="genomics",
    ),
    panel_grain,
)

# COMMAND ----------

# DBTITLE 1,Transform -- variant_review_queue (uncertain and conflicting variants)
queue = clinical.filter(F.col(label).isin(RULES["review_queue_labels"])).select(
    *VARIANT_LABELS, F.col(label).alias("clinical_significance_label")
)
queue = queue.join(
    clinical.select(
        "variant_key",
        *[
            F.col(pick_column(clinical, name, rule)).alias(name)
            for name, rule in REVIEW_VALUES.items()
        ],
    ),
    "variant_key",
    "left",
)
in_queue = Window.orderBy(
    F.col("pathogenicity_score").desc_nulls_last(),
    F.col("review_quality_score").asc_nulls_first(),
    "variant_key",
)
TABLES["variant_review_queue"] = (
    add_provenance(
        queue.join(gene_symbols, "gene_key", "left").withColumn(
            "queue_rank", F.row_number().over(in_queue)
        ),
        SOURCE,
        RID,
        ecosystem="genomics",
    ),
    ["variant_key"],
)

# COMMAND ----------

# DBTITLE 1,Transform -- population_risk (pathogenic variants x population group)
suffix = "_allele_frequency"
columns = set(clinical.columns)
# A population group has an allele frequency and a filter status column
# (variant_profile holds more columns than the frequency ones); its count
# columns exist only where the source holds counts, and the others are null.
groups = sorted(
    group
    for group in (c[: -len(suffix)] for c in columns if c.endswith(suffix))
    if f"{group}_filter_status" in columns
)
group_rows = [
    F.struct(
        F.lit(group).alias("population_group"),
        *[
            (F.col(f"{group}_{name}") if f"{group}_{name}" in columns else F.lit(None))
            .cast(kind)
            .alias(name)
            for name, kind in FREQUENCY_VALUES.items()
        ],
    )
    for group in groups
]
pathogenic = clinical.filter(F.col(label).isin(RULES["population_risk_labels"])).select(
    "variant_key", F.col(label).alias("clinical_significance_label")
)
TABLES["population_risk"] = (
    add_provenance(
        columns_to_rows(clinical, ["variant_key", "gene_key"], group_rows)
        .filter(F.col("allele_frequency").isNotNull())
        .join(pathogenic, "variant_key")
        .join(gene_symbols, "gene_key", "left"),
        SOURCE,
        RID,
        ecosystem="genomics",
    ),
    ["variant_key", "population_group"],
)

# COMMAND ----------

# DBTITLE 1,Transform -- classification_change_summary (changes per snapshot pair)
TABLES["classification_change_summary"] = (
    add_provenance(
        classification_change.groupBy(*CHANGE_GRAIN).agg(
            F.count(F.lit(1)).alias("change_count"),
            F.countDistinct("variant_key").alias("variant_count"),
        ),
        "clinvar",
        RID,
        ecosystem="genomics",
    ),
    CHANGE_GRAIN,
)

# COMMAND ----------

# DBTITLE 1,Write -- every table
write_tables(TABLES, **COMMON)

# COMMAND ----------

# DBTITLE 1,Grain gate -- every written table
gate_tables(TABLES, **COMMON)

# COMMAND ----------

# DBTITLE 1,Inspect and export -- every table
inspect_tables(TABLES, findings="genomics", **COMMON)

# COMMAND ----------

# DBTITLE 1,Validation -- every table exists and has rows
validate_tables(TABLES, **COMMON)
