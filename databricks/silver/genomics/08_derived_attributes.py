# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # SILVER -- DERIVED ATTRIBUTES (GENOMICS)
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** November 2026
# MAGIC
# MAGIC **Purpose:** every computed attribute (flags, labels, scores, bands, counts) with
# MAGIC its rule, matched text and as-of date; the rules are in the versioned
# MAGIC rules file. Each feature group (clinical, disease, pharmacogene, ...) has its
# MAGIC own rule name and rules section. Runs last.

# COMMAND ----------

# DBTITLE 1,Genomics library
# MAGIC %run ../_genomics_common

# COMMAND ----------

# DBTITLE 1,Configuration
from pyspark.sql import Window
from pyspark.sql import functions as F

SOURCE = "genomics"
COMPONENT = "silver/genomics/08_derived_attributes"
RID = run_id()
FINDINGS = "genomics"
RULES = load_genomics_rules()

# COMMAND ----------

# DBTITLE 1,Read Silver -- tables and rule values
master = read_silver("entity_master")


def master_of(entity_type, key):
    """The rows of one entity type, with the key under its own name."""
    return (
        master.filter(F.col("entity_type") == entity_type)
        .drop(key)
        .withColumnRenamed("entity_key", key)
    )


gene = master_of("gene", "gene_key")
protein = master_of("protein", "protein_key")
disease = master_of("disease", "disease_key")
relation_table = read_silver("entity_relation")
claims = read_silver("entity_claim")
names = read_silver("entity_name")
xwalk = read_silver("entity_id_crosswalk")
VERSION = genomics_rule_version(RULES)
AS_OF = F.to_date(F.lit(RID.split("-")[1][:8]), "yyyyMMdd")
COMMON = {"rules": RULES, "version": VERSION, "as_of": AS_OF}
RULE_FIELDS = {"version": VERSION, "as_of": AS_OF}
none_text = F.lit(None).cast("string")


def number_rows(
    df, entity_type, key, attribute, value, rule_name, source, method="count"
):
    """Rows of a numeric attribute: `value` is a Column of `df`."""
    return derived_rows(
        df,
        entity_type=entity_type,
        key=key,
        attribute=attribute,
        value_number=F.col(value) if isinstance(value, str) else value,
        rule_name=rule_name,
        method=method,
        text_source=source,
        **RULE_FIELDS,
    )


def text_rows(
    df,
    entity_type,
    key,
    attribute,
    value,
    rule_name,
    source,
    method="rule",
    matched=None,
):
    """Rows of a text attribute: `value` is a Column of `df`."""
    return derived_rows(
        df,
        entity_type=entity_type,
        key=key,
        attribute=attribute,
        value_text=F.col(value) if isinstance(value, str) else value,
        rule_name=rule_name,
        method=method,
        text_source=source,
        matched=matched,
        **RULE_FIELDS,
    )


def flag_rows(
    df, entity_type, key, attribute, condition, rule_name, source, method="rule"
):
    """True-flag rows of the entities of `df` that meet the condition."""
    return derived_rows(
        df.filter(condition),
        entity_type=entity_type,
        key=key,
        attribute=attribute,
        value_boolean=F.lit(True),
        rule_name=rule_name,
        method=method,
        text_source=source,
        **RULE_FIELDS,
    )


# COMMAND ----------

# DBTITLE 1,Transform -- gene texts


def claim_text(field):
    return (
        claims.filter((F.col("entity_type") == "gene") & (F.col("field") == field))
        .groupBy(F.col("entity_key").alias("gene_key"))
        .agg(F.concat_ws(" ", F.sort_array(F.collect_set("value_text"))).alias(field))
    )


def name_text(term_type, column):
    return (
        names.filter(
            (F.col("entity_type") == "gene") & (F.col("term_type") == term_type)
        )
        .groupBy(F.col("entity_key").alias("gene_key"))
        .agg(F.concat_ws(" ", F.sort_array(F.collect_set("term"))).alias(column))
    )


texts = (
    gene.select(
        "gene_key",
        "full_name",
        "description",
        "map_location",
        "cytoband_arm",
        "cytoband_band",
        "cytoband_sub_band",
    )
    .join(claim_text("function_descriptions"), "gene_key", "left")
    .join(claim_text("subcellular_locations"), "gene_key", "left")
    .join(name_text("designation", "designation_text"), "gene_key", "left")
    .withColumn(
        "function_text",
        F.concat_ws(" ", "full_name", "description", "function_descriptions"),
    )
    .withColumn(
        "location_text",
        F.concat_ws(" ", "subcellular_locations", "description", "designation_text"),
    )
    .withColumn(
        "process_text", F.concat_ws(" ", "description", "function_descriptions")
    )
    .withColumn("description_text", F.coalesce(F.col("description"), F.lit("")))
    .withColumn(
        "keyword_text",
        F.concat_ws(" | ", "description", "function_descriptions", "full_name"),
    )
    .withColumn("designation_text", F.coalesce(F.col("designation_text"), F.lit("")))
)

# COMMAND ----------

# DBTITLE 1,Transform -- keyword flags of genes
function_flags = keyword_rows(
    texts,
    entity_type="gene",
    key="gene_key",
    text="function_text",
    group="gene_function",
    text_source="gene.full_name+description+function_descriptions",
    **COMMON,
)
location_flags = keyword_rows(
    texts,
    entity_type="gene",
    key="gene_key",
    text="location_text",
    group="gene_location",
    text_source="subcellular_locations+gene.description+designations",
    **COMMON,
)
process_flags = keyword_rows(
    texts,
    entity_type="gene",
    key="gene_key",
    text="process_text",
    group="gene_process",
    text_source="gene.description+function_descriptions",
    **COMMON,
)
description_flags = keyword_rows(
    texts,
    entity_type="gene",
    key="gene_key",
    text="description_text",
    group="gene_description",
    text_source="gene.description",
    **COMMON,
)
description_values = captured_rows(
    texts,
    entity_type="gene",
    key="gene_key",
    text="description_text",
    group="gene_description_value",
    text_source="gene.description",
    numbers=("member_number", "type_number"),
    **COMMON,
)
designation_flags = keyword_rows(
    texts,
    entity_type="gene",
    key="gene_key",
    text="designation_text",
    group="gene_designation",
    text_source="designations",
    **COMMON,
)
keyword_flags = keyword_rows(
    texts,
    entity_type="gene",
    key="gene_key",
    text="keyword_text",
    group="gene_description_keywords",
    text_source="gene.description+function_descriptions+full_name",
    **COMMON,
)
category_text_flags = keyword_rows(
    texts,
    entity_type="gene",
    key="gene_key",
    text="keyword_text",
    group="gene_category_text",
    text_source="gene.description+function_descriptions+full_name",
    **COMMON,
)

# COMMAND ----------

# DBTITLE 1,Transform -- gene counts, lengths and database coverage
NCBI = "ncbi_gene.gene_info"
synonym_counts = number_rows(
    names.filter(
        (F.col("entity_type") == "gene")
        & (F.col("source_dataset") == NCBI)
        & (F.col("term_type") == "synonym")
    )
    .groupBy(F.col("entity_key").alias("gene_key"))
    .agg(F.count(F.lit(1)).alias("n")),
    "gene",
    "gene_key",
    "total_aliases",
    "n",
    "gene_name_counts",
    "entity_name",
)
designation_counts = number_rows(
    names.filter(
        (F.col("entity_type") == "gene")
        & (F.col("source_dataset") == NCBI)
        & (F.col("term_type") == "designation")
    )
    .groupBy(F.col("entity_key").alias("gene_key"))
    .agg(F.count(F.lit(1)).alias("n")),
    "gene",
    "gene_key",
    "total_designations",
    "n",
    "gene_name_counts",
    "entity_name",
)
database_per_gene = (
    xwalk.filter(
        (F.col("entity_type") == "gene")
        & (F.col("source_dataset") == NCBI)
        & F.col("source_field").isin("dbXrefs", "GeneID")
        & F.col("id_type").isin(RULES["gene_database_id_types"])
    )
    .groupBy(F.col("entity_key").alias("gene_key"))
    .agg(F.countDistinct("id_type").alias("n"))
)
database_counts = number_rows(
    database_per_gene,
    "gene",
    "gene_key",
    "database_count",
    "n",
    "gene_database_count",
    "entity_id_crosswalk",
)
well_annotated = flag_rows(
    database_per_gene,
    "gene",
    "gene_key",
    "is_well_annotated",
    F.col("n") >= RULES["gene_well_annotated_minimum"],
    "gene_database_count",
    "entity_id_crosswalk",
)
word_counts = number_rows(
    gene.filter(F.col("description").isNotNull()).select(
        "gene_key", F.size(F.split(F.trim(F.col("description")), r"\s+")).alias("n")
    ),
    "gene",
    "gene_key",
    "description_word_count",
    "n",
    "description_word_count",
    "gene.description",
)
gene_lengths = number_rows(
    gene.filter(
        F.col("grch38_start_position").isNotNull()
        & F.col("grch38_end_position").isNotNull()
    ).select(
        "gene_key",
        (F.col("grch38_end_position") - F.col("grch38_start_position") + 1).alias("n"),
    ),
    "gene",
    "gene_key",
    "gene_length",
    "n",
    "gene_length",
    "gene.grch38_interval",
    method="rule",
)
last_band_digit = text_rows(
    gene.filter(F.col("cytoband_sub_band").isNotNull()).select(
        "gene_key",
        F.substring(F.col("cytoband_sub_band"), -1, 1).alias("digit"),
        "map_location",
    ),
    "gene",
    "gene_key",
    "cytoband_last_band_digit",
    F.col("digit"),
    "cytoband_parts",
    "gene.map_location",
    matched=F.col("map_location"),
)

# COMMAND ----------

# DBTITLE 1,Transform -- cytoband flags
limits = RULES["cytoband_limits"]
band = F.col("cytoband_band").cast("int")
cytoband = texts.filter(
    F.col("cytoband_arm").isNotNull() & F.col("cytoband_band").isNotNull()
)
cytoband_rows = _union(
    [
        derived_rows(
            cytoband.filter(condition),
            entity_type="gene",
            key="gene_key",
            attribute=name,
            rule_name="cytoband_limits",
            method="rule",
            value_boolean=F.lit(True),
            matched=F.col("map_location"),
            text_source="gene.map_location",
            **RULE_FIELDS,
        )
        for name, condition in (
            ("is_telomeric", band >= limits["telomeric_from"]),
            ("is_centromeric", band <= limits["centromeric_to"]),
            (
                "is_pericentromeric",
                (band >= limits["pericentromeric_from"])
                & (band <= limits["pericentromeric_to"]),
            ),
        )
    ]
)

# COMMAND ----------

# DBTITLE 1,Transform -- gene scores and categories
# gene-level disease text and counts from the gene-disease records of the importance rules
importance = RULES["gene_importance"]
gene_disease = relation_table.filter(
    (F.col("relation_kind") == "gene_disease")
    & F.col("gene_key").isNotNull()
    & F.col("disease_key").isNotNull()
)
association = gene_disease.filter(
    F.col("relation_subkind").isin(importance["disease_record_kinds"])
)
disease_names = disease.select("disease_key", F.col("name").alias("disease_name"))
per_kind = association.groupBy("gene_key", "relation_subkind").agg(
    F.countDistinct("disease_key").alias("n")
)
disease_totals = per_kind.groupBy("gene_key").agg(
    F.sum("n").alias("total_disease_count")
)
disease_text = (
    association.select("gene_key", "disease_key")
    .distinct()
    .join(disease_names, "disease_key")
    .groupBy("gene_key")
    .agg(
        F.concat_ws(" | ", F.sort_array(F.collect_set("disease_name"))).alias(
            "all_disease_text"
        )
    )
    .withColumn("all_disease_text", F.lower(F.col("all_disease_text")))
)
disease_category_flags = _union(
    [
        derived_rows(
            disease_text.filter(F.col("all_disease_text").rlike("(?i)" + pattern)),
            entity_type="gene",
            key="gene_key",
            attribute=f"has_{name}_disease",
            rule_name="gene_disease_category",
            method="keyword",
            value_boolean=F.lit(True),
            matched=F.regexp_extract(
                F.col("all_disease_text"), "(?i)(" + pattern + ")", 1
            ),
            text_source="entity_relation+disease.name",
            **RULE_FIELDS,
        )
        for name, pattern in RULES["text_rules"]["disease_category"].items()
    ]
)
disease_total_rows = number_rows(
    disease_totals,
    "gene",
    "gene_key",
    "total_disease_count",
    F.col("total_disease_count"),
    "gene_disease_count",
    "entity_relation",
)
all_flags = (
    function_flags.unionByName(location_flags)
    .unionByName(keyword_flags)
    .unionByName(category_text_flags)
    .unionByName(disease_category_flags)
    .groupBy(F.col("entity_key").alias("gene_key"))
    .agg(F.collect_set("attribute").alias("flags"))
)
keyword_count_flags = RULES["function_keyword_flags"]
scores = (
    gene.select("gene_key", "description")
    .join(all_flags, "gene_key", "left")
    .join(disease_totals, "gene_key", "left")
    .withColumn("flags", F.coalesce(F.col("flags"), F.array().cast("array<string>")))
    .withColumn(
        "total_disease_count", F.coalesce(F.col("total_disease_count"), F.lit(0))
    )
    .withColumn(
        "function_keyword_count",
        F.size(
            F.array_intersect(
                F.col("flags"), F.array(*[F.lit(f) for f in keyword_count_flags])
            )
        ),
    )
    .withColumn(
        "has_detailed_description",
        F.length(F.col("description")) > RULES["detailed_description_length"],
    )
    .withColumn(
        "has_cancer_combined",
        F.array_contains(F.col("flags"), "has_cancer_disease")
        | F.array_contains(F.col("flags"), "has_cancer_keyword"),
    )
    .withColumn(
        "gene_importance_score",
        points_expr(importance["disease_count_points"], F.col("total_disease_count"))
        + F.col("function_keyword_count") * importance["function_keyword_points"]
        + F.when(
            F.col("has_detailed_description"),
            importance["detailed_description_points"],
        ).otherwise(0)
        + F.when(F.col("has_cancer_combined"), importance["cancer_points"]).otherwise(
            0
        ),
    )
    .withColumn(
        "gene_annotation_quality",
        from_band_expr(
            RULES["gene_annotation_quality"],
            F.col("gene_importance_score"),
            RULES["gene_annotation_quality_default"],
        ),
    )
    .withColumn(
        "primary_gene_category",
        any_match_expr(
            RULES["primary_gene_category"],
            F.col("flags"),
            lambda test: (
                f"has_{test['disease']}_disease"
                if "disease" in test
                else test.get("flag", test.get("text"))
            ),
            RULES["primary_gene_category_default"],
        ),
    )
)
score_rows = [
    number_rows(
        scores,
        "gene",
        "gene_key",
        "function_keyword_count",
        F.col("function_keyword_count"),
        "gene_description_enrichment",
        "gene_description_keywords",
        method="rule",
    ),
    number_rows(
        scores,
        "gene",
        "gene_key",
        "gene_importance_score",
        F.col("gene_importance_score"),
        "gene_importance",
        "entity_relation+gene_description_keywords",
        method="rule",
    ),
    text_rows(
        scores,
        "gene",
        "gene_key",
        "gene_annotation_quality",
        F.col("gene_annotation_quality"),
        "gene_annotation_quality",
        "gene_importance_score",
    ),
    text_rows(
        scores,
        "gene",
        "gene_key",
        "primary_gene_category",
        F.col("primary_gene_category"),
        "primary_gene_category",
        "description and disease flags",
    ),
    flag_rows(
        scores,
        "gene",
        "gene_key",
        "is_well_characterized",
        F.col("function_keyword_count") >= RULES["well_characterized_minimum"],
        "gene_description_enrichment",
        "gene_description_keywords",
    ),
    flag_rows(
        scores,
        "gene",
        "gene_key",
        "has_detailed_description",
        F.col("has_detailed_description"),
        "gene_description_enrichment",
        "gene.description",
    ),
]

# COMMAND ----------

# DBTITLE 1,Transform -- primary function and druggability
function_flag_sets = (
    function_flags.unionByName(location_flags)
    .groupBy(F.col("entity_key").alias("gene_key"))
    .agg(F.collect_set("attribute").alias("flags"))
)
function_scored = (
    gene.select("gene_key")
    .join(function_flag_sets, "gene_key", "left")
    .withColumn("flags", F.coalesce(F.col("flags"), F.array().cast("array<string>")))
    .withColumn(
        "primary_function",
        first_match_expr(
            RULES["primary_function"],
            F.col("flags"),
            "label",
            RULES["primary_function_default"],
        ),
    )
    .withColumn(
        "druggability_score",
        first_match_expr(
            RULES["druggability"],
            F.col("flags"),
            "score",
            float(RULES["druggability_default"]),
        ),
    )
)
primary_rows = text_rows(
    function_scored,
    "gene",
    "gene_key",
    "primary_function",
    F.col("primary_function"),
    "primary_function",
    "function flags",
)
druggability_rows = number_rows(
    function_scored,
    "gene",
    "gene_key",
    "druggability_score",
    F.col("druggability_score"),
    "druggability",
    "function and location flags",
    method="rule",
)

# COMMAND ----------

# DBTITLE 1,Transform -- protein name flags and disease categories
protein_text = protein.filter(F.col("name").isNotNull())
protein_flags = keyword_rows(
    protein_text,
    entity_type="protein",
    key="protein_key",
    text="name",
    group="protein_name",
    text_source="protein.name",
    **COMMON,
)
domain_counts = protein_flags.groupBy(F.col("entity_key").alias("protein_key")).agg(
    F.count(F.lit(1)).alias("n")
)
domain_per_protein = (
    protein_text.select("protein_key")
    .join(domain_counts, "protein_key", "left")
    .withColumn("n", F.coalesce(F.col("n"), F.lit(0)))
)
domain_count_rows = number_rows(
    domain_per_protein,
    "protein",
    "protein_key",
    "domain_count",
    F.col("n"),
    "protein_domain_count",
    "protein.name",
    method="rule",
)
functional_domain_rows = flag_rows(
    domain_per_protein,
    "protein",
    "protein_key",
    "has_functional_domain",
    F.col("n") > 0,
    "protein_domain_count",
    "protein.name",
)
disease_flags = keyword_rows(
    disease.filter(F.col("name").isNotNull()),
    entity_type="disease",
    key="disease_key",
    text="name",
    group="disease_category",
    text_source="disease.name",
    **COMMON,
)

# COMMAND ----------

# DBTITLE 1,Transform -- variant labels, quality, origin and type
variant = read_silver("variant")
germline = current_snapshot(read_silver("variant_classification"), RULES).filter(
    F.col("classification_type") == "germline"
)
term = F.lower(F.col("term"))
label = F.lit(RULES["clinical_significance_label_default"])
for entry in reversed(RULES["clinical_significance_labels"]):
    condition = F.lit(True)
    for word in entry["contains"]:
        condition = condition & term.contains(word)
    for word in entry["excludes"]:
        condition = condition & ~term.contains(word)
    label = F.when(condition, F.lit(entry["label"])).otherwise(label)
labelled = germline.withColumn("significance_label", label)
label_rows = [
    text_rows(
        labelled,
        "variant",
        "variant_key",
        "clinical_significance_label",
        F.col("significance_label"),
        "clinical_significance_label",
        "variant_classification.term",
        matched=F.col("term"),
    ),
    flag_rows(
        labelled,
        "variant",
        "variant_key",
        "is_pathogenic",
        F.col("significance_label").isin(RULES["pathogenic_labels"]),
        "clinical_significance_label",
        "variant_classification.term",
    ),
    flag_rows(
        labelled,
        "variant",
        "variant_key",
        "is_benign",
        F.col("significance_label").isin(RULES["benign_labels"]),
        "clinical_significance_label",
        "variant_classification.term",
    ),
    flag_rows(
        labelled,
        "variant",
        "variant_key",
        "is_vus",
        F.col("significance_label").isin(RULES["vus_labels"]),
        "clinical_significance_label",
        "variant_classification.term",
    ),
]
quality = germline.withColumn(
    "quality_tier",
    from_band_expr(
        [
            {"from": e["from"], "label": e["label"]}
            for e in RULES["review_quality_tiers"]
        ],
        F.col("review_stars"),
        RULES["review_quality_tier_default"],
    ),
).withColumn("days_since_evaluation", F.datediff(AS_OF, F.col("last_evaluated")))
quality_rows = [
    number_rows(
        quality,
        "variant",
        "variant_key",
        "review_quality_score",
        F.col("review_stars"),
        "review_quality",
        "variant_classification.review_stars",
        method="rule",
    ),
    text_rows(
        quality,
        "variant",
        "variant_key",
        "quality_tier",
        F.col("quality_tier"),
        "review_quality",
        "variant_classification.review_status",
    ),
    number_rows(
        quality.filter(F.col("days_since_evaluation").isNotNull()),
        "variant",
        "variant_key",
        "days_since_evaluation",
        F.col("days_since_evaluation"),
        "review_recency",
        "variant_classification.last_evaluated",
        method="rule",
    ),
    flag_rows(
        quality,
        "variant",
        "variant_key",
        "is_recently_evaluated",
        F.col("days_since_evaluation") <= RULES["recent_evaluation_days"],
        "review_recency",
        "variant_classification.last_evaluated",
    ),
]
origin_flags = keyword_rows(
    variant.filter(F.col("origin").isNotNull()),
    entity_type="variant",
    key="variant_key",
    text="origin",
    group="origin_flags",
    patterns=RULES["origin_flags"],
    text_source="variant.origin",
    **COMMON,
)
type_flags = keyword_rows(
    variant.filter(F.col("variant_type").isNotNull()),
    entity_type="variant",
    key="variant_key",
    text="variant_type",
    group="variant_type_flags",
    patterns=RULES["variant_type_flags"],
    text_source="variant.variant_type",
    **COMMON,
)
lengths = variant.filter(
    F.col("reference_allele").isNotNull() & F.col("alternate_allele").isNotNull()
)
reference_length = F.length("reference_allele")
alternate_length = F.length("alternate_allele")
allele_class = (
    F.when((reference_length == 1) & (alternate_length == 1), "single_nucleotide")
    .when(reference_length == alternate_length, "multi_nucleotide")
    .when(alternate_length > reference_length, "insertion")
    .otherwise("deletion")
)
allele_rows = text_rows(
    lengths.withColumn("allele_class", allele_class),
    "variant",
    "variant_key",
    "allele_class",
    F.col("allele_class"),
    "allele_class",
    "variant.reference_allele+alternate_allele",
)
conditions = (
    current_snapshot(read_silver("variant_condition"), RULES)
    .filter(F.col("condition_id").isNotNull())
    .select("variant_key", F.col("condition_id").alias("raw_id"))
    .distinct()
)
condition_type, condition_value = split_curie("raw_id", RULES)
phenotype_databases = (
    conditions.withColumn("id_type", condition_type)
    .filter(F.col("id_type").isin(RULES["condition_database_id_types"]))
    .groupBy("variant_key")
    .agg(F.countDistinct("id_type").alias("n"))
)
phenotype_rows = [
    number_rows(
        phenotype_databases,
        "variant",
        "variant_key",
        "phenotype_database_count",
        F.col("n"),
        "phenotype_databases",
        "variant_condition.condition_id",
        method="rule",
    ),
    flag_rows(
        phenotype_databases,
        "variant",
        "variant_key",
        "is_well_annotated",
        F.col("n") >= RULES["variant_well_annotated_minimum"],
        "phenotype_databases",
        "variant_condition.condition_id",
    ),
]

# COMMAND ----------

# DBTITLE 1,Transform -- variant impact and conservation
hgvs = (
    current_snapshot(read_silver("variant_hgvs"), RULES)
    .groupBy("variant_key")
    .agg(
        F.max(
            F.col("protein_edit_type").isin(RULES["loss_of_function_edit_types"])
        ).alias("is_lof"),
        F.max(F.col("coding_region").isin(RULES["splice_affecting_regions"])).alias(
            "is_splice"
        ),
        F.max(F.col("protein_edit_type") == "missense").alias("is_missense"),
    )
)
protein_overlap = (
    read_silver("variant_feature_overlap")
    .filter(F.col("feature_kind") == "protein_feature")
    .select("variant_key")
    .distinct()
    .withColumn("has_protein_feature", F.lit(True))
)
severity = RULES["mutation_severity"]
categories = RULES["protein_impact_categories"]
variant_positions = variant.filter(F.col("assembly") == "GRCh38").select(
    "variant_key",
    F.col("contig").alias("variant_contig"),
    F.col("position").alias("variant_position"),
)
locus_scores = read_silver("locus_score")
phylop_scores = (
    locus_scores.filter(F.col("variant_key").isNull())
    .filter(F.col("score_name") == "phylop_100way")
    .select(
        F.col("contig").alias("score_contig"),
        F.col("position").alias("score_position"),
        F.col("score_value").alias("phylop_score"),
    )
)
phylop = variant_positions.join(
    phylop_scores,
    (F.col("variant_contig") == F.col("score_contig"))
    & (F.col("variant_position") == F.col("score_position")),
    "inner",
).select("variant_key", "phylop_score")
cadd_scores = (
    locus_scores.filter(F.col("variant_key").isNotNull())
    .filter(F.col("score_name") == "cadd_phred")
    .groupBy("variant_key")
    .agg(F.max("score_value").alias("cadd_phred"))
)
cutoffs = RULES["conservation"]
impact = (
    hgvs.join(protein_overlap, "variant_key", "left")
    .join(phylop, "variant_key", "left")
    .join(cadd_scores, "variant_key", "left")
    .withColumn(
        "has_protein_feature",
        F.coalesce(F.col("has_protein_feature"), F.lit(False)),
    )
    .withColumn(
        "severity",
        F.when(F.col("is_lof"), severity["loss_of_function"])
        .when(F.col("is_splice"), severity["splice"])
        .when(F.col("is_missense"), severity["missense"])
        .otherwise(severity["other"]),
    )
    .withColumn(
        "impact_category",
        F.when(F.col("is_lof"), categories["loss_of_function"])
        .when(F.col("is_splice"), categories["splice"])
        .when(
            F.col("is_missense") & F.col("has_protein_feature"),
            categories["domain"],
        )
        .when(F.col("is_missense"), categories["missense"])
        .otherwise(categories["default"]),
    )
    .withColumn(
        "affects_functional_domain",
        F.col("has_protein_feature") & (F.col("is_missense") | F.col("is_lof")),
    )
    .withColumn(
        "has_conservation",
        F.col("phylop_score").isNotNull() | F.col("cadd_phred").isNotNull(),
    )
    .withColumn(
        "conservation_level",
        F.when(
            F.col("has_conservation"),
            F.when(
                F.col("phylop_score") > cutoffs["phylop_conserved_above"], 1
            ).otherwise(0)
            + F.when(
                F.col("cadd_phred") > cutoffs["cadd_deleterious_above"], 1
            ).otherwise(0),
        ),
    )
    .withColumn(
        "pathogenicity_score",
        F.col("severity") + F.coalesce(F.col("conservation_level"), F.lit(0)),
    )
)
with_bands = RULES["predicted_pathogenicity_with_conservation"]
without_bands = RULES["predicted_pathogenicity_without_conservation"]
impact = impact.withColumn(
    "predicted_pathogenicity",
    F.when(
        F.col("has_conservation"),
        from_band_expr(
            with_bands,
            F.col("pathogenicity_score"),
            RULES["predicted_pathogenicity_default"],
        ),
    ).otherwise(
        from_band_expr(
            without_bands,
            F.col("pathogenicity_score"),
            RULES["predicted_pathogenicity_default"],
        )
    ),
)
high_from = RULES["high_impact_from"]
very_high_from = RULES["very_high_impact_from"]
high_impact = F.col("pathogenicity_score") >= F.when(
    F.col("has_conservation"), high_from["with_conservation"]
).otherwise(high_from["without_conservation"])
very_high_impact = F.col("pathogenicity_score") >= F.when(
    F.col("has_conservation"), very_high_from["with_conservation"]
).otherwise(very_high_from["without_conservation"])
SOURCE_TEXT = "variant_hgvs+variant_feature_overlap+locus_score"
impact_rows = [
    number_rows(
        impact,
        "variant",
        "variant_key",
        "mutation_severity_score",
        F.col("severity"),
        "protein_impact",
        SOURCE_TEXT,
        method="rule",
    ),
    text_rows(
        impact,
        "variant",
        "variant_key",
        "protein_impact_category",
        F.col("impact_category"),
        "protein_impact",
        SOURCE_TEXT,
    ),
    number_rows(
        impact,
        "variant",
        "variant_key",
        "pathogenicity_score",
        F.col("pathogenicity_score"),
        "protein_impact",
        SOURCE_TEXT,
        method="rule",
    ),
    text_rows(
        impact,
        "variant",
        "variant_key",
        "predicted_pathogenicity",
        F.col("predicted_pathogenicity"),
        "protein_impact",
        SOURCE_TEXT,
    ),
    number_rows(
        impact.filter(F.col("conservation_level").isNotNull()),
        "variant",
        "variant_key",
        "conservation_level",
        F.col("conservation_level"),
        "conservation",
        "locus_score",
        method="rule",
    ),
    flag_rows(
        impact,
        "variant",
        "variant_key",
        "affects_functional_domain",
        F.col("affects_functional_domain"),
        "protein_impact",
        SOURCE_TEXT,
    ),
    flag_rows(
        impact,
        "variant",
        "variant_key",
        "is_high_impact",
        high_impact,
        "protein_impact",
        SOURCE_TEXT,
    ),
    flag_rows(
        impact,
        "variant",
        "variant_key",
        "is_very_high_impact",
        very_high_impact,
        "protein_impact",
        SOURCE_TEXT,
    ),
    flag_rows(
        impact,
        "variant",
        "variant_key",
        "is_loss_of_function",
        F.col("is_lof"),
        "protein_impact",
        SOURCE_TEXT,
    ),
    flag_rows(
        impact,
        "variant",
        "variant_key",
        "is_splice_affecting",
        F.col("is_splice"),
        "protein_impact",
        SOURCE_TEXT,
    ),
    flag_rows(
        impact,
        "variant",
        "variant_key",
        "is_highly_conserved_region",
        F.col("phylop_score") > cutoffs["phylop_conserved_above"],
        "conservation",
        "locus_score.phylop_100way",
    ),
    flag_rows(
        impact,
        "variant",
        "variant_key",
        "is_conservation_constrained",
        F.col("conservation_level") >= 1,
        "conservation",
        "locus_score",
    ),
    flag_rows(
        impact,
        "variant",
        "variant_key",
        "has_cadd_score",
        F.col("cadd_phred").isNotNull(),
        "conservation",
        "locus_score.cadd_phred",
    ),
    flag_rows(
        impact,
        "variant",
        "variant_key",
        "is_deleterious_by_cadd",
        F.col("cadd_phred") > cutoffs["cadd_deleterious_above"],
        "conservation",
        "locus_score.cadd_phred",
    ),
]

# COMMAND ----------

# DBTITLE 1,Transform -- variant frequency
frequency = (
    read_silver("variant_population_frequency")
    .filter(F.col("measure").isin("allele_frequency", "highest_group_allele_frequency"))
    .groupBy("variant_key")
    .agg(F.max("allele_frequency").alias("max_frequency"))
    .withColumn(
        "frequency_band",
        band_expr(
            RULES["frequency_bands"],
            F.col("max_frequency"),
            RULES["frequency_band_default"],
        ),
    )
)
flags_rule = RULES["frequency_flags"]
frequency_rows = [
    derived_rows(
        frequency,
        entity_type="variant",
        key="variant_key",
        attribute="frequency_band",
        value_text=F.col("frequency_band"),
        rule_name="frequency_bands",
        method="rule",
        matched=F.col("max_frequency").cast("string"),
        text_source="variant_population_frequency",
        **RULE_FIELDS,
    ),
    number_rows(
        frequency,
        "variant",
        "variant_key",
        "max_population_frequency",
        F.col("max_frequency"),
        "frequency_bands",
        "variant_population_frequency",
        method="rule",
    ),
    flag_rows(
        frequency,
        "variant",
        "variant_key",
        "is_rare",
        F.col("max_frequency") < flags_rule["rare_below"],
        "frequency_flags",
        "variant_population_frequency",
    ),
    flag_rows(
        frequency,
        "variant",
        "variant_key",
        "is_ultra_rare",
        F.col("max_frequency") < flags_rule["ultra_rare_below"],
        "frequency_flags",
        "variant_population_frequency",
    ),
    flag_rows(
        frequency,
        "variant",
        "variant_key",
        "is_common",
        F.col("max_frequency") >= flags_rule["common_from"],
        "frequency_flags",
        "variant_population_frequency",
    ),
    flag_rows(
        frequency,
        "variant",
        "variant_key",
        "is_common_variant",
        F.col("max_frequency") > RULES["conservation"]["allele_frequency_common_above"],
        "frequency_flags",
        "variant_population_frequency",
    ),
]

# COMMAND ----------

# DBTITLE 1,Transform -- tissue, cancer and genetic test attributes
expression = read_silver("gene_expression_median").filter(F.col("gene_key").isNotNull())
by_gene = Window.partitionBy("gene_key").orderBy(
    F.col("median_transcripts_per_million").desc(), F.col("tissue_key")
)
expressed = expression.filter(F.col("median_transcripts_per_million") > 0)
tissue_summary = (
    expressed.withColumn("rank", F.row_number().over(by_gene))
    .groupBy("gene_key")
    .agg(
        F.countDistinct("tissue_key").alias("tissues_expressed"),
        F.max("median_transcripts_per_million").alias("max_median"),
        F.max(F.when(F.col("rank") == 1, F.col("tissue_key"))).alias("primary_tissue"),
    )
)
specificity = RULES["tissue_specificity"]
tissue_summary = tissue_summary.withColumn(
    "tissue_specificity_category",
    F.when(
        F.col("tissues_expressed") <= specificity["highly_specific_up_to"],
        "highly_specific",
    )
    .when(
        F.col("tissues_expressed") <= specificity["moderately_specific_up_to"],
        "moderately_specific",
    )
    .otherwise(RULES["tissue_specificity_default"]),
)
tissue_rows = [
    number_rows(
        tissue_summary,
        "gene",
        "gene_key",
        "tissues_expressed",
        F.col("tissues_expressed"),
        "tissue_specificity",
        "gene_expression_median",
        method="rule",
    ),
    number_rows(
        tissue_summary,
        "gene",
        "gene_key",
        "max_median_transcripts_per_million",
        F.col("max_median"),
        "tissue_specificity",
        "gene_expression_median",
        method="rule",
    ),
    text_rows(
        tissue_summary,
        "gene",
        "gene_key",
        "primary_tissue",
        F.col("primary_tissue"),
        "tissue_specificity",
        "gene_expression_median",
    ),
    text_rows(
        tissue_summary,
        "gene",
        "gene_key",
        "tissue_specificity_category",
        F.col("tissue_specificity_category"),
        "tissue_specificity",
        "gene_expression_median",
    ),
]
somatic = read_silver("somatic_mutation")
mutation = RULES["cancer_mutation"]
record_class = F.col("variant_classification")
record_type = F.col("variant_type")
record_impact = (
    F.when(record_class.isin(mutation["high_impact"]), "high")
    .when(record_class.isin(mutation["moderate_impact"]), "moderate")
    .otherwise("low")
)
records = somatic.select("source_record_id", "variant_classification", "variant_type")
cancer_rows = [
    text_rows(
        records.withColumn("impact_category", record_impact),
        "somatic_mutation",
        "source_record_id",
        "impact_category",
        F.col("impact_category"),
        "cancer_impact",
        "somatic_mutation.variant_classification",
    ),
    flag_rows(
        records,
        "somatic_mutation",
        "source_record_id",
        "is_missense",
        record_class.isin(mutation["missense"]),
        "cancer_mutation_class",
        "somatic_mutation.variant_classification",
    ),
    flag_rows(
        records,
        "somatic_mutation",
        "source_record_id",
        "is_truncating",
        record_class.isin(mutation["truncating"]),
        "cancer_mutation_class",
        "somatic_mutation.variant_classification",
    ),
    flag_rows(
        records,
        "somatic_mutation",
        "source_record_id",
        "is_silent",
        record_class.isin(mutation["silent"]),
        "cancer_mutation_class",
        "somatic_mutation.variant_classification",
    ),
    flag_rows(
        records,
        "somatic_mutation",
        "source_record_id",
        "is_snv",
        record_type.isin(mutation["snv_types"]),
        "cancer_mutation_class",
        "somatic_mutation.variant_type",
    ),
    flag_rows(
        records,
        "somatic_mutation",
        "source_record_id",
        "is_indel",
        record_type.isin(mutation["indel_types"]),
        "cancer_mutation_class",
        "somatic_mutation.variant_type",
    ),
]
mutation_counts = (
    somatic.filter(F.col("gene_key").isNotNull())
    .groupBy("gene_key")
    .agg(
        F.count(F.lit(1)).alias("n"),
        F.countDistinct("sample_barcode").alias("samples"),
    )
)
mutation_rows = [
    number_rows(
        mutation_counts,
        "gene",
        "gene_key",
        "somatic_mutation_count",
        F.col("n"),
        "somatic_mutation_count",
        "somatic_mutation",
    ),
    number_rows(
        mutation_counts,
        "gene",
        "gene_key",
        "tumor_sample_count",
        F.col("samples"),
        "tumor_sample_count",
        "somatic_mutation",
    ),
    flag_rows(
        mutation_counts,
        "gene",
        "gene_key",
        "is_frequently_mutated",
        F.col("n") >= RULES["frequently_mutated_minimum"],
        "somatic_mutation_count",
        "somatic_mutation",
    ),
]
test_rows_all = read_silver("genetic_test")
tests = test_rows_all.groupBy("test_key").agg(
    F.max(F.coalesce(F.col("link_kind") == "gene", F.lit(False))).alias("has_gene"),
    F.max(F.coalesce(F.col("link_kind") == "condition", F.lit(False))).alias(
        "has_condition"
    ),
)
test_rows = [
    flag_rows(
        tests,
        "genetic_test",
        "test_key",
        "has_gene_info",
        F.col("has_gene"),
        "genetic_test_record",
        "genetic_test.link_kind",
    ),
    flag_rows(
        tests,
        "genetic_test",
        "test_key",
        "has_disease_info",
        F.col("has_condition"),
        "genetic_test_record",
        "genetic_test.link_kind",
    ),
    flag_rows(
        tests,
        "genetic_test",
        "test_key",
        "is_complete_record",
        F.col("has_gene") & F.col("has_condition"),
        "genetic_test_record",
        "genetic_test.link_kind",
    ),
]
tests_per_gene = (
    test_rows_all.filter((F.col("link_kind") == "gene") & F.col("gene_key").isNotNull())
    .groupBy("gene_key")
    .agg(F.countDistinct("test_key").alias("n"))
)
test_gene_rows = [
    number_rows(
        tests_per_gene,
        "gene",
        "gene_key",
        "gene_test_count",
        F.col("n"),
        "gene_test_count",
        "genetic_test.genes",
    ),
    flag_rows(
        tests_per_gene,
        "gene",
        "gene_key",
        "is_frequently_tested",
        F.col("n") >= RULES["frequently_tested_minimum"],
        "gene_test_count",
        "genetic_test.genes",
    ),
]

# COMMAND ----------

# DBTITLE 1,Transform -- pharmacogene evidence and counts
truthy = RULES["truthy_values"]


def truthy_claim(field):
    return (
        claims.filter(
            (F.col("entity_type") == "gene")
            & (F.col("field") == field)
            & F.lower(F.trim(F.col("value_text"))).isin(truthy)
        )
        .select(F.col("entity_key").alias("gene_key"))
        .distinct()
        .withColumn(field, F.lit(1))
    )


relations = relation_table.filter(
    (F.col("relation_kind") == "drug_gene") & F.col("gene_key").isNotNull()
)


def relation_kind(kind, column):
    return (
        relations.filter(F.col("drug_gene_relation_type") == kind)
        .select("gene_key")
        .distinct()
        .withColumn(column, F.lit(1))
    )


evidence = (
    gene.select("gene_key")
    .join(truthy_claim("has_cpic_dosing_guideline"), "gene_key", "left")
    .join(truthy_claim("has_variant_annotation"), "gene_key", "left")
    .join(truthy_claim("is_vip"), "gene_key", "left")
    .join(relation_kind("cpic_pair", "has_cpic_pair"), "gene_key", "left")
    .join(relation_kind("fda_label_biomarker", "has_fda_label"), "gene_key", "left")
    .withColumn(
        "clinical_guideline",
        F.coalesce("has_cpic_dosing_guideline", "has_cpic_pair", F.lit(0)),
    )
    .withColumn("fda_label", F.coalesce("has_fda_label", F.lit(0)))
    .withColumn(
        "pharmgkb_validated",
        F.coalesce("has_variant_annotation", "is_vip", F.lit(0)),
    )
    .withColumn(
        "source_count",
        F.col("clinical_guideline") + F.col("fda_label") + F.col("pharmgkb_validated"),
    )
    .filter(F.col("source_count") > 0)
)
pharmacogene = RULES["pharmacogene"]
evidence = evidence.withColumn(
    "evidence_level",
    F.when(F.col("source_count") >= pharmacogene["level_high_from"], "High")
    .when(F.col("source_count") >= pharmacogene["level_medium_from"], "Medium")
    .otherwise("Low"),
).withColumn(
    "category",
    F.when(F.col("clinical_guideline") == 1, "Clinical_Guideline")
    .when(F.col("fda_label") == 1, "FDA_Label")
    .otherwise("PharmGKB_Validated"),
)
PHARMACOGENE_SOURCE = "gene claims+entity_relation"
pharmacogene_rows = [
    flag_rows(
        evidence,
        "gene",
        "gene_key",
        "is_pharmacogene",
        F.lit(True),
        "pharmacogene",
        PHARMACOGENE_SOURCE,
    ),
    number_rows(
        evidence,
        "gene",
        "gene_key",
        "pharmacogene_source_count",
        F.col("source_count"),
        "pharmacogene",
        PHARMACOGENE_SOURCE,
        method="rule",
    ),
    text_rows(
        evidence,
        "gene",
        "gene_key",
        "pharmacogene_evidence_level",
        F.col("evidence_level"),
        "pharmacogene",
        PHARMACOGENE_SOURCE,
    ),
    text_rows(
        evidence,
        "gene",
        "gene_key",
        "pharmacogene_category",
        F.col("category"),
        "pharmacogene",
        PHARMACOGENE_SOURCE,
    ),
]
association_all = gene_disease
relation_counts = [
    number_rows(
        association_all.groupBy("gene_key").agg(
            F.countDistinct("disease_key").alias("n")
        ),
        "gene",
        "gene_key",
        "associated_disease_count",
        F.col("n"),
        "associated_disease_count",
        "entity_relation",
    ),
    number_rows(
        association_all.groupBy("disease_key").agg(
            F.countDistinct("gene_key").alias("n")
        ),
        "disease",
        "disease_key",
        "associated_gene_count",
        F.col("n"),
        "associated_gene_count",
        "entity_relation",
    ),
    number_rows(
        relations.filter(F.col("drug_key").isNotNull())
        .groupBy("drug_key")
        .agg(F.countDistinct("gene_key").alias("n")),
        "drug",
        "drug_key",
        "gene_target_count",
        F.col("n"),
        "gene_target_count",
        "entity_relation",
    ),
    number_rows(
        relations.filter(F.col("drug_key").isNotNull())
        .groupBy("gene_key")
        .agg(F.countDistinct("drug_key").alias("n")),
        "gene",
        "gene_key",
        "related_drug_count",
        F.col("n"),
        "related_drug_count",
        "entity_relation",
    ),
]
pathogenic_variants = (
    current_snapshot(read_silver("variant_classification"), RULES)
    .filter(
        (F.col("classification_type") == "germline")
        & F.col("term").isin(RULES["pathogenic_terms"])
    )
    .select("variant_key")
    .distinct()
)
relation_counts.append(
    number_rows(
        variant.filter((F.col("assembly") == "GRCh38") & F.col("gene_key").isNotNull())
        .join(pathogenic_variants, "variant_key")
        .groupBy("gene_key")
        .agg(F.countDistinct("variant_key").alias("n")),
        "gene",
        "gene_key",
        "pathogenic_variant_count",
        F.col("n"),
        "pathogenic_variant_count",
        "variant+variant_classification",
    )
)

# COMMAND ----------

# DBTITLE 1,Read Silver -- variant and gene inputs of the feature attributes
FEATURES = {
    name: RULES[f"{name}_features"]
    for name in (
        "clinical",
        "disease",
        "pharmacogene",
        "variant_impact",
        "structural_variant",
        "pharmacogene_gene",
        "drug_response",
        "expression",
        "protein_family",
        "genetic_test",
        "cancer",
        "population",
    )
}
VALUE_KINDS = {"number": "number", "text": "text", "flag": "boolean"}


def attribute_rows(df, entity_type, key, rule_name, source, specs):
    """Rows of several attributes of one frame in one pass: `specs` lists
    (attribute, Column, kind); kind `number`, `text` or `flag` (a flag gives a row
    only when true, a number or text only when not null)."""
    items = [
        F.struct(
            F.lit(attribute).alias("attribute"),
            (value.cast("string") if kind == "text" else none_text).alias("value_text"),
            (
                value.cast("double") if kind == "number" else F.lit(None).cast("double")
            ).alias("value_number"),
            (F.when(value, F.lit(True)) if kind == "flag" else F.lit(None))
            .cast("boolean")
            .alias("value_boolean"),
            F.lit(VALUE_KINDS[kind]).alias("value_type"),
        )
        for attribute, value, kind in specs
    ]
    rows = df.select(
        F.col(key).cast("string").alias("entity_key"),
        F.explode(F.array(*items)).alias("item"),
    ).filter(F.col("entity_key").isNotNull())
    return rows.filter(
        F.col("item.value_text").isNotNull()
        | F.col("item.value_number").isNotNull()
        | F.col("item.value_boolean").isNotNull()
    ).select(
        F.lit(entity_type).alias("entity_type"),
        "entity_key",
        F.col("item.attribute").alias("attribute"),
        F.col("item.value_text").alias("value_text"),
        F.col("item.value_number").alias("value_number"),
        F.col("item.value_boolean").alias("value_boolean"),
        F.col("item.value_type").alias("value_type"),
        F.lit("rule").alias("method"),
        none_text.alias("matched_text"),
        F.lit(source).alias("text_source"),
        F.lit(rule_name).alias("rule_name"),
        F.lit(VERSION).alias("rule_version"),
        AS_OF.alias("as_of_date"),
    )


def band(entries, value, default):
    """The label of the first band (falling `from`) the value reaches."""
    return from_band_expr(entries, value, default)


def count_if(condition):
    return F.sum(F.when(condition, 1).otherwise(0))


def flag(name):
    """True when the attribute set `flags` of the row holds `name`."""
    return F.coalesce(F.array_contains(F.col("flags"), name), F.lit(False))


def any_flag(names):
    result = F.lit(False)
    for name in names:
        result = result | flag(name)
    return result


# -- Read Silver -- variant flags (current ClinVar state; every snapshot stays in the
# ClinVar tables)
hgvs_types = (
    current_snapshot(read_silver("variant_hgvs"), RULES)
    .groupBy("variant_key")
    .agg(
        F.max(F.col("protein_edit_type") == "frameshift").alias("is_frameshift"),
        F.max(F.col("protein_edit_type") == "nonsense").alias("is_nonsense"),
    )
)
significance = labelled.groupBy("variant_key").agg(
    F.max(F.col("significance_label").isin(RULES["pathogenic_labels"])).alias(
        "is_pathogenic"
    ),
    F.max(F.col("significance_label").isin(RULES["benign_labels"])).alias("is_benign"),
    F.max(F.col("significance_label").isin(RULES["vus_labels"])).alias("is_vus"),
    F.max("review_stars").alias("review_stars"),
)
VARIANT_FLAGS = (
    "is_lof",
    "is_splice",
    "is_missense",
    "is_frameshift",
    "is_nonsense",
    "affects_functional_domain",
    "has_protein_feature",
    "is_pathogenic",
    "is_benign",
    "is_vus",
)
variant_base = (
    variant.filter(F.col("in_clinvar"))
    .select(
        "variant_key",
        "gene_key",
        "contig",
        "position",
        "reference_allele",
        "alternate_allele",
        "in_pharmgkb",
        "origin",
    )
    .join(
        impact.select(
            "variant_key",
            "is_lof",
            "is_splice",
            "is_missense",
            "has_protein_feature",
            "phylop_score",
            "cadd_phred",
            "severity",
            "conservation_level",
            "pathogenicity_score",
            "affects_functional_domain",
            "has_conservation",
        ),
        "variant_key",
        "left",
    )
    .join(hgvs_types, "variant_key", "left")
    .join(significance, "variant_key", "left")
    .join(frequency.select("variant_key", "max_frequency"), "variant_key", "left")
)
for name in VARIANT_FLAGS:
    variant_base = variant_base.withColumn(name, F.coalesce(F.col(name), F.lit(False)))
variant_base = (
    variant_base.withColumn(
        "is_highly_conserved", F.col("phylop_score") > cutoffs["phylop_conserved_above"]
    )
    .withColumn(
        "is_deleterious_by_cadd",
        F.col("cadd_phred") > cutoffs["cadd_deleterious_above"],
    )
    .withColumn(
        "is_common",
        F.coalesce(
            F.col("max_frequency") >= RULES["frequency_flags"]["common_from"],
            F.lit(False),
        ),
    )
    .withColumn(
        "is_rare",
        F.coalesce(
            F.col("max_frequency") < RULES["frequency_flags"]["rare_below"],
            F.lit(False),
        ),
    )
    .withColumn("is_high_impact", high_impact)
    .withColumn("is_very_high_impact", very_high_impact)
)

# -- Read Silver -- gene attributes (function flags, disease categories, expression,
# cancer counts, PharmGKB and OMIM ids, protein domain flags)
gene_flags = (
    function_flags.unionByName(disease_category_flags)
    .unionByName(keyword_flags)
    .groupBy(F.col("entity_key").alias("gene_key"))
    .agg(F.collect_set("attribute").alias("flags"))
)
gene_ids_of = xwalk.filter(F.col("entity_type") == "gene")
pharmgkb_genes = (
    gene_ids_of.filter(F.col("id_type") == "pharmgkb")
    .select(F.col("entity_key").alias("gene_key"))
    .distinct()
    .withColumn("has_pharmgkb_annotation", F.lit(True))
)
omim_ids = (
    gene_ids_of.filter(F.col("id_type") == "omim")
    .groupBy(F.col("entity_key").alias("gene_key"))
    .agg(F.min("id_value").alias("gene_mim_id"))
)
omim_diseases = (
    gene_disease.filter(F.col("relation_subkind") == "omim_gene_link")
    .groupBy("gene_key")
    .agg(F.countDistinct("disease_key").alias("omim_disease_count"))
)
liver = FEATURES["pharmacogene"]["liver_tissues"]
kidney = FEATURES["pharmacogene"]["kidney_tissues"]
expressed_genes = (
    expression.filter(
        F.col("median_transcripts_per_million")
        > FEATURES["clinical"]["expression_tpm_above"]
    )
    .groupBy("gene_key")
    .agg(
        F.countDistinct("tissue_key").alias("tissues"),
        F.max("median_transcripts_per_million").alias("max_tpm"),
        F.avg("median_transcripts_per_million").alias("average_tpm"),
        F.max(F.col("tissue_key").isin(liver)).alias("is_liver_expressed"),
        F.max(F.col("tissue_key").isin(kidney)).alias("is_kidney_expressed"),
    )
)
all_expression = expression.groupBy("gene_key").agg(
    F.countDistinct(
        F.when(F.col("median_transcripts_per_million") > 0, F.col("tissue_key"))
    ).alias("tissues_with_expression"),
    F.max("median_transcripts_per_million").alias("peak_tpm"),
    F.avg("median_transcripts_per_million").alias("mean_tpm"),
)
protein_genes = read_silver("protein_gene_link").select("protein_key", "gene_key")
protein_flag_sets = protein_flags.groupBy(F.col("entity_key").alias("protein_key")).agg(
    F.collect_set("attribute").alias("flags")
)
DOMAIN_FLAGS = [
    "has_zinc_finger",
    "has_kinase_in_name",
    "has_receptor_in_name",
    "has_sh2_domain",
    "has_sh3_domain",
    "has_ph_domain",
    "has_death_domain",
    "has_leucine_zipper",
    "has_helix_loop_helix",
    "has_immunoglobulin",
]
gene_domains = (
    domain_per_protein.join(protein_flag_sets, "protein_key", "left")
    .join(protein_genes, "protein_key")
    .groupBy("gene_key")
    .agg(
        F.countDistinct("protein_key").alias("protein_count"),
        F.max("n").alias("max_domain_count"),
        count_if(F.col("n") > 0).alias("proteins_with_functional_domain"),
        *[count_if(flag(name)).alias(f"proteins_{name}") for name in DOMAIN_FLAGS],
    )
)
gene_base = (
    gene.select("gene_key", "description", "key_source", "ensembl_id", "symbol")
    .join(gene_flags, "gene_key", "left")
    .join(function_scored.select("gene_key", "druggability_score"), "gene_key", "left")
    .join(
        evidence.select("gene_key", F.lit(True).alias("is_pharmacogene")),
        "gene_key",
        "left",
    )
    .join(pharmgkb_genes, "gene_key", "left")
    .join(omim_ids, "gene_key", "left")
    .join(omim_diseases, "gene_key", "left")
    .join(disease_totals, "gene_key", "left")
    .join(expressed_genes, "gene_key", "left")
    .join(all_expression, "gene_key", "left")
    .join(mutation_counts, "gene_key", "left")
    .join(gene_domains, "gene_key", "left")
    .withColumn("flags", F.coalesce(F.col("flags"), F.array().cast("array<string>")))
)
for name in ("is_pharmacogene", "has_pharmgkb_annotation"):
    gene_base = gene_base.withColumn(name, F.coalesce(F.col(name), F.lit(False)))
for name in ("is_liver_expressed", "is_kidney_expressed"):
    gene_base = gene_base.withColumn(name, F.coalesce(F.col(name), F.lit(False)))
for name in (
    "omim_disease_count",
    "total_disease_count",
    "tissues",
    "tissues_with_expression",
    "n",
    "samples",
    "max_domain_count",
    "protein_count",
    "proteins_with_functional_domain",
    *[f"proteins_{name}" for name in DOMAIN_FLAGS],
):
    gene_base = gene_base.withColumn(name, F.coalesce(F.col(name), F.lit(0)))
gene_base = gene_base.withColumnRenamed("n", "cancer_mutation_count").withColumnRenamed(
    "samples", "cancer_samples"
)
GENE_SOURCE = "gene attributes+entity_relation+gene_expression_median+somatic_mutation"
VARIANT_SOURCE = "variant+variant_hgvs+variant_classification+locus_score"

# COMMAND ----------

# DBTITLE 1,Transform -- clinical variant and gene features
rule = FEATURES["clinical"]
clinical = variant_base.withColumn(
    "has_conservation_data",
    F.col("phylop_score").isNotNull() | F.col("cadd_phred").isNotNull(),
)
inheritance = F.lit(rule["inheritance_default"])
for contig, label in reversed(list(rule["inheritance"].items())):
    inheritance = F.when(F.col("contig") == contig, F.lit(label)).otherwise(inheritance)
points = rule["combined_risk_points"]
clinical_variant_rows = attribute_rows(
    clinical,
    "variant",
    "variant_key",
    "clinical_features",
    VARIANT_SOURCE,
    [
        (
            "is_coding_variant",
            F.col("is_missense") | F.col("is_frameshift") | F.col("is_nonsense"),
            "flag",
        ),
        ("is_regulatory_variant", F.col("is_splice"), "flag"),
        (
            "has_strong_evidence",
            F.col("review_stars") >= rule["strong_evidence_review_from"],
            "flag",
        ),
        (
            "combined_pathogenicity_risk",
            F.coalesce(F.col("severity"), F.lit(0))
            + F.coalesce(F.col("conservation_level"), F.lit(0))
            + F.when(
                F.col("affects_functional_domain"), points["domain_affecting"]
            ).otherwise(0)
            + F.when(
                F.col("is_deleterious_by_cadd"), points["deleterious_by_cadd"]
            ).otherwise(0),
            "number",
        ),
        ("has_conservation_data", F.col("has_conservation_data"), "flag"),
        (
            "has_complete_annotation",
            F.col("severity").isNotNull() & F.col("has_conservation_data"),
            "flag",
        ),
        ("clinical_significance_is_uncertain", F.col("review_stars") == 0, "flag"),
        ("inheritance_pattern", inheritance, "text"),
        ("is_mitochondrial_variant", F.col("contig") == "MT", "flag"),
        ("is_y_linked_variant", F.col("contig") == "Y", "flag"),
        ("is_x_linked_variant", F.col("contig") == "X", "flag"),
        ("is_autosomal_variant", ~F.col("contig").isin("X", "Y", "MT"), "flag"),
        (
            "x_linked_risk_modifier",
            F.when(F.col("contig") == "X", "X_Linked_Risk").otherwise("No_X_Risk"),
            "text",
        ),
        (
            "inheritance_pathogenicity_modifier",
            F.when(F.col("contig") == "MT", "Mitochondrial_Risk")
            .when(F.col("contig") == "X", "X_Linked_Risk")
            .otherwise("Standard"),
            "text",
        ),
        ("frequency_pathogenicity_conflict", F.col("is_common"), "flag"),
    ],
)
gene_variants = variant_base.filter(F.col("gene_key").isNotNull()).groupBy("gene_key")
gene_stats = gene_variants.agg(
    F.count(F.lit(1)).alias("total"),
    count_if(F.col("is_pathogenic")).alias("pathogenic"),
    count_if(F.col("is_benign")).alias("benign"),
    count_if(F.col("is_vus")).alias("vus"),
    count_if(F.col("is_missense")).alias("missense"),
    count_if(F.col("is_frameshift")).alias("frameshift"),
    count_if(F.col("is_nonsense")).alias("nonsense"),
    count_if(F.col("is_splice")).alias("splice"),
    count_if(F.col("is_lof")).alias("loss_of_function"),
    count_if(F.col("is_high_impact")).alias("high_impact"),
    count_if(F.col("is_very_high_impact")).alias("very_high_impact"),
    count_if(F.col("affects_functional_domain")).alias("domain_affecting"),
    F.avg("review_stars").alias("average_review"),
    count_if(
        F.col("review_stars") >= FEATURES["disease"]["high_quality_review_from"]
    ).alias("high_quality"),
    count_if(F.col("affects_functional_domain") & F.col("is_pathogenic")).alias(
        "domain_pathogenic"
    ),
    count_if(F.col("affects_functional_domain") & F.col("is_very_high_impact")).alias(
        "critical_domain"
    ),
    F.avg("pathogenicity_score").alias("average_pathogenicity"),
)
ratio = {name: F.col(name) / F.col("total") for name in ("pathogenic", "benign", "vus")}
lof_ratio = (F.col("frameshift") + F.col("nonsense")) / F.col("total")
pathogenic_enriched = (
    F.coalesce(ratio["pathogenic"], F.lit(0.0)) > rule["pathogenic_enriched_above"]
)
benign_enriched = (
    F.coalesce(ratio["benign"], F.lit(0.0)) > rule["benign_enriched_above"]
)
vus_enriched = F.coalesce(ratio["vus"], F.lit(0.0)) > rule["vus_enriched_above"]
clinical_gene = gene_base.join(gene_stats, "gene_key", "left")
clinical_gene_rows = attribute_rows(
    clinical_gene,
    "gene",
    "gene_key",
    "clinical_features",
    GENE_SOURCE + "+variant",
    [
        ("gene_total_variants", F.col("total"), "number"),
        ("gene_pathogenic_count", F.col("pathogenic"), "number"),
        ("gene_benign_count", F.col("benign"), "number"),
        ("gene_vus_count", F.col("vus"), "number"),
        ("gene_missense_count", F.col("missense"), "number"),
        ("gene_frameshift_count", F.col("frameshift"), "number"),
        ("gene_nonsense_count", F.col("nonsense"), "number"),
        ("gene_splice_count", F.col("splice"), "number"),
        ("gene_average_review_quality", F.col("average_review"), "number"),
        ("gene_pathogenic_ratio", ratio["pathogenic"], "number"),
        ("gene_benign_ratio", ratio["benign"], "number"),
        ("gene_vus_ratio", ratio["vus"], "number"),
        ("gene_loss_of_function_variant_ratio", lof_ratio, "number"),
        (
            "gene_mutation_burden",
            band(
                rule["mutation_burden"], F.col("total"), rule["mutation_burden_default"]
            ),
            "text",
        ),
        ("gene_is_pathogenic_enriched", pathogenic_enriched, "flag"),
        ("gene_is_benign_enriched", benign_enriched, "flag"),
        ("gene_is_vus_enriched", vus_enriched, "flag"),
        (
            "gene_variant_profile",
            F.when(
                F.col("total").isNotNull(),
                F.when(pathogenic_enriched, "Pathogenic_Enriched")
                .when(benign_enriched, "Benign_Enriched")
                .when(vus_enriched, "VUS_Enriched")
                .otherwise("Mixed_Profile"),
            ),
            "text",
        ),
        (
            "gene_has_high_loss_of_function_burden",
            F.coalesce(lof_ratio, F.lit(0.0)) > rule["high_lof_burden_above"],
            "flag",
        ),
        (
            "gene_has_quality_annotations",
            F.coalesce(F.col("average_review"), F.lit(0.0))
            >= rule["quality_annotations_from"],
            "flag",
        ),
        ("gene_is_validated", F.col("key_source") == "ncbi_gene_id", "flag"),
        ("gene_has_omim", F.col("gene_mim_id").isNotNull(), "flag"),
        ("gene_has_ensembl", F.col("ensembl_id").isNotNull(), "flag"),
        (
            "gene_description_length",
            F.coalesce(F.length("description"), F.lit(0)),
            "number",
        ),
        (
            "gene_is_well_characterized",
            F.coalesce(F.length("description"), F.lit(0))
            > rule["well_characterized_description_above"],
            "flag",
        ),
        ("tissues_expressed_count", F.col("tissues"), "number"),
        ("max_expression_transcripts_per_million", F.col("max_tpm"), "number"),
        ("average_expression_transcripts_per_million", F.col("average_tpm"), "number"),
        (
            "is_broadly_expressed",
            F.col("tissues") >= rule["broadly_expressed_tissues_from"],
            "flag",
        ),
        (
            "is_highly_expressed",
            F.col("max_tpm") >= rule["highly_expressed_tpm_from"],
            "flag",
        ),
        (
            "expression_context",
            band(
                rule["expression_context"],
                F.col("tissues"),
                rule["expression_context_default"],
            ),
            "text",
        ),
        (
            "is_cancer_gene",
            F.col("cancer_mutation_count") >= rule["cancer_gene_mutations_from"],
            "flag",
        ),
        ("is_disease_gene", F.col("total_disease_count") >= 1, "flag"),
    ],
)

# COMMAND ----------

# DBTITLE 1,Transform -- disease association and polygenic features
rule = FEATURES["disease"]
coverage_types = rule["coverage_id_types"]
condition_rows = (
    current_snapshot(read_silver("variant_condition"), RULES)
    .select("variant_key", "disease_key", F.col("condition_id").alias("raw_id"))
    .withColumn("id_type", condition_type)
)
variant_disease = condition_rows.groupBy("variant_key").agg(
    *[
        F.max(F.col("id_type") == name).alias(f"has_{name}_disease")
        for name in coverage_types
    ]
)
coverage = sum(
    F.when(F.coalesce(F.col(f"has_{name}_disease"), F.lit(False)), 1).otherwise(0)
    for name in coverage_types
)
disease_variant = (
    variant_base.join(variant_disease, "variant_key", "left")
    .withColumn("coverage", coverage)
    .join(
        gene_base.select("gene_key", "omim_disease_count"),
        "gene_key",
        "left",
    )
)
# disease-level counts over the variants of each disease
disease_variants = (
    condition_rows.filter(F.col("disease_key").isNotNull())
    .select("variant_key", "disease_key")
    .distinct()
    .join(variant_base, "variant_key")
)
disease_stats = (
    disease_variants.groupBy("disease_key")
    .agg(
        F.count(F.lit(1)).alias("total"),
        count_if(F.col("is_pathogenic")).alias("pathogenic"),
        count_if(F.col("is_benign")).alias("benign"),
        count_if(F.col("is_vus")).alias("vus"),
        F.countDistinct("gene_key").alias("genes"),
    )
    .withColumn("pathogenic_ratio", F.col("pathogenic") / F.col("total"))
)
polygenic = disease_stats.filter(
    F.col("genes") >= rule["polygenic_gene_count_from"]
).select("disease_key")
variant_polygenic = (
    disease_variants.join(polygenic, "disease_key")
    .select("variant_key")
    .distinct()
    .withColumn("in_polygenic_disease", F.lit(True))
)
disease_variant = disease_variant.join(variant_polygenic, "variant_key", "left")
in_polygenic = F.coalesce(F.col("in_polygenic_disease"), F.lit(False))
omim_gene = F.coalesce(F.col("omim_disease_count"), F.lit(0)) >= 1
DISEASE_SOURCE = "variant_condition+variant_classification+entity_relation"
disease_variant_rows = attribute_rows(
    disease_variant,
    "variant",
    "variant_key",
    "disease_features",
    DISEASE_SOURCE,
    [
        *[
            (f"has_{name}_disease", F.col(f"has_{name}_disease"), "flag")
            for name in coverage_types
        ],
        ("disease_database_coverage", F.col("coverage"), "number"),
        (
            "disease_is_well_annotated",
            F.col("coverage") >= rule["well_annotated_coverage_from"],
            "flag",
        ),
        (
            "variant_disease_link_quality",
            F.when(F.col("has_omim_disease") & omim_gene, "High_Quality")
            .when(
                F.col("coverage") >= rule["well_annotated_coverage_from"],
                "Medium_Quality",
            )
            .when(F.col("coverage") >= 1, "Low_Quality")
            .otherwise("No_Link"),
            "text",
        ),
        (
            "polygenic_risk_contribution",
            F.when(F.col("is_pathogenic") & in_polygenic, "High_Risk_Contributor")
            .when(F.col("is_vus") & in_polygenic, "Moderate_Risk_Contributor")
            .when(in_polygenic, "Low_Risk_Contributor")
            .otherwise("Not_Applicable"),
            "text",
        ),
        (
            "has_high_conservation",
            F.col("is_highly_conserved") | F.col("is_deleterious_by_cadd"),
            "flag",
        ),
    ],
)
disease_level_rows = attribute_rows(
    disease_stats.join(
        disease.select("disease_key", F.col("name").alias("disease_name")),
        "disease_key",
        "left",
    ),
    "disease",
    "disease_key",
    "disease_features",
    DISEASE_SOURCE,
    [
        ("disease_total_variants", F.col("total"), "number"),
        ("disease_pathogenic_variants", F.col("pathogenic"), "number"),
        ("disease_benign_variants", F.col("benign"), "number"),
        ("disease_vus_variants", F.col("vus"), "number"),
        ("disease_gene_count", F.col("genes"), "number"),
        ("disease_pathogenic_ratio", F.col("pathogenic_ratio"), "number"),
        (
            "is_polygenic_disease",
            F.col("genes") >= rule["polygenic_gene_count_from"],
            "flag",
        ),
        (
            "disease_complexity",
            band(
                rule["disease_complexity"],
                F.col("genes"),
                rule["disease_complexity_default"],
            ),
            "text",
        ),
        (
            "disease_complexity_score",
            band(
                rule["disease_complexity_score"],
                F.col("genes"),
                rule["disease_complexity_score_default"],
            ).cast("int"),
            "number",
        ),
        (
            "disease_has_high_pathogenic_burden",
            F.coalesce(F.col("pathogenic_ratio"), F.lit(0.0))
            > rule["high_pathogenic_burden_above"],
            "flag",
        ),
        (
            "disease_name_is_generic",
            F.col("disease_name").rlike("(?i)" + rule["generic_name_pattern"])
            & (F.length("disease_name") < rule["generic_name_shorter_than"]),
            "flag",
        ),
    ],
)
# gene-level prioritization over the variants of each gene
disease_gene = (
    disease_variant.filter(F.col("gene_key").isNotNull())
    .groupBy("gene_key")
    .agg(
        count_if(F.col("has_omim_disease")).alias("omim_variants"),
        count_if(F.col("has_mondo_disease")).alias("mondo_variants"),
        count_if(F.col("coverage") >= rule["well_annotated_coverage_from"]).alias(
            "well_annotated_variants"
        ),
    )
)
diversity = (
    disease_variants.filter(F.col("gene_key").isNotNull())
    .groupBy("gene_key")
    .agg(F.countDistinct("disease_key").alias("disease_diversity"))
)
weights = rule["utility_weights"]
prioritized = (
    gene_base.join(gene_stats, "gene_key", "left")
    .join(disease_gene, "gene_key", "left")
    .join(diversity, "gene_key", "left")
    .fillna(
        0,
        [
            "total",
            "pathogenic",
            "high_quality",
            "omim_variants",
            "mondo_variants",
            "well_annotated_variants",
            "disease_diversity",
        ],
    )
    .withColumn(
        "utility",
        F.col("pathogenic") * weights["pathogenic"]
        + F.col("high_quality") * weights["high_quality"]
        + F.col("disease_diversity") * weights["disease_diversity"],
    )
    .withColumn(
        "annotation",
        F.col("omim_variants")
        + F.col("mondo_variants")
        + F.col("well_annotated_variants"),
    )
)
disease_gene_rows = attribute_rows(
    prioritized,
    "gene",
    "gene_key",
    "disease_features",
    GENE_SOURCE + "+variant_condition",
    [
        (
            "disease_count_category",
            band(
                rule["disease_count_category"],
                F.col("total_disease_count"),
                rule["disease_count_category_default"],
            ),
            "text",
        ),
        (
            "is_multi_disease_gene",
            F.col("total_disease_count") >= rule["multi_disease_from"],
            "flag",
        ),
        (
            "disease_association_strength",
            band(
                rule["association_strength"],
                F.col("total_disease_count"),
                rule["association_strength_default"],
            ).cast("int"),
            "number",
        ),
        ("is_omim_gene", F.col("omim_disease_count") >= 1, "flag"),
        ("omim_disease_count", F.col("omim_disease_count"), "number"),
        ("gene_high_quality_count", F.col("high_quality"), "number"),
        ("gene_disease_diversity", F.col("disease_diversity"), "number"),
        ("gene_omim_variants", F.col("omim_variants"), "number"),
        ("gene_mondo_variants", F.col("mondo_variants"), "number"),
        ("gene_well_annotated_variants", F.col("well_annotated_variants"), "number"),
        ("gene_clinical_utility_score", F.col("utility"), "number"),
        (
            "gene_priority_tier",
            band(
                rule["priority_tier"], F.col("utility"), rule["priority_tier_default"]
            ),
            "text",
        ),
        (
            "is_clinically_actionable",
            (F.col("pathogenic") >= rule["actionable"]["pathogenic_from"])
            & (F.col("high_quality") >= rule["actionable"]["high_quality_from"]),
            "flag",
        ),
        (
            "is_research_candidate",
            (F.col("total") >= rule["research_candidate"]["variants_from"])
            & (
                F.col("disease_diversity")
                >= rule["research_candidate"]["diversity_from"]
            ),
            "flag",
        ),
        ("gene_annotation_score", F.col("annotation"), "number"),
        (
            "has_excellent_annotation",
            F.col("annotation") >= rule["excellent_annotation_from"],
            "flag",
        ),
        (
            "annotation_priority_level",
            band(
                rule["annotation_priority"],
                F.col("annotation"),
                rule["annotation_priority_default"],
            ),
            "text",
        ),
        (
            "is_cancer_hotspot_gene",
            F.col("cancer_mutation_count")
            >= rule["cancer_hotspot_gene_mutations_from"],
            "flag",
        ),
        ("gene_domain_count", F.col("max_domain_count"), "number"),
        (
            "has_kinase_domain",
            F.col("proteins_has_kinase_in_name") > 0,
            "flag",
        ),
        (
            "is_complex_protein",
            F.col("max_domain_count") >= rule["complex_protein_domains_from"],
            "flag",
        ),
    ],
)

# COMMAND ----------

# DBTITLE 1,Transform -- pharmacogene variant and gene features
rule = FEATURES["pharmacogene"]
target = any_flag(rule["drug_target_flags"])
metabolizing = any_flag(rule["metabolizing_flags"])
target_category = F.lit(rule["drug_target_category_default"])
for entry in reversed(rule["drug_target_category"]):
    target_category = F.when(any_flag(entry["any"]), F.lit(entry["label"])).otherwise(
        target_category
    )
points = rule["enhanced_druggability_points"]
pharma_gene = (
    gene_base.withColumn("is_drug_target", target)
    .withColumn("is_metabolizing_enzyme", metabolizing)
    .withColumn(
        "enhanced_druggability_score",
        F.coalesce(F.col("druggability_score"), F.lit(0.0))
        + F.when(F.col("is_pharmacogene"), points["pharmacogene"]).otherwise(0)
        + F.when(target, points["drug_target"]).otherwise(0)
        + F.when(
            F.col("has_pharmgkb_annotation"), points["pharmgkb_annotation"]
        ).otherwise(0),
    )
)
pharma_variant = (
    variant_base.join(
        pharma_gene.select(
            "gene_key",
            "flags",
            "is_drug_target",
            "is_metabolizing_enzyme",
            "is_pharmacogene",
            "enhanced_druggability_score",
            "proteins_has_kinase_in_name",
        ),
        "gene_key",
        "left",
    )
    .withColumn("flags", F.coalesce(F.col("flags"), F.array().cast("array<string>")))
    .withColumn("is_pharmacogene", F.coalesce(F.col("is_pharmacogene"), F.lit(False)))
    .withColumn("changes_protein", F.col("is_missense") | F.col("is_lof"))
    .withColumn(
        "interaction_potential",
        (F.col("is_pharmacogene") | target | metabolizing) & F.col("changes_protein"),
    )
    .withColumn("metabolizer", metabolizing & F.col("changes_protein"))
    .withColumn("transporter", flag("is_transporter") & F.col("changes_protein"))
    .withColumn(
        "kinase_domain",
        flag("is_kinase")
        & (F.coalesce(F.col("proteins_has_kinase_in_name"), F.lit(0)) > 0),
    )
)
PHARMA_SOURCE = VARIANT_SOURCE + "+gene attributes"
pharma_variant_rows = attribute_rows(
    pharma_variant,
    "variant",
    "variant_key",
    "pharmacogene_features",
    PHARMA_SOURCE,
    [
        ("has_drug_interaction_potential", F.col("interaction_potential"), "flag"),
        (
            "drug_response_impact",
            F.when(
                F.col("interaction_potential") & F.col("is_pathogenic"), "High_Impact"
            )
            .when(F.col("interaction_potential") & F.col("is_vus"), "Moderate_Impact")
            .when(F.col("interaction_potential"), "Low_Impact")
            .otherwise("No_Impact"),
            "text",
        ),
        ("is_metabolizer_variant", F.col("metabolizer"), "flag"),
        (
            "metabolizer_phenotype_risk",
            F.when(F.col("metabolizer") & F.col("is_lof"), "Poor_Metabolizer_Risk")
            .when(
                F.col("metabolizer") & F.col("is_pathogenic"),
                "Altered_Metabolizer_Risk",
            )
            .otherwise("Normal_Metabolizer"),
            "text",
        ),
        ("is_transporter_variant", F.col("transporter"), "flag"),
        (
            "transporter_impact_level",
            F.when(
                F.col("transporter") & F.col("is_pathogenic"), "High_Transport_Impact"
            )
            .when(F.col("transporter") & F.col("is_vus"), "Moderate_Transport_Impact")
            .when(F.col("transporter"), "Low_Transport_Impact")
            .otherwise("No_Transport_Impact"),
            "text",
        ),
        (
            "kinase_variant_therapeutic_relevance",
            F.when(
                F.col("kinase_domain")
                & F.col("is_missense")
                & F.col("affects_functional_domain"),
                "High_Therapeutic_Relevance",
            )
            .when(
                F.col("kinase_domain") & F.col("is_missense"),
                "Moderate_Therapeutic_Relevance",
            )
            .when(F.col("kinase_domain"), "Low_Therapeutic_Relevance")
            .otherwise("No_Therapeutic_Relevance"),
            "text",
        ),
        (
            "drug_response_frequency_context",
            F.when(
                F.col("is_common") & F.col("interaction_potential"),
                "Common_Drug_Response_Variant",
            )
            .when(
                F.col("is_rare") & F.col("interaction_potential"),
                "Rare_Drug_Response_Variant",
            )
            .otherwise("Standard_Frequency"),
            "text",
        ),
    ],
)
gene_pharma_stats = (
    pharma_variant.filter(
        F.col("gene_key").isNotNull()
        & (
            F.col("is_pharmacogene")
            | F.coalesce(F.col("is_drug_target"), F.lit(False))
            | F.coalesce(F.col("is_metabolizing_enzyme"), F.lit(False))
        )
    )
    .groupBy("gene_key")
    .agg(
        F.count(F.lit(1)).alias("variants"),
        count_if(F.col("interaction_potential")).alias("interaction"),
        count_if(F.col("metabolizer")).alias("metabolizer"),
        count_if(F.col("transporter")).alias("transporter"),
        count_if(F.col("is_pathogenic")).alias("pathogenic"),
        F.avg("enhanced_druggability_score").alias("average_druggability"),
    )
)
oncology = F.col("cancer_mutation_count") >= rule["oncology_target_mutations_from"]
indication = (
    F.when(flag("has_cancer_disease") & F.col("is_pharmacogene"), "Oncology")
    .when(flag("has_cardiovascular_disease") & F.col("is_pharmacogene"), "Cardiology")
    .when(flag("has_neurological_disease") & F.col("is_pharmacogene"), "Neurology")
    .when(F.col("is_pharmacogene"), "Other_Indication")
    .otherwise("Not_Applicable")
)
pharma_gene_rows = attribute_rows(
    pharma_gene.join(gene_pharma_stats, "gene_key", "left"),
    "gene",
    "gene_key",
    "pharmacogene_features",
    GENE_SOURCE,
    [
        ("is_drug_transporter", flag("is_transporter"), "flag"),
        ("is_drug_target", F.col("is_drug_target"), "flag"),
        ("is_metabolizing_enzyme", F.col("is_metabolizing_enzyme"), "flag"),
        (
            "metabolizing_enzyme_type",
            F.when(flag("is_enzyme"), "Phase2_Enzyme").otherwise(
                "Not_Metabolizing_Enzyme"
            ),
            "text",
        ),
        ("drug_target_category", target_category, "text"),
        ("enhanced_druggability_score", F.col("enhanced_druggability_score"), "number"),
        (
            "is_kinase_inhibitor_target",
            flag("is_kinase") & (F.col("proteins_has_kinase_in_name") > 0),
            "flag",
        ),
        ("has_pharmgkb_annotation", F.col("has_pharmgkb_annotation"), "flag"),
        ("gene_pharmacogene_variants", F.col("variants"), "number"),
        ("gene_drug_interaction_variants", F.col("interaction"), "number"),
        ("gene_metabolizer_variants", F.col("metabolizer"), "number"),
        ("gene_transporter_variants", F.col("transporter"), "number"),
        ("gene_pharmacogene_pathogenic", F.col("pathogenic"), "number"),
        ("gene_average_druggability", F.col("average_druggability"), "number"),
        ("gene_has_multiple_drug_variants", F.col("interaction") > 1, "flag"),
        (
            "gene_pharmacogene_burden",
            band(rule["burden"], F.col("pathogenic"), rule["burden_default"]),
            "text",
        ),
        (
            "gene_pharmacogene_priority",
            band(rule["priority"], F.col("interaction"), rule["priority_default"]),
            "text",
        ),
        (
            "gene_description_mentions_drug",
            F.coalesce(
                F.col("description").rlike("(?i)" + rule["drug_description_pattern"]),
                F.lit(False),
            ),
            "flag",
        ),
        ("is_liver_expressed", F.col("is_liver_expressed"), "flag"),
        ("is_kidney_expressed", F.col("is_kidney_expressed"), "flag"),
        (
            "expression_breadth",
            F.when(
                F.col("tissues") > 0,
                band(
                    rule["expression_breadth"],
                    F.col("tissues"),
                    rule["expression_breadth_default"],
                ),
            ),
            "text",
        ),
        (
            "drug_metabolism_context",
            F.when(F.col("is_liver_expressed") & metabolizing, "Hepatic_Metabolizer")
            .when(
                F.col("is_kidney_expressed") & flag("is_transporter"),
                "Renal_Transporter",
            )
            .when(metabolizing, "Other_Metabolizer")
            .otherwise("Non_Metabolic"),
            "text",
        ),
        ("is_oncology_target", oncology, "flag"),
        (
            "is_cancer_drug_target",
            oncology & (flag("is_kinase") | flag("is_receptor")),
            "flag",
        ),
        ("primary_indication_category", indication, "text"),
    ],
)

# COMMAND ----------

# DBTITLE 1,Transform -- variant impact features
# A protein domain type of a variant is a domain flag of a protein of its gene.
rule = FEATURES["variant_impact"]
points = rule["combined_impact_points"]
gene_context = gene_base.select(
    "gene_key",
    "flags",
    "druggability_score",
    "is_pharmacogene",
    "tissues",
    "max_tpm",
    "cancer_mutation_count",
    "total_disease_count",
    *[f"proteins_{name}" for name in rule["domain_type_flags"]],
)
domain_types = sum(
    F.when(F.coalesce(F.col(f"proteins_{name}"), F.lit(0)) > 0, 1).otherwise(0)
    for name in rule["domain_type_flags"]
)
impact_variant = (
    variant_base.join(gene_context, "gene_key", "left")
    .withColumn("flags", F.coalesce(F.col("flags"), F.array().cast("array<string>")))
    .withColumn("is_pharmacogene", F.coalesce(F.col("is_pharmacogene"), F.lit(False)))
    .fillna(0, ["tissues", "cancer_mutation_count", "total_disease_count"])
    .withColumn("domain_types", domain_types)
    .withColumn(
        "combined_impact",
        F.coalesce(F.col("severity"), F.lit(0))
        + F.coalesce(F.col("conservation_level"), F.lit(0))
        + F.when(F.col("affects_functional_domain"), points["domain"]).otherwise(0)
        + F.when(
            F.col("is_deleterious_by_cadd"), points["deleterious_by_cadd"]
        ).otherwise(0)
        + F.when(F.col("is_highly_conserved"), points["highly_conserved"]).otherwise(0),
    )
    .withColumn(
        "domain_severity",
        F.when(F.col("affects_functional_domain") & F.col("is_lof"), "Critical")
        .when(F.col("affects_functional_domain") & F.col("is_missense"), "High")
        .when(F.col("has_protein_feature") & F.col("is_missense"), "Moderate")
        .when(F.col("has_protein_feature"), "Low")
        .otherwise("Unknown"),
    )
    .withColumn(
        "druggable",
        F.coalesce(F.col("druggability_score"), F.lit(0.0))
        >= rule["druggable_gene_from"],
    )
    .withColumn(
        "cancer_gene",
        F.col("cancer_mutation_count")
        >= FEATURES["clinical"]["cancer_gene_mutations_from"],
    )
    .withColumn(
        "broadly_expressed",
        F.col("tissues") >= FEATURES["clinical"]["broadly_expressed_tissues_from"],
    )
    .withColumn(
        "highly_expressed",
        F.coalesce(
            F.col("max_tpm") >= FEATURES["clinical"]["highly_expressed_tpm_from"],
            F.lit(False),
        ),
    )
)
key_type = flag("is_kinase") | flag("is_receptor") | flag("is_enzyme")
high = F.coalesce(F.col("is_high_impact"), F.lit(False))
impact_variant_rows = attribute_rows(
    impact_variant,
    "variant",
    "variant_key",
    "variant_impact_features",
    VARIANT_SOURCE + "+gene attributes",
    [
        ("domain_impact_severity", F.col("domain_severity"), "text"),
        ("domain_type_count", F.col("domain_types"), "number"),
        ("has_multiple_domain_types", F.col("domain_types") > 1, "flag"),
        (
            "conservation_impact_class",
            F.when(
                F.col("is_highly_conserved") & F.col("is_deleterious_by_cadd"),
                "High_Conservation_High_Deleteriousness",
            )
            .when(F.col("is_highly_conserved"), "High_Conservation")
            .when(F.col("is_deleterious_by_cadd"), "High_Deleteriousness")
            .when(
                F.col("phylop_score").isNotNull() | F.col("cadd_phred").isNotNull(),
                "Low_Conservation",
            )
            .otherwise("No_Conservation_Data"),
            "text",
        ),
        ("combined_impact_score", F.col("combined_impact"), "number"),
        (
            "variant_impact_tier",
            band(
                rule["impact_tier"],
                F.col("combined_impact"),
                rule["impact_tier_default"],
            ),
            "text",
        ),
        ("is_splice_site_variant", F.col("is_splice"), "flag"),
        (
            "splice_impact_severity",
            F.when(F.col("is_splice") & F.col("is_pathogenic"), "High_Splice_Impact")
            .when(F.col("is_splice"), "Low_Splice_Impact")
            .otherwise("No_Splice_Impact"),
            "text",
        ),
        (
            "loss_of_function_category",
            F.when(F.col("is_nonsense"), "Nonsense_Mediated")
            .when(F.col("is_frameshift"), "Frameshift")
            .when(F.col("is_splice"), "Splice_Disruption")
            .when(F.col("is_lof"), "Other_LoF")
            .otherwise("Not_LoF"),
            "text",
        ),
        ("is_druggable_gene", F.col("druggable"), "flag"),
        ("is_key_protein_type", key_type, "flag"),
        (
            "clinical_impact_priority",
            F.when(
                F.col("is_pharmacogene")
                & F.col("is_pathogenic")
                & (F.col("domain_severity") == "Critical"),
                "Top_Priority",
            )
            .when(
                (F.col("druggable") | F.col("is_pharmacogene"))
                & F.col("is_pathogenic"),
                "High_Priority",
            )
            .when(key_type & F.col("is_pathogenic"), "Medium_Priority")
            .when(F.col("is_pathogenic"), "Standard_Priority")
            .otherwise("Low_Priority"),
            "text",
        ),
        (
            "expression_impact_context",
            F.when(F.col("broadly_expressed") & high, "High_Impact_Ubiquitous")
            .when(F.col("highly_expressed") & high, "High_Impact_Highly_Expressed")
            .when(F.col("broadly_expressed"), "Ubiquitous_Expression")
            .when(F.col("tissues") > 0, "Tissue_Specific")
            .otherwise("Low_Expression"),
            "text",
        ),
        (
            "is_cancer_relevant_variant",
            F.col("cancer_gene") & (F.col("is_missense") | F.col("is_lof")),
            "flag",
        ),
        (
            "cancer_variant_priority",
            F.when(
                F.col("cancer_gene") & F.col("is_pathogenic") & high,
                "Cancer_High_Priority",
            )
            .when(
                F.col("cancer_gene") & F.col("is_pathogenic"), "Cancer_Medium_Priority"
            )
            .when(
                F.col("cancer_gene") & (F.col("is_missense") | F.col("is_lof")),
                "Cancer_Research_Candidate",
            )
            .otherwise("Not_Cancer_Priority"),
            "text",
        ),
        (
            "disease_specific_priority",
            F.when(flag("has_cancer_disease") & high, "Cancer_Disease_Priority")
            .when(
                flag("has_neurological_disease") & high, "Neurological_Disease_Priority"
            )
            .when(
                flag("has_cardiovascular_disease") & high,
                "Cardiovascular_Disease_Priority",
            )
            .when(flag("has_metabolic_disease") & high, "Metabolic_Disease_Priority")
            .otherwise("No_Specific_Disease_Priority"),
            "text",
        ),
    ],
)
impact_gene = (
    impact_variant.filter(F.col("gene_key").isNotNull())
    .groupBy("gene_key")
    .agg(
        count_if(F.col("is_high_impact")).alias("high"),
        count_if(F.col("is_very_high_impact")).alias("very_high"),
        count_if(F.col("is_lof")).alias("loss_of_function"),
        count_if(F.col("is_splice")).alias("splice"),
        count_if(F.col("affects_functional_domain")).alias("domain_affecting"),
        F.avg("combined_impact").alias("average_impact"),
        F.max("combined_impact").alias("max_impact"),
    )
)
burden = band(
    rule["gene_impact_burden"], F.col("high"), rule["gene_impact_burden_default"]
)
impact_gene = impact_gene.withColumn("burden", burden)
impact_gene_rows = attribute_rows(
    impact_gene.join(
        gene_base.select("gene_key", "total_disease_count"), "gene_key", "left"
    ),
    "gene",
    "gene_key",
    "variant_impact_features",
    VARIANT_SOURCE,
    [
        ("gene_high_impact_count", F.col("high"), "number"),
        ("gene_very_high_impact_count", F.col("very_high"), "number"),
        ("gene_loss_of_function_count", F.col("loss_of_function"), "number"),
        ("gene_splice_variant_count", F.col("splice"), "number"),
        ("gene_domain_affecting_count", F.col("domain_affecting"), "number"),
        ("gene_average_impact_score", F.col("average_impact"), "number"),
        ("gene_max_impact_score", F.col("max_impact"), "number"),
        ("gene_impact_burden", F.col("burden"), "text"),
        (
            "gene_loss_of_function_tolerance",
            band(
                rule["lof_tolerance"],
                F.col("loss_of_function"),
                rule["lof_tolerance_default"],
            ),
            "text",
        ),
        ("is_disease_associated_gene", F.col("total_disease_count") >= 1, "flag"),
        (
            "disease_impact_category",
            band(
                rule["disease_impact_category"],
                F.coalesce(F.col("total_disease_count"), F.lit(0)),
                rule["disease_impact_category_default"],
            ),
            "text",
        ),
    ],
)
very_high_by_gene = impact_gene.select("gene_key", "burden")
gene_variant_priority = attribute_rows(
    impact_variant.join(very_high_by_gene, "gene_key", "left"),
    "variant",
    "variant_key",
    "variant_impact_features",
    VARIANT_SOURCE,
    [
        (
            "gene_variant_impact_priority",
            F.when(
                F.col("burden").isin("Very_High_Burden", "High_Burden")
                & F.coalesce(F.col("is_very_high_impact"), F.lit(False)),
                "Critical_Gene_Critical_Variant",
            )
            .when(
                F.col("burden").isin("Very_High_Burden", "High_Burden"),
                "High_Impact_Gene",
            )
            .when(
                F.coalesce(F.col("is_very_high_impact"), F.lit(False)),
                "Critical_Variant_Standard_Gene",
            )
            .otherwise("Standard_Priority"),
            "text",
        ),
    ],
)

# COMMAND ----------

# DBTITLE 1,Transform -- structural variant features
rule = FEATURES["structural_variant"]
structural = variant.filter(
    F.col("in_dbvar") & (F.col("assembly") == "GRCh38") & F.col("position").isNotNull()
).select(
    "variant_key",
    "contig",
    F.col("position").alias("start_position"),
    F.coalesce(F.col("end_position"), F.col("position")).alias("end_position"),
    F.col("variant_type").alias("sv_type"),
)
structural = structural.withColumn(
    "size", F.abs(F.col("end_position") - F.col("start_position"))
)
type_class = F.lit(rule["type_class_default"])
for entry in reversed(rule["type_classes"]):
    type_class = F.when(
        F.coalesce(F.col("sv_type"), F.lit("")).rlike("(?i)" + entry["pattern"]),
        F.lit(entry["label"]),
    ).otherwise(type_class)
structural = structural.withColumn("type_class", type_class)
risk = rule["risk"]
gene_spans = gene.filter(F.col("grch38_start_position").isNotNull()).select(
    "gene_key",
    F.col("grch38_start_position").alias("gene_start"),
    F.col("grch38_end_position").alias("gene_end"),
)
sv_genes = (
    read_silver("variant_feature_overlap")
    .filter(
        (F.col("feature_kind") == "gene") & F.col("variant_key").startswith("dbvar:")
    )
    .select("variant_key", F.col("feature_key").alias("gene_key"))
    .distinct()
    .join(
        structural.select("variant_key", "start_position", "end_position", "size"),
        "variant_key",
    )
    .join(gene_spans, "gene_key")
    .withColumn(
        "overlap",
        F.least("end_position", "gene_end")
        - F.greatest("start_position", "gene_start"),
    )
    .withColumn(
        "gene_fraction",
        F.when(
            (F.col("gene_end") - F.col("gene_start")) > 0,
            F.col("overlap") / (F.col("gene_end") - F.col("gene_start") + 1),
        ),
    )
    .join(
        gene_base.select(
            "gene_key",
            "flags",
            "is_pharmacogene",
            "gene_mim_id",
            "druggability_score",
            "total_disease_count",
            "tissues",
        ),
        "gene_key",
        "left",
    )
    .withColumn("flags", F.coalesce(F.col("flags"), F.array().cast("array<string>")))
)
sv_gene_summary = sv_genes.groupBy("variant_key").agg(
    F.countDistinct("gene_key").alias("genes"),
    count_if(F.col("is_pharmacogene")).alias("pharmacogenes"),
    count_if(F.col("gene_mim_id").isNotNull()).alias("omim_genes"),
    count_if(flag("is_kinase")).alias("kinase_genes"),
    count_if(flag("is_receptor")).alias("receptor_genes"),
    F.max("gene_fraction").alias("max_fraction"),
    F.avg("druggability_score").alias("average_druggability"),
    F.sum(F.coalesce(F.col("total_disease_count"), F.lit(0))).alias("disease_links"),
    count_if(flag("has_cancer_disease")).alias("cancer_genes"),
    count_if(flag("has_neurological_disease")).alias("neurological_genes"),
    count_if(F.col("tissues") >= rule["essential_gene_tissues_from"]).alias(
        "broadly_expressed_genes"
    ),
)
sv = (
    structural.join(sv_gene_summary, "variant_key", "left")
    .fillna(
        0,
        [
            "genes",
            "pharmacogenes",
            "omim_genes",
            "kinase_genes",
            "receptor_genes",
            "disease_links",
            "cancer_genes",
            "neurological_genes",
            "broadly_expressed_genes",
        ],
    )
    .withColumn("critical", (F.col("pharmacogenes") > 0) | (F.col("omim_genes") > 0))
)
impact_score = (
    band(
        rule["gene_count_points"], F.col("genes"), rule["gene_count_points_default"]
    ).cast("int")
    + F.coalesce(
        band(rule["pharmacogene_points"], F.col("pharmacogenes"), "0"), F.lit("0")
    ).cast("int")
    + F.coalesce(
        band(rule["omim_gene_points"], F.col("omim_genes"), "0"), F.lit("0")
    ).cast("int")
    + F.coalesce(band(rule["size_points"], F.col("size"), "0"), F.lit("0")).cast("int")
    + F.when(F.col("type_class") == "Deletion", rule["deletion_points"]).otherwise(0)
)
sv = sv.withColumn("impact", impact_score)
size_category = F.lit(rule["size_category_default"])
for entry in reversed(rule["size_categories"]):
    size_category = F.when(
        F.col("size") < entry["below"], F.lit(entry["label"])
    ).otherwise(size_category)
structural_rows = attribute_rows(
    sv,
    "variant",
    "variant_key",
    "structural_variant_features",
    "variant+variant_feature_overlap+gene attributes",
    [
        ("structural_variant_size", F.col("size"), "number"),
        ("structural_variant_type_class", F.col("type_class"), "text"),
        ("structural_variant_size_category", size_category, "text"),
        (
            "structural_variant_pathogenicity_risk",
            F.when(
                (F.col("type_class") == "Deletion")
                & (F.col("size") > risk["deletion_above"]),
                "High_Risk",
            )
            .when(
                F.col("type_class").isin("Deletion", "Duplication")
                & (F.col("size") > risk["deletion_duplication_above"]),
                "Moderate_Risk",
            )
            .when(F.col("size") > risk["low_above"], "Low_Risk")
            .otherwise("Minimal_Risk"),
            "text",
        ),
        ("genes_overlapped", F.col("genes"), "number"),
        ("pharmacogenes_affected", F.col("pharmacogenes"), "number"),
        ("omim_genes_affected", F.col("omim_genes"), "number"),
        ("kinase_genes_affected", F.col("kinase_genes"), "number"),
        ("receptor_genes_affected", F.col("receptor_genes"), "number"),
        ("max_gene_disruption_fraction", F.col("max_fraction"), "number"),
        (
            "average_druggability_affected_genes",
            F.col("average_druggability"),
            "number",
        ),
        (
            "gene_count_category",
            F.when(F.col("genes") == 0, "No_Genes")
            .when(F.col("genes") == 1, "Single_Gene")
            .when(F.col("genes") <= 5, "Few_Genes")
            .when(F.col("genes") <= 20, "Multiple_Genes")
            .otherwise("Many_Genes"),
            "text",
        ),
        ("has_critical_gene_disruption", F.col("critical"), "flag"),
        (
            "structural_variant_clinical_priority",
            F.when(F.col("pharmacogenes") >= 3, "Critical_Priority")
            .when(F.col("pharmacogenes") >= 1, "High_Priority")
            .when(F.col("omim_genes") >= 3, "High_Priority")
            .when(F.col("omim_genes") >= 1, "Medium_Priority")
            .when(F.col("genes") >= 10, "Medium_Priority")
            .when(F.col("genes") >= 1, "Low_Priority")
            .otherwise("Minimal_Priority"),
            "text",
        ),
        ("total_disease_associations", F.col("disease_links"), "number"),
        ("cancer_genes_affected", F.col("cancer_genes"), "number"),
        ("neurological_genes_affected", F.col("neurological_genes"), "number"),
        ("has_disease_associated_genes", F.col("disease_links") > 0, "flag"),
        (
            "disease_structural_variant_priority",
            F.when(
                F.col("cancer_genes") >= rule["cancer_genes_from"],
                "Cancer_Gene_Disruption",
            )
            .when(
                F.col("neurological_genes") >= rule["neurological_genes_from"],
                "Neuro_Gene_Disruption",
            )
            .when(F.col("disease_links") > 0, "Disease_Gene_Involved")
            .otherwise("No_Disease_Association"),
            "text",
        ),
        (
            "broadly_expressed_genes_affected",
            F.col("broadly_expressed_genes"),
            "number",
        ),
        ("affects_essential_genes", F.col("broadly_expressed_genes") >= 1, "flag"),
        ("structural_variant_combined_impact_score", F.col("impact"), "number"),
        (
            "structural_variant_impact_tier",
            band(
                FEATURES["variant_impact"]["impact_tier"],
                F.col("impact"),
                FEATURES["variant_impact"]["impact_tier_default"],
            ),
            "text",
        ),
        (
            "structural_variant_classification",
            F.when(
                F.col("critical") & (F.col("size") > 100000),
                "Critical_Large_Disruption",
            )
            .when(F.col("critical"), "Critical_Gene_Affected")
            .when(
                (F.col("genes") >= 10) & (F.col("size") > 100000), "Large_MultiGene_SV"
            )
            .when(F.col("genes") >= 10, "MultiGene_SV")
            .when(F.col("genes") == 1, "Single_Gene_SV")
            .when(F.col("genes") == 0, "Intergenic_SV")
            .otherwise("Other_SV"),
            "text",
        ),
    ],
)
disruption_rows = attribute_rows(
    sv_genes.withColumn(
        "pair_key", F.concat_ws("|", "variant_key", "gene_key")
    ).withColumn(
        "disruption",
        F.when(
            F.col("gene_fraction") > 0,
            band(
                rule["disruption"], F.col("gene_fraction"), rule["disruption_default"]
            ),
        ).otherwise("No_Disruption"),
    ),
    "structural_variant_gene",
    "pair_key",
    "structural_variant_features",
    "variant_feature_overlap+gene coordinates",
    [
        ("overlap_length", F.col("overlap"), "number"),
        ("gene_coverage_fraction", F.col("gene_fraction"), "number"),
        (
            "structural_variant_coverage_fraction",
            F.when(F.col("size") > 0, F.col("overlap") / F.col("size")),
            "number",
        ),
        ("gene_disruption_level", F.col("disruption"), "text"),
    ],
)

# COMMAND ----------

# DBTITLE 1,Transform -- feature overlap counts per variant (no pair rows)
spans = (
    variant.filter(
        (F.col("assembly") == "GRCh38")
        & F.col("position").isNotNull()
        & F.col("contig").isNotNull()
    )
    .select(
        "variant_key",
        "contig",
        F.col("position").alias("start_position"),
        # a structural variant spans to its END; a small variant spans its allele
        F.greatest(
            F.coalesce(F.col("end_position"), F.col("position")),
            F.col("position") + F.greatest(F.length("reference_allele"), F.lit(1)) - 1,
        ).alias("end_position"),
    )
    .distinct()
)
grch38_coordinates = read_silver("entity_coordinate").filter(
    (F.col("assembly") == "GRCh38") & F.col("is_primary")
)


def intervals(frame, key, contig, start, end):
    return (
        frame.select(
            F.col(key).alias("feature_key"),
            F.col(contig).alias("contig"),
            F.col(start).alias("f_start"),
            F.col(end).alias("f_end"),
        )
        .filter(
            F.col("contig").isNotNull()
            & F.col("f_start").isNotNull()
            & F.col("f_end").isNotNull()
        )
        .distinct()
    )


def coordinate_intervals(entity_type):
    return intervals(
        grch38_coordinates.filter(F.col("entity_type") == entity_type),
        "entity_key",
        "contig",
        "start_position",
        "end_position",
    )


GENCODE = "gencode.annotation"
structure = (
    read_genomics_bronze("gencode_annotation")
    .filter(F.col("feature").isin("exon", "CDS", "UTR"))
    .select(
        "feature",
        ensembl_unversioned_expr(F.col("transcript_id")).alias("transcript_key"),
        chromosome_expr("seqname").alias("structure_contig"),
        typed_number("start", "bigint", RULES, f"{GENCODE}.start").alias("f_start"),
        typed_number("end", "bigint", RULES, f"{GENCODE}.end").alias("f_end"),
    )
)


def structure_intervals(feature):
    return intervals(
        structure.filter(F.col("feature") == feature),
        "transcript_key",
        "structure_contig",
        "f_start",
        "f_end",
    )


OVERLAP_COUNTS = [
    ("overlapping_transcript_count", coordinate_intervals("transcript")),
    (
        "overlapping_regulatory_element_count",
        coordinate_intervals("regulatory_element"),
    ),
    (
        "overlapping_enhancer_element_count",
        intervals(
            read_silver("regulatory_element_gene_link"),
            "element_key",
            "contig",
            "start_position",
            "end_position",
        ),
    ),
    ("overlapping_exon_count", structure_intervals("exon")),
    ("overlapping_coding_sequence_count", structure_intervals("CDS")),
    ("overlapping_untranslated_region_count", structure_intervals("UTR")),
]
overlap_count_rows = _union(
    [
        number_rows(
            interval_overlap_counts(spans, features, attribute),
            "variant",
            "variant_key",
            attribute,
            attribute,
            "feature_overlap_counts",
            "variant+entity_coordinate+regulatory_element_gene_link+gencode.annotation",
        )
        for attribute, features in OVERLAP_COUNTS
    ]
)

# COMMAND ----------

# DBTITLE 1,Transform -- pharmacogene gene features
rule = FEATURES["pharmacogene_gene"]
drug_gene_pharmgkb = relations.filter(
    F.col("relation_subkind") == "pharmgkb_relationship"
)
pharmgkb_relations = (
    relation_table.filter(
        (F.col("relation_kind") == "pharmacogenomic")
        & (F.lower(F.col("subject_type")) == "gene")
        & F.col("subject_key").isNotNull()
    )
    .select(
        F.col("subject_key").alias("gene_key"),
        F.lower(F.col("object_type")).alias("object_kind"),
        F.col("pharmacogenomic_evidence").alias("evidence"),
    )
    .unionByName(
        drug_gene_pharmgkb.select(
            "gene_key",
            F.lit("chemical").alias("object_kind"),
            F.col("evidence_level").alias("evidence"),
        )
    )
    .groupBy("gene_key")
    .agg(
        F.count(F.lit(1)).alias("relationships"),
        F.countDistinct("object_kind").alias("object_kinds"),
        count_if(F.col("object_kind") == "chemical").alias("drug_relationships"),
        count_if(F.col("object_kind") == "disease").alias("disease_relationships"),
        count_if(F.col("object_kind") == "variant").alias("variant_relationships"),
        count_if(F.col("evidence").isNotNull()).alias("evidence_count"),
    )
)
pharmacogene_genes = (
    gene_base.filter(F.col("has_pharmgkb_annotation"))
    .join(pharmgkb_relations.filter(F.col("relationships") > 0), "gene_key")
    .join(gene_stats, "gene_key", "left")
    .fillna(
        0, ["pathogenic", "domain_affecting", "loss_of_function", "missense", "total"]
    )
    .withColumn("has_variants", F.col("total") > 0)
    .withColumn(
        "drug_metabolizer", flag("is_metabolic") & (F.col("drug_relationships") > 0)
    )
    .withColumn(
        "drug_transporter", flag("is_transporter") & (F.col("drug_relationships") > 0)
    )
    .withColumn(
        "drug_target",
        (flag("is_kinase") | flag("is_receptor") | flag("is_enzyme"))
        & (F.col("drug_relationships") > 0),
    )
    .withColumn(
        "high_druggability",
        F.coalesce(F.col("druggability_score"), F.lit(0.0))
        >= rule["high_druggability_from"],
    )
    .withColumn(
        "oncology",
        F.col("cancer_mutation_count") >= rule["oncology_target_mutations_from"],
    )
    .withColumn("hepatic", F.col("is_liver_expressed") & F.col("drug_metabolizer"))
    .withColumn("renal", F.col("is_kidney_expressed") & F.col("drug_transporter"))
)
evidence_points = rule["evidence_points"]
interaction_points = rule["interaction_points"]
utility_points = rule["utility_points"]
weights = rule["variant_impact_weights"]
metabolism_points = rule["metabolism_points"]
pharmacogene_genes = (
    pharmacogene_genes.withColumn(
        "evidence_score",
        F.col("evidence_count")
        + evidence_points["source"]
        + F.when(
            F.col("high_druggability"), evidence_points["high_druggability"]
        ).otherwise(0)
        + F.when(
            F.col("has_variants"), evidence_points["pharmacogene_variants"]
        ).otherwise(0),
    )
    .withColumn(
        "interaction_score",
        F.col("drug_relationships") * interaction_points["drug_relationship"]
        + F.col("evidence_count")
        + F.when(
            F.col("is_liver_expressed") | F.col("is_kidney_expressed"),
            interaction_points["liver_or_kidney"],
        ).otherwise(0)
        + F.when(F.col("pathogenic") > 0, interaction_points["pathogenic"]).otherwise(
            0
        ),
    )
    .withColumn(
        "utility_score",
        F.lit(utility_points["source"])
        + F.when(
            F.col("high_druggability"), utility_points["high_druggability"]
        ).otherwise(0)
        + F.col("drug_relationships") * utility_points["drug_relationship"]
        + F.when(
            F.col("total_disease_count") >= rule["many_diseases_from"],
            utility_points["many_diseases"],
        ).otherwise(0)
        + F.when(F.col("oncology"), utility_points["oncology"]).otherwise(0),
    )
    .withColumn(
        "priority",
        band(rule["priority"], F.col("utility_score"), rule["priority_default"]),
    )
)
pharmacogene_gene_rows = attribute_rows(
    pharmacogene_genes,
    "gene",
    "gene_key",
    "pharmacogene_gene_features",
    GENE_SOURCE + "+pharmacogenomic relations",
    [
        ("total_relationships", F.col("relationships"), "number"),
        ("entity_type_count", F.col("object_kinds"), "number"),
        ("drug_relationships", F.col("drug_relationships"), "number"),
        ("disease_relationships", F.col("disease_relationships"), "number"),
        ("variant_relationships", F.col("variant_relationships"), "number"),
        ("evidence_count", F.col("evidence_count"), "number"),
        ("has_pharmacogene_variants", F.col("has_variants"), "flag"),
        ("average_pathogenicity_score", F.col("average_pathogenicity"), "number"),
        ("is_drug_metabolizer", F.col("drug_metabolizer"), "flag"),
        ("is_drug_transporter_gene", F.col("drug_transporter"), "flag"),
        ("is_drug_target_gene", F.col("drug_target"), "flag"),
        ("has_high_druggability", F.col("high_druggability"), "flag"),
        ("is_hepatic_metabolizer", F.col("hepatic"), "flag"),
        ("is_renal_transporter", F.col("renal"), "flag"),
        ("is_oncology_drug_target", F.col("oncology"), "flag"),
        (
            "is_validated_cancer_target",
            F.col("oncology") & (flag("is_kinase") | flag("is_receptor")),
            "flag",
        ),
        (
            "is_complex_drug_target",
            F.col("max_domain_count") >= rule["complex_target_domains_from"],
            "flag",
        ),
        ("pharmacogene_evidence_score", F.col("evidence_score"), "number"),
        ("drug_interaction_score", F.col("interaction_score"), "number"),
        ("clinical_utility_score", F.col("utility_score"), "number"),
        (
            "pharmacogene_variant_impact_score",
            F.col("pathogenic") * weights["pathogenic"]
            + F.col("domain_affecting") * weights["domain_affecting"]
            + F.col("loss_of_function") * weights["loss_of_function"],
            "number",
        ),
        (
            "metabolism_context_score",
            F.when(F.col("hepatic"), metabolism_points["hepatic"]).otherwise(0)
            + F.when(F.col("renal"), metabolism_points["renal"]).otherwise(0)
            + F.when(F.col("is_liver_expressed"), metabolism_points["liver"]).otherwise(
                0
            ),
            "number",
        ),
        (
            "variant_impact_burden",
            band(
                rule["variant_impact_burden"],
                F.col("pathogenic"),
                rule["variant_impact_burden_default"],
            ),
            "text",
        ),
        (
            "drug_metabolism_tissue_expression",
            F.when(
                F.col("is_liver_expressed") & F.col("is_kidney_expressed"),
                "Hepato_Renal",
            )
            .when(F.col("is_liver_expressed"), "Hepatic")
            .when(F.col("is_kidney_expressed"), "Renal")
            .otherwise("Other"),
            "text",
        ),
        (
            "cancer_mutation_burden",
            band(
                rule["cancer_mutation_burden"],
                F.col("cancer_samples"),
                rule["cancer_mutation_burden_default"],
            ),
            "text",
        ),
        (
            "primary_indication_category",
            F.when(flag("has_cancer_disease"), "Oncology")
            .when(flag("has_cardiovascular_disease"), "Cardiology")
            .when(flag("has_neurological_disease"), "Neurology")
            .when(flag("has_metabolic_disease"), "Metabolism")
            .otherwise("Other"),
            "text",
        ),
        ("pharmacogene_priority", F.col("priority"), "text"),
        (
            "pharmacogene_category_enhanced",
            F.when(
                F.col("hepatic") & (F.col("drug_relationships") > 0),
                "hepatic_metabolizer",
            )
            .when(
                F.col("renal") & (F.col("drug_relationships") > 0), "renal_transporter"
            )
            .when(F.col("drug_target") & F.col("oncology"), "oncology_target")
            .when(F.col("drug_target"), "drug_target")
            .when(F.col("drug_metabolizer"), "metabolizer")
            .when(F.col("drug_transporter"), "transporter")
            .when(F.col("drug_relationships") > 0, "interaction")
            .otherwise("other"),
            "text",
        ),
        (
            "clinical_actionability_tier",
            F.when(
                (F.col("priority") == "critical") & F.col("has_variants"),
                "Tier_1_Actionable",
            )
            .when(F.col("priority").isin("critical", "high"), "Tier_2_High_Evidence")
            .otherwise("Tier_3_PharmGKB_Annotated"),
            "text",
        ),
    ],
)

# COMMAND ----------

# DBTITLE 1,Transform -- drug response variant features
# A variant counts as PharmGKB-annotated when its gene has a PharmGKB variant (a gene
# rule, as in the earlier genomics project).
rule = FEATURES["drug_response"]
annotated_genes = (
    variant.filter(F.col("in_pharmgkb") & F.col("gene_key").isNotNull())
    .select("gene_key")
    .distinct()
    .withColumn("annotated_gene", F.lit(True))
)
variant_relations = (
    relation_table.filter(
        (F.col("relation_kind") == "pharmacogenomic")
        & (F.lower(F.col("subject_type")) == "variant")
        & F.col("subject_key").isNotNull()
    )
    .groupBy(F.col("subject_key").alias("variant_key"))
    .agg(
        F.count(F.lit(1)).alias("interactions"),
        F.countDistinct(F.lower(F.col("object_type"))).alias("interaction_kinds"),
        count_if(F.lower(F.col("object_type")) == "chemical").alias(
            "drug_interactions"
        ),
        count_if(F.lower(F.col("object_type")) == "disease").alias(
            "disease_interactions"
        ),
        count_if(F.col("pharmacogenomic_evidence").isNotNull()).alias("evidence"),
    )
)
functional = rule["functional_points"]
significance_points = rule["significance_points"]
population_points = rule["population_points"]
tissue_points = rule["tissue_points"]
weights = rule["weights"]
response = (
    impact_variant.join(annotated_genes, "gene_key", "left")
    .join(variant_relations, "variant_key", "left")
    .join(gene_base.select("gene_key", "is_liver_expressed"), "gene_key", "left")
    .withColumn("annotated", F.coalesce(F.col("annotated_gene"), F.lit(False)))
    .withColumn("liver", F.coalesce(F.col("is_liver_expressed"), F.lit(False)))
    .withColumn("metabolism", F.col("has_protein_feature") & F.col("annotated"))
    .withColumn("hepatic", F.col("liver") & F.col("metabolism"))
    .withColumn(
        "resistance",
        (F.col("cancer_mutation_count") >= rule["cancer_gene_mutations_from"])
        & F.col("is_missense"),
    )
    .withColumn(
        "efficacy",
        F.col("annotated")
        & (F.col("is_missense") | F.col("affects_functional_domain")),
    )
    .withColumn(
        "functional_score",
        F.when(
            F.col("affects_functional_domain"), functional["domain_affecting"]
        ).otherwise(0)
        + F.when(F.col("is_missense"), functional["missense"]).otherwise(0)
        + F.when(F.col("is_nonsense"), functional["nonsense"]).otherwise(0)
        + F.when(F.col("is_frameshift"), functional["frameshift"]).otherwise(0)
        + F.coalesce(F.col("conservation_level"), F.lit(0))
        + F.coalesce(F.col("severity"), F.lit(0)),
    )
    .withColumn(
        "significance_score",
        F.when(F.col("is_pathogenic"), significance_points["pathogenic"])
        .when(F.col("is_benign"), significance_points["benign"])
        .otherwise(0),
    )
    .withColumn(
        "population_score",
        F.when(
            F.col("is_common") & F.col("annotated"),
            population_points["common_annotated"],
        ).otherwise(0)
        + F.when(
            F.col("is_rare") & F.col("annotated"), population_points["rare_annotated"]
        ).otherwise(0),
    )
    .withColumn(
        "tissue_score",
        F.when(F.col("hepatic"), tissue_points["hepatic_metabolism"]).otherwise(0)
        + F.when(
            F.col("liver") & F.col("annotated"), tissue_points["liver_annotated"]
        ).otherwise(0),
    )
    .withColumn(
        "priority_score",
        F.col("functional_score") * weights["functional"]
        + F.col("significance_score") * weights["significance"]
        + F.col("population_score") * weights["population"]
        + F.col("tissue_score") * weights["tissue"],
    )
)
indication = (
    F.when(flag("has_cancer_disease"), "Oncology")
    .when(flag("has_cardiovascular_disease"), "Cardiology")
    .when(flag("has_neurological_disease"), "Neurology")
    .otherwise("Other")
)
response_rows = attribute_rows(
    response,
    "variant",
    "variant_key",
    "drug_response_features",
    VARIANT_SOURCE + "+pharmacogenomic relations+gene attributes",
    [
        ("is_pharmgkb_annotated_variant", F.col("annotated"), "flag"),
        ("total_interactions", F.col("interactions"), "number"),
        ("interaction_type_count", F.col("interaction_kinds"), "number"),
        ("drug_interaction_count", F.col("drug_interactions"), "number"),
        ("disease_interaction_count", F.col("disease_interactions"), "number"),
        ("evidence_count", F.col("evidence"), "number"),
        ("has_high_conservation", F.col("conservation_level") >= 1, "flag"),
        ("affects_drug_metabolism", F.col("metabolism"), "flag"),
        (
            "is_high_impact_variant",
            F.col("is_pathogenic") & F.col("annotated"),
            "flag",
        ),
        ("is_hepatic_drug_metabolism_variant", F.col("hepatic"), "flag"),
        (
            "is_common_pharmacogene_variant",
            F.col("is_common") & F.col("annotated"),
            "flag",
        ),
        ("is_potential_resistance_variant", F.col("resistance"), "flag"),
        ("functional_impact_score", F.col("functional_score"), "number"),
        ("drug_response_significance_points", F.col("significance_score"), "number"),
        ("population_adjusted_score", F.col("population_score"), "number"),
        ("tissue_specific_response_score", F.col("tissue_score"), "number"),
        ("drug_response_priority_score", F.col("priority_score"), "number"),
        (
            "pharmacogene_annotation_score",
            F.when(F.col("annotated"), rule["annotation_points"]).otherwise(0),
            "number",
        ),
        ("affects_drug_efficacy", F.col("efficacy"), "flag"),
        (
            "drug_response_frequency_context",
            F.when(F.col("is_common"), "Common_Drug_Response")
            .when(F.col("is_rare"), "Rare_Drug_Response")
            .otherwise("Standard_Frequency"),
            "text",
        ),
        ("primary_indication_category", indication, "text"),
        (
            "drug_response_priority",
            band(rule["priority"], F.col("priority_score"), rule["priority_default"]),
            "text",
        ),
        (
            "drug_response_category",
            F.when(F.col("hepatic"), "hepatic_metabolism")
            .when(F.col("metabolism"), "metabolism")
            .when(F.col("efficacy"), "efficacy")
            .when(F.col("resistance"), "resistance")
            .when(F.col("annotated"), "pharmacogene_variant")
            .otherwise("unknown"),
            "text",
        ),
        (
            "clinical_actionability",
            F.when(
                F.col("is_pathogenic") & F.col("annotated") & F.col("is_pharmacogene"),
                "tier_1_actionable",
            )
            .when(F.col("annotated") & F.col("is_pharmacogene"), "tier_2_high_evidence")
            .when(F.col("annotated"), "tier_3_pharmgkb_annotated")
            .otherwise("tier_4_research_only"),
            "text",
        ),
        ("indication_specific_actionability", indication != "Other", "flag"),
    ],
)

# COMMAND ----------

# DBTITLE 1,Transform -- transcript expression gene features
# The primary tissue count is the number of tissues at the gene's peak median.
rule = FEATURES["expression"]
peak_tissues = (
    expression.join(all_expression.select("gene_key", "peak_tpm"), "gene_key")
    .filter(
        (F.col("median_transcripts_per_million") == F.col("peak_tpm"))
        & (F.col("peak_tpm") > 0)
    )
    .groupBy("gene_key")
    .agg(F.countDistinct("tissue_key").alias("primary_tissues"))
)
expression_gene = (
    gene_base.join(peak_tissues, "gene_key", "left")
    .join(
        gene_stats.select("gene_key", "splice", "domain_affecting", "total"),
        "gene_key",
        "left",
    )
    .fillna(0, ["primary_tissues", "splice", "domain_affecting"])
    .withColumn(
        "specific", F.col("tissues_with_expression") <= rule["tissue_specific_up_to"]
    )
    .withColumn(
        "high",
        F.coalesce(F.col("peak_tpm") >= rule["highly_expressed_from"], F.lit(False)),
    )
    .withColumn("disease_gene", F.col("total_disease_count") >= 1)
    .withColumn(
        "disease_categories",
        sum(
            F.when(flag(f"has_{name}_disease"), 1).otherwise(0)
            for name in ("cancer", "neurological", "metabolic")
        ),
    )
    .withColumn(
        "cancer_gene",
        F.col("cancer_mutation_count")
        >= FEATURES["clinical"]["cancer_gene_mutations_from"],
    )
    .withColumn("has_domain", F.col("proteins_with_functional_domain") > 0)
    .withColumn(
        "expression_variants", (F.col("splice") > 0) | (F.col("domain_affecting") > 0)
    )
)
breadth = F.lit(rule["breadth_category_default"])
for entry in reversed(rule["breadth_category"]):
    breadth = F.when(
        F.col("tissues_with_expression") <= entry["up_to"], F.lit(entry["label"])
    ).otherwise(breadth)
disease_points = rule["disease_points"]
cancer_points = rule["cancer_points"]
functional_points = rule["functional_points"]
relevance = rule["relevance_points"]
burden = rule["cancer_burden"]
correlation = rule["domain_correlation"]
expression_rows = attribute_rows(
    expression_gene.filter(F.col("peak_tpm").isNotNull()),
    "gene",
    "gene_key",
    "expression_features",
    "gene_expression_median+gene attributes",
    [
        ("max_expression_transcripts_per_million", F.col("peak_tpm"), "number"),
        ("average_expression_transcripts_per_million", F.col("mean_tpm"), "number"),
        ("total_tissues_expressed", F.col("tissues_with_expression"), "number"),
        ("primary_tissue_count", F.col("primary_tissues"), "number"),
        (
            "is_ubiquitously_expressed",
            F.col("tissues_with_expression") >= rule["ubiquitous_from"],
            "flag",
        ),
        ("is_tissue_specific", F.col("specific"), "flag"),
        ("is_highly_expressed", F.col("high"), "flag"),
        (
            "is_lowly_expressed",
            F.col("peak_tpm") < rule["lowly_expressed_below"],
            "flag",
        ),
        ("expression_breadth_category", breadth, "text"),
        (
            "expression_level_category",
            band(
                rule["level_category"],
                F.col("peak_tpm"),
                rule["level_category_default"],
            ),
            "text",
        ),
        (
            "tissue_specificity_score",
            F.lit(100.0) / (F.col("tissues_with_expression") + 1),
            "number",
        ),
        (
            "expression_significance_score",
            band(
                rule["significance_points"],
                F.col("peak_tpm"),
                rule["significance_points_default"],
            ).cast("int"),
            "number",
        ),
        (
            "clinical_relevance_score",
            F.when(F.col("specific"), relevance["tissue_specific"]).otherwise(0)
            + F.when(F.col("high"), relevance["highly_expressed"]).otherwise(0)
            + F.col("primary_tissues") * relevance["primary_tissue"],
            "number",
        ),
        (
            "expression_priority",
            F.when(F.col("specific") & F.col("high"), "critical")
            .when(F.col("specific") | F.col("high"), "high")
            .when(
                F.col("tissues_with_expression")
                <= rule["priority_medium_tissues_up_to"],
                "medium",
            )
            .otherwise("low"),
            "text",
        ),
        ("disease_category_count", F.col("disease_categories"), "number"),
        (
            "cancer_expression_relevance",
            F.when(
                F.col("cancer_mutation_count") >= burden["high_from"],
                "high_cancer_burden",
            )
            .when(
                F.col("cancer_mutation_count") >= burden["moderate_from"],
                "moderate_cancer_burden",
            )
            .otherwise("low_cancer_burden"),
            "text",
        ),
        (
            "domain_expression_correlation",
            F.when(
                F.col("max_domain_count") >= correlation["multi_domain_from"],
                "multi_domain",
            )
            .when(
                F.col("max_domain_count") >= correlation["few_domains_from"],
                "few_domains",
            )
            .otherwise("single_or_none"),
            "text",
        ),
        ("has_expression_variants", F.col("expression_variants"), "flag"),
        (
            "disease_expression_score",
            F.when(
                F.col("disease_gene") & F.col("high"),
                disease_points["highly_expressed"],
            ).otherwise(0)
            + F.when(
                F.col("disease_gene") & F.col("specific"),
                disease_points["tissue_specific"],
            ).otherwise(0)
            + F.col("disease_categories") * disease_points["category"],
            "number",
        ),
        (
            "cancer_expression_score",
            F.when(
                F.col("cancer_gene") & F.col("high"), cancer_points["highly_expressed"]
            ).otherwise(0)
            + F.when(
                F.col("cancer_samples") >= rule["cancer_many_samples_from"],
                cancer_points["many_samples"],
            ).otherwise(0),
            "number",
        ),
        (
            "functional_expression_score",
            F.when(
                F.col("has_domain") & F.col("high"),
                functional_points["domain_highly_expressed"],
            ).otherwise(0)
            + F.when(
                F.col("expression_variants"), functional_points["expression_variants"]
            ).otherwise(0)
            + F.when(
                F.col("is_pharmacogene") & F.col("high"),
                functional_points["pharmacogene_highly_expressed"],
            ).otherwise(0),
            "number",
        ),
        (
            "disease_specific_expression_pattern",
            F.when(
                flag("has_cancer_disease") & F.col("specific"), "cancer_tissue_specific"
            )
            .when(
                flag("has_neurological_disease") & F.col("specific"),
                "neuro_tissue_specific",
            )
            .when(F.col("disease_gene") & F.col("high"), "disease_high_expression")
            .otherwise("other"),
            "text",
        ),
        (
            "expression_function_correlation",
            F.when(flag("is_kinase") & F.col("high"), "kinase_high")
            .when(flag("is_transcription_factor") & F.col("specific"), "tf_specific")
            .when(flag("is_enzyme"), "enzyme_expressed")
            .otherwise("other"),
            "text",
        ),
    ],
)

# COMMAND ----------

# DBTITLE 1,Transform -- protein family gene features
rule = FEATURES["protein_family"]
families = rule["families"]


def has_family(name):
    result = F.lit(False)
    for member in families[name]:
        result = result | (F.col(f"proteins_{member}") > 0)
    return result


diversity_points = rule["diversity_points"]
complexity_points = rule["complexity_points"]
druggability_points = rule["druggability_points"]
domain_weights = rule["variant_domain_weights"]
cancer_points = rule["cancer_points"]
disease_points = rule["disease_points"]
somatic_classes = (
    somatic.filter(F.col("gene_key").isNotNull())
    .groupBy("gene_key")
    .agg(
        count_if(record_class.isin(mutation["missense"])).alias("cancer_missense"),
        count_if(record_class.isin(mutation["truncating"])).alias("cancer_truncating"),
    )
)
family = (
    gene_base.filter(F.col("protein_count") > 0)
    .join(
        gene_stats.select(
            "gene_key", "domain_affecting", "domain_pathogenic", "critical_domain"
        ),
        "gene_key",
        "left",
    )
    .join(somatic_classes, "gene_key", "left")
    .fillna(
        0,
        [
            "domain_affecting",
            "domain_pathogenic",
            "critical_domain",
            "cancer_missense",
            "cancer_truncating",
        ],
    )
    .withColumn("signaling", has_family("signaling"))
    .withColumn("dna_binding", has_family("dna_binding"))
    .withColumn("membrane", has_family("membrane"))
    .withColumn("apoptosis", has_family("apoptosis"))
    .withColumn("immune", has_family("immune"))
    .withColumn("multi_domain", F.col("max_domain_count") >= rule["multi_domain_from"])
    .withColumn(
        "cancer_relevant",
        F.col("cancer_samples") >= rule["cancer_relevant_samples_from"],
    )
    .withColumn(
        "disease_associated",
        F.col("total_disease_count") >= rule["disease_associated_from"],
    )
    .withColumn(
        "druggability_potential",
        F.when(
            F.col("proteins_has_kinase_in_name") > 0, druggability_points["kinase"]
        ).otherwise(0)
        + F.when(
            F.col("proteins_has_receptor_in_name") > 0, druggability_points["receptor"]
        ).otherwise(0)
        + F.when(F.col("signaling"), druggability_points["signaling"]).otherwise(0),
    )
)
family_rows = attribute_rows(
    family,
    "gene",
    "gene_key",
    "protein_family_features",
    "protein names+protein_gene_link+variant+somatic_mutation",
    [
        ("protein_count", F.col("protein_count"), "number"),
        ("max_domain_count", F.col("max_domain_count"), "number"),
        *[
            (
                f"proteins_with_{name.removeprefix('has_')}",
                F.col(f"proteins_{name}"),
                "number",
            )
            for name in DOMAIN_FLAGS
        ],
        (
            "proteins_with_functional_domain",
            F.col("proteins_with_functional_domain"),
            "number",
        ),
        ("has_signaling_domain", F.col("signaling"), "flag"),
        ("has_dna_binding_domain", F.col("dna_binding"), "flag"),
        ("has_membrane_domain", F.col("membrane"), "flag"),
        ("has_apoptosis_domain", F.col("apoptosis"), "flag"),
        ("has_immune_domain", F.col("immune"), "flag"),
        ("is_multi_domain_protein", F.col("multi_domain"), "flag"),
        (
            "domain_diversity_score",
            F.col("max_domain_count") * diversity_points["domain"]
            + F.when(F.col("signaling"), diversity_points["signaling"]).otherwise(0)
            + F.when(F.col("dna_binding"), diversity_points["dna_binding"]).otherwise(0)
            + F.when(F.col("membrane"), diversity_points["membrane"]).otherwise(0),
            "number",
        ),
        (
            "functional_complexity_score",
            F.col("proteins_with_functional_domain")
            * complexity_points["functional_protein"]
            + F.when(
                F.col("multi_domain"), complexity_points["multi_domain"]
            ).otherwise(0),
            "number",
        ),
        ("druggability_potential_score", F.col("druggability_potential"), "number"),
        ("domain_affecting_variants", F.col("domain_affecting"), "number"),
        ("domain_pathogenic_variants", F.col("domain_pathogenic"), "number"),
        ("critical_domain_variants", F.col("critical_domain"), "number"),
        ("has_domain_variants", F.col("domain_affecting") > 0, "flag"),
        (
            "tissue_specific_protein_expression",
            F.col("tissues") <= rule["tissue_specific_up_to"],
            "flag",
        ),
        ("cancer_missense_mutations", F.col("cancer_missense"), "number"),
        ("cancer_truncating_mutations", F.col("cancer_truncating"), "number"),
        ("cancer_samples_affected", F.col("cancer_samples"), "number"),
        ("cancer_relevant_protein_family", F.col("cancer_relevant"), "flag"),
        (
            "oncogenic_domain_alterations",
            F.when(F.col("cancer_missense") + F.col("cancer_truncating") == 0, "none")
            .when(
                F.col("cancer_missense") >= F.col("cancer_truncating"),
                "missense_dominant",
            )
            .otherwise("truncating_dominant"),
            "text",
        ),
        ("disease_associated_protein_family", F.col("disease_associated"), "flag"),
        (
            "disease_specific_domains",
            F.when(flag("has_cancer_disease"), "cancer_associated")
            .when(flag("has_neurological_disease"), "neuro_associated")
            .otherwise("other"),
            "text",
        ),
        (
            "variant_domain_impact_score",
            F.col("domain_affecting") * domain_weights["domain_affecting"]
            + F.col("domain_pathogenic") * domain_weights["domain_pathogenic"]
            + F.col("critical_domain") * domain_weights["critical_domain"],
            "number",
        ),
        (
            "cancer_protein_family_score",
            F.when(
                F.col("cancer_relevant") & F.col("signaling"),
                cancer_points["signaling"],
            ).otherwise(0)
            + F.when(F.col("cancer_relevant"), cancer_points["relevant"]).otherwise(0)
            + F.col("cancer_samples") * cancer_points["per_sample"],
            "number",
        ),
        (
            "disease_protein_family_score",
            F.when(
                F.col("disease_associated") & F.col("multi_domain"),
                disease_points["multi_domain"],
            ).otherwise(0)
            + F.when(
                F.col("disease_associated"), disease_points["associated"]
            ).otherwise(0)
            + F.col("total_disease_count") * disease_points["per_disease"],
            "number",
        ),
        (
            "protein_functional_category",
            F.when(F.col("signaling"), "signaling")
            .when(F.col("dna_binding"), "transcription")
            .when(F.col("membrane"), "membrane")
            .when(F.col("immune"), "immune")
            .otherwise("other"),
            "text",
        ),
        (
            "protein_family_priority",
            F.when(F.col("signaling") & F.col("multi_domain"), "critical")
            .when(F.col("signaling") | F.col("multi_domain"), "high")
            .when(
                F.col("druggability_potential")
                >= rule["priority_medium_druggability_from"],
                "medium",
            )
            .otherwise("low"),
            "text",
        ),
        (
            "variant_disease_domain_correlation",
            F.when(
                (F.col("domain_affecting") > 0) & F.col("disease_associated"),
                "variant_disease_linked",
            )
            .when(F.col("domain_affecting") > 0, "variant_only")
            .when(F.col("disease_associated"), "disease_only")
            .otherwise("none"),
            "text",
        ),
        (
            "cancer_protein_classification",
            F.when(F.col("cancer_relevant") & F.col("signaling"), "oncogenic_signaling")
            .when(F.col("cancer_relevant"), "cancer_associated")
            .otherwise("non_cancer"),
            "text",
        ),
    ],
)

# COMMAND ----------

# DBTITLE 1,Transform -- genetic test gene features
rule = FEATURES["genetic_test"]
test_records = test_rows_all.join(
    tests.select("test_key", "has_gene", "has_condition"), "test_key", "left"
)
frequently_tested = tests_per_gene.filter(
    F.col("n") >= RULES["frequently_tested_minimum"]
).select("gene_key", F.lit(True).alias("frequent_gene"))
gene_tests = (
    test_records.filter((F.col("link_kind") == "gene") & F.col("gene_key").isNotNull())
    .select("gene_key", "test_key", "has_gene", "has_condition")
    .distinct()
    .join(
        test_rows_all.filter(F.col("link_kind") == "condition").select(
            "test_key", "condition_name"
        ),
        "test_key",
        "left",
    )
    .groupBy("gene_key")
    .agg(
        F.count(F.lit(1)).alias("test_rows"),
        F.countDistinct("test_key").alias("tests"),
        F.countDistinct("condition_name").alias("test_diseases"),
        F.countDistinct(F.when(F.col("has_gene"), F.col("test_key"))).alias(
            "with_gene"
        ),
        F.countDistinct(F.when(F.col("has_condition"), F.col("test_key"))).alias(
            "with_disease"
        ),
        F.countDistinct(
            F.when(F.col("has_gene") & F.col("has_condition"), F.col("test_key"))
        ).alias("complete"),
    )
)
rare_pathogenic = (
    variant_base.filter(
        F.col("is_rare") & F.col("is_pathogenic") & F.col("gene_key").isNotNull()
    )
    .groupBy("gene_key")
    .agg(F.count(F.lit(1)).alias("rare_pathogenic"))
)
access = rule["accessibility_points"]
utility = rule["utility_points"]
quality_points = rule["quality_points"]
composite = rule["composite_points"]
testing = (
    gene_tests.join(gene_base, "gene_key", "left")
    .join(gene_stats.select("gene_key", "pathogenic", "total"), "gene_key", "left")
    .join(rare_pathogenic, "gene_key", "left")
    .join(frequently_tested, "gene_key", "left")
    .fillna(0, ["pathogenic", "total", "rare_pathogenic"])
    .withColumn("multiple", F.col("tests") >= rule["multiple_tests_from"])
    .withColumn("comprehensive", F.col("tests") >= rule["comprehensive_tests_from"])
    .withColumn(
        "well_tested",
        (F.col("complete") >= rule["well_tested"]["complete_from"])
        & (F.col("test_diseases") >= rule["well_tested"]["diseases_from"]),
    )
    .withColumn(
        "utility",
        F.col("complete") * utility["per_complete_test"]
        + F.col("test_diseases") * utility["per_disease"]
        + F.when(F.col("well_tested"), utility["well_tested"]).otherwise(0),
    )
    .withColumn(
        "multi_disease", F.col("total_disease_count") >= rule["multi_disease_from"]
    )
    .withColumn(
        "cancer_panel",
        F.col("cancer_mutation_count") >= rule["cancer_panel_mutations_from"],
    )
    .withColumn(
        "hereditary", F.col("cancer_samples") >= rule["hereditary_cancer_samples_from"]
    )
    .withColumn("carrier", F.col("rare_pathogenic") >= rule["carrier_screening_from"])
)
many_pathogenic = F.col("pathogenic") >= rule["many_pathogenic_from"]
test_gene_rows_gold = attribute_rows(
    testing,
    "gene",
    "gene_key",
    "genetic_test_features",
    "genetic_test+gene attributes+variant",
    [
        ("total_test_count", F.col("test_rows"), "number"),
        ("unique_test_count", F.col("tests"), "number"),
        ("test_disease_count", F.col("test_diseases"), "number"),
        ("tests_with_gene_info", F.col("with_gene"), "number"),
        ("tests_with_disease_info", F.col("with_disease"), "number"),
        ("complete_test_count", F.col("complete"), "number"),
        (
            "frequent_test_count",
            F.when(F.col("frequent_gene"), F.col("tests")).otherwise(0),
            "number",
        ),
        ("has_clinical_test", F.col("tests") > 0, "flag"),
        ("has_multiple_tests", F.col("multiple"), "flag"),
        ("has_comprehensive_testing", F.col("comprehensive"), "flag"),
        ("is_well_tested_gene", F.col("well_tested"), "flag"),
        (
            "test_availability_category",
            band(
                rule["availability_category"],
                F.col("tests"),
                rule["availability_category_default"],
            ),
            "text",
        ),
        (
            "test_accessibility_score",
            F.col("tests") * access["per_test"]
            + F.when(F.col("multiple"), access["multiple"]).otherwise(0)
            + F.when(F.col("comprehensive"), access["comprehensive"]).otherwise(0),
            "number",
        ),
        ("clinical_utility_score", F.col("utility"), "number"),
        (
            "test_quality_score",
            F.col("with_gene") * quality_points["gene_info"]
            + F.col("with_disease") * quality_points["disease_info"]
            + F.col("complete") * quality_points["complete"],
            "number",
        ),
        ("multi_disease_testing", F.col("multi_disease"), "flag"),
        (
            "disease_test_correlation",
            band(
                rule["disease_test_correlation"],
                F.col("total_disease_count"),
                rule["disease_test_correlation_default"],
            ),
            "text",
        ),
        ("pathogenic_variants_in_tested_gene", F.col("pathogenic"), "number"),
        ("test_covered_variants", F.col("total"), "number"),
        (
            "variant_test_coverage_level",
            band(
                rule["variant_test_coverage_level"],
                F.col("pathogenic"),
                rule["variant_test_coverage_level_default"],
            ),
            "text",
        ),
        ("is_cancer_panel_gene", F.col("cancer_panel"), "flag"),
        ("hereditary_cancer_testing", F.col("hereditary"), "flag"),
        ("rare_pathogenic_variants", F.col("rare_pathogenic"), "number"),
        ("carrier_screening_relevant", F.col("carrier"), "flag"),
        (
            "population_test_priority",
            band(
                rule["population_test_priority"],
                F.col("rare_pathogenic"),
                rule["population_test_priority_default"],
            ),
            "text",
        ),
        (
            "clinical_test_utility_score",
            F.col("utility")
            + F.when(F.col("multi_disease"), composite["multi_disease"]).otherwise(0)
            + F.when(many_pathogenic, composite["many_pathogenic"]).otherwise(0),
            "number",
        ),
        (
            "variant_test_coverage_score",
            F.col("pathogenic") * composite["per_pathogenic"]
            + F.when(many_pathogenic, composite["coverage_bonus"]).otherwise(0),
            "number",
        ),
        (
            "population_test_relevance_score",
            F.when(F.col("carrier"), composite["carrier"]).otherwise(0)
            + F.when(F.col("hereditary"), composite["hereditary"]).otherwise(0)
            + F.col("rare_pathogenic") * composite["per_rare_pathogenic"],
            "number",
        ),
        (
            "test_priority",
            F.when(F.col("comprehensive") & F.col("well_tested"), "critical")
            .when(F.col("tests") > 0, "high")
            .when(F.col("utility") >= rule["priority_medium_utility_from"], "medium")
            .otherwise("low"),
            "text",
        ),
        (
            "primary_test_type",
            F.when(F.col("cancer_panel"), "cancer_panel")
            .when(F.col("carrier"), "carrier_screening")
            .when(F.col("tests") > 0, "clinical_diagnostic")
            .otherwise("none"),
            "text",
        ),
        (
            "test_recommendation_tier",
            band(
                rule["test_recommendation_tier"],
                F.col("utility"),
                rule["test_recommendation_tier_default"],
            ),
            "text",
        ),
    ],
)

# COMMAND ----------

# DBTITLE 1,Transform -- cancer site and gene features
# A somatic site is the GRCh37 position and alleles of TCGA (no variant key needed).
rule = FEATURES["cancer"]
site_key = F.concat_ws(
    ":",
    "assembly",
    "contig",
    F.col("start_position").cast("string"),
    "reference_allele",
    "tumor_allele2",
)
somatic_sites = (
    somatic.withColumn("site_key", site_key)
    .withColumn("missense", record_class.isin(mutation["missense"]))
    .withColumn("truncating", record_class.isin(mutation["truncating"]))
    .withColumn("silent", record_class.isin(mutation["silent"]))
)
site_stats = somatic_sites.groupBy("site_key").agg(
    F.countDistinct("sample_barcode").alias("samples"),
    F.count(F.lit(1)).alias("records"),
    F.countDistinct("variant_classification").alias("classes"),
    F.countDistinct("variant_type").alias("types"),
    count_if(F.col("missense")).alias("missense"),
    count_if(F.col("truncating")).alias("truncating"),
    count_if(F.col("silent")).alias("silent"),
    count_if(record_type.isin(mutation["snv_types"])).alias("snv"),
    count_if(record_type.isin(mutation["indel_types"])).alias("indel"),
    F.max("gene_key").alias("gene_key"),
    F.max("variant_key").alias("variant_key"),
)
gene_cancer = (
    somatic_sites.filter(F.col("gene_key").isNotNull())
    .groupBy("gene_key")
    .agg(
        F.count(F.lit(1)).alias("records"),
        F.countDistinct("sample_barcode").alias("samples"),
        F.countDistinct(F.concat_ws(":", "contig", F.col("start_position"))).alias(
            "sites"
        ),
        count_if(F.col("missense")).alias("missense"),
        count_if(F.col("truncating")).alias("truncating"),
        count_if(F.col("silent")).alias("silent"),
    )
    .withColumn(
        "suppressor",
        (F.col("truncating") > F.col("missense"))
        & (F.col("samples") >= rule["tumor_suppressor_samples_from"]),
    )
    .withColumn(
        "oncogene",
        (F.col("missense") > F.col("truncating"))
        & (F.col("samples") >= rule["oncogene_samples_from"]),
    )
)
burden_weights = rule["burden_weights"]
impact_weights = rule["impact_weights"]
priority_points = rule["priority_points"]
cancer_gene_rows = attribute_rows(
    gene_cancer,
    "gene",
    "gene_key",
    "cancer_features",
    "somatic_mutation",
    [
        ("total_samples_affected", F.col("records"), "number"),
        ("unique_samples_affected", F.col("samples"), "number"),
        ("unique_mutation_sites", F.col("sites"), "number"),
        ("missense_mutations", F.col("missense"), "number"),
        ("truncating_mutations", F.col("truncating"), "number"),
        ("silent_mutations", F.col("silent"), "number"),
        (
            "is_cancer_gene",
            F.col("samples") >= rule["cancer_gene_samples_from"],
            "flag",
        ),
        (
            "is_frequently_mutated_gene",
            F.col("sites") >= rule["frequently_mutated_sites_from"],
            "flag",
        ),
        ("is_tumor_suppressor_candidate", F.col("suppressor"), "flag"),
        ("is_oncogene_candidate", F.col("oncogene"), "flag"),
        (
            "gene_cancer_role",
            F.when(F.col("suppressor"), "tumor_suppressor")
            .when(F.col("oncogene"), "oncogene")
            .when(F.col("samples") >= 1, "cancer_associated")
            .otherwise("other"),
            "text",
        ),
        (
            "cancer_mutation_burden_score",
            F.col("samples") * burden_weights["per_sample"]
            + F.col("sites") * burden_weights["per_site"],
            "number",
        ),
        (
            "functional_impact_score",
            F.col("truncating") * impact_weights["truncating"]
            + F.col("missense") * impact_weights["missense"]
            + F.col("silent") * impact_weights["silent"],
            "number",
        ),
        (
            "cancer_priority_score",
            F.when(F.col("suppressor"), priority_points["tumor_suppressor"]).otherwise(
                0
            )
            + F.when(F.col("oncogene"), priority_points["oncogene"]).otherwise(0)
            + F.col("samples") * priority_points["per_sample"],
            "number",
        ),
    ],
)
site_points = rule["site_points"]
therapeutic_points = rule["therapeutic_points"]
prognostic_points = rule["prognostic_points"]
site = (
    site_stats.join(
        gene_cancer.select("gene_key", "suppressor", "oncogene"), "gene_key", "left"
    )
    .join(
        gene_base.select("gene_key", "flags", "proteins_has_kinase_in_name", "tissues"),
        "gene_key",
        "left",
    )
    .join(
        variant_base.select(
            "variant_key", "is_pathogenic", "phylop_score", "max_frequency", "is_rare"
        ),
        "variant_key",
        "left",
    )
    .withColumn("flags", F.coalesce(F.col("flags"), F.array().cast("array<string>")))
    .fillna(False, ["suppressor", "oncogene", "is_pathogenic", "is_rare"])
    .withColumn("recurrent", F.col("samples") >= rule["recurrent_from"])
    .withColumn("hotspot", F.col("samples") >= rule["hotspot_from"])
    .withColumn(
        "oncogenic_domain",
        F.coalesce(F.col("proteins_has_kinase_in_name"), F.lit(0)) > 0,
    )
    .withColumn(
        "hereditary",
        flag("has_cancer_disease") & (F.col("suppressor") | F.col("oncogene")),
    )
)
cancer_site_rows = attribute_rows(
    site,
    "somatic_site",
    "site_key",
    "cancer_features",
    "somatic_mutation+variant+gene attributes",
    [
        ("sample_count", F.col("samples"), "number"),
        ("total_mutation_count", F.col("records"), "number"),
        ("variant_class_count", F.col("classes"), "number"),
        ("variant_type_count", F.col("types"), "number"),
        ("missense_sample_count", F.col("missense"), "number"),
        ("truncating_sample_count", F.col("truncating"), "number"),
        ("silent_sample_count", F.col("silent"), "number"),
        ("snv_sample_count", F.col("snv"), "number"),
        ("indel_sample_count", F.col("indel"), "number"),
        ("is_recurrent_mutation", F.col("recurrent"), "flag"),
        ("is_hotspot_mutation", F.col("hotspot"), "flag"),
        (
            "is_high_impact_cancer_variant",
            (F.col("truncating") >= rule["high_impact"]["truncating_from"])
            & (F.col("samples") >= rule["high_impact"]["samples_from"]),
            "flag",
        ),
        (
            "mutation_frequency_category",
            band(
                rule["mutation_frequency_category"],
                F.col("samples"),
                rule["mutation_frequency_category_default"],
            ),
            "text",
        ),
        ("clinvar_is_pathogenic", F.col("is_pathogenic"), "flag"),
        ("conservation_score", F.col("phylop_score"), "number"),
        (
            "expression_change_relevance",
            F.when(F.col("tissues") <= 5, "tissue_specific")
            .when(F.col("tissues") <= 20, "moderately_expressed")
            .when(F.col("tissues").isNotNull(), "broadly_expressed"),
            "text",
        ),
        ("hereditary_cancer_syndrome", F.col("hereditary"), "flag"),
        ("affected_oncogenic_domains", F.col("oncogenic_domain"), "flag"),
        ("germline_variant_frequency", F.col("max_frequency"), "number"),
        (
            "somatic_versus_germline_classification",
            F.when(
                F.col("max_frequency") > rule["likely_germline_frequency_above"],
                "likely_germline",
            )
            .when(F.col("is_rare"), "likely_somatic")
            .otherwise("unknown"),
            "text",
        ),
        (
            "driver_likelihood_score",
            F.when(F.col("recurrent"), site_points["recurrent"]).otherwise(0)
            + F.when(F.col("is_pathogenic"), site_points["pathogenic"]).otherwise(0)
            + F.when(
                F.col("phylop_score") > rule["conserved_phylop_above"],
                site_points["conserved"],
            ).otherwise(0)
            + F.when(
                F.col("oncogenic_domain"), site_points["oncogenic_domain"]
            ).otherwise(0),
            "number",
        ),
        (
            "therapeutic_target_score",
            F.when(
                F.col("hotspot") & F.col("oncogenic_domain"),
                therapeutic_points["hotspot_kinase"],
            ).otherwise(0)
            + F.when(F.col("oncogene"), therapeutic_points["oncogene"]).otherwise(0)
            + F.when(
                F.col("oncogenic_domain"), therapeutic_points["oncogenic_domain"]
            ).otherwise(0),
            "number",
        ),
        (
            "prognostic_value_score",
            F.when(
                F.col("suppressor") & F.col("is_pathogenic"),
                prognostic_points["suppressor_pathogenic"],
            ).otherwise(0)
            + F.when(F.col("hereditary"), prognostic_points["hereditary"]).otherwise(0)
            + F.col("samples") * prognostic_points["per_sample"],
            "number",
        ),
    ],
)

# COMMAND ----------

# DBTITLE 1,Transform -- population frequency variant features
rule = FEATURES["population"]
tier = F.lit(rule["tier_default"])
for entry in reversed(rule["tiers"]):
    tier = F.when(F.col("frequency") < entry["below"], F.lit(entry["label"])).otherwise(
        tier
    )


def tier_points(points):
    expr = F.lit(points[rule["tier_default"]])
    for entry in reversed(rule["tiers"]):
        expr = F.when(
            F.col("tier") == entry["label"], F.lit(points[entry["label"]])
        ).otherwise(expr)
    return expr


somatic_frequency = (
    somatic.filter(F.col("variant_key").isNotNull())
    .groupBy("variant_key")
    .agg(F.countDistinct("sample_barcode").alias("somatic_samples"))
)
population = (
    variant_base.filter(F.col("max_frequency").isNotNull())
    .withColumn("frequency", F.col("max_frequency"))
    .withColumn("tier", tier)
    .join(
        impact_gene.select("gene_key", F.col("loss_of_function").alias("lof")),
        "gene_key",
        "left",
    )
    .join(
        gene_base.select("gene_key", "total_disease_count", "tissues_with_expression"),
        "gene_key",
        "left",
    )
    .join(somatic_frequency, "variant_key", "left")
    .withColumn("rare_or_ultra", F.col("tier").isin("rare", "ultra_rare"))
    .withColumn("rare_pathogenic", F.col("rare_or_ultra") & F.col("is_pathogenic"))
    .withColumn("conflict", (F.col("tier") == "common") & F.col("is_pathogenic"))
    .withColumn(
        "tolerance",
        band(
            FEATURES["variant_impact"]["lof_tolerance"],
            F.coalesce(F.col("lof"), F.lit(0)),
            "No_LoF",
        ),
    )
    .withColumn("constraint", F.lit(100.0) / (F.coalesce(F.col("lof"), F.lit(0)) + 1))
    .withColumn("rarity", tier_points(rule["rarity_points"]))
    .withColumn("carrier", tier_points(rule["carrier_points"]))
    .withColumn("likelihood", tier_points(rule["likelihood_points"]))
    .withColumn("actionable", F.col("rare_or_ultra") & F.col("is_pathogenic"))
    .withColumn(
        "screening",
        (F.col("tier") == "rare")
        & F.coalesce(F.lower(F.col("origin")).contains("germline"), F.lit(False)),
    )
)
by_disease = rule["carrier_by_disease"]
significance_weights = rule["significance_weights"]
carrier_adjustment = rule["carrier_adjustment"]
likelihood_adjustment = rule["likelihood_adjustment"]
population_rows = attribute_rows(
    population,
    "variant",
    "variant_key",
    "population_features",
    "variant_population_frequency+variant+somatic_mutation+gene attributes",
    [
        ("is_ultra_rare_variant", F.col("tier") == "ultra_rare", "flag"),
        ("is_very_rare_variant", F.col("tier") == "very_rare", "flag"),
        ("is_rare_variant", F.col("tier") == "rare", "flag"),
        ("is_low_frequency_variant", F.col("tier") == "low_frequency", "flag"),
        ("is_common_variant", F.col("tier") == "common", "flag"),
        ("frequency_tier", F.col("tier"), "text"),
        ("rarity_score", F.col("rarity"), "number"),
        ("carrier_risk_score", F.col("carrier"), "number"),
        ("pathogenicity_likelihood_score", F.col("likelihood"), "number"),
        ("pathogenicity_frequency_conflict", F.col("conflict"), "flag"),
        ("rare_pathogenic_variant", F.col("rare_pathogenic"), "flag"),
        (
            "common_benign_validation",
            (F.col("tier") == "common") & F.col("is_benign"),
            "flag",
        ),
        ("gene_mutation_tolerance", F.col("tolerance"), "text"),
        ("gene_constraint_score", F.col("constraint"), "number"),
        (
            "disease_allele_frequency",
            band(
                rule["disease_allele_frequency"],
                F.coalesce(F.col("total_disease_count"), F.lit(0)),
                rule["disease_allele_frequency_default"],
            ),
            "text",
        ),
        (
            "carrier_frequency_by_disease",
            F.when(
                F.col("rare_pathogenic")
                & (F.col("total_disease_count") >= by_disease["high_from"]),
                "High_Risk_Carrier",
            )
            .when(
                F.col("rare_pathogenic")
                & (F.col("total_disease_count") >= by_disease["medium_from"]),
                "Medium_Risk_Carrier",
            )
            .otherwise("Low_Risk_Carrier"),
            "text",
        ),
        ("somatic_frequency", F.col("somatic_samples"), "number"),
        (
            "germline_cancer_predisposition",
            F.col("rare_or_ultra")
            & (F.coalesce(F.col("somatic_samples"), F.lit(0)) > 0),
            "flag",
        ),
        (
            "expression_frequency_correlation",
            band(
                rule["expression_frequency_correlation"],
                F.coalesce(F.col("tissues_with_expression"), F.lit(0)),
                rule["expression_frequency_correlation_default"],
            ),
            "text",
        ),
        (
            "tissue_specific_allele_effects",
            F.coalesce(F.col("tissues_with_expression"), F.lit(0)) < 10,
            "flag",
        ),
        ("is_clinically_actionable_rare_variant", F.col("actionable"), "flag"),
        ("is_carrier_screening_candidate", F.col("screening"), "flag"),
        (
            "population_priority",
            F.when(F.col("actionable"), "high")
            .when((F.col("tier") == "rare") & F.col("is_vus"), "medium")
            .otherwise("low"),
            "text",
        ),
        (
            "screening_recommendation",
            F.when(F.col("actionable"), "recommended")
            .when(F.col("screening"), "consider")
            .otherwise("not_indicated"),
            "text",
        ),
        (
            "clinical_significance_frequency_score",
            F.coalesce(F.col("pathogenicity_score"), F.lit(0))
            * significance_weights["pathogenicity"]
            + F.col("rarity") * significance_weights["rarity"]
            + F.when(
                F.col("rare_pathogenic"), significance_weights["rare_pathogenic"]
            ).otherwise(0),
            "number",
        ),
        (
            "carrier_risk_score_adjusted",
            F.col("carrier")
            + F.when(
                F.col("tolerance") == "LoF_Sensitive",
                carrier_adjustment["lof_sensitive"],
            ).otherwise(0)
            + F.col("constraint") * carrier_adjustment["per_constraint"],
            "number",
        ),
        (
            "pathogenicity_likelihood_refined",
            F.col("likelihood")
            + F.coalesce(F.col("conservation_level"), F.lit(0))
            * likelihood_adjustment["per_conservation"]
            + F.when(F.col("conflict"), likelihood_adjustment["conflict"]).otherwise(0),
            "number",
        ),
    ],
)

# COMMAND ----------

# DBTITLE 1,Transform -- gene age, essentiality, structure confidence, ClinVar history, ACMG
# Gene age: the most distant species with an orthologue (Ensembl Compara) and the
# consensus gene age (13 orthology algorithms); paralogue count; DepMap essentiality;
# AlphaFold pLDDT per protein and per variant; first and last ClinVar snapshot.
depth = RULES["orthologue_depth"]
species_rank = F.create_map(
    *[
        F.lit(part)
        for name, entry in depth["species"].items()
        for part in (name, entry["rank"])
    ]
)
levels = {entry["rank"]: entry["level"] for entry in depth["species"].values()}
level_by_rank = F.create_map(*[F.lit(part) for item in levels.items() for part in item])
homology = relation_table.filter(
    (F.col("relation_kind") == "homology") & F.col("gene_a_key").isNotNull()
)
orthologue_depth = (
    homology.filter(~F.col("relation_subkind").isin(depth["paralogue_types"]))
    .withColumn("rank", species_rank[F.col("organism_b")])
    .groupBy(F.col("gene_a_key").alias("gene_key"))
    .agg(F.max("rank").alias("rank"))
    .withColumn("level", level_by_rank[F.col("rank")])
)
paralogues = (
    homology.filter(F.col("relation_subkind").isin(depth["paralogue_types"]))
    .groupBy(F.col("gene_a_key").alias("gene_key"))
    .agg(F.countDistinct("interactor_b_id").alias("paralogues"))
)
GENE_AGES = "gene_ages.consensus"
age_text = text_for(GENE_AGES)
consensus = link_entities(
    read_genomics_bronze("gene_ages_consensus"),
    [
        (age_text("protein_accession"), "uniprot"),
        (ensembl_unversioned_expr(age_text("protein_accession")), "ensembl_protein"),
    ],
    entity_id_map(xwalk, "protein"),
    "protein_key",
    "protein_link_method",
).join(
    read_silver("protein_gene_link").select("protein_key", "gene_key"), "protein_key"
)
age_number = number_for(GENE_AGES)
age_classes = depth["consensus_classes"]
consensus_age = consensus.groupBy("gene_key").agg(
    F.first(age_text("modeAge"), ignorenulls=True).alias("mode_age"),
    F.max(age_number("entropy")).alias("entropy"),
    F.max(age_number("NumDBsContributing")).alias("databases"),
    F.max(age_number("NodeError")).alias("node_error"),
    F.max(age_number("Bimodality")).alias("bimodality"),
    F.max(F.lower(age_text("HGT_flag")) == "true").alias("transfer_flag"),
    *[F.max(age_number(column)).alias(f"share_{column}") for column in age_classes],
)
essential = RULES["gene_essentiality"]
effects = read_silver("gene_dependency").filter(F.col("gene_key").isNotNull())
essentiality = (
    effects.groupBy("gene_key")
    .agg(
        F.count(F.lit(1)).alias("lines"),
        count_if(F.col("gene_effect") <= essential["dependent_at_or_below"]).alias(
            "dependent"
        ),
        count_if(
            F.col("gene_effect") <= essential["strongly_essential_at_or_below"]
        ).alias("strong"),
        F.avg("gene_effect").alias("mean_effect"),
        F.min("gene_effect").alias("min_effect"),
    )
    .withColumn("dependent_share", F.col("dependent") / F.col("lines"))
)
age_rows = attribute_rows(
    gene.select("gene_key")
    .join(orthologue_depth, "gene_key", "left")
    .join(paralogues, "gene_key", "left")
    .join(consensus_age, "gene_key", "left")
    .join(essentiality, "gene_key", "left"),
    "gene",
    "gene_key",
    "gene_age_and_essentiality",
    "entity_relation homology+gene_ages+gene_dependency",
    [
        ("orthologue_depth", F.col("level"), "text"),
        ("orthologue_depth_rank", F.col("rank"), "number"),
        ("paralogue_count", F.col("paralogues"), "number"),
        ("gene_age_consensus", F.col("mode_age"), "text"),
        ("gene_age_entropy", F.col("entropy"), "number"),
        ("gene_age_contributing_databases", F.col("databases"), "number"),
        ("gene_age_node_error", F.col("node_error"), "number"),
        ("gene_age_bimodality", F.col("bimodality"), "number"),
        ("gene_age_horizontal_transfer_flag", F.col("transfer_flag"), "flag"),
        *[
            (f"gene_age_share_{column.lower()}", F.col(f"share_{column}"), "number")
            for column in age_classes
        ],
        ("dependent_cell_line_share", F.col("dependent_share"), "number"),
        ("strongly_essential_cell_lines", F.col("strong"), "number"),
        ("mean_gene_effect", F.col("mean_effect"), "number"),
        ("minimum_gene_effect", F.col("min_effect"), "number"),
        (
            "essentiality_class",
            F.when(
                F.col("lines").isNotNull(),
                band(
                    essential["classes"],
                    F.col("dependent_share"),
                    essential["class_default"],
                ),
            ),
            "text",
        ),
    ],
)
# AlphaFold confidence: per protein, and at the protein position of a variant
residues = read_silver("protein_residue_score").filter(F.col("protein_key").isNotNull())
protein_confidence = attribute_rows(
    residues.groupBy("protein_key").agg(
        F.avg("score_value").alias("mean_plddt"),
        (count_if(F.col("score_band") == "very_low") / F.count(F.lit(1))).alias(
            "disordered_share"
        ),
    ),
    "protein",
    "protein_key",
    "plddt_bands",
    "protein_residue_score",
    [
        ("mean_plddt", F.col("mean_plddt"), "number"),
        ("very_low_confidence_residue_share", F.col("disordered_share"), "number"),
    ],
)
variant_residues = (
    current_snapshot(read_silver("variant_hgvs"), RULES)
    .filter(F.col("protein_position").isNotNull() & F.col("transcript_key").isNotNull())
    .select("variant_key", "transcript_key", "protein_position")
    .join(
        master.filter(
            (F.col("entity_type") == "transcript") & F.col("protein_key").isNotNull()
        ).select(F.col("entity_key").alias("transcript_key"), "protein_key"),
        "transcript_key",
    )
    .join(
        residues.select(
            "protein_key",
            F.col("residue_number").alias("protein_position"),
            "score_value",
            "score_band",
        ),
        ["protein_key", "protein_position"],
    )
    .groupBy("variant_key")
    .agg(F.max("score_value").alias("plddt"))
)
bands = RULES["plddt_bands"]
variant_confidence = attribute_rows(
    variant_residues,
    "variant",
    "variant_key",
    "plddt_bands",
    "variant_hgvs+protein_residue_score",
    [
        ("plddt_at_variant", F.col("plddt"), "number"),
        (
            "plddt_band_at_variant",
            band(bands["bands"], F.col("plddt"), bands["band_default"]),
            "text",
        ),
    ],
)
# ClinVar history over all snapshots (dates of non-date labels from the rules)
snapshot_dates = RULES["clinvar_snapshot_dates"]
snapshot_sort = F.coalesce(
    F.create_map(*[F.lit(part) for item in snapshot_dates.items() for part in item])[
        F.col("source_snapshot")
    ],
    F.col("source_snapshot"),
)
history = (
    expand_snapshot_runs(
        read_silver("variant_classification"), clinvar_snapshot_order(RULES)
    )
    .withColumn("snapshot_date", snapshot_sort)
    .groupBy("variant_key")
    .agg(
        F.min("snapshot_date").alias("first"),
        F.max("snapshot_date").alias("last"),
        F.countDistinct("source_snapshot").alias("snapshots"),
        F.max(F.col("source_snapshot") == RULES["clinvar_current_snapshot"]).alias(
            "in_current"
        ),
    )
)
history_rows = attribute_rows(
    history,
    "variant",
    "variant_key",
    "clinvar_snapshot_history",
    "variant_classification.first_source_snapshot+last_source_snapshot",
    [
        ("clinvar_first_snapshot", F.col("first"), "text"),
        ("clinvar_last_snapshot", F.col("last"), "text"),
        ("clinvar_snapshot_count", F.col("snapshots"), "number"),
        ("in_current_clinvar", F.col("in_current"), "flag"),
    ],
)
# ACMG secondary-findings genes
acmg_rows = attribute_rows(
    relation_table.filter(
        (F.col("relation_subkind") == "acmg_secondary_findings")
        & F.col("gene_key").isNotNull()
    )
    .select("gene_key")
    .distinct()
    .withColumn("listed", F.lit(True)),
    "gene",
    "gene_key",
    "acmg_secondary_findings",
    "entity_relation acmg_secondary_findings",
    [("is_acmg_secondary_finding_gene", F.col("listed"), "flag")],
)

# COMMAND ----------

# DBTITLE 1,Transform -- union and provenance
derived = _union(
    [
        function_flags,
        location_flags,
        process_flags,
        description_flags,
        description_values,
        designation_flags,
        keyword_flags,
        category_text_flags,
        disease_category_flags,
        synonym_counts,
        designation_counts,
        database_counts,
        well_annotated,
        word_counts,
        gene_lengths,
        last_band_digit,
        cytoband_rows,
        primary_rows,
        druggability_rows,
        disease_total_rows,
        protein_flags,
        domain_count_rows,
        functional_domain_rows,
        disease_flags,
        allele_rows,
        *score_rows,
        *label_rows,
        *quality_rows,
        origin_flags,
        type_flags,
        *phenotype_rows,
        *impact_rows,
        *frequency_rows,
        *tissue_rows,
        *cancer_rows,
        *mutation_rows,
        *test_rows,
        *test_gene_rows,
        *pharmacogene_rows,
        *relation_counts,
        clinical_variant_rows,
        clinical_gene_rows,
        disease_variant_rows,
        disease_level_rows,
        disease_gene_rows,
        pharma_variant_rows,
        pharma_gene_rows,
        impact_variant_rows,
        impact_gene_rows,
        gene_variant_priority,
        structural_rows,
        disruption_rows,
        overlap_count_rows,
        pharmacogene_gene_rows,
        response_rows,
        expression_rows,
        family_rows,
        test_gene_rows_gold,
        cancer_gene_rows,
        cancer_site_rows,
        population_rows,
        age_rows,
        protein_confidence,
        variant_confidence,
        history_rows,
        acmg_rows,
    ]
)
derived = derived.distinct().withColumn(
    "_srid",
    sha_key(
        "entity_type",
        "entity_key",
        "attribute",
        "rule_name",
        "value_text",
        "value_number",
    ),
)
derived = conform_genomics(
    add_genomics_provenance(derived, "_srid", RID), "entity_derived_attribute"
)

# COMMAND ----------

# DBTITLE 1,Write, inspect and export -- entity_derived_attribute
write_inspect_export(
    derived,
    "entity_derived_attribute",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    findings=FINDINGS,
    key_cols=["source_record_id"],
    extra_checks={
        "attributes": lambda t: t.select("rule_name", "attribute").distinct().count(),
        "genes_with_the_default_primary_function": lambda t: (
            t.filter(F.col("rule_name") == "primary_function")
            .filter(F.col("value_text") == RULES["primary_function_default"])
            .count()
        ),
        "entity_types": lambda t: t.select("entity_type").distinct().count(),
        "variants_with_an_impact_score": lambda t: t.filter(
            F.col("attribute") == "pathogenicity_score"
        ).count(),
    },
)

# COMMAND ----------

# DBTITLE 1,Check and export -- rejected values of the lenient number reads
export_rejected_values(FINDINGS, COMPONENT, read_genomics_bronze)
