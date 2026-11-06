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
# MAGIC rules file.

# COMMAND ----------

# DBTITLE 1,Shared library
# MAGIC %run ../_silver_common

# COMMAND ----------

# DBTITLE 1,Inspection library
# MAGIC %run ../_silver_inspect

# COMMAND ----------

# DBTITLE 1,Genomics library
# MAGIC %run ../_genomics_common

# COMMAND ----------

# DBTITLE 1,Entity helpers
# MAGIC %run ../_genomics_entities

# COMMAND ----------

# DBTITLE 1,Configuration
from pyspark.sql import Window
from pyspark.sql import functions as F

SOURCE = "genomics"
COMPONENT = "silver/genomics/11_derived_attributes"
RID = run_id()
FINDINGS = "genomics"
RULES = load_genomics_rules()

# COMMAND ----------

# DBTITLE 1,Read Silver -- tables and rule values
gene = read_silver("gene")
protein = read_silver("protein")
disease = read_silver("disease")
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
association = read_silver("gene_disease_association").filter(
    F.col("gene_key").isNotNull()
    & F.col("disease_key").isNotNull()
    & F.col("record_kind").isin(importance["disease_record_kinds"])
)
disease_names = disease.select("disease_key", F.col("name").alias("disease_name"))
per_kind = association.groupBy("gene_key", "record_kind").agg(
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
            text_source="gene_disease_association+disease.name",
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
    "gene_disease_association",
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
        "gene_disease_association+gene_description_keywords",
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
conditions = current_snapshot(read_silver("variant_condition"), RULES).select(
    "variant_key", F.explode("condition_ids").alias("raw_id")
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
        "variant_condition.condition_ids",
        method="rule",
    ),
    flag_rows(
        phenotype_databases,
        "variant",
        "variant_key",
        "is_well_annotated",
        F.col("n") >= RULES["variant_well_annotated_minimum"],
        "phenotype_databases",
        "variant_condition.condition_ids",
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
phylop_scores = (
    read_silver("position_score")
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
    read_silver("variant_score")
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
SOURCE_TEXT = "variant_hgvs+variant_feature_overlap+position_score+variant_score"
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
        "position_score+variant_score",
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
        "position_score.phylop_100way",
    ),
    flag_rows(
        impact,
        "variant",
        "variant_key",
        "is_conservation_constrained",
        F.col("conservation_level") >= 1,
        "conservation",
        "position_score+variant_score",
    ),
    flag_rows(
        impact,
        "variant",
        "variant_key",
        "has_cadd_score",
        F.col("cadd_phred").isNotNull(),
        "conservation",
        "variant_score.cadd_phred",
    ),
    flag_rows(
        impact,
        "variant",
        "variant_key",
        "is_deleterious_by_cadd",
        F.col("cadd_phred") > cutoffs["cadd_deleterious_above"],
        "conservation",
        "variant_score.cadd_phred",
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
tests = read_silver("genetic_test")
test_rows = [
    flag_rows(
        tests,
        "genetic_test",
        "test_key",
        "has_gene_info",
        F.size("genes") > 0,
        "genetic_test_record",
        "genetic_test.genes",
    ),
    flag_rows(
        tests,
        "genetic_test",
        "test_key",
        "has_disease_info",
        F.size("conditions") > 0,
        "genetic_test_record",
        "genetic_test.conditions",
    ),
    flag_rows(
        tests,
        "genetic_test",
        "test_key",
        "is_complete_record",
        (F.size("genes") > 0) & (F.size("conditions") > 0),
        "genetic_test_record",
        "genetic_test.genes+conditions",
    ),
]
tests_per_gene = (
    tests.select("test_key", F.explode("genes").alias("g"))
    .select("test_key", F.col("g.gene_key").alias("gene_key"))
    .filter(F.col("gene_key").isNotNull())
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


relations = read_silver("drug_gene_relation").filter(F.col("gene_key").isNotNull())


def relation_kind(kind, column):
    return (
        relations.filter(F.col("relation_type") == kind)
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
PHARMACOGENE_SOURCE = "gene claims+drug_gene_relation"
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
association_all = read_silver("gene_disease_association").filter(
    F.col("gene_key").isNotNull() & F.col("disease_key").isNotNull()
)
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
        "gene_disease_association",
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
        "gene_disease_association",
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
        "drug_gene_relation",
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
        "drug_gene_relation",
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
write_silver(
    derived, "entity_derived_attribute", source=SOURCE, component=COMPONENT, rid=RID
)

written = spark.table(genomics_table("entity_derived_attribute"))
blocks = inspect_table(
    written,
    "entity_derived_attribute",
    source=FINDINGS,
    component=COMPONENT,
    rid=RID,
    key_cols=["source_record_id"],
    extra_checks={
        "attributes": written.select("rule_name", "attribute").distinct().count(),
        "genes_with_the_default_primary_function": written.filter(
            F.col("rule_name") == "primary_function"
        )
        .filter(F.col("value_text") == RULES["primary_function_default"])
        .count(),
        "entity_types": written.select("entity_type").distinct().count(),
        "variants_with_an_impact_score": written.filter(
            F.col("attribute") == "pathogenicity_score"
        ).count(),
        **{
            f"{column}_counts": str(
                {
                    r[column]: r["count"]
                    for r in written.groupBy(column).count().collect()
                }
            )
            for column in written.columns
            if column.endswith("_link_method")
        },
    },
)

write_silver_findings(
    FINDINGS,
    f"{COMPONENT.split('/')[-1]}__entity_derived_attribute",
    "entity_derived_attribute",
    blocks,
)
