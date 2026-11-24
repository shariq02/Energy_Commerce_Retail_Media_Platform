# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # SILVER -- CLAIM ATTRIBUTES, SAMPLE FEATURES AND SEQUENCE (GENOMICS)
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** November 2026
# MAGIC
# MAGIC **Purpose:** the Open Targets constraint, annotation and class claims, the ClinVar
# MAGIC variant claims, the disease areas, the drug stages, the tumour sample features
# MAGIC and the paralogue groups as rows of `entity_derived_attribute`; the amino acid
# MAGIC substitution table; the reference sequence window of each variant. Runs after
# MAGIC `08` and `09`.

# COMMAND ----------

# DBTITLE 1,Genomics library
# MAGIC %run ../_genomics_common

# COMMAND ----------

# DBTITLE 1,Configuration
from functools import reduce

import pandas as pd
from pyspark.sql import functions as F

SOURCE = "genomics"
COMPONENT = "silver/genomics/10_claim_attributes"
RID = run_id()
FINDINGS = "genomics"
RULES = load_genomics_rules()
ATTRIBUTES = RULES["claim_attributes"]
MUTATION = RULES["cancer_mutation"]
VERSION = genomics_rule_version(RULES)
AS_OF = F.to_date(F.lit(RID.split("-")[1][:8]), "yyyyMMdd")
OPEN_TARGETS = "opentargets.target"
CLINVAR = "clinvar.variant_summary"
WRITTEN_RULES = set()


def rows(df, entity_type, key, rule_name, source, *, attribute, **value):
    """Attribute rows of `df`: `attribute` is a text or a Column; `value` is one of
    `number`, `text` (Columns) or `flag` (a Column that is true), and `method`."""
    WRITTEN_RULES.add(rule_name)
    flag = value.pop("flag", None)
    method = value.pop("method", "claim")
    for column in (value.get("number"), value.get("text")):
        if column is not None:
            df = df.filter(column.isNotNull())
    if flag is not None:
        df = df.filter(flag)
        value["boolean"] = F.lit(True)
    return derived_rows(
        df,
        entity_type=entity_type,
        key=key,
        attribute=attribute,
        rule_name=rule_name,
        method=method,
        text_source=source,
        version=VERSION,
        as_of=AS_OF,
        value_number=value.get("number"),
        value_text=value.get("text"),
        value_boolean=value.get("boolean"),
    )


def slug(column):
    """A lower-case token of a text: letters and digits, one `_` between words."""
    text = F.regexp_replace(F.lower(F.trim(column)), r"[^a-z0-9]+", "_")
    return F.regexp_replace(text, r"^_+|_+$", "")


def count_rows(df, entity_type, field, attribute, rule_name, source):
    """The number of distinct values of one claim field per entity."""
    counts = (
        df.filter((F.col("field") == field) & F.col("value_text").isNotNull())
        .groupBy("entity_key")
        .agg(F.countDistinct("value_text").alias("n"))
    )
    return rows(
        counts,
        entity_type,
        "entity_key",
        rule_name,
        source,
        attribute=attribute,
        number=F.col("n"),
        method="count",
    )


def term_flags(df, entity_type, field, prefix, rule_name, source, source_field=None):
    """One true flag per entity and value of a claim field: `<prefix><value token>`."""
    chosen = df.filter(F.col("field") == field)
    if source_field:
        chosen = chosen.filter(F.col("source_field") == source_field)
    terms = (
        chosen.select("entity_key", slug(F.col("value_text")).alias("term"))
        .filter(F.col("term") != "")
        .distinct()
    )
    return rows(
        terms,
        entity_type,
        "entity_key",
        rule_name,
        source,
        attribute=F.concat(F.lit(prefix), F.col("term")),
        flag=F.lit(True),
    )


# COMMAND ----------

# DBTITLE 1,Read Silver -- claims, masters, variants and mutations
claims = read_silver("entity_claim")
master = read_silver("entity_master")
variants = read_silver("variant")
somatic = read_silver("somatic_mutation")
gene_keys = master.filter(F.col("entity_type") == "gene").select(
    F.col("entity_key").alias("gene_key")
)
gene_claims = claims.filter(F.col("entity_type") == "gene").join(
    gene_keys, F.col("entity_key") == F.col("gene_key"), "left_semi"
)
target_claims = gene_claims.filter(F.col("source_dataset") == OPEN_TARGETS)
variant_claims = current_snapshot(
    claims.filter(
        (F.col("entity_type") == "variant") & (F.col("source_dataset") == CLINVAR)
    ),
    RULES,
)

# COMMAND ----------

# DBTITLE 1,Transform -- gene constraint and annotation (Open Targets claims)
constraint = (
    target_claims.filter(
        F.col("field").startswith("constraint_") & F.col("value_number").isNotNull()
    )
    .groupBy("entity_key", "field")
    .agg(F.max("value_number").alias("value"))
)
ontology_names = F.create_map(
    *[F.lit(x) for pair in ATTRIBUTES["gene_ontology_counts"].items() for x in pair]
)
ontology_counts = (
    target_claims.filter(F.col("field").isin(*ATTRIBUTES["gene_ontology_counts"]))
    .groupBy("entity_key", "field")
    .agg(F.countDistinct("value_text").alias("n"))
)
ANNOTATION = "gene_open_targets_annotation"
gene_rows = [
    rows(
        constraint,
        "gene",
        "entity_key",
        "gene_open_targets_constraint",
        OPEN_TARGETS,
        attribute=F.col("field"),
        number=F.col("value"),
    ),
    rows(
        ontology_counts,
        "gene",
        "entity_key",
        ANNOTATION,
        OPEN_TARGETS,
        attribute=F.try_element_at(ontology_names, F.col("field")),
        number=F.col("n"),
        method="count",
    ),
    count_rows(
        target_claims,
        "gene",
        "pathway_top_level_term",
        "reactome_top_level_term_count",
        ANNOTATION,
        OPEN_TARGETS,
    ),
    term_flags(
        target_claims,
        "gene",
        "pathway_top_level_term",
        "reactome_top_level_",
        ANNOTATION,
        OPEN_TARGETS,
    ),
    count_rows(
        target_claims,
        "gene",
        "target_class",
        "target_class_count",
        ANNOTATION,
        OPEN_TARGETS,
    ),
    term_flags(
        target_claims,
        "gene",
        "target_class",
        "target_class_",
        ANNOTATION,
        OPEN_TARGETS,
        source_field="targetClass:l1",
    ),
    count_rows(
        target_claims,
        "gene",
        "subcellular_location",
        "subcellular_location_count",
        ANNOTATION,
        OPEN_TARGETS,
    ),
    count_rows(
        target_claims,
        "gene",
        "cancer_hallmark",
        "cancer_hallmark_count",
        ANNOTATION,
        OPEN_TARGETS,
    ),
    term_flags(
        target_claims,
        "gene",
        "cancer_hallmark",
        "cancer_hallmark_",
        ANNOTATION,
        OPEN_TARGETS,
    ),
    count_rows(
        gene_claims,
        "gene",
        "description",
        "description_count",
        "gene_description_texts",
        "entity_claim.description",
    ),
]

# COMMAND ----------

# DBTITLE 1,Transform -- variant ClinVar claims and the locus block
VARIANT_RULE = "variant_clinvar_claims"
category = variant_claims.filter(F.col("field") == "submitter_categories")
category_text = F.trim(F.col("value_text"))
category_value = F.when(category_text.rlike(r"^[0-9]+$"), category_text.cast("double"))
register_lenient(
    "entity_claim.submitter_categories", category, category_text, category_value
)
guidelines = variant_claims.filter(F.col("field") == "guidelines").select(
    "entity_key", F.explode(F.split("value_text", ",")).alias("guideline")
)
location = variant_claims.filter(F.col("field") == "cytogenetic_location")
variant_rows = [
    rows(
        category.groupBy("entity_key").agg(F.max(category_value).alias("category")),
        "variant",
        "entity_key",
        VARIANT_RULE,
        CLINVAR,
        attribute="clinvar_submitter_category",
        number=F.col("category"),
    ),
    rows(
        variant_claims.filter(F.col("field") == "tested_in_genetic_testing_registry"),
        "variant",
        "entity_key",
        VARIANT_RULE,
        CLINVAR,
        attribute="clinvar_tested_in_genetic_testing_registry",
        flag=F.col("value_text") == "Y",
    ),
    rows(
        guidelines.groupBy("entity_key").agg(
            F.countDistinct(F.trim("guideline")).alias("n")
        ),
        "variant",
        "entity_key",
        VARIANT_RULE,
        CLINVAR,
        attribute="clinvar_guideline_count",
        number=F.col("n"),
        method="count",
    ),
    count_rows(
        variant_claims,
        "variant",
        "gene_key",
        "variant_gene_count",
        VARIANT_RULE,
        CLINVAR,
    ),
    count_rows(
        variant_claims,
        "variant",
        "other_ids",
        "clinvar_other_id_count",
        VARIANT_RULE,
        CLINVAR,
    ),
    *[
        rows(
            location.select("entity_key", part.alias("part"))
            .filter(F.col("part").isNotNull())
            .distinct(),
            "variant",
            "entity_key",
            VARIANT_RULE,
            CLINVAR,
            attribute=f"variant_{name}",
            text=F.col("part"),
        )
        for name, part in cytoband_parts(F.col("value_text"))
    ],
    rows(
        variants.filter(F.col("contig").isNotNull() & F.col("position").isNotNull()),
        "variant",
        "variant_key",
        "variant_locus_block",
        "variant",
        attribute="locus_block",
        text=F.concat_ws(
            ":",
            "contig",
            F.floor((F.col("position") - 1) / ATTRIBUTES["locus_block_bases"]).cast(
                "string"
            ),
        ),
        method="rule",
    ),
]

# COMMAND ----------

# DBTITLE 1,Transform -- disease areas and drug stages
disease_claims = claims.filter(F.col("entity_type") == "disease")
areas = (
    disease_claims.filter(F.col("field") == "therapeutic_areas")
    .select(
        "entity_key",
        F.explode(F.from_json("value_text", "array<string>")).alias("area_id"),
    )
    .distinct()
)
area_ids = (
    read_silver("entity_id_crosswalk")
    .filter(
        (F.col("entity_type") == "disease")
        & (F.col("id_type") == "opentargets_disease")
    )
    .select(F.col("id_value").alias("area_id"), F.col("entity_key").alias("area_key"))
    .distinct()
)
area_names = master.filter(F.col("entity_type") == "disease").select(
    F.col("entity_key").alias("area_key"), "name"
)
named_areas = areas.join(area_ids, "area_id").join(area_names, "area_key")
large_areas = (
    named_areas.groupBy("area_key")
    .agg(F.countDistinct("entity_key").alias("diseases"))
    .filter(F.col("diseases") >= ATTRIBUTES["therapeutic_area_minimum_diseases"])
    .select("area_key")
)
DISEASE_RULE = "disease_therapeutic_areas"
stage_rank = F.create_map(
    *[
        F.lit(part)
        for rank, name in enumerate(ATTRIBUTES["drug_stage_order"], 1)
        for part in (name, rank)
    ]
)
drug_claims = claims.filter(F.col("entity_type") == "drug")
stages = (
    drug_claims.filter(F.col("field") == "max_clinical_stage")
    .select(
        "entity_key", F.try_element_at(stage_rank, F.col("value_text")).alias("rank")
    )
    .filter(F.col("rank").isNotNull())
    .groupBy("entity_key")
    .agg(F.max("rank").alias("rank"))
)
trade_names = (
    read_silver("entity_name")
    .filter((F.col("entity_type") == "drug") & (F.col("term_type") == "trade_name"))
    .groupBy("entity_key")
    .agg(F.countDistinct("term").alias("n"))
)
child_compounds = (
    drug_claims.filter(F.col("field") == "child_chembl_ids")
    .select("entity_key", F.size(F.from_json("value_text", "array<string>")).alias("n"))
    .groupBy("entity_key")
    .agg(F.max("n").alias("n"))
)
DRUG_RULE = "drug_open_targets"
disease_drug_rows = [
    rows(
        areas.groupBy("entity_key").agg(F.count(F.lit(1)).alias("n")),
        "disease",
        "entity_key",
        DISEASE_RULE,
        "opentargets.disease",
        attribute="therapeutic_area_count",
        number=F.col("n"),
        method="count",
    ),
    rows(
        named_areas.join(large_areas, "area_key")
        .select("entity_key", slug(F.col("name")).alias("term"))
        .distinct(),
        "disease",
        "entity_key",
        DISEASE_RULE,
        "opentargets.disease",
        attribute=F.concat(F.lit("therapeutic_area_"), F.col("term")),
        flag=F.lit(True),
    ),
    rows(
        stages,
        "drug",
        "entity_key",
        DRUG_RULE,
        "opentargets.drug_molecule",
        attribute="clinical_stage_rank",
        number=F.col("rank"),
        method="rule",
    ),
    rows(
        trade_names,
        "drug",
        "entity_key",
        DRUG_RULE,
        "entity_name",
        attribute="trade_name_count",
        number=F.col("n"),
        method="count",
    ),
    rows(
        child_compounds,
        "drug",
        "entity_key",
        DRUG_RULE,
        "opentargets.drug_molecule",
        attribute="child_compound_count",
        number=F.col("n"),
        method="count",
    ),
]

# COMMAND ----------

# DBTITLE 1,Transform -- tumour sample features
record_class = F.col("variant_classification")
non_silent = record_class.isNotNull() & ~record_class.isin(*MUTATION["silent"])
fraction = F.when(
    F.col("tumor_depth") > 0, F.col("tumor_alt_count") / F.col("tumor_depth")
)
low = ATTRIBUTES["allele_fraction_low_below"]
roles = (
    read_silver("entity_derived_attribute")
    .filter(F.col("attribute") == "gene_cancer_role")
    .select(F.col("entity_key").alias("gene_key"), F.col("value_text").alias("role"))
    .distinct()
)
samples = somatic.filter(F.col("sample_barcode").isNotNull())
sample_totals = (
    samples.groupBy("sample_barcode")
    .agg(
        F.avg(fraction).alias("mean"),
        F.percentile_approx(fraction, 0.5).alias("median"),
        F.avg(F.when(fraction.isNotNull(), (fraction < low).cast("int"))).alias("low"),
        F.sum(non_silent.cast("int")).alias("non_silent"),
        F.sum(F.col("variant_type").isin(*MUTATION["indel_types"]).cast("int")).alias(
            "indels"
        ),
        F.countDistinct("gene_key").alias("genes"),
    )
    .withColumn(
        "burden_high",
        F.col("non_silent") >= ATTRIBUTES["tumor_mutational_burden_high_from"],
    )
)
complement = F.create_map(
    *[
        F.lit(x)
        for pair in (("A", "T"), ("C", "G"), ("G", "C"), ("T", "A"))
        for x in pair
    ]
)
reference = F.upper("reference_allele")
alternate = F.upper("tumor_allele2")
pyrimidine_first = reference.isin("C", "T")
spectrum = (
    samples.filter(
        F.col("variant_type").isin(*MUTATION["snv_types"])
        & reference.isin("A", "C", "G", "T")
        & alternate.isin("A", "C", "G", "T")
        & (reference != alternate)
    )
    .withColumn(
        "substitution",
        F.concat(
            F.when(pyrimidine_first, reference).otherwise(
                F.try_element_at(complement, reference)
            ),
            F.lit("_to_"),
            F.when(pyrimidine_first, alternate).otherwise(
                F.try_element_at(complement, alternate)
            ),
        ),
    )
    .groupBy("sample_barcode", "substitution")
    .agg(F.count(F.lit(1)).alias("n"))
)
driver_counts = (
    samples.filter(non_silent)
    .join(roles, "gene_key")
    .groupBy("sample_barcode", "role")
    .agg(F.count(F.lit(1)).alias("n"))
    .filter(F.col("role").isin("oncogene", "tumor_suppressor"))
)
SAMPLE_RULE = "tumor_sample_features"
sample_rows = [
    rows(
        sample_totals,
        "tumor_sample",
        "sample_barcode",
        SAMPLE_RULE,
        "somatic_mutation",
        attribute=name,
        number=F.col(column),
        method="rule",
    )
    for name, column in (
        ("allele_fraction_mean", "mean"),
        ("allele_fraction_median", "median"),
        ("low_allele_fraction_share", "low"),
        ("non_silent_mutation_count", "non_silent"),
        ("indel_count", "indels"),
        ("mutated_gene_count", "genes"),
    )
] + [
    rows(
        sample_totals,
        "tumor_sample",
        "sample_barcode",
        SAMPLE_RULE,
        "somatic_mutation",
        attribute="is_tumor_mutational_burden_high",
        flag=F.col("burden_high"),
        method="rule",
    ),
    rows(
        spectrum,
        "tumor_sample",
        "sample_barcode",
        SAMPLE_RULE,
        "somatic_mutation",
        attribute=F.concat(
            F.lit("substitution_"), F.lower("substitution"), F.lit("_count")
        ),
        number=F.col("n"),
        method="count",
    ),
    rows(
        driver_counts,
        "tumor_sample",
        "sample_barcode",
        SAMPLE_RULE,
        "somatic_mutation+gene_cancer_role",
        attribute=F.concat(F.col("role"), F.lit("_mutation_count")),
        number=F.col("n"),
        method="count",
    ),
]

# COMMAND ----------

# DBTITLE 1,Transform -- paralogue groups (genes that share a paralogue chain)
paralogue_types = RULES["orthologue_depth"]["paralogue_types"]
edges = (
    read_silver("entity_relation")
    .filter(
        (F.col("relation_kind") == "homology")
        & F.col("relation_subkind").isin(*paralogue_types)
        & F.col("gene_a_key").isNotNull()
        & F.col("gene_b_key").isNotNull()
    )
    .select("gene_a_key", "gene_b_key")
    .distinct()
    .collect()
)
parent = {}


def find(gene):
    while parent.setdefault(gene, gene) != gene:
        parent[gene] = parent[parent[gene]]
        gene = parent[gene]
    return gene


for first, second in edges:
    low_root, high_root = sorted((find(first), find(second)))
    parent[high_root] = low_root
groups = spark.createDataFrame(
    [(gene, find(gene)) for gene in parent], "gene_key string, group_key string"
)
sizes = groups.groupBy("group_key").agg(F.count(F.lit(1)).alias("members"))
group_rows = groups.join(sizes, "group_key").join(gene_keys, "gene_key", "left_semi")
PARALOGUE_RULE = "gene_paralogue_group"
paralogue_rows = [
    rows(
        group_rows,
        "gene",
        "gene_key",
        PARALOGUE_RULE,
        "entity_relation.homology",
        attribute="paralogue_group_key",
        text=F.col("group_key"),
        method="rule",
    ),
    rows(
        group_rows,
        "gene",
        "gene_key",
        PARALOGUE_RULE,
        "entity_relation.homology",
        attribute="paralogue_group_size",
        number=F.col("members"),
        method="count",
    ),
]

# COMMAND ----------

# DBTITLE 1,Transform -- the attribute rows in the structure of entity_derived_attribute
derived = reduce(
    lambda left, right: left.unionByName(right),
    [*gene_rows, *variant_rows, *disease_drug_rows, *sample_rows, *paralogue_rows],
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
OWN_RULES = ", ".join(f"'{name}'" for name in sorted(WRITTEN_RULES))

# COMMAND ----------

# DBTITLE 1,Write, inspect and export -- entity_derived_attribute (the claim rules)
write_inspect_export(
    derived,
    "entity_derived_attribute",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    findings=FINDINGS,
    key_cols=["source_record_id"],
    replace_where=f"rule_name IN ({OWN_RULES})",
    section="entity_derived_attribute -- claim attributes",
    extra_checks={
        "rows_by_entity_type": lambda t: str(
            {
                r["entity_type"]: r["count"]
                for r in t.groupBy("entity_type").count().collect()
            }
        ),
        "attributes": lambda t: t.select("rule_name", "attribute").distinct().count(),
        "rules_written": lambda t: t.select("rule_name").distinct().count(),
        "therapeutic_area_ids_not_in_the_crosswalk": lambda t: (
            areas.join(area_ids, "area_id", "left_anti").select("area_id").distinct()
        ).count(),
    },
)

# COMMAND ----------

# DBTITLE 1,Build -- amino_acid_substitution
BLOSUM = "blosum62.scores"
blosum_text = text_for(BLOSUM)
blosum_number = number_for(BLOSUM)
blosum = {
    (r["from_amino_acid"], r["to_amino_acid"]): r["score"]
    for r in read_genomics_bronze("blosum62_scores")
    .select(
        blosum_text("from_amino_acid").alias("from_amino_acid"),
        blosum_text("to_amino_acid").alias("to_amino_acid"),
        blosum_number("score", "int").alias("score"),
    )
    .collect()
}
substitution = pd.DataFrame(
    substitution_rows(ATTRIBUTES["amino_acids"], ATTRIBUTES["grantham"], blosum)
)
unscored = int(substitution["blosum62_score"].isna().sum())
if unscored:
    raise RuntimeError(f"BLOSUM62 lacks {unscored} of the amino acid pairs")
substitution = (
    spark.createDataFrame(substitution)
    .withColumn("source_dataset", F.lit(BLOSUM))
    .withColumn("_srid", F.concat_ws(">", "from_amino_acid", "to_amino_acid"))
)
substitution = conform_genomics(
    add_genomics_provenance(substitution, "_srid", RID), "amino_acid_substitution"
)

# COMMAND ----------

# DBTITLE 1,Write, inspect and export -- amino_acid_substitution
write_inspect_export(
    substitution,
    "amino_acid_substitution",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    findings=FINDINGS,
    key_cols=["from_amino_acid", "to_amino_acid"],
)

# COMMAND ----------

# DBTITLE 1,Build -- variant_sequence_window
GENOME = "genome_sequence.chunk"
FLANK = ATTRIBUTES["sequence_window_flank"]
CHUNK = ATTRIBUTES["genome_chunk_bases"]
WINDOW_COLUMNS = [
    "variant_key",
    "contig",
    "position",
    "reference_base",
    "sequence_window",
    "reference_matches_genome",
    "guanine_cytosine_share",
    "is_cpg_site",
    "homopolymer_length",
    "is_transition",
]
WINDOW_SCHEMA = (
    "variant_key string, contig string, position long, reference_base string, "
    "sequence_window string, reference_matches_genome boolean, "
    "guanine_cytosine_share double, is_cpg_site boolean, homopolymer_length int, "
    "is_transition boolean"
)


def window_batch(variant_rows, chunk_rows):
    """The windows of the variants of one genome chunk."""
    if variant_rows.empty or chunk_rows.empty:
        return pd.DataFrame(columns=WINDOW_COLUMNS)
    start = int(chunk_rows["chunk_start"].iloc[0])
    sequence = chunk_rows["sequence"].iloc[0]
    found = []
    for key, contig, position, reference, alternate in variant_rows[
        ["variant_key", "contig", "position", "reference_allele", "alternate_allele"]
    ].itertuples(index=False):
        features = window_features(
            sequence, start, FLANK, int(position), reference, alternate
        )
        if features:
            found.append(
                {
                    "variant_key": key,
                    "contig": contig,
                    "position": int(position),
                    **features,
                }
            )
    return pd.DataFrame(found, columns=WINDOW_COLUMNS)


genome_number = number_for(GENOME)
chunks = read_genomics_bronze("genome_sequence_chunk").select(
    "contig", genome_number("chunk_start", "bigint").alias("chunk_start"), "sequence"
)
grch38_variants = (
    variants.filter(
        (F.col("assembly") == "GRCh38")
        & F.col("contig").isNotNull()
        & F.col("position").isNotNull()
    )
    .withColumn("contig", F.regexp_replace("contig", "^chr", ""))
    .withColumn(
        "chunk_start", F.floor((F.col("position") - 1) / CHUNK) * CHUNK + F.lit(1)
    )
    .select(
        "variant_key",
        "contig",
        "position",
        "reference_allele",
        "alternate_allele",
        F.col("chunk_start").cast("bigint").alias("chunk_start"),
    )
)
windows = (
    grch38_variants.groupBy("contig", "chunk_start")
    .cogroup(chunks.groupBy("contig", "chunk_start"))
    .applyInPandas(window_batch, schema=WINDOW_SCHEMA)
    .withColumn("source_dataset", F.lit(GENOME))
    .withColumn("_srid", F.col("variant_key"))
)
windows = conform_genomics(
    add_genomics_provenance(windows, "_srid", RID), "variant_sequence_window"
)

# COMMAND ----------

# DBTITLE 1,Write, inspect and export -- variant_sequence_window
write_inspect_export(
    windows,
    "variant_sequence_window",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    findings=FINDINGS,
    key_cols=["variant_key"],
    extra_checks={
        "variants_without_a_window": lambda t: grch38_variants.count() - t.count(),
        "reference_does_not_match_the_genome": lambda t: t.filter(
            ~F.col("reference_matches_genome")
        ).count(),
        "windows_with_unknown_bases": lambda t: t.filter(
            F.col("sequence_window").contains("N")
        ).count(),
    },
)

# COMMAND ----------

# DBTITLE 1,Check and export -- rejected values of the lenient reads
export_rejected_values(FINDINGS, COMPONENT, read_genomics_bronze)
