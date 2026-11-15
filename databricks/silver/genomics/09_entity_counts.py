# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # SILVER -- ENTITY COUNTS (GENOMICS)
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** November 2026
# MAGIC
# MAGIC **Purpose:** the counts of the rows that link to a gene, disease, drug, protein,
# MAGIC transcript, variant, cell line, epitope, clinical trial, tumour sample or
# MAGIC genetic test, as rows of `entity_derived_attribute`. 

# COMMAND ----------

# DBTITLE 1,Genomics library
# MAGIC %run ../_genomics_common

# COMMAND ----------

# DBTITLE 1,Configuration
from functools import reduce

from pyspark.sql import functions as F

SOURCE = "genomics"
COMPONENT = "silver/genomics/09_entity_counts"
RID = run_id()
FINDINGS = "genomics"
RULES = load_genomics_rules()
COUNTS = RULES["entity_counts"]
VERSION = genomics_rule_version(RULES)
AS_OF = F.to_date(F.lit(RID.split("-")[1][:8]), "yyyyMMdd")
WRITTEN_RULES = set()


def distinct_count(df, entity_type, key, value, attribute, rule_name, source):
    """Rows of the number of distinct `value` per `key`; `attribute` is a text,
    or a Column of `df` that splits the count (one attribute per value)."""
    split = not isinstance(attribute, str)
    groups = [key, attribute.alias("_attribute")] if split else [key]
    counts = (
        df.filter(F.col(key).isNotNull() & F.col(value).isNotNull())
        .groupBy(*groups)
        .agg(F.countDistinct(value).alias("_count"))
    )
    if split:
        counts = counts.filter(F.col("_attribute").isNotNull())
    return derived_rows(
        counts,
        entity_type=entity_type,
        key=key,
        attribute=F.col("_attribute") if split else attribute,
        value_number=F.col("_count"),
        rule_name=rule_name,
        method="count",
        text_source=source,
        version=VERSION,
        as_of=AS_OF,
    )


def rule_counts(entity_type, rule_name, source, specs):
    """The counts of one rule: `specs` lists (frame, key, value, attribute)."""
    WRITTEN_RULES.add(rule_name)
    return [
        distinct_count(frame, entity_type, key, value, attribute, rule_name, source)
        for frame, key, value, attribute in specs
    ]


def name_part(column, values):
    """The lower-case token of a vocabulary value; null outside the vocabulary."""
    token = F.regexp_replace(F.lower(F.col(column)), r"[^a-z0-9]+", "_")
    return F.when(F.col(column).isin(*values), token)


def both_directions(df, left, right, key, partner):
    """(key, partner) pairs of an undirected pair table, each way once."""
    pairs = df.select(F.col(left).alias(key), F.col(right).alias(partner))
    flipped = df.select(F.col(right).alias(key), F.col(left).alias(partner))
    return pairs.unionByName(flipped).dropna().distinct()


# COMMAND ----------

# DBTITLE 1,Read Silver -- relations, keys and linked tables
relations = read_silver("entity_relation")
master = read_silver("entity_master")


def keys_of(entity_type, name):
    return (
        master.filter(F.col("entity_type") == entity_type)
        .select(F.col("entity_key").alias(name))
        .distinct()
    )


def relation(kind, *subkinds):
    rows = relations.filter(F.col("relation_kind") == kind)
    return rows.filter(F.col("relation_subkind").isin(*subkinds)) if subkinds else rows


protein_genes = (
    read_silver("protein_gene_link")
    .select("protein_key", "gene_key")
    .dropna()
    .distinct()
)
trials = read_silver("clinical_trial").select("nct_id", "phase", "overall_status")
perturbation = read_silver("perturbation_expression")
somatic = read_silver("somatic_mutation")
tests = read_silver("genetic_test")
mhc_assays = read_silver("epitope_mhc_assay")
drug_keys = keys_of("drug", "drug_key")
trial_phase = F.concat(
    F.lit("clinical_trials_in_"), name_part("phase", COUNTS["trial_phases"])
)
trial_status = F.concat(
    F.lit("clinical_trials_"), name_part("overall_status", COUNTS["trial_statuses"])
)
TRIALS = "entity_relation.clinical_trial"

# COMMAND ----------

# DBTITLE 1,Transform -- gene counts (pathways, phenotypes, partners, regulation)
members = relation("pathway_member").select("member_type", "member_key", "pathway_key")
gene_pathways = (
    members.filter(F.col("member_type") == "gene")
    .select(F.col("member_key").alias("gene_key"), "pathway_key")
    .unionByName(
        members.filter(F.col("member_type") == "protein")
        .join(protein_genes, F.col("member_key") == F.col("protein_key"))
        .select("gene_key", "pathway_key")
    )
)
phenotypes = relation("phenotype")
biogrid = both_directions(
    relation("protein_interaction", "biogrid"),
    "gene_a_key",
    "gene_b_key",
    "gene_key",
    "partner",
)
string_proteins = both_directions(
    relation("protein_interaction", "string"),
    "protein_a_key",
    "protein_b_key",
    "protein_key",
    "partner",
)
partner_genes = protein_genes.select(
    F.col("protein_key").alias("partner"), F.col("gene_key").alias("partner_gene")
)
string_genes = string_proteins.join(protein_genes, "protein_key").join(
    partner_genes, "partner"
)
regulation = relation("regulatory_interaction")
elements = read_silver("regulatory_element_gene_link")
gene_trials = (
    relation("clinical_trial", "genetic")
    .select("subject_id", F.col("object_key").alias("gene_key"))
    .join(keys_of("gene", "gene_key"), "gene_key")
)
gene_counts = [
    *rule_counts(
        "gene",
        "pathway_membership",
        "entity_relation.pathway_member",
        [(gene_pathways, "gene_key", "pathway_key", "pathway_count")],
    ),
    *rule_counts(
        "gene",
        "phenotype_annotation",
        "entity_relation.phenotype",
        [
            (
                phenotypes.filter(F.col("relation_subkind") == "gene"),
                "subject_key",
                "phenotype_key",
                "phenotype_count",
            )
        ],
    ),
    *rule_counts(
        "gene",
        "interaction_partners",
        "entity_relation.protein_interaction",
        [
            (biogrid, "gene_key", "partner", "biogrid_partner_count"),
            (string_genes, "gene_key", "partner_gene", "string_partner_count"),
        ],
    ),
    *rule_counts(
        "gene",
        "regulatory_network",
        "entity_relation.regulatory_interaction",
        [
            (regulation, "gene_a_key", "gene_b_key", "regulated_gene_count"),
            (regulation, "gene_b_key", "gene_a_key", "regulator_count"),
        ],
    ),
    *rule_counts(
        "gene",
        "regulatory_element_links",
        "regulatory_element_gene_link",
        [
            (elements, "gene_key", "element_key", "regulatory_element_count"),
            (elements, "gene_key", "biosample", "regulatory_element_biosample_count"),
        ],
    ),
    *rule_counts(
        "gene",
        "perturbation_response",
        "perturbation_expression",
        [(perturbation, "gene_key", "drug_key", "perturbation_drug_count")],
    ),
    *rule_counts(
        "gene",
        "clinical_trial_links",
        TRIALS,
        [(gene_trials, "gene_key", "subject_id", "clinical_trial_count")],
    ),
]

# COMMAND ----------

# DBTITLE 1,Transform -- disease counts (phenotypes, trials, ontology, tests)
disease_keys = keys_of("disease", "disease_key")
disease_trials = (
    relation("clinical_trial", "condition", "mesh_condition_list")
    .select("subject_id", F.col("object_key").alias("disease_key"))
    .join(trials, F.col("subject_id") == F.col("nct_id"), "left")
)
ancestor_trials = relation("clinical_trial", "mesh_condition_ancestor").select(
    "subject_id", F.col("object_key").alias("disease_key")
)
ontology = read_silver("ontology_relation")
child_side = ontology.join(disease_keys, F.col("child_key") == F.col("disease_key"))
parent_side = ontology.join(disease_keys, F.col("parent_key") == F.col("disease_key"))
disease_counts = [
    *rule_counts(
        "disease",
        "phenotype_annotation",
        "entity_relation.phenotype",
        [
            (
                phenotypes.filter(F.col("relation_subkind") == "disease"),
                "subject_key",
                "phenotype_key",
                "phenotype_count",
            )
        ],
    ),
    *rule_counts(
        "disease",
        "clinical_trial_links",
        TRIALS,
        [
            (disease_trials, "disease_key", "subject_id", "clinical_trial_count"),
            (
                ancestor_trials,
                "disease_key",
                "subject_id",
                "clinical_trial_count_via_ancestor_terms",
            ),
            (disease_trials, "disease_key", "subject_id", trial_phase),
            (disease_trials, "disease_key", "subject_id", trial_status),
        ],
    ),
    *rule_counts(
        "disease",
        "ontology_position",
        "ontology_relation",
        [
            (child_side, "disease_key", "parent_key", "parent_count"),
            (parent_side, "disease_key", "child_key", "child_count"),
        ],
    ),
    *rule_counts(
        "disease",
        "genetic_test_links",
        "genetic_test",
        [(tests, "disease_key", "test_key", "genetic_test_count")],
    ),
]

# COMMAND ----------

# DBTITLE 1,Transform -- drug counts (FAERS, DDInter, trials, perturbation, PharmGKB)
case_drugs = read_silver("faers_case_drug")
# the case id of the drug side is kept; faers_case_item has its own copy
case_items = read_silver("faers_case_item").drop("case_id")
drug_cases = case_drugs.select("primary_id", "case_id", "drug_key").distinct()
reactions = case_items.filter(F.col("item_kind") == "reaction").join(
    drug_cases, "primary_id"
)
outcomes = case_items.filter(F.col("item_kind") == "outcome").join(
    drug_cases, "primary_id"
)
interactions = relation("drug_interaction")
levels = (
    interactions.select(
        F.col("subject_key").alias("drug_key"),
        F.col("object_key").alias("partner"),
        "evidence_level",
    )
    .unionByName(
        interactions.select(
            F.col("object_key").alias("drug_key"),
            F.col("subject_key").alias("partner"),
            "evidence_level",
        )
    )
    .dropna(subset=["drug_key", "partner"])
)
drug_trials = (
    relation("clinical_trial", "drug", "mesh_intervention_list")
    .select("subject_id", F.col("object_key").alias("drug_key"))
    .join(drug_keys, "drug_key")
    .join(trials, F.col("subject_id") == F.col("nct_id"), "left")
)
drug_ancestor_trials = (
    relation("clinical_trial", "mesh_intervention_ancestor")
    .select("subject_id", F.col("object_key").alias("drug_key"))
    .join(drug_keys, "drug_key")
)
pharmacogenomic = relation("pharmacogenomic")
chemical_rows = (
    pharmacogenomic.filter(F.col("subject_type") == "Chemical")
    .select(F.col("subject_key").alias("drug_key"), "source_record_id")
    .unionByName(
        pharmacogenomic.filter(F.col("object_type") == "Chemical").select(
            F.col("object_key").alias("drug_key"), "source_record_id"
        )
    )
)
role = F.concat(
    F.lit("faers_cases_with_role_"), name_part("role_code", COUNTS["faers_roles"])
)
outcome = F.concat(
    F.lit("faers_cases_with_outcome_"),
    name_part("item_value", COUNTS["faers_outcomes"]),
)
level = F.concat(
    F.lit("interacting_drug_count_"),
    name_part("evidence_level", COUNTS["interaction_levels"]),
)
drug_counts = [
    *rule_counts(
        "drug",
        "adverse_event_reports",
        "faers_case_drug and faers_case_item",
        [
            (case_drugs, "drug_key", "case_id", "faers_case_count"),
            (case_drugs, "drug_key", "case_id", role),
            (reactions, "drug_key", "item_value", "faers_reaction_count"),
            (outcomes, "drug_key", "case_id", "faers_cases_with_an_outcome"),
            (outcomes, "drug_key", "case_id", outcome),
        ],
    ),
    *rule_counts(
        "drug",
        "drug_interactions",
        "entity_relation.drug_interaction",
        [
            (levels, "drug_key", "partner", "interacting_drug_count"),
            (levels, "drug_key", "partner", level),
        ],
    ),
    *rule_counts(
        "drug",
        "clinical_trial_links",
        TRIALS,
        [
            (drug_trials, "drug_key", "subject_id", "clinical_trial_count"),
            (
                drug_ancestor_trials,
                "drug_key",
                "subject_id",
                "clinical_trial_count_via_ancestor_terms",
            ),
            (drug_trials, "drug_key", "subject_id", trial_phase),
        ],
    ),
    *rule_counts(
        "drug",
        "perturbation_response",
        "perturbation_expression and perturbation_sample",
        [
            (perturbation, "drug_key", "gene_key", "perturbation_gene_count"),
            (
                read_silver("perturbation_sample"),
                "drug_key",
                "sample_id",
                "perturbation_sample_count",
            ),
        ],
    ),
    *rule_counts(
        "drug",
        "pharmacogenomic_relationships",
        "entity_relation.pharmacogenomic",
        [
            (
                chemical_rows,
                "drug_key",
                "source_record_id",
                "pharmgkb_relationship_count",
            )
        ],
    ),
]

# COMMAND ----------

# DBTITLE 1,Transform -- protein, transcript and variant counts
hgvs = read_silver("variant_hgvs")
conditions = current_snapshot(read_silver("variant_condition"), RULES)
edit_type = F.concat(F.lit("variants_with_"), F.col("protein_edit_type"))
other_counts = [
    *rule_counts(
        "protein",
        "interaction_partners",
        "entity_relation.protein_interaction",
        [(string_proteins, "protein_key", "partner", "string_partner_count")],
    ),
    *rule_counts(
        "protein",
        "pathway_membership",
        "entity_relation.pathway_member",
        [
            (
                members.filter(F.col("member_type") == "protein"),
                "member_key",
                "pathway_key",
                "pathway_count",
            )
        ],
    ),
    *rule_counts(
        "protein",
        "epitope_links",
        "epitope and epitope_mhc_assay",
        [
            (read_silver("epitope"), "protein_key", "epitope_id", "epitope_count"),
            (mhc_assays, "protein_key", "assay_id", "epitope_assay_count"),
        ],
    ),
    *rule_counts(
        "protein",
        "assay_links",
        "dms_assay",
        [(read_silver("dms_assay"), "protein_key", "assay_id", "dms_assay_count")],
    ),
    *rule_counts(
        "transcript",
        "transcript_variants",
        "variant_hgvs and somatic_mutation",
        [
            (hgvs, "transcript_key", "variant_key", "variant_count"),
            (hgvs, "transcript_key", "variant_key", edit_type),
            (somatic, "transcript_key", "source_record_id", "somatic_mutation_count"),
        ],
    ),
    *rule_counts(
        "variant",
        "variant_links",
        "variant_condition, entity_relation and somatic_mutation",
        [
            (conditions, "variant_key", "condition_name", "condition_count"),
            (conditions, "variant_key", "disease_key", "linked_disease_count"),
            (
                relation("variation_member"),
                "member_key",
                "source_record_id",
                "variation_membership_count",
            ),
            (somatic, "variant_key", "sample_barcode", "tumor_sample_count"),
        ],
    ),
]

# COMMAND ----------

# DBTITLE 1,Transform -- cell line, epitope, clinical trial, tumour sample and test counts
essential = RULES["gene_essentiality"]
mutation = RULES["cancer_mutation"]
dependency = read_silver("gene_dependency")
dependent = dependency.filter(
    F.col("gene_effect") <= essential["dependent_at_or_below"]
)
strongly = dependency.filter(
    F.col("gene_effect") <= essential["strongly_essential_at_or_below"]
)
positive_assays = mhc_assays.filter(
    F.col("assay_qualitative_measurement").startswith(COUNTS["positive_assay_prefix"])
)
trial_rows = relation("clinical_trial")
record_class = F.col("variant_classification")
record_counts = [
    *rule_counts(
        "cell_line",
        "cell_line_dependency",
        "gene_dependency",
        [
            (dependency, "cell_line_key", "gene_key", "tested_gene_count"),
            (dependent, "cell_line_key", "gene_key", "dependent_gene_count"),
            (strongly, "cell_line_key", "gene_key", "strongly_dependent_gene_count"),
        ],
    ),
    *rule_counts(
        "cell_line",
        "perturbation_response",
        "perturbation_expression",
        [(perturbation, "cell_line_key", "drug_key", "perturbation_drug_count")],
    ),
    *rule_counts(
        "epitope",
        "epitope_assays",
        "epitope_mhc_assay",
        [
            (mhc_assays, "epitope_id", "assay_id", "assay_count"),
            (positive_assays, "epitope_id", "assay_id", "positive_assay_count"),
            (mhc_assays, "epitope_id", "mhc_restriction_name", "mhc_allele_count"),
            (mhc_assays, "epitope_id", "host_name", "host_count"),
        ],
    ),
    *rule_counts(
        "clinical_trial",
        "clinical_trial_content",
        TRIALS,
        [
            (
                trial_rows.filter(F.col("relation_subkind") == "condition"),
                "subject_id",
                "source_record_id",
                "condition_count",
            ),
            (
                trial_rows.filter(F.col("object_type") == "intervention"),
                "subject_id",
                "source_record_id",
                "intervention_count",
            ),
            (
                trial_rows.filter(F.col("relation_subkind") == "keyword"),
                "subject_id",
                "source_record_id",
                "keyword_count",
            ),
            (
                trial_rows.filter(F.col("object_type") == "disease"),
                "subject_id",
                "object_key",
                "linked_disease_count",
            ),
            (
                trial_rows.join(
                    drug_keys.withColumnRenamed("drug_key", "_known_drug_key"),
                    F.col("object_key") == F.col("_known_drug_key"),
                ),
                "subject_id",
                "object_key",
                "linked_drug_count",
            ),
        ],
    ),
    *rule_counts(
        "tumor_sample",
        "tumor_sample_mutations",
        "somatic_mutation",
        [
            (somatic, "sample_barcode", "source_record_id", "mutation_count"),
            *[
                (
                    somatic.filter(record_class.isin(mutation[group])),
                    "sample_barcode",
                    "source_record_id",
                    f"{group}_mutation_count",
                )
                for group in ("missense", "truncating", "silent")
            ],
        ],
    ),
    *rule_counts(
        "genetic_test",
        "genetic_test_links",
        "genetic_test",
        [
            (tests, "test_key", "gene_key", "gene_count"),
            (tests, "test_key", "disease_key", "disease_count"),
        ],
    ),
]

# COMMAND ----------

# DBTITLE 1,Transform -- the count rows in the structure of entity_derived_attribute
derived = reduce(
    lambda left, right: left.unionByName(right),
    [*gene_counts, *disease_counts, *drug_counts, *other_counts, *record_counts],
)
derived = derived.withColumn(
    "_srid",
    sha_key("entity_type", "entity_key", "attribute", "rule_name", "value_number"),
)
derived = conform_genomics(
    add_genomics_provenance(derived, "_srid", RID), "entity_derived_attribute"
)
OWN_RULES = ", ".join(f"'{name}'" for name in sorted(WRITTEN_RULES))

# COMMAND ----------

# DBTITLE 1,Write, inspect and export -- entity_derived_attribute (the count rules)
write_inspect_export(
    derived,
    "entity_derived_attribute",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    findings=FINDINGS,
    key_cols=["source_record_id"],
    replace_where=f"rule_name IN ({OWN_RULES})",
    section="entity_derived_attribute -- entity counts",
    extra_checks={
        "rows_by_entity_type": lambda t: str(
            {
                r["entity_type"]: r["count"]
                for r in t.groupBy("entity_type").count().collect()
            }
        ),
        "attributes": lambda t: t.select("rule_name", "attribute").distinct().count(),
        "rules_written": lambda t: t.select("rule_name").distinct().count(),
    },
)