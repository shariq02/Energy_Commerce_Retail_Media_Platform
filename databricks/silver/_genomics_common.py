# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # GENOMICS SILVER SHARED LIBRARY
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** November 2026
# MAGIC
# MAGIC **Purpose:** the declared genomics structures, the rules loader, the pure
# MAGIC parsers and the claim helpers. Pulled in with `%run` after `_silver_common`;
# MAGIC the constants and pure functions run without Spark.

# COMMAND ----------

# DBTITLE 1,Imports
import re as _re
from functools import reduce as _reduce

# COMMAND ----------

# DBTITLE 1,Configuration constants
GENOMICS_ECOSYSTEM = "genomics"
GENOMICS_BRONZE_SCHEMA = "genomics_bronze"
GENOMICS_SCHEMA = "genomics_silver"
GENOMICS_REFERENCE_SCHEMA = "genomics_silver_reference"
GENOMICS_RULES_PATH = "src/schemas/mappings/genomics_rules.yml"

# Entities, shared plumbing and reference tables.
GENOMICS_REFERENCE_TABLES = {
    "entity_id_crosswalk",
    "entity_claim",
    "entity_name",
    "entity_derived_attribute",
    "ontology_relation",
    "entity_coordinate",
    "gene",
    "transcript",
    "protein",
    "protein_gene_link",
    "protein_feature",
    "disease",
    "drug",
    "cell_line",
    "genetic_test",
}

# Variants, relations and facts.
GENOMICS_MAIN_TABLES = {
    "variant",
    "variant_hgvs",
    "variant_condition",
    "variant_classification",
    "variant_score",
    "position_score",
    "variant_population_frequency",
    "variant_feature_overlap",
    "benchmark_variant",
    "embedding_split",
    "gene_disease_association",
    "drug_gene_relation",
    "pharmacogenomic_association",
    "phenotype_annotation",
    "pathway_membership",
    "protein_interaction",
    "gene_expression_median",
    "tumor_sample",
    "somatic_mutation",
    "dms_assay",
    "dms_assay_mutant",
    "perturbation_sample",
    "perturbation_expression",
    "faers_case",
    "faers_case_drug",
}

# One row per ...
GENOMICS_TABLE_GRAIN = {
    "entity_id_crosswalk": "entity, id type, id value, source column",
    "entity_claim": "entity, field, source column, value",
    "entity_name": "entity, term, term type",
    "entity_derived_attribute": "entity, attribute, rule",
    "ontology_relation": "ontology, parent, child, relation",
    "entity_coordinate": "entity, source, assembly, contig, interval",
    "gene": "gene",
    "transcript": "transcript",
    "protein": "protein",
    "protein_gene_link": "protein, gene, source",
    "protein_feature": "protein, feature record",
    "disease": "disease",
    "drug": "drug",
    "cell_line": "cell line",
    "genetic_test": "test",
    "variant": "variant",
    "variant_hgvs": "variant, transcript, snapshot",
    "variant_condition": "variant, condition position, snapshot",
    "variant_classification": "variant, classification type, term, source, snapshot",
    "variant_score": "variant, score name, source",
    "position_score": "assembly, contig, position, score name",
    "variant_population_frequency": "variant, population, source",
    "variant_feature_overlap": "variant, feature kind, feature",
    "benchmark_variant": "benchmark set, record",
    "embedding_split": "split file",
    "gene_disease_association": "gene, disease, source, record kind",
    "drug_gene_relation": "drug, gene, source, relation",
    "pharmacogenomic_association": "subject, object, source",
    "phenotype_annotation": "subject, phenotype, source record",
    "pathway_membership": "member, pathway, source",
    "protein_interaction": "source interaction record",
    "gene_expression_median": "gene, tissue, source",
    "tumor_sample": "sample barcode",
    "somatic_mutation": "sample, mutation record",
    "dms_assay": "assay",
    "dms_assay_mutant": "assay, mutant",
    "perturbation_sample": "sample",
    "perturbation_expression": "gene, drug, cell line, concentration, plate",
    "faers_case": "case version",
    "faers_case_drug": "case version, drug sequence",
}

# Domain columns (name, Spark type) per table; the generator adds the governance
# columns. A table without an entry has none declared yet.
GENOMICS_STRUCTURES = {
    "entity_id_crosswalk": [
        ("entity_type", "string"),
        ("entity_key", "string"),
        ("id_type", "string"),
        ("id_value", "string"),
        ("source_dataset", "string"),
        ("source_field", "string"),
        ("match_method", "string"),
        ("entities_sharing_id", "int"),
    ],
    "entity_claim": [
        ("entity_type", "string"),
        ("entity_key", "string"),
        ("field", "string"),
        ("value_text", "string"),
        ("value_number", "double"),
        ("value_date", "date"),
        ("value_type", "string"),
        ("source_dataset", "string"),
        ("source_field", "string"),
        ("source_rank", "int"),
        ("source_type", "string"),
    ],
    "entity_name": [
        ("entity_type", "string"),
        ("entity_key", "string"),
        ("term", "string"),
        ("term_type", "string"),
        ("source_dataset", "string"),
        ("source_field", "string"),
        ("entities_sharing_term", "int"),
    ],
    "entity_derived_attribute": [
        ("entity_type", "string"),
        ("entity_key", "string"),
        ("attribute", "string"),
        ("value_text", "string"),
        ("value_number", "double"),
        ("value_boolean", "boolean"),
        ("value_type", "string"),
        ("method", "string"),
        ("matched_text", "string"),
        ("text_source", "string"),
        ("rule_name", "string"),
        ("rule_version", "string"),
        ("as_of_date", "date"),
    ],
    "ontology_relation": [
        ("ontology", "string"),
        ("parent_key", "string"),
        ("child_key", "string"),
        ("relation", "string"),
        ("source_dataset", "string"),
        ("source_field", "string"),
    ],
    "entity_coordinate": [
        ("entity_type", "string"),
        ("entity_key", "string"),
        ("feature_level", "string"),
        ("source_dataset", "string"),
        ("assembly", "string"),
        ("contig", "string"),
        ("start_position", "bigint"),
        ("end_position", "bigint"),
        ("interval_length", "bigint"),
        ("strand", "string"),
        ("coordinate_system", "string"),
        ("coordinate_conversion", "string"),
        ("is_primary", "boolean"),
    ],
}

# Resolved fields of the gene master; each has `<field>_source` and `_method`.
GENE_FIELDS = (
    "symbol",
    "full_name",
    "gene_type",
    "ensembl_id",
    "hgnc_id",
    "description",
    "chromosome",
    "map_location",
)
GENOMICS_STRUCTURES["gene"] = [
    ("gene_key", "string"),
    ("key_source", "string"),
    *[
        column
        for field in GENE_FIELDS
        for column in (
            (field, "string"),
            (f"{field}_source", "string"),
            (f"{field}_method", "string"),
        )
    ],
    ("cytoband_arm", "string"),
    ("cytoband_band", "string"),
    ("cytoband_sub_band", "string"),
    ("grch38_contig", "string"),
    ("grch38_start_position", "bigint"),
    ("grch38_end_position", "bigint"),
    ("grch38_strand", "string"),
    ("grch38_source", "string"),
]

TRANSCRIPT_FIELDS = (
    "gene_key",
    "protein_key",
    "transcript_type",
    "transcript_name",
    "version",
    "level",
)
PROTEIN_FIELDS = (
    "name",
    "length",
    "description",
    "gene_symbol",
    "entry_name",
    "target_type",
    "tax_id",
)
DISEASE_FIELDS = ("name", "description", "ontology_prefix", "therapeutic_areas")
DRUG_FIELDS = (
    "name",
    "type",
    "smiles",
    "inchi",
    "inchi_key",
    "max_clinical_stage",
    "description",
    "parent_chembl_id",
)


def _resolved(fields):
    """Columns of resolved fields: the value, its source and its method."""
    return [
        column
        for field in fields
        for column in (
            (field, "string"),
            (f"{field}_source", "string"),
            (f"{field}_method", "string"),
        )
    ]


_GRCH38 = [
    ("grch38_contig", "string"),
    ("grch38_start_position", "bigint"),
    ("grch38_end_position", "bigint"),
    ("grch38_strand", "string"),
    ("grch38_source", "string"),
]
GENOMICS_STRUCTURES.update(
    {
        "transcript": [
            ("transcript_key", "string"),
            ("key_source", "string"),
            *_resolved(TRANSCRIPT_FIELDS),
            ("tags", "array<string>"),
            *_GRCH38,
        ],
        "protein": [
            ("protein_key", "string"),
            ("key_source", "string"),
            *_resolved(PROTEIN_FIELDS),
        ],
        "protein_gene_link": [
            ("protein_key", "string"),
            ("gene_key", "string"),
            ("link_method", "string"),
            ("genes_sharing_symbol", "int"),
            ("source_dataset", "string"),
            ("source_field", "string"),
        ],
        "protein_feature": [
            ("protein_key", "string"),
            ("feature_type", "string"),
            ("location_raw", "string"),
            ("start_position", "int"),
            ("end_position", "int"),
            ("note", "string"),
            ("evidence_codes", "array<string>"),
            ("pubmed_ids", "array<string>"),
            ("dbsnp_ids", "array<string>"),
            ("qualifiers", "string"),
        ],
        "disease": [
            ("disease_key", "string"),
            ("key_source", "string"),
            *_resolved(DISEASE_FIELDS),
        ],
        "drug": [
            ("drug_key", "string"),
            ("key_source", "string"),
            *_resolved(DRUG_FIELDS),
        ],
        "cell_line": [
            ("cell_line_key", "string"),
            ("cell_name", "string"),
            ("cellosaur_id", "string"),
            ("organ", "string"),
            (
                "drivers",
                (
                    "array<struct<gene_symbol:string,variant_zygosity:string,"
                    "variant_type:string,effect:string,mechanism:string,gene_type:string>>"
                ),
            ),
        ],
        "genetic_test": [
            ("test_key", "string"),
            ("latest_version", "string"),
            ("versions", "int"),
            ("test_type", "string"),
            ("genes", "array<struct<gene_key:string,gene_symbol:string>>"),
            (
                "conditions",
                (
                    "array<struct<disease_key:string,condition_name:string,"
                    "mim_number:string,snomed_ct_id:string>>"
                ),
            ),
        ],
        "ontology_relation": [
            ("ontology", "string"),
            ("parent_key", "string"),
            ("child_key", "string"),
            ("relation", "string"),
            ("direction", "string"),
            ("source_dataset", "string"),
            ("source_field", "string"),
        ],
    }
)

VARIANT_FIELDS = ("variant_type", "rsid", "gene_key", "origin", "name")


def _text(*names):
    return [(name, "string") for name in names]


def _number(kind, *names):
    return [(name, kind) for name in names]


GENOMICS_STRUCTURES.update(
    {
        "variant": [
            ("variant_key", "string"),
            ("key_source", "string"),
            *_text("assembly", "contig"),
            ("position", "bigint"),
            *_text("reference_allele", "alternate_allele"),
            ("end_position", "bigint"),
            ("source_datasets", "array<string>"),
            *_resolved(VARIANT_FIELDS),
        ],
        "variant_hgvs": [
            *_text("variant_key", "transcript_accession"),
            ("transcript_version", "int"),
            *_text("transcript_key", "gene_symbol", "coding_notation", "coding_region"),
            *_number(
                "int",
                "coding_start",
                "coding_start_offset",
                "coding_end",
                "coding_end_offset",
            ),
            *_text(
                "coding_edit_type",
                "coding_reference_base",
                "coding_alternate_base",
                "coding_inserted_bases",
            ),
            *_text("protein_notation", "protein_reference_amino_acid"),
            ("protein_position", "int"),
            ("protein_end_reference_amino_acid", "string"),
            ("protein_end_position", "int"),
            *_text(
                "protein_edit_type",
                "protein_alternate_amino_acid",
                "protein_inserted_amino_acids",
                "parse_status",
            ),
            *_text("source_dataset", "source_field", "source_snapshot"),
        ],
        "variant_condition": [
            ("variant_key", "string"),
            ("condition_position", "int"),
            *_text("condition_name", "clinvar_condition_accession"),
            ("condition_ids", "array<string>"),
            *_text("disease_key", "disease_link_method"),
            ("is_generic_label", "boolean"),
            *_text("pairing_status", "source_dataset", "source_snapshot"),
        ],
        "variant_classification": [
            *_text("variant_key", "classification_type", "term", "review_status"),
            ("review_stars", "int"),
            ("last_evaluated", "date"),
            ("number_submitters", "int"),
            ("clinical_significance_simple", "string"),
            ("clinvar_submission_accessions", "array<string>"),
            ("source_dataset", "string"),
            ("source_snapshot", "string"),
        ],
        "variant_score": [
            *_text("variant_key", "score_name"),
            ("score_value", "double"),
            *_text("score_label", "context", "source_dataset", "source_field"),
        ],
        "position_score": [
            *_text("assembly", "contig"),
            ("position", "bigint"),
            ("score_name", "string"),
            ("score_value", "double"),
            ("source_dataset", "string"),
        ],
        "variant_population_frequency": [
            *_text("variant_key", "population", "measure"),
            ("allele_frequency", "double"),
            *_number("bigint", "allele_count", "allele_number", "homozygote_count"),
            *_text("filter_status", "source_dataset"),
        ],
        "variant_feature_overlap": [
            *_text(
                "variant_key",
                "feature_kind",
                "feature_key",
                "feature_type",
                "overlap_method",
                "source_dataset",
            ),
        ],
        "benchmark_variant": [
            *_text(
                "benchmark_set",
                "genomic_mutation_id",
                "variant_key",
                "assembly_basis",
                "contig",
            ),
            ("position", "bigint"),
            *_text("reference_allele", "alternate_allele", "label"),
            ("label_integer", "int"),
            *_text(
                "split",
                "source_label",
                "consequence",
                "review_status",
                "record_id",
                "gene",
                "omim",
                "pubmed_id",
            ),
            *_number("bigint", "sample_count", "total_sample_count"),
            *_number(
                "double",
                "frequency",
                "allele_count",
                "allele_number",
                "allele_frequency",
                "minor_allele_frequency",
                "minor_allele_count",
            ),
            ("tissue", "string"),
            *_number(
                "double",
                "distance_to_nearest_transcription_start_site",
                "gpn_star_v100",
                "gpn_star_p243",
                "gpn_star_m447",
            ),
            *_text("protein_id", "mutant"),
            ("protein_position", "int"),
            *_text("wild_type_aa", "mutant_aa"),
            ("sequence_length", "int"),
            ("stars", "int"),
            *_text("cluster", "shuffle_rank", "split_file", "source_dataset"),
        ],
        "embedding_split": [
            ("split_file", "string"),
            ("matrix_rows", "int"),
            ("matrix_columns", "int"),
            *_text("index_values", "mean_embedding"),
            ("variant_rows", "bigint"),
            *_number("int", "distinct_index_values", "distinct_mean_embedding_values"),
        ],
        "gene_disease_association": [
            *_text("gene_key", "disease_key", "record_kind", "source_dataset"),
            *_text("source_gene_id", "source_disease_id"),
            *_text("gene_link_method", "disease_link_method"),
            ("score", "double"),
            *_number("bigint", "evidence_count", "supporting_count"),
            *_text(
                "relation_type",
                "classification",
                "mode_of_inheritance",
                "evaluation_date",
                "study_id",
                "aggregation_value",
                "report_url",
                "comment",
            ),
        ],
        "drug_gene_relation": [
            *_text("drug_key", "gene_key", "relation_type", "relation_detail"),
            *_text(
                "evidence_level", "source_dataset", "source_drug_id", "source_gene_id"
            ),
            *_text("drug_link_method", "gene_link_method"),
            ("pmids", "array<string>"),
            *_text("labeling_sections", "therapeutic_area"),
        ],
        "pharmacogenomic_association": [
            *_text("subject_type", "subject_id", "subject_name", "subject_key"),
            *_text("object_type", "object_id", "object_name", "object_key"),
            *_text("evidence", "association", "pharmacokinetic", "pharmacodynamic"),
            ("pmids", "array<string>"),
            ("source_dataset", "string"),
        ],
        "phenotype_annotation": [
            *_text("subject_type", "subject_key", "subject_source_id"),
            *_text("subject_link_method", "subject_name", "phenotype_key"),
            ("is_negated", "boolean"),
            *_text(
                "frequency",
                "onset",
                "sex",
                "modifier",
                "aspect",
                "evidence",
                "reference",
                "biocuration",
                "source_dataset",
            ),
        ],
        "pathway_membership": [
            *_text("member_type", "member_key", "member_id_type", "member_id"),
            *_text("member_link_method", "pathway_key", "evidence", "species"),
            ("source_dataset", "string"),
        ],
        "protein_interaction": [
            *_text("interaction_source", "interaction_id"),
            *_text("interactor_a_id", "interactor_b_id"),
            *_text("protein_a_key", "protein_b_key", "gene_a_key", "gene_b_key"),
            *_text("experimental_system", "experimental_system_type", "throughput"),
            ("score", "double"),
            ("channel_scores", "array<int>"),
            ("combined_score", "int"),
            *_text(
                "author",
                "publication_source",
                "modification",
                "qualifications",
                "tags",
                "source_database",
                "organism_a",
                "organism_b",
                "ontology_term_ids",
                "ontology_term_names",
                "ontology_term_categories",
                "source_dataset",
            ),
        ],
        "gene_expression_median": [
            *_text("gene_key", "gene_ensembl_id", "gene_symbol", "tissue_key"),
            ("median_transcripts_per_million", "double"),
            *_text("gene_link_method", "source_dataset"),
        ],
        "tumor_sample": [
            *_text(
                "sample_barcode",
                "participant_barcode",
                "project",
                "tissue_source_site",
                "sample_type_code",
                "vial",
                "portion",
                "analyte",
                "plate",
                "center",
                "parse_status",
            ),
            ("mutation_records", "bigint"),
            ("source_dataset", "string"),
        ],
        "somatic_mutation": [
            *_text("sample_barcode", "gene_symbol", "gene_key", "gene_link_method"),
            *_text("ensembl_gene_id", "assembly", "contig"),
            *_number("bigint", "start_position", "end_position"),
            *_text("strand", "variant_classification", "variant_type"),
            *_text("reference_allele", "tumor_allele1", "tumor_allele2", "rsid"),
            *_text(
                "coding_change_notation",
                "protein_change_short_notation",
                "protein_reference_amino_acid",
            ),
            ("protein_position", "int"),
            *_text(
                "protein_alternate_amino_acid",
                "protein_edit_type",
                "transcript_key",
                "transcript_source_id",
            ),
            ("exon_number", "string"),
            *_number("int", "exon_index", "exon_count"),
            *_number("int", "tumor_depth", "tumor_ref_count", "tumor_alt_count"),
            *_text(
                "impact",
                "filter_status",
                "variant_key",
                "variant_link_method",
                "source_dataset",
            ),
        ],
        "dms_assay": [
            *_text("assay_set", "assay_id"),
            ("assay_index", "int"),
            *_text("assay_filename", "uniprot_id", "protein_key", "taxon"),
            *_text("source_organism", "target_sequence"),
            ("sequence_length", "int"),
            ("includes_multiple_mutants", "boolean"),
            *_number("int", "total_mutants", "single_mutants", "multiple_mutants"),
            ("binarization_cutoff", "double"),
            *_text("binarization_method", "first_author", "title"),
            ("publication_year", "int"),
            *_text(
                "journal",
                "region_mutated",
                "molecule_name",
                "selection_assay",
                "selection_type",
                "coarse_selection_type",
                "alignment_filename",
            ),
            *_number("int", "alignment_start", "alignment_end", "alignment_length"),
            *_number("double", "alignment_bit_score", "alignment_theta"),
            ("alignment_sequence_count", "int"),
            ("alignment_percent_coverage", "double"),
            ("alignment_coverage_count", "int"),
            ("alignment_effective_sequences", "double"),
            ("alignment_effective_sequences_per_length", "double"),
            ("alignment_effective_sequences_per_length_category", "string"),
            ("alignment_significant_count", "int"),
            ("alignment_significant_per_length", "double"),
            *_text(
                "raw_scan_filename",
                "raw_scan_phenotype_name",
                "raw_scan_directionality",
                "raw_scan_mutant_column",
                "weight_file_name",
                "protein_structure_file",
                "protein_structure_range",
                "proteingym_version",
            ),
            ("raw_mutation_offset", "int"),
            ("eve_model_path", "string"),
            ("file_length", "int"),
            *_text("alignment_source", "source_dataset"),
        ],
        "dms_assay_mutant": [
            *_text("assay_set", "assay_id", "mutant"),
            ("mutation_count", "int"),
            ("wild_type_aa", "string"),
            ("position", "int"),
            ("mutant_aa", "string"),
            ("scan_score", "double"),
            ("scan_score_binarized", "int"),
            ("source_dataset", "string"),
        ],
        "perturbation_sample": [
            *_text("sample_id", "plate"),
            *_number(
                "double",
                "mean_gene_count",
                "mean_tscp_count",
                "mean_mread_count",
                "mean_pcnt_mito",
            ),
            *_text("drug_name", "drug_key", "drug_link_method", "drugname_drugconc"),
        ],
        "perturbation_expression": [
            *_text("gene_symbol", "gene_key", "gene_link_method"),
            *_number(
                "double",
                "base_mean",
                "log2_fold_change",
                "log2_fold_change_standard_error",
                "statistic",
            ),
            *_number("double", "p_value", "adjusted_p_value"),
            ("plate", "string"),
            *_number("int", "n_cells_treated", "n_cells_control"),
            *_text("cell_line_key", "cellosaur_id", "cell_name_vevo"),
            *_text("drug_name", "drug_key", "drug_link_method"),
            ("concentration", "double"),
            ("concentration_unit", "string"),
        ],
        "faers_case": [
            *_text(
                "quarter",
                "primary_id",
                "case_id",
                "case_version",
                "initial_or_followup",
            ),
            *_text(
                "event_date_raw",
                "manufacturer_date_raw",
                "initial_fda_date_raw",
                "fda_date_raw",
                "report_date_raw",
            ),
            ("event_date", "date"),
            ("manufacturer_date", "date"),
            ("initial_fda_date", "date"),
            ("fda_date", "date"),
            ("report_date", "date"),
            *_text("report_code", "manufacturer_number", "manufacturer_sender"),
            ("age", "double"),
            *_text("age_unit", "age_group", "sex", "electronic_submission"),
            ("weight", "double"),
            *_text(
                "weight_unit",
                "occupation_code",
                "reporter_country",
                "occurrence_country",
            ),
            (
                "reactions",
                "array<struct<reaction:string,drug_recurrence_action:string>>",
            ),
            ("outcomes", "array<string>"),
            ("report_sources", "array<string>"),
        ],
        "faers_case_drug": [
            *_text(
                "quarter",
                "primary_id",
                "case_id",
                "drug_sequence",
                "role_code",
                "drug_name",
            ),
            *_text(
                "active_ingredient",
                "drug_name_validation_code",
                "route",
                "dose_verbatim_text",
            ),
            ("cumulative_dose", "double"),
            *_text("cumulative_dose_unit", "dechallenge", "rechallenge", "lot_number"),
            *_text("expiry_date_raw", "new_drug_application_number"),
            ("dose_amount", "double"),
            *_text(
                "dose_unit",
                "dose_form",
                "dose_frequency",
                "drug_key",
                "drug_link_method",
            ),
            ("indications", "array<string>"),
            (
                "therapies",
                "array<struct<start_dt:string,end_dt:string,duration:string,duration_unit:string>>",
            ),
        ],
    }
)

# Tables whose domain columns are all computed in Silver.
GENOMICS_DERIVED_TABLES = {"entity_derived_attribute"}
# Computed columns of the other declared tables (and every `_source`/`_method`).
GENOMICS_DERIVED_COLUMNS = {
    "entities_sharing_id",
    "entities_sharing_term",
    "source_rank",
    "interval_length",
    "is_primary",
    "key_source",
    "cytoband_arm",
    "cytoband_band",
    "cytoband_sub_band",
    "grch38_contig",
    "grch38_start_position",
    "grch38_end_position",
    "grch38_strand",
    "tags",
    "drivers",
    "genes",
    "conditions",
    "versions",
    "latest_version",
    "link_method",
    "genes_sharing_symbol",
    "evidence_codes",
    "pubmed_ids",
    "dbsnp_ids",
    "source_datasets",
    "disease_link_method",
    "gene_link_method",
    "drug_link_method",
    "member_link_method",
    "subject_link_method",
    "variant_link_method",
    "is_generic_label",
    "pairing_status",
    "parse_status",
    "review_stars",
    "is_negated",
    "variant_rows",
    "distinct_index_values",
    "distinct_mean_embedding_values",
    "mutation_records",
}


def genomics_target_schema(table: str) -> str:
    if table in GENOMICS_REFERENCE_TABLES:
        return GENOMICS_REFERENCE_SCHEMA
    if table in GENOMICS_MAIN_TABLES:
        return GENOMICS_SCHEMA
    message = f"{table!r} is not a declared genomics Silver table"
    raise KeyError(message)


# COMMAND ----------

# DBTITLE 1,Source check (strict)


def assert_genomics_source(source: str) -> None:
    """Fail when SOURCE is not a genomics source of the governed map."""
    if ecosystem_for(source, strict=True) != GENOMICS_ECOSYSTEM:
        message = f"source {source!r} is not mapped to the genomics ecosystem"
        raise ValueError(message)


def read_genomics_bronze(table: str):
    return read_bronze(table, schema=GENOMICS_BRONZE_SCHEMA)


def bronze_table_name(source_dataset: str) -> str:
    """`ncbi_gene.gene_info` -> `ncbi_gene_gene_info`."""
    return source_dataset.replace(".", "_")


# COMMAND ----------

# DBTITLE 1,Rules file: load, check, precedence

_RULE_SECTIONS = (
    "version",
    "clinvar_current_snapshot",
    "placeholders",
    "assembly_labels",
    "ucsc_assembly_names",
    "clinvar_review_stars",
    "id_type_aliases",
    "string_alias_id_types",
    "text_rules",
    "cytoband_limits",
    "primary_function",
    "druggability",
    "precedence",
    "generic_condition_labels",
    "pathogenic_terms",
    "frequency_bands",
    "frequency_band_default",
    "truthy_values",
    "gene_database_id_types",
    "gene_well_annotated_minimum",
    "condition_database_id_types",
    "variant_well_annotated_minimum",
    "function_keyword_flags",
    "well_characterized_minimum",
    "detailed_description_length",
    "gene_importance",
    "gene_annotation_quality",
    "gene_annotation_quality_default",
    "primary_gene_category",
    "primary_gene_category_default",
    "clinical_significance_labels",
    "clinical_significance_label_default",
    "pathogenic_labels",
    "benign_labels",
    "vus_labels",
    "origin_flags",
    "variant_type_flags",
    "review_quality_tiers",
    "review_quality_tier_default",
    "recent_evaluation_days",
    "conservation",
    "frequency_flags",
    "mutation_severity",
    "loss_of_function_edit_types",
    "splice_affecting_regions",
    "protein_impact_categories",
    "predicted_pathogenicity_with_conservation",
    "predicted_pathogenicity_without_conservation",
    "predicted_pathogenicity_default",
    "high_impact_from",
    "very_high_impact_from",
    "tissue_specificity",
    "tissue_specificity_default",
    "cancer_mutation",
    "frequently_mutated_minimum",
    "frequently_tested_minimum",
    "pharmacogene",
)


def load_genomics_rules() -> dict:
    return load_yaml(GENOMICS_RULES_PATH)


def genomics_rule_version(rules: dict) -> str:
    return str(rules["version"])


def _source_dataset_known(text: str, bronze_sources: dict, parts: int) -> bool:
    pieces = text.split(".")
    if len(pieces) != parts:
        return False
    source, dataset = pieces[0], pieces[1]
    return dataset in bronze_sources.get(source, [])


def validate_genomics_rules(rules: dict, bronze_sources: dict) -> list:
    """Problems in the rules file (empty = sound); `bronze_sources` is the
    Bronze SOURCES dict."""
    problems = [f"missing section {s!r}" for s in _RULE_SECTIONS if s not in rules]
    if problems:
        return problems
    if not isinstance(rules["version"], int):
        problems.append("version must be an integer")
    for key in rules["placeholders"].get("by_column", {}):
        if not _source_dataset_known(key, bronze_sources, 3):
            problems.append(f"placeholder column {key!r} is not a Bronze column")
    for entry in rules["assembly_labels"]:
        try:
            _re.compile(entry["pattern"])
        except (_re.error, KeyError) as exc:
            problems.append(f"bad assembly label rule {entry!r}: {exc}")
    for status, stars in rules["clinvar_review_stars"].items():
        if not isinstance(stars, int) or not 0 <= stars <= 4:
            problems.append(f"review status {status!r} has stars {stars!r}")
    for label, canonical in rules["id_type_aliases"].items():
        if not isinstance(canonical, str) or label != label.lower() or " " in label:
            problems.append(f"bad id type alias {label!r}: {canonical!r}")
    patterns = [
        (f"text rule {group}.{name}", pattern)
        for group, group_rules in rules["text_rules"].items()
        for name, pattern in group_rules.items()
    ] + [
        ("string alias rule", entry.get("pattern", ""))
        for entry in rules["string_alias_id_types"]
    ]
    for label, pattern in patterns:
        try:
            _re.compile(pattern)
        except _re.error as exc:
            problems.append(f"{label}: {exc}")
    flags = set(rules["text_rules"].get("gene_function", {})) | set(
        rules["text_rules"].get("gene_location", {})
    )
    for table in ("primary_function", "druggability"):
        for entry in rules[table]:
            unknown = (set(entry.get("all", [])) | set(entry.get("none", []))) - flags
            if unknown:
                problems.append(f"{table}: unknown flag(s) {sorted(unknown)}")
    for table in ("frequency_bands",):
        limits = [entry.get("below") for entry in rules[table]]
        if not limits or limits != sorted(limits) or len(set(limits)) != len(limits):
            problems.append(f"{table}: limits must rise: {limits}")
    for group in ("origin_flags", "variant_type_flags"):
        for name, pattern in rules[group].items():
            try:
                _re.compile(pattern)
            except _re.error as exc:
                problems.append(f"{group}.{name}: {exc}")
    keywords = set(rules["text_rules"].get("gene_description_keywords", {}))
    missing = set(rules["function_keyword_flags"]) - keywords
    if missing:
        problems.append(f"function_keyword_flags: unknown flag(s) {sorted(missing)}")
    for entry in rules["primary_gene_category"]:
        for test in entry.get("any", []):
            if "flag" in test and test["flag"] not in keywords:
                problems.append(f"primary_gene_category: unknown flag {test['flag']!r}")
            if (
                "text" in test
                and test["text"] not in rules["text_rules"]["gene_category_text"]
            ):
                problems.append(
                    f"primary_gene_category: unknown text rule {test['text']!r}"
                )
            if (
                "disease" in test
                and test["disease"] not in rules["text_rules"]["disease_category"]
            ):
                problems.append(
                    f"primary_gene_category: unknown disease category {test['disease']!r}"
                )
    for name, entries in (
        ("gene_annotation_quality", rules["gene_annotation_quality"]),
        ("review_quality_tiers", rules["review_quality_tiers"]),
        (
            "predicted_pathogenicity_with_conservation",
            rules["predicted_pathogenicity_with_conservation"],
        ),
        (
            "predicted_pathogenicity_without_conservation",
            rules["predicted_pathogenicity_without_conservation"],
        ),
        (
            "gene_importance.disease_count_points",
            rules["gene_importance"]["disease_count_points"],
        ),
    ):
        starts = [entry["from"] for entry in entries]
        if starts != sorted(starts, reverse=True):
            problems.append(f"{name}: `from` values must fall: {starts}")
    if set(rules["mutation_severity"]) != {
        "loss_of_function",
        "splice",
        "missense",
        "other",
    }:
        problems.append(
            "mutation_severity needs loss_of_function, splice, missense and other"
        )
    for entity, body in rules["precedence"].items():
        if isinstance(body, list):
            # a source order of whole datasets (`<source>.<dataset>`)
            for entry in body:
                if not _source_dataset_known(entry, bronze_sources, 2):
                    problems.append(f"{entity}: unknown source {entry!r}")
            continue
        for field, order in body.items():
            if not isinstance(order, list) or not order:
                problems.append(f"{entity}.{field}: empty precedence list")
                continue
            for entry in order:
                if not _source_dataset_known(entry, bronze_sources, 3):
                    problems.append(f"{entity}.{field}: unknown source {entry!r}")
    return problems


def precedence_rank(
    rules: dict,
    entity_type: str,
    field: str,
    source_dataset: str,
    source_field: str,
):
    """1-based place of the column in the field's precedence list; None if absent."""
    body = rules["precedence"].get(entity_type, {})
    order = body.get(field, []) if isinstance(body, dict) else []
    key = f"{source_dataset}.{source_field}"
    return order.index(key) + 1 if key in order else None


# COMMAND ----------

# DBTITLE 1,Pure parsers: chromosome, keys, versions, lists

_AMINO_ACIDS = {
    "Ala": "A",
    "Arg": "R",
    "Asn": "N",
    "Asp": "D",
    "Cys": "C",
    "Gln": "Q",
    "Glu": "E",
    "Gly": "G",
    "His": "H",
    "Ile": "I",
    "Leu": "L",
    "Lys": "K",
    "Met": "M",
    "Phe": "F",
    "Pro": "P",
    "Ser": "S",
    "Thr": "T",
    "Trp": "W",
    "Tyr": "Y",
    "Val": "V",
    "Sec": "U",
    "Pyl": "O",
    "Ter": "*",
}


def one_letter_amino_acid(code):
    """`Arg` -> `R`, `Ter` -> `*`; a one-letter code is returned as it is."""
    if code is None:
        return None
    if code in _AMINO_ACIDS:
        return _AMINO_ACIDS[code]
    if len(code) == 1 and (code.isalpha() or code == "*"):
        return code.upper()
    return None


def normalise_chromosome(value):
    """`chr1` -> `1`, `M` -> `MT`; a long contig name keeps its case."""
    if value is None:
        return None
    text = str(value).strip()
    if text.lower().startswith("chr"):
        text = text[3:]
    if not text:
        return None
    if len(text) <= 2:
        text = text.upper()
    return "MT" if text == "M" else text


def variant_key(assembly, chromosome, position, reference, alternate):
    """`GRCh38:1:100:A:T`; None when a part is missing or the position is bad."""
    chrom = normalise_chromosome(chromosome)
    pos = None if position is None else str(position).strip()
    if pos is None or not _re.fullmatch(r"\d+", pos):
        return None
    parts = [assembly, chrom, pos, reference, alternate]
    if any(p is None or not str(p).strip() for p in parts):
        return None
    cleaned = [str(p).strip() for p in parts]
    cleaned[3] = cleaned[3].upper()
    cleaned[4] = cleaned[4].upper()
    return ":".join(cleaned)


def bed_to_closed(start, end):
    """0-based half-open interval (BED) -> 1-based closed interval."""
    return start + 1, end


def strip_version(accession):
    """`ENSG00000135596.20` -> (`ENSG00000135596`, `20`); no version -> None."""
    if accession is None:
        return None, None
    text = str(accession).strip()
    found = _re.fullmatch(r"(.+?)\.(\d+)", text)
    if found:
        return found.group(1), found.group(2)
    return (text or None), None


def split_values(text, separator="|", keep_empty=False):
    """Trimmed parts of a delimited text; `keep_empty` keeps list positions."""
    if text is None:
        return []
    parts = [p.strip() for p in str(text).split(separator)]
    return parts if keep_empty else [p for p in parts if p]


def parse_id_list(text, separators=",;|") -> list:
    """Trimmed ids of a list column delimited by any of the separators."""
    if text is None:
        return []
    parts = _re.split("[" + _re.escape(separators) + "]", str(text))
    return [p.strip() for p in parts if p.strip()]


def parse_xref_pairs(text, separator="|") -> list:
    """`DB:id` list -> [(label, value)]; the value keeps later colons (`HGNC:HGNC:5`)."""
    pairs = []
    for part in split_values(text, separator):
        label, colon, value = part.partition(":")
        if colon and label.strip() and value.strip():
            pairs.append((label.strip(), value.strip()))
    return pairs


def normalise_id_type(label, aliases: dict):
    """Lower-case id type, `_` for spaces and hyphens, then the alias map."""
    if label is None:
        return None
    text = _re.sub(r"[\s\-]+", "_", label.strip().lower())
    return aliases.get(text, text) if text else None


def normalise_hgnc(value):
    """`5`, `HGNC:5` and `HGNC:HGNC:5` -> `HGNC:5`; other text is kept."""
    if value is None:
        return None
    text = value.strip()
    found = _re.fullmatch(r"(?:HGNC:)*(\d+)", text)
    return f"HGNC:{found.group(1)}" if found else text


def assembly_label(raw, rules: dict):
    """Short assembly label from the rules; None when no rule matches."""
    if raw is None:
        return None
    for rule in rules["assembly_labels"]:
        if _re.search(rule["pattern"], raw):
            return rule["label"]
    return None


_GTF_PAIR = _re.compile(r'\s*([A-Za-z_]\w*)\s+(?:"([^"]*)"|([^;\s]+))\s*;?')


def parse_gtf_attributes(text) -> dict:
    """GTF `attributes` -> {key: [values]}; a repeated key (`tag`) keeps all."""
    found: dict = {}
    if not text:
        return found
    for pair in _GTF_PAIR.finditer(text):
        value = pair.group(2) if pair.group(2) is not None else pair.group(3)
        found.setdefault(pair.group(1), []).append(value)
    return found


# COMMAND ----------

# DBTITLE 1,Pure parsers: ClinVar conditions, HGVS, TCGA barcode


def parse_clinvar_conditions(ids_text, names_text, accessions_text) -> list:
    """Pair the parallel ClinVar condition lists by position (`|` between
    conditions, `,` between ids); unequal lists give `count_mismatch`."""
    ids = split_values(ids_text, "|", keep_empty=True)
    names = split_values(names_text, "|", keep_empty=True)
    accessions = split_values(accessions_text, "|", keep_empty=True)
    paired = len(ids) == len(names) == len(accessions)
    rows = []
    for index in range(max(len(ids), len(names), len(accessions))):
        group = ids[index] if index < len(ids) else ""
        name = names[index] if index < len(names) else ""
        accession = accessions[index] if index < len(accessions) else ""
        rows.append(
            {
                "position": index + 1,
                "ids": [i.strip() for i in group.split(",") if i.strip()],
                "name": name or None,
                "accession": accession or None,
                "pairing_status": "paired" if paired else "count_mismatch",
            }
        )
    return rows


_POSITION = r"(?:[-*]?\d+(?:[+-]\d+)?)"
_CODING = _re.compile(
    rf"^c\.(?P<start>{_POSITION})(?:_(?P<end>{_POSITION}))?(?P<edit>.*)$"
)
_CODING_POSITION = _re.compile(r"^(?P<utr>[-*])?(?P<number>\d+)(?P<offset>[+-]\d+)?$")
_AMINO = r"(?:[A-Z][a-z]{2}|[A-Z*])"
_PROTEIN = _re.compile(
    rf"^p\.\(?(?P<ref1>{_AMINO})(?P<pos1>\d+)"
    rf"(?:_(?P<ref2>{_AMINO})(?P<pos2>\d+))?(?P<rest>.*?)\)?$"
)
_FRAMESHIFT = _re.compile(rf"^(?P<alt>{_AMINO})?fs")
_NAME = _re.compile(
    r"^(?P<transcript>[A-Za-z]{2}_\d+\.\d+)(?:\((?P<gene>[^)]+)\))?:"
    r"(?P<coding>c\.\S+)(?:\s+\((?P<protein>p\.[^)]*)\))?\s*$"
)

_CODING_KEYS = (
    "c_notation",
    "c_region",
    "c_start",
    "c_start_offset",
    "c_end",
    "c_end_offset",
    "c_edit_type",
    "c_reference",
    "c_alternate",
    "c_inserted",
)
_PROTEIN_KEYS = (
    "p_notation",
    "p_reference",
    "p_position",
    "p_end_reference",
    "p_end_position",
    "p_edit_type",
    "p_alternate",
    "p_inserted",
)


def _coding_position(text):
    found = _CODING_POSITION.match(text)
    if not found:
        return None, None, None
    number = int(found.group("number"))
    offset = int(found.group("offset")) if found.group("offset") else None
    if found.group("utr") == "-":
        number, region = -number, "five_prime_utr"
    elif found.group("utr") == "*":
        region = "three_prime_utr"
    else:
        region = "coding"
    return number, offset, ("intronic" if offset is not None else region)


def parse_hgvs_coding(text) -> dict:
    """`c.` notation -> position and edit parts; every key is present."""
    out = dict.fromkeys(_CODING_KEYS)
    found = _CODING.match(text.strip()) if text else None
    if not found:
        return out
    out["c_notation"] = text.strip()
    out["c_start"], out["c_start_offset"], out["c_region"] = _coding_position(
        found.group("start")
    )
    if found.group("end"):
        out["c_end"], out["c_end_offset"], _ = _coding_position(found.group("end"))
    edit = found.group("edit")
    substitution = _re.fullmatch(r"([ACGT])>([ACGT])", edit)
    if substitution:
        out["c_edit_type"] = "substitution"
        out["c_reference"], out["c_alternate"] = substitution.groups()
    elif edit.startswith("delins"):
        out["c_edit_type"], out["c_inserted"] = "delins", edit[6:] or None
    elif edit.startswith("del"):
        out["c_edit_type"] = "deletion"
    elif edit.startswith("dup"):
        out["c_edit_type"] = "duplication"
    elif edit.startswith("ins"):
        out["c_edit_type"], out["c_inserted"] = "insertion", edit[3:] or None
    elif edit.startswith("inv"):
        out["c_edit_type"] = "inversion"
    elif edit == "=":
        out["c_edit_type"] = "no_change"
    else:
        out["c_edit_type"] = "other"
    return out


def parse_hgvs_protein(text) -> dict:
    """`p.` notation (three- or one-letter codes) -> reference, position, edit."""
    out = dict.fromkeys(_PROTEIN_KEYS)
    found = _PROTEIN.match(text.strip()) if text else None
    if not found:
        return out
    out["p_notation"] = text.strip()
    out["p_reference"] = one_letter_amino_acid(found.group("ref1"))
    out["p_position"] = int(found.group("pos1"))
    if found.group("ref2"):
        out["p_end_reference"] = one_letter_amino_acid(found.group("ref2"))
        out["p_end_position"] = int(found.group("pos2"))
    rest = found.group("rest")
    frameshift = _FRAMESHIFT.match(rest)
    if rest == "=":
        out["p_edit_type"], out["p_alternate"] = "synonymous", out["p_reference"]
    elif rest == "?":
        out["p_edit_type"] = "unknown"
    elif rest.startswith("delins"):
        out["p_edit_type"], out["p_inserted"] = "delins", rest[6:] or None
    elif rest.startswith("del"):
        out["p_edit_type"] = "deletion"
    elif rest.startswith("dup"):
        out["p_edit_type"] = "duplication"
    elif rest.startswith("ins"):
        out["p_edit_type"], out["p_inserted"] = "insertion", rest[3:] or None
    elif "ext" in rest:
        out["p_edit_type"] = "extension"
    elif frameshift:
        out["p_edit_type"] = "frameshift"
        out["p_alternate"] = one_letter_amino_acid(frameshift.group("alt"))
    elif _re.fullmatch(_AMINO, rest):
        alternate = one_letter_amino_acid(rest)
        out["p_alternate"] = alternate
        out["p_edit_type"] = "nonsense" if alternate == "*" else "missense"
    else:
        out["p_edit_type"] = "other"
    return out


def parse_hgvs_name(text) -> dict:
    """A ClinVar name, `NM_014855.3(AP5Z1):c.80_83delinsTG (p.Arg27_...)`;
    `parse_status` is `ok`, `partial` (protein part unread) or `unparsed`."""
    out = {
        "transcript_accession": None,
        "transcript_version": None,
        "gene_symbol": None,
        **dict.fromkeys(_CODING_KEYS),
        **dict.fromkeys(_PROTEIN_KEYS),
        "parse_status": "unparsed",
    }
    found = _NAME.match(text.strip()) if text else None
    if not found:
        return out
    accession, version = strip_version(found.group("transcript"))
    out["transcript_accession"] = accession
    out["transcript_version"] = int(version) if version else None
    out["gene_symbol"] = found.group("gene")
    out.update(parse_hgvs_coding(found.group("coding")))
    if out["c_notation"] is None:
        return out
    out["parse_status"] = "ok"
    if found.group("protein"):
        out.update(parse_hgvs_protein(found.group("protein")))
        if out["p_notation"] is None:
            out["parse_status"] = "partial"
    return out


_TCGA_KEYS = (
    "project",
    "tissue_source_site",
    "participant_barcode",
    "sample_barcode",
    "sample_type_code",
    "vial",
    "portion",
    "analyte",
    "plate",
    "center",
)


def parse_tcga_barcode(barcode) -> dict:
    """`TCGA-NH-A5IV-01A-42D-A36X-10` -> its parts; `parse_status` is `ok`,
    `partial` (short barcode) or `unparsed`."""
    out = dict.fromkeys(_TCGA_KEYS)
    out["parse_status"] = "unparsed"
    parts = barcode.strip().split("-") if barcode else []
    if len(parts) < 4 or parts[0] != "TCGA":
        return out
    sample = _re.fullmatch(r"(\d{2})([A-Z])", parts[3])
    if not sample:
        return out
    out["project"], out["tissue_source_site"] = parts[0], parts[1]
    out["participant_barcode"] = "-".join(parts[:3])
    out["sample_barcode"] = "-".join(parts[:4])
    out["sample_type_code"], out["vial"] = sample.groups()
    if len(parts) > 4:
        portion = _re.fullmatch(r"(\d{2})([A-Z])", parts[4])
        if portion:
            out["portion"], out["analyte"] = portion.groups()
    if len(parts) > 5:
        out["plate"] = parts[5]
    if len(parts) > 6:
        out["center"] = parts[6]
    out["parse_status"] = "ok" if len(parts) == 7 else "partial"
    return out


# COMMAND ----------

# DBTITLE 1,Claims and resolved masters (Spark)


def clean_placeholders(column, rules: dict, qualified: str | None = None):
    """Trimmed text, null for a placeholder; `qualified` adds the column's own."""
    values = set(rules["placeholders"]["general"])
    if qualified:
        values |= set(rules["placeholders"].get("by_column", {}).get(qualified, []))
    text = F.trim(F.col(column) if isinstance(column, str) else column)
    return F.when(text.isin(sorted(values)), F.lit(None).cast("string")).otherwise(text)


def typed_number(column, spark_type: str, rules: dict, qualified: str | None = None):
    """A cleaned text column as a number (`int`, `bigint` or `double`); null when
    the text is no number. The cast goes through double, so `12.0` is read."""
    text = clean_placeholders(column, rules, qualified)
    number = text.rlike(r"^[-+]?\d+(\.\d+)?([eE][-+]?\d+)?$")
    value = text.cast("double")
    if spark_type == "double":
        return F.when(number, value)
    target = {"int": "int", "bigint": "long"}[spark_type]
    return F.when(number, value.cast(target))


def claims_frame(
    df,
    *,
    entity_type: str,
    key_col: str,
    fields: dict,
    source_dataset: str,
    rules: dict,
    numeric_fields=(),
    source_type: str = "source",
):
    """One claim row per entity, field and value; `fields` maps a field to its
    source column (a dict, or a list of pairs when a field has several columns).
    A null or placeholder gives no row."""
    parts = []
    for field, column in fields.items() if isinstance(fields, dict) else fields:
        value = clean_placeholders(column, rules, f"{source_dataset}.{column}")
        numeric = field in numeric_fields
        rank = precedence_rank(rules, entity_type, field, source_dataset, column)
        parts.append(
            df.select(
                F.lit(entity_type).alias("entity_type"),
                F.col(key_col).cast("string").alias("entity_key"),
                F.lit(field).alias("field"),
                value.alias("value_text"),
                (value.cast("double") if numeric else F.lit(None).cast("double")).alias(
                    "value_number"
                ),
                F.lit(None).cast("date").alias("value_date"),
                F.lit("number" if numeric else "text").alias("value_type"),
                F.lit(source_dataset).alias("source_dataset"),
                F.lit(column).alias("source_field"),
                F.lit(rank).cast("int").alias("source_rank"),
                F.lit(source_type).alias("source_type"),
            ).filter(F.col("entity_key").isNotNull() & F.col("value_text").isNotNull())
        )
    return _reduce(lambda left, right: left.unionByName(right), parts)


def resolve_master(claims, *, entity_type: str, fields):
    """(masters, conflicts) from a claims frame. A master row has each field, its
    `<field>_source` and `<field>_method` (`primary`, `filled` or `unranked`);
    left-join it onto the entity list. `conflicts` lists fields whose sources
    gave more than one value."""
    fields = list(fields)
    scoped = claims.filter(
        (F.col("entity_type") == entity_type) & F.col("field").isin(fields)
    )
    pair = Window.partitionBy("entity_key", "field")
    ranked = scoped.withColumn(
        "_distinct_values", F.size(F.collect_set("value_text").over(pair))
    ).withColumn(
        "_order",
        F.row_number().over(
            pair.orderBy(
                F.col("source_rank").asc_nulls_last(),
                F.col("source_dataset").asc(),
                F.col("source_field").asc(),
                F.col("value_text").asc(),
            )
        ),
    )
    picked = ranked.filter(F.col("_order") == 1)
    method = (
        F.when(F.col("source_rank") == 1, F.lit("primary"))
        .when(F.col("source_rank").isNull(), F.lit("unranked"))
        .otherwise(F.lit("filled"))
    )
    columns = []
    for field in fields:
        mine = F.col("field") == field
        columns += [
            F.max(F.when(mine, F.col("value_text"))).alias(field),
            F.max(
                F.when(
                    mine,
                    F.concat_ws(".", F.col("source_dataset"), F.col("source_field")),
                )
            ).alias(f"{field}_source"),
            F.max(F.when(mine, method)).alias(f"{field}_method"),
        ]
    masters = picked.groupBy("entity_key").agg(*columns)
    conflicts = picked.filter(F.col("_distinct_values") > 1).select(
        "entity_key",
        "field",
        F.col("value_text").alias("chosen_value"),
        F.col("_distinct_values").alias("distinct_values"),
        "source_dataset",
        "source_field",
    )
    return masters, conflicts


def chromosome_expr(column):
    """Column expr of `normalise_chromosome`."""
    text = F.col(column) if isinstance(column, str) else column
    stripped = F.regexp_replace(F.trim(text), r"(?i)^chr", "")
    upper = F.when(F.length(stripped) <= 2, F.upper(stripped)).otherwise(stripped)
    return F.when(upper == "M", F.lit("MT")).otherwise(upper)


def variant_key_expr(assembly, chromosome, position, reference, alternate):
    """Column expr of `variant_key`; `assembly` is a text or a Column, the other
    arguments are names or Columns."""

    def column(value):
        return F.col(value) if isinstance(value, str) else value

    contig = chromosome_expr(column(chromosome))
    pos = F.trim(column(position))
    parts = [
        F.lit(assembly) if isinstance(assembly, str) else assembly,
        contig,
        F.when(pos.rlike(r"^\d+$"), pos),
        F.upper(F.trim(column(reference))),
        F.upper(F.trim(column(alternate))),
    ]
    valid = F.lit(True)
    for part in parts:
        valid = valid & part.isNotNull() & (part != "")
    return F.when(valid, F.concat_ws(":", *parts))


# COMMAND ----------

# DBTITLE 1,Entity helpers: tables, provenance, ids, keys, names


def genomics_table(name: str) -> str:
    return f"{CATALOG}.{genomics_target_schema(name)}.{name}"


def add_genomics_provenance(df, sr_id_col: str, rid: str):
    """Fixed identity and provenance columns of an integrated genomics record."""
    out = (
        df
        if sr_id_col == "source_record_id"
        else df.withColumnRenamed(sr_id_col, "source_record_id")
    )
    return (
        out.withColumn("source_system", F.lit("genomics"))
        .withColumn("ecosystem", F.lit(GENOMICS_ECOSYSTEM))
        .withColumn("_silver_loaded_at", F.current_timestamp())
        .withColumn("_silver_run_id", F.lit(rid))
    )


def hgnc_expr(column):
    """`5`, `HGNC:5` and `HGNC:HGNC:5` -> `HGNC:5`; other text is kept."""
    text = F.trim(F.col(column) if isinstance(column, str) else column)
    digits = F.regexp_extract(text, r"(\d+)$", 1)
    return F.when(
        text.rlike(r"^(HGNC:)*\d+$"), F.concat(F.lit("HGNC:"), digits)
    ).otherwise(text)


def rank_expr(rules: dict, entity_type: str, field: str):
    """Column expr: place of (`source_dataset`, `source_field`) in the field's
    precedence list; null when the column is not listed."""
    order = rules["precedence"].get(entity_type, {}).get(field, [])
    expr = F.lit(None).cast("int")
    for position, entry in enumerate(order, start=1):
        dataset, _, column = entry.rpartition(".")
        expr = F.when(
            (F.col("source_dataset") == dataset) & (F.col("source_field") == column),
            F.lit(position),
        ).otherwise(expr)
    return expr


def dataset_rank_expr(rules: dict, column: str = "source_dataset"):
    """Column expr: place of a dataset in the coordinate source order."""
    expr = F.lit(None).cast("int")
    for position, dataset in enumerate(rules["precedence"]["coordinate"], start=1):
        expr = F.when(F.col(column) == dataset, F.lit(position)).otherwise(expr)
    return expr


def assembly_expr(column, rules: dict):
    """Short assembly label by the rules; else the cleaned text; else `unspecified`."""
    text = clean_placeholders(column, rules)
    label = F.lit(None).cast("string")
    for rule in reversed(rules["assembly_labels"]):
        label = F.when(text.rlike(rule["pattern"]), F.lit(rule["label"])).otherwise(
            label
        )
    return F.coalesce(label, text, F.lit("unspecified"))


def id_type_expr(label, rules: dict):
    """Column expr of `normalise_id_type` with the rule aliases."""
    aliases = F.create_map(
        [F.lit(x) for kv in rules["id_type_aliases"].items() for x in kv]
    )
    norm = F.regexp_replace(F.lower(F.trim(label)), r"[\s\-]+", "_")
    return F.coalesce(F.try_element_at(aliases, norm), norm)


def id_value_expr(id_type, value):
    """Normalise an id value by its type (a Column each): HGNC, MONDO and HPO
    get their prefix, OMIM and Orphanet keep the digits."""
    text = F.trim(value)
    digits = F.regexp_extract(text, r"(\d+)$", 1)
    return (
        F.when(id_type == "hgnc", hgnc_expr(text))
        .when(
            (id_type == "mondo") & text.rlike(r"^(MONDO:)*\d+$"),
            F.concat(F.lit("MONDO:"), digits),
        )
        .when(
            (id_type == "hpo") & text.rlike(r"^(HP:)*\d+$"),
            F.concat(F.lit("HP:"), digits),
        )
        .when(
            id_type.isin("omim", "orphanet")
            & text.rlike(r"^(OMIM:|Orphanet:|ORPHA:|ORDO:)*\d+$"),
            digits,
        )
        .otherwise(text)
    )


def curie_expr(column):
    """`http://.../HP_0000001` and `MONDO_0005148` -> `HP:0000001`, `MONDO:0005148`."""
    text = F.trim(F.col(column) if isinstance(column, str) else column)
    last = F.regexp_extract(text, r"([^/]+)$", 1)
    return F.regexp_replace(last, r"^([A-Za-z]+)_(\d+)$", "$1:$2")


def ensembl_unversioned_expr(column):
    """`ENSG1.20` -> `ENSG1`; the `_PAR_Y` suffix of a chromosome Y copy is kept."""
    return F.regexp_replace(column, r"\.\d+(?=(_PAR_Y)?$)", "")


def accession_unversioned_expr(column, rules: dict, qualified: str):
    """`NM_130786.4` -> `NM_130786`; a placeholder gives null."""
    return F.regexp_replace(clean_placeholders(column, rules, qualified), r"\.\d+$", "")


def link_by_ids(pairs, id_map):
    """(own, entity_key) of the own ids whose ids all lead to exactly one entity
    of `id_map` (id_type, id_value, entity_key; unique ids only). `pairs` has
    own, id_type and id_value."""
    return (
        pairs.join(id_map, ["id_type", "id_value"], "inner")
        .groupBy("own")
        .agg(
            F.countDistinct("entity_key").alias("candidates"),
            F.min("entity_key").alias("entity_key"),
        )
        .filter(F.col("candidates") == 1)
        .select("own", "entity_key")
    )


def unique_id_map(crosswalk, id_types=None):
    """(id_type, id_value, entity_key) of the ids (of `id_types`, or all) that
    one entity holds; the input is a finished crosswalk."""
    unique = crosswalk.filter(F.col("entities_sharing_id") == 1)
    if id_types is not None:
        unique = unique.filter(F.col("id_type").isin(list(id_types)))
    return unique.select("id_type", "id_value", "entity_key").distinct()


def typed_pairs(pairs, rules: dict):
    """(own, id_type, id_value) from a (entity_key, label, value, ...) frame, the
    label and the value normalised as in the crosswalk."""
    id_type = id_type_expr(F.col("label"), rules)
    return pairs.select(
        F.col("entity_key").alias("own"),
        id_type.alias("id_type"),
        id_value_expr(id_type, F.col("value")).alias("id_value"),
    )


def array_claims(df, *, entity_type, key_col, field, array, dataset, source_field):
    """Claim rows from an array column (a Column): one row per element."""
    return df.select(
        F.lit(entity_type).alias("entity_type"),
        F.col(key_col).cast("string").alias("entity_key"),
        F.lit(field).alias("field"),
        F.explode(array).alias("value_text"),
    ).select(
        "entity_type",
        "entity_key",
        "field",
        "value_text",
        F.lit(None).cast("double").alias("value_number"),
        F.lit(None).cast("date").alias("value_date"),
        F.lit("text").alias("value_type"),
        F.lit(dataset).alias("source_dataset"),
        F.lit(source_field).alias("source_field"),
        F.lit(None).cast("int").alias("source_rank"),
        F.lit("source").alias("source_type"),
    )


def coordinate_rows(
    df,
    *,
    entity_type,
    key,
    level,
    dataset,
    assembly,
    contig,
    start,
    end,
    strand,
    system,
    conversion=None,
):
    """A frame in the column order of `entity_coordinate` (before the computed
    columns); `key` is a column name, the others are Columns."""
    return df.select(
        F.lit(entity_type).alias("entity_type"),
        F.col(key).cast("string").alias("entity_key"),
        F.lit(level).alias("feature_level"),
        F.lit(dataset).alias("source_dataset"),
        assembly.alias("assembly"),
        contig.alias("contig"),
        start.cast("long").alias("start_position"),
        end.cast("long").alias("end_position"),
        strand.alias("strand"),
        F.lit(system).alias("coordinate_system"),
        F.lit(conversion).cast("string").alias("coordinate_conversion"),
    )


def id_list_pairs(
    df, own_col: str, id_type: str, column: str, rules: dict, dataset: str
):
    """(own, id_type, id_value) from a list column (`,`, `;` or `|`)."""
    parse = F.udf(lambda text: parse_id_list(text), "array<string>")
    cleaned = clean_placeholders(column, rules, f"{dataset}.{column}")
    return df.select(
        F.col(own_col).alias("own"),
        F.lit(id_type).alias("id_type"),
        F.explode(parse(cleaned)).alias("id_value"),
    )


_XREF_TYPE = "array<struct<label:string,value:string>>"


def xref_pairs_frame(
    df, key_col: str, text_col: str, separator: str = "|", method=None
):
    """(entity_key, label, value, method) from a `label:id` list column."""
    parse = F.udf(lambda text: parse_xref_pairs(text, separator), _XREF_TYPE)
    pairs = df.select(
        F.col(key_col).cast("string").alias("entity_key"),
        F.explode(parse(F.col(text_col))).alias("pair"),
        (F.lit("exact_id") if method is None else method).alias("method"),
    )
    return pairs.select(
        "entity_key",
        F.col("pair.label").alias("label"),
        F.col("pair.value").alias("value"),
        "method",
    )


def crosswalk_frame(pairs, *, entity_type: str, dataset: str, field: str, rules: dict):
    """Crosswalk rows from a (entity_key, label, value, method) frame: the label
    becomes the id type (lower case, aliases applied) and the value is
    normalised by its type."""
    id_type = id_type_expr(F.col("label"), rules)
    value = id_value_expr(id_type, F.col("value"))
    return pairs.select(
        F.lit(entity_type).alias("entity_type"),
        "entity_key",
        id_type.alias("id_type"),
        value.alias("id_value"),
        F.lit(dataset).alias("source_dataset"),
        F.lit(field).alias("source_field"),
        F.col("method").alias("match_method"),
    )


def single_id_frame(
    df,
    *,
    key,
    value,
    id_type: str,
    entity_type: str,
    dataset: str,
    field: str,
    rules: dict,
    method="exact_id",
):
    """Crosswalk rows of one id column (a name or a Column); a placeholder or a
    null gives no row."""
    key_col = F.col(key) if isinstance(key, str) else key
    cleaned = clean_placeholders(value, rules, f"{dataset}.{field}")
    how = F.lit(method) if isinstance(method, str) else method
    return df.select(
        F.lit(entity_type).alias("entity_type"),
        key_col.cast("string").alias("entity_key"),
        F.lit(id_type).alias("id_type"),
        cleaned.alias("id_value"),
        F.lit(dataset).alias("source_dataset"),
        F.lit(field).alias("source_field"),
        how.alias("match_method"),
    ).filter(F.col("entity_key").isNotNull() & F.col("id_value").isNotNull())


def finish_crosswalk(frame):
    """Distinct rows plus `entities_sharing_id` (entities holding the same id)."""
    same = Window.partitionBy("entity_type", "id_type", "id_value")
    return (
        frame.filter(F.col("id_value").isNotNull() & (F.col("id_value") != ""))
        .distinct()
        .withColumn(
            "entities_sharing_id", F.size(F.collect_set("entity_key").over(same))
        )
    )


def resolve_gene_key(
    df, id_map, *, geneid=None, ensembl=None, hgnc=None, accession=None
):
    """Add `gene_key` and `key_method`. The NCBI GeneID when the source has it;
    else through an exact Ensembl or HGNC id of an NCBI gene (`id_map`: id_type,
    id_value, entity_key of unique ids); else the Ensembl id or the source
    accession as an own key. Arguments are Columns."""
    none = F.lit(None).cast("string")
    ens_map = id_map.filter(F.col("id_type") == "ensembl").select(
        F.col("id_value").alias("_rk_ens_id"), F.col("entity_key").alias("_rk_ens_key")
    )
    hgnc_map = id_map.filter(F.col("id_type") == "hgnc").select(
        F.col("id_value").alias("_rk_hgnc_id"),
        F.col("entity_key").alias("_rk_hgnc_key"),
    )
    out = (
        df.withColumn("_rk_geneid", none if geneid is None else geneid)
        .withColumn("_rk_ensembl", none if ensembl is None else ensembl)
        .withColumn("_rk_hgnc", none if hgnc is None else hgnc)
        .withColumn("_rk_accession", none if accession is None else accession)
    )
    out = out.join(
        F.broadcast(ens_map), F.col("_rk_ensembl") == F.col("_rk_ens_id"), "left"
    )
    out = out.join(
        F.broadcast(hgnc_map), F.col("_rk_hgnc") == F.col("_rk_hgnc_id"), "left"
    )
    key = F.coalesce(
        F.col("_rk_geneid"),
        F.col("_rk_ens_key"),
        F.col("_rk_hgnc_key"),
        F.col("_rk_ensembl"),
        F.col("_rk_accession"),
    )
    method = (
        F.when(F.col("_rk_geneid").isNotNull(), F.lit("ncbi_gene_id"))
        .when(F.col("_rk_ens_key").isNotNull(), F.lit("ensembl_via_ncbi"))
        .when(F.col("_rk_hgnc_key").isNotNull(), F.lit("hgnc_via_ncbi"))
        .when(F.col("_rk_ensembl").isNotNull(), F.lit("own_ensembl_id"))
        .when(F.col("_rk_accession").isNotNull(), F.lit("own_source_id"))
    )
    return (
        out.withColumn("gene_key", key)
        .withColumn("key_method", method)
        .drop(*[c for c in out.columns if c.startswith("_rk_")])
    )


def with_entity_key(df, column: str, xwalk, dataset: str, id_type: str):
    """Add `entity_key` from the crosswalk rows of one source and id type; rows
    without a crosswalk id are dropped."""
    keys = (
        xwalk.filter(
            (F.col("source_dataset") == dataset) & (F.col("id_type") == id_type)
        )
        .select(F.col("id_value").alias("_xk_id"), F.col("entity_key"))
        .distinct()
    )
    return df.join(keys, df[column] == keys["_xk_id"], "inner").drop("_xk_id")


def xwalk_claims(xwalk, rules: dict, entity_type: str, mapping: dict):
    """Claim rows from crosswalk rows: `mapping` is {claim field: id type}."""
    parts = [
        xwalk.filter(F.col("id_type") == id_type).select(
            F.lit(entity_type).alias("entity_type"),
            "entity_key",
            F.lit(field).alias("field"),
            F.col("id_value").alias("value_text"),
            F.lit(None).cast("double").alias("value_number"),
            F.lit(None).cast("date").alias("value_date"),
            F.lit("text").alias("value_type"),
            "source_dataset",
            "source_field",
            rank_expr(rules, entity_type, field).alias("source_rank"),
            F.lit("source").alias("source_type"),
        )
        for field, id_type in mapping.items()
    ]
    return _reduce(lambda left, right: left.unionByName(right), parts)


def names_frame(
    df, *, entity_type: str, key_col: str, dataset: str, specs, rules: dict
):
    """Name rows. `specs` lists (column, term type, kind[, suffix column]); the
    term type is a text or a Column. Kind `text` is one name, `pipe` and `comma`
    a delimited list, `json` a JSON list of {label, source} (the source goes
    into `source_field`), `jsonstr` a JSON list of texts, `jsonmix` either
    JSON form. A suffix column is appended to `source_field` after a colon."""
    label_type = "array<struct<label:string,source:string>>"
    parts = []
    for spec in specs:
        column, term_type, kind = spec[:3]
        key = F.col(key_col).cast("string").alias("entity_key")
        kept = [
            (F.lit(term_type) if isinstance(term_type, str) else term_type).alias(
                "term_type"
            ),
            (F.col(spec[3]) if len(spec) > 3 else F.lit(None).cast("string")).alias(
                "suffix"
            ),
        ]
        field = F.lit(column)
        if kind == "json":
            frame = df.select(
                key,
                F.explode(F.from_json(F.col(column), label_type)).alias("item"),
                *kept,
            )
            frame = frame.withColumn("term", F.col("item.label"))
            field = F.concat_ws(":", F.lit(column), F.col("item.source"))
        elif kind == "jsonmix":
            labelled = F.from_json(F.col(column), label_type)
            items = F.when(
                F.size(F.filter(labelled, lambda x: x["label"].isNotNull())) > 0,
                F.transform(labelled, lambda x: x["label"]),
            ).otherwise(F.from_json(F.col(column), "array<string>"))
            frame = df.select(key, F.explode(items).alias("term"), *kept)
        elif kind == "jsonstr":
            terms = F.from_json(F.col(column), "array<string>")
            frame = df.select(key, F.explode(terms).alias("term"), *kept)
        else:
            cleaned = clean_placeholders(column, rules, f"{dataset}.{column}")
            separator = {"pipe": r"\|", "comma": ","}.get(kind)
            terms = (
                F.array(cleaned) if separator is None else F.split(cleaned, separator)
            )
            frame = df.select(key, F.explode(terms).alias("term"), *kept)
        parts.append(
            frame.select(
                F.lit(entity_type).alias("entity_type"),
                "entity_key",
                F.trim(F.col("term")).alias("term"),
                "term_type",
                F.lit(dataset).alias("source_dataset"),
                F.concat_ws(":", field, F.col("suffix")).alias("source_field"),
            )
        )
    union = _reduce(lambda left, right: left.unionByName(right), parts)
    return union.filter(
        F.col("entity_key").isNotNull()
        & F.col("term").isNotNull()
        & (F.col("term") != "")
    )


def finish_names(frame):
    """Distinct rows plus `entities_sharing_term` (entities with the same term,
    case ignored)."""
    same = Window.partitionBy("entity_type", F.upper(F.col("term")))
    return frame.distinct().withColumn(
        "entities_sharing_term", F.size(F.collect_set("entity_key").over(same))
    )


GOVERNANCE_COLUMNS = [
    "source_system",
    "source_record_id",
    "ecosystem",
    "_silver_loaded_at",
    "_silver_run_id",
]


def conform_genomics(df, table: str):
    """Select the declared columns of a table, then the governance columns; a
    missing column fails here."""
    columns = [name for name, _ in GENOMICS_STRUCTURES[table]]
    return df.select(*columns, *GOVERNANCE_COLUMNS)
