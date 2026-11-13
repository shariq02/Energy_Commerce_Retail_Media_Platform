# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
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
# MAGIC parsers, the claim helpers and the entity builders (crosswalk, names,
# MAGIC claims, coordinates, derived rows, linking). A notebook pulls it in with
# MAGIC one `%run ../_genomics_common`; the library runs `_silver_common` itself.
# MAGIC The constants and pure functions run without Spark. No Silver column is an
# MAGIC array, struct or map.

# COMMAND ----------

# MAGIC %run ./_silver_common

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
    "entity_master",
    "protein_gene_link",
    "protein_feature",
    "cell_line",
    "genetic_test",
}

# Variants, relations and facts.
GENOMICS_MAIN_TABLES = {
    "variant",
    "variant_hgvs",
    "variant_condition",
    "variant_classification",
    "locus_score",
    "variant_population_frequency",
    "variant_feature_overlap",
    "benchmark_variant",
    "embedding_split",
    "entity_relation",
    "gene_expression_median",
    "tumor_sample",
    "somatic_mutation",
    "dms_assay",
    "dms_assay_mutant",
    "perturbation_sample",
    "perturbation_expression",
    "faers_case",
    "faers_case_drug",
    "faers_case_item",
    "clinical_trial",
    "protein_residue_score",
    "gene_dependency",
    "regulatory_element_gene_link",
    "epitope",
    "epitope_mhc_assay",
    "gene_disease_score_year",
}

# One row per ...
GENOMICS_TABLE_GRAIN = {
    "entity_id_crosswalk": "entity, id type, id value, source column",
    "entity_claim": "entity, field, source column, value, snapshot run",
    "entity_name": "entity, term, term type",
    "entity_derived_attribute": "entity, attribute, rule",
    "ontology_relation": "ontology, parent, child, relation",
    "entity_coordinate": "entity, source, assembly, contig, interval",
    "entity_master": "entity type, entity",
    "protein_gene_link": "protein, gene, source",
    "protein_feature": "protein, feature record, reference",
    "cell_line": "cell line, driver",
    "genetic_test": "test, link",
    "variant": "variant",
    "variant_hgvs": "variant, transcript, snapshot run",
    "variant_condition": "variant, condition position, condition id, snapshot run",
    "variant_classification": (
        "variant, classification type, term, submission, snapshot run"
    ),
    "locus_score": "variant or position, score name, source",
    "variant_population_frequency": "variant, population, source",
    "variant_feature_overlap": "variant, feature kind, feature",
    "benchmark_variant": "benchmark set, record",
    "embedding_split": "split file",
    "entity_relation": "relation kind, record, publication",
    "gene_expression_median": "gene, tissue, source",
    "tumor_sample": "sample barcode",
    "somatic_mutation": "sample, mutation record",
    "dms_assay": "assay",
    "dms_assay_mutant": "assay, mutant",
    "perturbation_sample": "sample",
    "perturbation_expression": "gene, drug, cell line, concentration, plate",
    "faers_case": "case version",
    "faers_case_drug": "case version, drug sequence, dose record",
    "faers_case_item": "case version, drug sequence, item kind, item",
    "clinical_trial": "study",
    "protein_residue_score": "protein, fragment, residue, score name",
    "gene_dependency": "cell line, gene",
    "regulatory_element_gene_link": "element, target gene, biosample, cell type, score",
    "epitope": "epitope",
    "epitope_mhc_assay": "assay",
    "gene_disease_score_year": "gene, disease, association level, datasource, year",
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
        # the unbroken run of ClinVar snapshots of a ClinVar claim (null for others)
        ("first_source_snapshot", "string"),
        ("last_source_snapshot", "string"),
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
        # the GenBank gi of the genomic contig (gene2refseq)
        ("contig_genbank_gi", "string"),
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
# The five masters (gene, transcript, protein, disease, drug) are one table:
# the resolved fields of every type side by side (a field two types share, such
# as `name` or `description`, is one column), the cytoband parts of genes and
# the GRCh38 interval of genes and transcripts.
MASTER_FIELDS = {
    "gene": GENE_FIELDS,
    "transcript": TRANSCRIPT_FIELDS,
    "protein": PROTEIN_FIELDS,
    "disease": DISEASE_FIELDS,
    "drug": DRUG_FIELDS,
}
ALL_MASTER_FIELDS = tuple(
    dict.fromkeys(field for fields in MASTER_FIELDS.values() for field in fields)
)
CYTOBAND_COLUMNS = ("cytoband_arm", "cytoband_band", "cytoband_sub_band")
# DepMap model columns of `cell_line` (source column -> Silver column); the model id
# is `cell_line_key`, the RRID is `cellosaur_id` and the cell line name `cell_name`.
DEPMAP_MODEL_COLUMNS = {
    "PatientID": "patient_id",
    "StrippedCellLineName": "stripped_cell_line_name",
    "CCLEName": "ccle_name",
    "DepmapModelType": "depmap_model_type",
    "ModelType": "model_type",
    "PediatricModelType": "pediatric_model_type",
    "OncotreeLineage": "oncotree_lineage",
    "OncotreePrimaryDisease": "oncotree_primary_disease",
    "OncotreeSubtype": "oncotree_subtype",
    "OncotreeCode": "oncotree_code",
    "Age": "age",
    "AgeCategory": "age_category",
    "Sex": "sex",
    "PatientRace": "patient_race",
    "PrimaryOrMetastasis": "primary_or_metastasis",
    "SourceType": "source_type",
    "SourceDetail": "source_detail",
    "CatalogNumber": "catalog_number",
    "SampleCollectionSite": "sample_collection_site",
    "ModelSubtypeFeatures": "model_subtype_features",
    "PatientSubtypeFeatures": "patient_subtype_features",
    "TissueOrigin": "tissue_origin",
    "ModelDerivationMaterial": "model_derivation_material",
    "ModelTreatment": "model_treatment",
    "PatientTreatmentStatus": "patient_treatment_status",
    "PatientTreatmentType": "patient_treatment_type",
    "PatientTreatmentDetails": "patient_treatment_details",
    "Stage": "stage",
    "StagingSystem": "staging_system",
    "PatientTumorGrade": "patient_tumor_grade",
    "PatientTreatmentResponse": "patient_treatment_response",
    "GrowthPattern": "growth_pattern",
    "OnboardedMedia": "onboarded_media",
    "FormulationID": "formulation_id",
    "SerumFreeMedia": "serum_free_media",
    "PlateCoating": "plate_coating",
    "EngineeredModel": "engineered_model",
    "EngineeredModelDetails": "engineered_model_details",
    "CulturedResistanceDrug": "cultured_resistance_drug",
    "PublicComments": "public_comments",
    "ModelIDAlias": "model_id_alias",
    "HCMIID": "hcmi_id",
    "SangerModelID": "sanger_model_id",
    "WTSIMasterCellID": "wtsi_master_cell_id",
    "COSMICID": "cosmic_id",
}
GENOMICS_STRUCTURES.update(
    {
        "entity_master": [
            ("entity_type", "string"),
            ("entity_key", "string"),
            ("key_source", "string"),
            *_resolved(ALL_MASTER_FIELDS),
            *[(name, "string") for name in CYTOBAND_COLUMNS],
            *_GRCH38,
        ],
        "protein_gene_link": [
            ("protein_key", "string"),
            ("gene_key", "string"),
            ("link_method", "string"),
            ("genes_sharing_symbol", "int"),
            ("source_dataset", "string"),
            ("source_field", "string"),
        ],
        # One row per feature and reference: `feature_id` names the feature,
        # `reference_kind` is evidence_code, pubmed_id or dbsnp_id; a feature
        # without a reference keeps one row with no reference.
        "protein_feature": [
            ("protein_key", "string"),
            ("feature_id", "string"),
            ("feature_type", "string"),
            ("location_raw", "string"),
            ("start_position", "int"),
            ("end_position", "int"),
            ("note", "string"),
            ("qualifiers", "string"),
            ("reference_kind", "string"),
            ("reference_value", "string"),
        ],
        # One row per cell line and driver (a cell line without a driver keeps one
        # row): the Tahoe lines with their drivers and every DepMap model.
        "cell_line": [
            ("cell_line_key", "string"),
            ("cell_name", "string"),
            ("cellosaur_id", "string"),
            ("organ", "string"),
            ("driver_gene_symbol", "string"),
            ("driver_variant_zygosity", "string"),
            ("driver_variant_type", "string"),
            ("driver_effect", "string"),
            ("driver_mechanism", "string"),
            ("driver_gene_type", "string"),
            ("in_tahoe", "boolean"),
            ("in_depmap", "boolean"),
            *[(name, "string") for name in DEPMAP_MODEL_COLUMNS.values()],
        ],
        # One row per test and link; `link_kind` is gene or condition.
        "genetic_test": [
            ("test_key", "string"),
            ("latest_version", "string"),
            ("versions", "int"),
            ("test_type", "string"),
            ("link_kind", "string"),
            ("gene_key", "string"),
            ("gene_symbol", "string"),
            ("disease_key", "string"),
            ("condition_name", "string"),
            ("mim_number", "string"),
            ("snomed_ct_id", "string"),
        ],
        "ontology_relation": [
            ("ontology", "string"),
            ("parent_key", "string"),
            ("child_key", "string"),
            ("relation", "string"),
            ("direction", "string"),
            ("source_dataset", "string"),
            ("source_field", "string"),
            # MedGen relation id, atom ids and the source type of the first atom
            *[
                (name, "string")
                for name in (
                    "relation_id",
                    "parent_atom_id",
                    "child_atom_id",
                    "parent_source_type",
                )
            ],
        ],
    }
)

VARIANT_FIELDS = ("variant_type", "rsid", "gene_key", "origin", "name")
# The seven STRING evidence channels, in the order of the source columns.
STRING_CHANNELS = (
    "neighborhood",
    "fusion",
    "cooccurence",
    "coexpression",
    "experimental",
    "database",
    "textmining",
)
# The source datasets that hold a variant: one boolean column per dataset.
VARIANT_SOURCE_FLAGS = {
    "clinvar.variant_summary": "in_clinvar",
    "gnomad.variants": "in_gnomad",
    "alphamissense.variant_scores": "in_alphamissense",
    "cadd.scores": "in_cadd",
    "dbvar.variants": "in_dbvar",
    "pharmgkb.variants": "in_pharmgkb",
    "long_range_benchmark.pathogenic_coding": "in_long_range_pathogenic_coding",
    "long_range_benchmark.pathogenic_non_coding": (
        "in_long_range_pathogenic_non_coding"
    ),
    "long_range_benchmark.causal_eqtl": "in_long_range_causal_eqtl",
    "clinvar_vs_benign.variants": "in_clinvar_vs_benign",
    "gnomad_balanced.variants": "in_gnomad_balanced",
    "gnomad_mtdna.variants": "in_gnomad_mtdna",
    "thousand_genomes.variants": "in_thousand_genomes",
}


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
            *[(flag, "boolean") for flag in VARIANT_SOURCE_FLAGS.values()],
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
            *_text("source_dataset", "source_field"),
            *_text("first_source_snapshot", "last_source_snapshot"),
        ],
        "variant_condition": [
            ("variant_key", "string"),
            ("condition_position", "int"),
            *_text("condition_name", "clinvar_condition_accession", "condition_id"),
            *_text("disease_key", "disease_link_method"),
            ("is_generic_label", "boolean"),
            *_text("pairing_status", "source_dataset"),
            *_text("first_source_snapshot", "last_source_snapshot"),
        ],
        "variant_classification": [
            *_text("variant_key", "classification_type", "term", "review_status"),
            ("review_stars", "int"),
            ("last_evaluated", "date"),
            ("number_submitters", "int"),
            ("clinical_significance_simple", "string"),
            ("submission_accession", "string"),
            ("source_dataset", "string"),
            ("first_source_snapshot", "string"),
            ("last_source_snapshot", "string"),
        ],
        # A score of a variant (variant_key set) or of a genome position
        # (assembly, contig, position set).
        "locus_score": [
            ("variant_key", "string"),
            *_text("assembly", "contig"),
            ("position", "bigint"),
            ("score_name", "string"),
            ("score_value", "double"),
            *_text("score_label", "context", "source_dataset", "source_field"),
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
        # The flat typed union of the gene-disease, drug-gene, pharmacogenomic,
        # phenotype, pathway-member, protein-interaction and ClinVar
        # variation-member records; a row
        # fills the columns of its own `relation_kind` and leaves the others
        # null. A name two kinds would share with a different meaning (score,
        # relation type, evidence) carries the kind. One row per publication
        # where a record names publications.
        "entity_relation": [
            *_text("relation_kind", "relation_subkind", "source_dataset"),
            # gene-disease
            *_text("gene_key", "disease_key", "source_gene_id", "source_disease_id"),
            *_text("gene_link_method", "disease_link_method"),
            ("gene_disease_score", "double"),
            *_number("bigint", "evidence_count", "supporting_count"),
            *_text(
                "gene_disease_relation_type",
                "classification",
                "mode_of_inheritance",
                "evaluation_date",
                "study_id",
                "aggregation_value",
                "report_url",
                "comment",
            ),
            # drug-gene
            *_text("drug_key", "source_drug_id", "drug_link_method"),
            *_text("drug_gene_relation_type", "relation_detail", "evidence_level"),
            *_text("labeling_sections", "therapeutic_area", "pubmed_id"),
            # pharmacogenomic association and phenotype annotation (a subject
            # and an object, or a subject and a phenotype)
            *_text("subject_type", "subject_id", "subject_name", "subject_key"),
            *_text("subject_source_id", "subject_link_method"),
            *_text("object_type", "object_id", "object_name", "object_key"),
            *_text("pharmacogenomic_evidence", "association"),
            *_text("pharmacokinetic", "pharmacodynamic"),
            *_text("phenotype_key"),
            ("is_negated", "boolean"),
            *_text("frequency", "onset", "sex", "modifier", "aspect"),
            *_text("phenotype_evidence", "reference", "biocuration"),
            # pathway member
            *_text("member_type", "member_key", "member_id_type", "member_id"),
            *_text("member_link_method", "pathway_key", "pathway_evidence", "species"),
            # protein interaction
            *_text("interaction_id", "interactor_a_id", "interactor_b_id"),
            *_text("protein_a_key", "protein_b_key", "gene_a_key", "gene_b_key"),
            *_text("experimental_system", "experimental_system_type", "throughput"),
            ("interaction_score", "double"),
            *[(f"channel_{name}_score", "int") for name in STRING_CHANNELS],
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
                "ontology_term_qualifier_ids",
                "ontology_term_qualifier_names",
                "ontology_term_types",
            ),
            # ClinVar variation member (a complex variation and one of its alleles)
            ("variation_interpreted", "boolean"),
            # the link of an object (trial condition or intervention, drug B of a
            # drug interaction)
            ("object_link_method", "string"),
            # homology (Ensembl Compara): identity of gene A to gene B and back
            *_number("double", "percent_identity", "homology_percent_identity"),
            *_number(
                "double",
                "gene_order_conservation_score",
                "whole_genome_alignment_coverage",
            ),
            ("is_high_confidence", "boolean"),
            # source labels of a gene-disease record (ClinGen) and the gene region
            *_text("source_gene_symbol", "source_disease_name"),
            *_text("standard_operating_procedure", "cytogenetic_band"),
            ("genomic_location", "string"),
            # Open Targets: association novelty, GWAS evidence fields, mechanism target
            ("novelty_score", "double"),
            *_text("target_from_source_id", "disease_from_source_mapped_id"),
            *_text("datatype_id", "curation_date", "publication_date"),
            ("resource_score", "double"),
            *_text("quality_controls", "target_name", "target_type"),
            # CPIC pair attributes
            *_text("clinpgx_level", "pharmacogenomic_testing"),
            *_number("boolean", "used_for_recommendation", "is_provisional"),
            # HPO phenotype name of a gene annotation
            ("phenotype_name", "string"),
            # BioGRID interactor details (A and B)
            *[
                (f"{name}_{side}", "string")
                for name in (
                    "source_gene",
                    "systematic_name",
                    "official_symbol",
                    "synonyms",
                    "swiss_prot_accessions",
                    "trembl_accessions",
                    "refseq_accessions",
                )
                for side in ("a", "b")
            ],
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
            *_text(
                "authority_number",
                "literature_reference",
                "sent_to_manufacturer",
            ),
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
            ("dose_record_number", "int"),
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
        ],
        # The reactions, outcomes and report sources of a case, and the
        # indications and therapies of a drug sequence, as rows; `item_kind`
        # names the list. Case items carry no drug sequence.
        "faers_case_item": [
            *_text("quarter", "primary_id", "case_id", "drug_sequence"),
            *_text("item_kind", "item_value", "drug_recurrence_action"),
            *_text(
                "therapy_start_date_raw",
                "therapy_end_date_raw",
                "therapy_duration",
                "therapy_duration_unit",
            ),
        ],
    }
)

# Clinical trial columns (AACT studies and the 1:1 eligibilities): source column ->
# Silver column; an abbreviation of the source is spelled out.
TRIAL_STUDY_COLUMNS = {
    "nlm_download_date_description": "nlm_download_date_description",
    "study_first_submitted_date": "study_first_submitted_date",
    "results_first_submitted_date": "results_first_submitted_date",
    "disposition_first_submitted_date": "disposition_first_submitted_date",
    "last_update_submitted_date": "last_update_submitted_date",
    "study_first_submitted_qc_date": "study_first_submitted_quality_check_date",
    "study_first_posted_date": "study_first_posted_date",
    "study_first_posted_date_type": "study_first_posted_date_type",
    "results_first_submitted_qc_date": "results_first_submitted_quality_check_date",
    "results_first_posted_date": "results_first_posted_date",
    "results_first_posted_date_type": "results_first_posted_date_type",
    "disposition_first_submitted_qc_date": (
        "disposition_first_submitted_quality_check_date"
    ),
    "disposition_first_posted_date": "disposition_first_posted_date",
    "disposition_first_posted_date_type": "disposition_first_posted_date_type",
    "last_update_submitted_qc_date": "last_update_submitted_quality_check_date",
    "last_update_posted_date": "last_update_posted_date",
    "last_update_posted_date_type": "last_update_posted_date_type",
    "start_month_year": "start_month_year",
    "start_date_type": "start_date_type",
    "start_date": "start_date",
    "verification_month_year": "verification_month_year",
    "verification_date": "verification_date",
    "completion_month_year": "completion_month_year",
    "completion_date_type": "completion_date_type",
    "completion_date": "completion_date",
    "primary_completion_month_year": "primary_completion_month_year",
    "primary_completion_date_type": "primary_completion_date_type",
    "primary_completion_date": "primary_completion_date",
    "target_duration": "target_duration",
    "study_type": "study_type",
    "acronym": "acronym",
    "baseline_population": "baseline_population",
    "brief_title": "brief_title",
    "official_title": "official_title",
    "overall_status": "overall_status",
    "last_known_status": "last_known_status",
    "phase": "phase",
    "enrollment": "enrollment",
    "enrollment_type": "enrollment_type",
    "source": "sponsor_source",
    "limitations_and_caveats": "limitations_and_caveats",
    "number_of_arms": "number_of_arms",
    "number_of_groups": "number_of_groups",
    "why_stopped": "why_stopped",
    "has_expanded_access": "has_expanded_access",
    "expanded_access_type_individual": "expanded_access_type_individual",
    "expanded_access_type_intermediate": "expanded_access_type_intermediate",
    "expanded_access_type_treatment": "expanded_access_type_treatment",
    "has_dmc": "has_data_monitoring_committee",
    "is_fda_regulated_drug": "is_fda_regulated_drug",
    "is_fda_regulated_device": "is_fda_regulated_device",
    "is_unapproved_device": "is_unapproved_device",
    "is_ppsd": "is_pediatric_postmarket_surveillance_device",
    "is_us_export": "is_us_export",
    "biospec_retention": "biospecimen_retention",
    "biospec_description": "biospecimen_description",
    "ipd_time_frame": "individual_participant_data_time_frame",
    "ipd_access_criteria": "individual_participant_data_access_criteria",
    "ipd_url": "individual_participant_data_url",
    "plan_to_share_ipd": "plan_to_share_individual_participant_data",
    "plan_to_share_ipd_description": (
        "plan_to_share_individual_participant_data_description"
    ),
    "created_at": "registry_created_at",
    "updated_at": "registry_updated_at",
    "source_class": "sponsor_source_class",
    "delayed_posting": "delayed_posting",
    "expanded_access_nctid": "expanded_access_nct_id",
    "expanded_access_status_for_nctid": "expanded_access_status_for_nct_id",
    "fdaaa801_violation": "fda_amendments_act_801_violation",
    "baseline_type_units_analyzed": "baseline_type_units_analyzed",
    "patient_registry": "patient_registry",
}
TRIAL_ELIGIBILITY_COLUMNS = {
    "sampling_method": "sampling_method",
    "gender": "eligible_gender",
    "minimum_age": "minimum_age",
    "maximum_age": "maximum_age",
    "healthy_volunteers": "healthy_volunteers",
    "population": "eligible_population",
    "criteria": "eligibility_criteria",
    "gender_description": "gender_description",
    "gender_based": "gender_based",
    "adult": "accepts_adults",
    "child": "accepts_children",
    "older_adult": "accepts_older_adults",
}
# IEDB columns (source column -> Silver column) of the epitope export and of the
# MHC ligand assays.
IEDB_EPITOPE_COLUMNS = {
    "Epitope_Object_Type": "epitope_object_type",
    "Epitope_Name": "epitope_name",
    "Epitope_Modified_Residue_s": "epitope_modified_residues",
    "Epitope_Modifications": "epitope_modifications",
    "Epitope_Starting_Position": "epitope_starting_position",
    "Epitope_Ending_Position": "epitope_ending_position",
    "Epitope_IRI": "epitope_iri",
    "Epitope_Synonyms": "epitope_synonyms",
    "Epitope_Source_Molecule": "source_molecule",
    "Epitope_Source_Molecule_IRI": "source_molecule_iri",
    "Epitope_Molecule_Parent": "molecule_parent",
    "Epitope_Molecule_Parent_IRI": "molecule_parent_iri",
    "Epitope_Source_Organism": "source_organism",
    "Epitope_Source_Organism_IRI": "source_organism_iri",
    "Epitope_Species": "species",
    "Epitope_Species_IRI": "species_iri",
    "Related_Object_Epitope_Relation": "related_object_epitope_relation",
    "Related_Object_Object_Type": "related_object_type",
    "Related_Object_Name": "related_object_name",
    "Related_Object_Starting_Position": "related_object_starting_position",
    "Related_Object_Ending_Position": "related_object_ending_position",
    "Related_Object_IRI": "related_object_iri",
    "Related_Object_Synonyms": "related_object_synonyms",
    "Related_Object_Source_Molecule": "related_object_source_molecule",
    "Related_Object_Source_Molecule_IRI": "related_object_source_molecule_iri",
    "Related_Object_Molecule_Parent": "related_object_molecule_parent",
    "Related_Object_Molecule_Parent_IRI": "related_object_molecule_parent_iri",
    "Related_Object_Source_Organism": "related_object_source_organism",
    "Related_Object_Source_Organism_IRI": "related_object_source_organism_iri",
    "Related_Object_Species": "related_object_species",
    "Related_Object_Species_IRI": "related_object_species_iri",
}
IEDB_ASSAY_COLUMNS = {
    "Reference_PMID": "reference_pubmed_id",
    "Reference_Date": "reference_date",
    "Epitope_Object_Type": "epitope_object_type",
    "Epitope_Name": "epitope_name",
    "Epitope_Reference_Name": "epitope_reference_name",
    "Epitope_Modified_residues": "epitope_modified_residues",
    "Epitope_Modifications": "epitope_modifications",
    "Epitope_Starting_Position": "epitope_starting_position",
    "Epitope_Ending_Position": "epitope_ending_position",
    "Epitope_Source_Molecule": "source_molecule",
    "Epitope_Source_Molecule_IRI": "source_molecule_iri",
    "Epitope_Molecule_Parent": "molecule_parent",
    "Epitope_Molecule_Parent_IRI": "molecule_parent_iri",
    "Epitope_Source_Organism": "source_organism",
    "Epitope_Species": "species",
    "Epitope_Species_IRI": "species_iri",
    "Related_Object_Epitope_Relation": "related_object_epitope_relation",
    "Related_Object_Name": "related_object_name",
    "Host_Name": "host_name",
    "Host_IRI": "host_iri",
    "in_vivo_Process_Process_Type": "in_vivo_process_type",
    "in_vivo_Process_Disease": "in_vivo_process_disease",
    "in_vivo_Process_Disease_IRI": "in_vivo_process_disease_iri",
    "Assay_Method": "assay_method",
    "Assay_Response_measured": "assay_response_measured",
    "Assay_Qualitative_Measurement": "assay_qualitative_measurement",
    "Assay_Measurement_Inequality": "assay_measurement_inequality",
    "Assay_Quantitative_measurement": "assay_quantitative_measurement",
    "Assay_Units": "assay_units",
    "Antigen_Presenting_Cell_Name": "antigen_presenting_cell_name",
    "MHC_Restriction_Name": "mhc_restriction_name",
    "MHC_Restriction_Class": "mhc_restriction_class",
    "MHC_Restriction_Evidence_Code": "mhc_restriction_evidence_code",
}
GENOMICS_STRUCTURES.update(
    {
        # One wide row per registered study, with its eligibility.
        "clinical_trial": [
            ("nct_id", "string"),
            *[(name, "string") for name in TRIAL_STUDY_COLUMNS.values()],
            *[(name, "string") for name in TRIAL_ELIGIBILITY_COLUMNS.values()],
            ("source_dataset", "string"),
        ],
        # A per-residue score of a protein model (AlphaFold pLDDT and its band).
        "protein_residue_score": [
            *_text("protein_key", "uniprot_accession", "protein_link_method"),
            ("fragment", "string"),
            ("residue_number", "int"),
            *_text("residue", "score_name"),
            ("score_value", "double"),
            *_text("score_band", "source_dataset"),
        ],
        # The CRISPR gene effect of a gene in a cell line (DepMap Chronos score).
        "gene_dependency": [
            *_text("cell_line_key", "gene_key", "gene_symbol", "ncbi_gene_id"),
            ("gene_link_method", "string"),
            ("gene_effect", "double"),
            ("source_dataset", "string"),
        ],
        # A predicted enhancer-gene link of one biosample (ENCODE-rE2G).
        "regulatory_element_gene_link": [
            *_text("element_key", "contig"),
            *_number("bigint", "start_position", "end_position"),
            *_text("element_class", "gene_key", "gene_link_method"),
            *_text("target_gene_symbol", "target_gene_ensembl_id"),
            ("target_gene_transcription_start_site", "bigint"),
            ("is_self_promoter", "boolean"),
            *_text("biosample", "cell_type"),
            ("score", "double"),
            ("source_dataset", "string"),
        ],
        "epitope": [
            ("epitope_id", "string"),
            *[(name, "string") for name in IEDB_EPITOPE_COLUMNS.values()],
            *_text("protein_key", "protein_link_method", "source_dataset"),
        ],
        # The yearly Open Targets association score of a gene and a disease.
        "gene_disease_score_year": [
            *_text("gene_key", "disease_key", "source_gene_id", "source_disease_id"),
            *_text("gene_link_method", "disease_link_method"),
            *_text("association_level", "datasource_id"),
            ("year", "int"),
            *_number("double", "association_score", "novelty", "yearly_evidence_count"),
            ("source_dataset", "string"),
        ],
        "epitope_mhc_assay": [
            *_text("assay_id", "epitope_id"),
            *[(name, "string") for name in IEDB_ASSAY_COLUMNS.values()],
            *_text("protein_key", "protein_link_method", "source_dataset"),
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
    "versions",
    "latest_version",
    "link_method",
    "genes_sharing_symbol",
    "feature_id",
    "reference_kind",
    "link_kind",
    "item_kind",
    "relation_kind",
    "relation_subkind",
    "disease_link_method",
    "gene_link_method",
    "drug_link_method",
    "member_link_method",
    "subject_link_method",
    "variant_link_method",
    "object_link_method",
    "protein_link_method",
    "score_band",
    "in_tahoe",
    "in_depmap",
    "is_generic_label",
    "pairing_status",
    "parse_status",
    "review_stars",
    "is_negated",
    "variant_rows",
    "distinct_index_values",
    "distinct_mean_embedding_values",
    "mutation_records",
    *VARIANT_SOURCE_FLAGS.values(),
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
    "crosswalk_method_precedence",
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
    "clinvar_snapshot_dates",
    "dbvar_assembly_by_study",
    "clinical_features",
    "disease_features",
    "pharmacogene_features",
    "variant_impact_features",
    "structural_variant_features",
    "pharmacogene_gene_features",
    "drug_response_features",
    "expression_features",
    "protein_family_features",
    "genetic_test_features",
    "cancer_features",
    "population_features",
    "orthologue_depth",
    "gene_essentiality",
    "plddt_bands",
)

# Band lists of the feature rules: `from` falls (first match wins).
_FEATURE_BANDS = (
    ("clinical_features", "expression_context"),
    ("clinical_features", "mutation_burden"),
    ("disease_features", "disease_count_category"),
    ("disease_features", "association_strength"),
    ("disease_features", "disease_complexity"),
    ("disease_features", "disease_complexity_score"),
    ("disease_features", "priority_tier"),
    ("disease_features", "annotation_priority"),
    ("pharmacogene_features", "burden"),
    ("pharmacogene_features", "priority"),
    ("pharmacogene_features", "expression_breadth"),
    ("variant_impact_features", "impact_tier"),
    ("variant_impact_features", "disease_impact_category"),
    ("variant_impact_features", "gene_impact_burden"),
    ("variant_impact_features", "lof_tolerance"),
    ("structural_variant_features", "disruption"),
    ("structural_variant_features", "gene_count_points"),
    ("structural_variant_features", "pharmacogene_points"),
    ("structural_variant_features", "omim_gene_points"),
    ("structural_variant_features", "size_points"),
    ("pharmacogene_gene_features", "variant_impact_burden"),
    ("pharmacogene_gene_features", "cancer_mutation_burden"),
    ("pharmacogene_gene_features", "priority"),
    ("drug_response_features", "priority"),
    ("expression_features", "level_category"),
    ("expression_features", "significance_points"),
    ("genetic_test_features", "availability_category"),
    ("genetic_test_features", "disease_test_correlation"),
    ("genetic_test_features", "variant_test_coverage_level"),
    ("genetic_test_features", "population_test_priority"),
    ("genetic_test_features", "test_recommendation_tier"),
    ("cancer_features", "mutation_frequency_category"),
    ("population_features", "disease_allele_frequency"),
    ("population_features", "expression_frequency_correlation"),
    ("gene_essentiality", "classes"),
    ("plddt_bands", "bands"),
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
    order = rules["crosswalk_method_precedence"]
    if not order or len(set(order)) != len(order):
        problems.append(f"crosswalk_method_precedence must be unique: {order}")
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
    for section, name in _FEATURE_BANDS:
        starts = [entry["from"] for entry in rules[section][name]]
        if starts != sorted(starts, reverse=True):
            problems.append(f"{section}.{name}: `from` values must fall: {starts}")
    ranks = [entry["rank"] for entry in rules["orthologue_depth"]["species"].values()]
    if len(set(ranks)) != len(rules["orthologue_depth"]["levels"]):
        problems.append(
            f"orthologue_depth: species ranks must name each level: {ranks}"
        )
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


def register_bronze_read(qualified: str, text, parsed) -> None:
    """Register a lenient read of `<source>.<dataset>.<column>` (Bronze table
    `<source>_<dataset>`) for the rejected-values check."""
    dataset, column = qualified.rsplit(".", 1)
    register_lenient(qualified, bronze_table_name(dataset), text, parsed, column)


NUMBER_TEXT = r"^[-+]?(\d+(\.\d*)?|\.\d+)([eE][-+]?\d+)?$"


def typed_number(column, spark_type: str, rules: dict, qualified: str | None = None):
    """A cleaned text column as a number (`int`, `bigint` or `double`); null when
    the text is no number. The cast goes through double, so `12.0` and `.5` are
    read."""
    text = clean_placeholders(column, rules, qualified)
    number = text.rlike(NUMBER_TEXT)
    value = text.cast("double")
    if spark_type == "double":
        parsed = F.when(number, value)
    else:
        target = {"int": "int", "bigint": "long"}[spark_type]
        parsed = F.when(number, value.cast(target))
    if qualified:
        register_bronze_read(qualified, text, parsed)
    return parsed


def label_number(column, labels: dict, rules: dict, qualified: str):
    """An integer from a text label by the `labels` map; null for another label
    (counted by the rejected-values check)."""
    text = clean_placeholders(column, rules, qualified)
    parsed = F.lit(None).cast("int")
    for label, number in labels.items():
        parsed = F.when(text == label, F.lit(int(number))).otherwise(parsed)
    register_bronze_read(qualified, text, parsed)
    return parsed


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
    snapshot_col: str | None = None,
):
    """One claim row per entity, field and value; `fields` maps a field to its
    source column (a dict, or a list of pairs when a field has several columns).
    A null or placeholder gives no row. `snapshot_col` names the ClinVar snapshot
    column of the frame (`source_snapshot` is null without it)."""
    snapshot = F.col(snapshot_col) if snapshot_col else F.lit(None).cast("string")
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
                snapshot.alias("source_snapshot"),
            ).filter(F.col("entity_key").isNotNull() & F.col("value_text").isNotNull())
        )
    return _reduce(lambda left, right: left.unionByName(right), parts)


def resolve_master(claims, *, entity_type: str, fields):
    """(masters, conflicts) from a claims frame. A master row has each field, its
    `<field>_source` and `<field>_method` (`primary`, `filled` or `unranked`);
    left-join it onto the entity list. `conflicts` lists fields whose sources
    gave more than one value. Among claims of one rank the claim whose snapshot
    run ends newest wins; the older runs stay as claims."""
    fields = list(fields)
    scoped = claims.filter(
        (F.col("entity_type") == entity_type) & F.col("field").isin(fields)
    )
    snapshot = (
        F.col("last_source_snapshot")
        if "last_source_snapshot" in claims.columns
        else F.lit(None).cast("string")
    )
    pair = Window.partitionBy("entity_key", "field")
    ranked = scoped.withColumn(
        "_distinct_values", F.size(F.collect_set("value_text").over(pair))
    ).withColumn(
        "_order",
        F.row_number().over(
            pair.orderBy(
                F.col("source_rank").asc_nulls_last(),
                F.when(snapshot == "current", F.lit("9999"))
                .otherwise(snapshot)
                .desc_nulls_last(),
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
        F.lit(None).cast("string").alias("source_snapshot"),
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
    contig_gi=None,
):
    """A frame in the column order of `entity_coordinate` (before the computed
    columns); `key` is a column name, the others are Columns; `contig_gi` is the
    GenBank gi of the contig where the source gives one."""
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
        (contig_gi if contig_gi is not None else F.lit(None))
        .cast("string")
        .alias("contig_genbank_gi"),
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


def keep_best_match_method(frame, rules):
    """One row per entity, id and source column: the row whose match method is
    first in `crosswalk_method_precedence` (a method not in the list comes last,
    then by name)."""
    order = rules["crosswalk_method_precedence"]
    position = F.array_position(
        F.array(*[F.lit(method) for method in order]), F.col("match_method")
    )
    rank = F.when(position == 0, F.lit(len(order) + 1)).otherwise(position)
    best = Window.partitionBy(
        "entity_type",
        "entity_key",
        "id_type",
        "id_value",
        "source_dataset",
        "source_field",
    ).orderBy(rank, F.col("match_method"))
    return (
        frame.withColumn("_method_order", F.row_number().over(best))
        .filter(F.col("_method_order") == 1)
        .drop("_method_order")
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
            F.lit(None).cast("string").alias("source_snapshot"),
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


def conform_union(df, table: str):
    """Like `conform_genomics`, for a table that unites several kinds of rows:
    a declared column the frame does not have becomes null."""
    return df.select(
        *[
            (F.col(name) if name in df.columns else F.lit(None)).cast(kind).alias(name)
            for name, kind in GENOMICS_STRUCTURES[table]
        ],
        *GOVERNANCE_COLUMNS,
    )


# COMMAND ----------

# DBTITLE 1,Entity builders: Bronze reads and unions


def _bronze(table):
    return read_genomics_bronze(table)


def _union(frames):
    return _reduce(lambda left, right: left.unionByName(right), frames)


def genetic_test(rules=None):
    """GTR rows with `test_key` (the accession without its version)."""
    return _bronze("gtr_test_condition_gene").withColumn(
        "test_key", F.regexp_replace(F.trim(F.col("accession_version")), r"\.\d+$", "")
    )


# COMMAND ----------

# DBTITLE 1,Entity builders: derived attribute rows


def derived_rows(
    df,
    *,
    entity_type,
    key,
    attribute,
    rule_name,
    method,
    version,
    as_of,
    value_text=None,
    value_number=None,
    value_boolean=None,
    matched=None,
    text_source=None,
):
    """Rows in the column order of `entity_derived_attribute`; `attribute`,
    `value_*` and `matched` are Columns or texts, `key` is a column name."""

    def column(value, kind):
        return (
            (F.lit(value) if isinstance(value, str) else value)
            if value is not None
            else F.lit(None).cast(kind)
        )

    if value_boolean is not None:
        value_type = "boolean"
    elif value_number is not None:
        value_type = "number"
    else:
        value_type = "text"
    return df.select(
        F.lit(entity_type).alias("entity_type"),
        F.col(key).cast("string").alias("entity_key"),
        column(attribute, "string").alias("attribute"),
        column(value_text, "string").alias("value_text"),
        column(value_number, "double").cast("double").alias("value_number"),
        column(value_boolean, "boolean").alias("value_boolean"),
        F.lit(value_type).alias("value_type"),
        F.lit(method).alias("method"),
        column(matched, "string").alias("matched_text"),
        F.lit(text_source).cast("string").alias("text_source"),
        F.lit(rule_name).alias("rule_name"),
        F.lit(version).alias("rule_version"),
        as_of.alias("as_of_date"),
    )


def keyword_rows(
    df,
    *,
    entity_type,
    key,
    text,
    group,
    rules,
    text_source,
    version,
    as_of,
    patterns=None,
    suffix=None,
):
    """One true-flag row per entity and keyword rule that matches the text column;
    the first match is kept in `matched_text`. The rules are `patterns` (a dict) or
    the text rules of `group`; a `suffix` text is added to each attribute name."""
    parts = []
    for attribute, pattern in (patterns or rules["text_rules"][group]).items():
        hit = df.filter(F.col(text).rlike("(?i)" + pattern))
        parts.append(
            derived_rows(
                hit,
                entity_type=entity_type,
                key=key,
                attribute=attribute + (suffix or ""),
                rule_name=group,
                method="keyword",
                version=version,
                as_of=as_of,
                value_boolean=F.lit(True),
                matched=F.regexp_extract(F.col(text), "(?i)(" + pattern + ")", 1),
                text_source=text_source,
            )
        )
    return _union(parts)


def captured_rows(
    df, *, entity_type, key, text, group, rules, text_source, version, as_of, numbers=()
):
    """One row per entity and capture rule of `group`: the first capture group
    of the first match, as text or (for `numbers`) as a number."""
    parts = []
    for attribute, pattern in rules["text_rules"][group].items():
        regex = "(?i)" + pattern
        value = F.nullif(F.regexp_extract(F.col(text), regex, 1), F.lit(""))
        hit = df.filter(F.col(text).rlike(regex)).withColumn("_captured", value)
        hit = hit.filter(F.col("_captured").isNotNull())
        extra = (
            {"value_number": F.col("_captured").cast("double")}
            if attribute in numbers
            else {"value_text": F.col("_captured")}
        )
        parts.append(
            derived_rows(
                hit,
                entity_type=entity_type,
                key=key,
                attribute=attribute,
                rule_name=group,
                method="keyword",
                version=version,
                as_of=as_of,
                matched=F.regexp_extract(F.col(text), regex, 0),
                text_source=text_source,
                **extra,
            )
        )
    return _union(parts)


def first_match_expr(entries, flags, value_key, default):
    """Column expr: the `value_key` of the first rule whose `all` flags are in
    the array column `flags` and whose `none` flags are not; else `default`.
    (`flags` is an intermediate list of flag names inside one expression, not a
    column of a Silver table.)"""
    expr = F.lit(default)
    for entry in reversed(entries):
        condition = F.lit(True)
        for name in entry.get("all", []):
            condition = condition & F.array_contains(flags, name)
        for name in entry.get("none", []):
            condition = condition & ~F.array_contains(flags, name)
        expr = F.when(condition, F.lit(entry[value_key])).otherwise(expr)
    return expr


# COMMAND ----------

# DBTITLE 1,Entity builders: variant datasets
CLINVAR = "clinvar.variant_summary"
GNOMAD = "gnomad.variants"
ALPHAMISSENSE = "alphamissense.variant_scores"
CADD = "cadd.scores"
DBVAR = "dbvar.variants"
PHARMGKB_VARIANTS = "pharmgkb.variants"
LR_CODING = "long_range_benchmark.pathogenic_coding"
LR_NON_CODING = "long_range_benchmark.pathogenic_non_coding"
LR_EQTL = "long_range_benchmark.causal_eqtl"
CLINVAR_BENIGN = "clinvar_vs_benign.variants"
GNOMAD_BALANCED = "gnomad_balanced.variants"
GNOMAD_MTDNA = "gnomad_mtdna.variants"
THOUSAND_GENOMES = "thousand_genomes.variants"

# COMMAND ----------

# DBTITLE 1,Entity builders: one frame per variant source, with its key and ids

ID_ARRAY = "array<struct<id_type:string,id_value:string,source_field:string>>"


def _clean(name, rules, dataset):
    return clean_placeholders(name, rules, f"{dataset}.{name}")


def _id_item(id_type, value, field):
    return F.struct(
        F.lit(id_type).alias("id_type"),
        value.cast("string").alias("id_value"),
        F.lit(field).alias("source_field"),
    )


def _variant_frame(
    df, *, dataset, assembly, chrom, pos, ref, alt, end, ids, method, own=None
):
    """Add the common variant columns to a source frame; the source columns stay.
    `own` is the key of a row without usable alleles (else the key stays null).
    `v_ids` is an intermediate list of ids that the crosswalk explodes; it is not
    a column of a Silver table."""
    ids_column = F.array(*ids) if ids else F.array().cast(ID_ARRAY)
    key = variant_key_expr(assembly, chrom, pos, ref, alt)
    how = F.lit(method)
    if own is not None:
        how = F.when(key.isNotNull(), how).otherwise(F.lit("own_source_id"))
        key = F.coalesce(key, own)
    return (
        df.withColumn("variant_key", key)
        .withColumn("key_method", how)
        .withColumn(
            "v_assembly", assembly if not isinstance(assembly, str) else F.lit(assembly)
        )
        .withColumn("v_contig", chromosome_expr(chrom))
        .withColumn("v_position", pos)
        .withColumn("v_reference", F.upper(F.trim(ref)))
        .withColumn("v_alternate", F.upper(F.trim(alt)))
        .withColumn("v_end", end)
        .withColumn("v_source", F.lit(dataset))
        .withColumn(
            "v_ids", F.filter(ids_column, lambda item: item["id_value"].isNotNull())
        )
        .filter(F.col("variant_key").isNotNull())
    )


def _rsid(column, rules, dataset):
    digits = _clean(column, rules, dataset)
    return F.when(digits.rlike(r"^\d+$"), F.concat(F.lit("rs"), digits))


def clinvar_frame(rules):
    d = CLINVAR
    assembly = F.coalesce(_clean("Assembly", rules, d), F.lit("unspecified"))
    frame = _bronze("clinvar_variant_summary")
    return _variant_frame(
        frame.withColumn("RS_dbSNP", _rsid("RS_dbSNP", rules, d)).withColumn(
            "GeneID",
            F.when(
                _clean("GeneID", rules, d).rlike(r"^\d+$"), _clean("GeneID", rules, d)
            ),
        ),
        dataset=d,
        assembly=assembly,
        chrom=_clean("Chromosome", rules, d),
        pos=F.coalesce(_clean("PositionVCF", rules, d), _clean("Start", rules, d)),
        ref=_clean("ReferenceAlleleVCF", rules, d),
        alt=_clean("AlternateAlleleVCF", rules, d),
        end=_clean("Stop", rules, d),
        ids=[
            _id_item("clinvar_allele", F.col("AlleleID"), "AlleleID"),
            _id_item("clinvar_variation", F.col("VariationID"), "VariationID"),
            _id_item("rsid", F.col("RS_dbSNP"), "RS_dbSNP"),
            _id_item("dbvar", _clean("nsv_esv_dbVar", rules, d), "nsv_esv_dbVar"),
        ],
        method="vcf_alleles",
        own=F.concat(F.lit("clinvar_allele:"), F.col("AlleleID"), F.lit(":"), assembly),
    )


def _vcf_frame(
    table,
    dataset,
    rules,
    *,
    chrom,
    pos,
    ref,
    alt,
    assembly="GRCh38",
    method="vcf_alleles_assumed_grch38",
    ids=(),
):
    return _variant_frame(
        _bronze(table),
        dataset=dataset,
        assembly=assembly,
        chrom=_clean(chrom, rules, dataset),
        pos=_clean(pos, rules, dataset),
        ref=_clean(ref, rules, dataset),
        alt=_clean(alt, rules, dataset),
        end=F.lit(None).cast("string"),
        ids=list(ids),
        method=method,
    )


def alphamissense_frame(rules):
    d = ALPHAMISSENSE
    names = rules["ucsc_assembly_names"]
    mapped = F.lit(None).cast("string")
    for label, canonical in names.items():
        mapped = F.when(
            F.lower(F.trim(F.col("genome"))) == label, F.lit(canonical)
        ).otherwise(mapped)
    assembly = F.coalesce(mapped, _clean("genome", rules, d), F.lit("unspecified"))
    return _variant_frame(
        _bronze("alphamissense_variant_scores"),
        dataset=d,
        assembly=assembly,
        chrom=_clean("CHROM", rules, d),
        pos=_clean("POS", rules, d),
        ref=_clean("REF", rules, d),
        alt=_clean("ALT", rules, d),
        end=F.lit(None).cast("string"),
        ids=[],
        method="vcf_alleles",
    )


def dbvar_frame(rules):
    """dbVar structural variants; the assembly comes from the study (the staged
    file names carry it), the interval end from `END`."""
    d = DBVAR
    own = F.concat(F.lit("dbvar:"), F.col("study"), F.lit(":"), F.col("ID"))
    by_study = rules["dbvar_assembly_by_study"]
    pairs = [F.lit(part) for item in by_study.items() for part in item]
    assembly = F.coalesce(F.create_map(*pairs)[F.col("study")], F.lit("unspecified"))
    return _variant_frame(
        _bronze("dbvar_variants"),
        dataset=d,
        assembly=assembly,
        chrom=_clean("CHROM", rules, d),
        pos=_clean("POS", rules, d),
        ref=F.lit(None).cast("string"),
        alt=F.lit(None).cast("string"),
        end=_clean("END", rules, d),
        ids=[_id_item("dbvar", _clean("ID", rules, d), "ID")],
        method="vcf_alleles",
        own=own,
    )


def current_snapshot(df, rules):
    """The rows whose snapshot run reaches the current ClinVar snapshot and the
    rows of sources without one."""
    return df.filter(
        F.col("last_source_snapshot").isNull()
        | (F.col("last_source_snapshot") == rules["clinvar_current_snapshot"])
    )


def clinvar_snapshot_order(rules):
    """The ClinVar snapshot labels in Bronze, oldest first (a label that is no
    date sorts by its date in `clinvar_snapshot_dates`)."""
    dates = rules["clinvar_snapshot_dates"]
    labels = [
        row["snapshot"]
        for row in read_genomics_bronze("clinvar_variant_summary")
        .select("snapshot")
        .distinct()
        .collect()
        if row["snapshot"]
    ]
    return sorted(labels, key=lambda label: dates.get(label, label))


def snapshot_runs(df, order):
    """`df` with `source_snapshot` replaced by `first_source_snapshot` and
    `last_source_snapshot`: one row per value and unbroken run of snapshots in
    `order`; a value missing from a snapshot starts a new run. Rows without a
    snapshot keep both null."""
    values = [c for c in df.columns if c != "source_snapshot"]
    rank = F.create_map(
        *[F.lit(part) for i, label in enumerate(order, 1) for part in (label, i)]
    )
    labels = F.array(*[F.lit(label) for label in order])
    plain = (
        df.filter(F.col("source_snapshot").isNull())
        .drop("source_snapshot")
        .distinct()
        .withColumn("first_source_snapshot", F.lit(None).cast("string"))
        .withColumn("last_source_snapshot", F.lit(None).cast("string"))
    )
    ranked = (
        df.filter(F.col("source_snapshot").isNotNull())
        .withColumn("_rank", rank[F.col("source_snapshot")])
        .drop("source_snapshot")
        .distinct()
    )
    runs = (
        ranked.withColumn(
            "_run",
            F.col("_rank")
            - F.row_number().over(Window.partitionBy(*values).orderBy("_rank")),
        )
        .groupBy(*values, "_run")
        .agg(F.min("_rank").alias("_first"), F.max("_rank").alias("_last"))
        .select(
            *values,
            F.element_at(labels, F.col("_first")).alias("first_source_snapshot"),
            F.element_at(labels, F.col("_last")).alias("last_source_snapshot"),
        )
    )
    return runs.unionByName(plain)


def expand_snapshot_runs(df, order):
    """One row per snapshot of each run, in `source_snapshot` (rows without a
    snapshot are dropped)."""
    rank = F.create_map(
        *[F.lit(part) for i, label in enumerate(order, 1) for part in (label, i)]
    )
    labels = F.array(*[F.lit(label) for label in order])
    return df.filter(F.col("first_source_snapshot").isNotNull()).withColumn(
        "source_snapshot",
        F.explode(
            F.slice(
                labels,
                rank[F.col("first_source_snapshot")],
                rank[F.col("last_source_snapshot")]
                - rank[F.col("first_source_snapshot")]
                + 1,
            )
        ),
    )


def interval_overlap_counts(spans, features, column: str):
    """(variant_key, `column`): the number of feature intervals that overlap each
    variant span, from running counts per contig, so no variant x feature row is
    built. `spans`: variant_key, contig, start_position, end_position; `features`:
    contig, f_start, f_end (one row per interval). Only counts above 0."""

    def running(feature_position, variant_position, feature_first):
        events = features.select(
            "contig",
            F.col(feature_position).alias("_position"),
            F.lit(1).alias("_feature"),
            F.lit(None).cast("string").alias("variant_key"),
        ).unionByName(
            spans.select(
                "contig",
                F.col(variant_position).alias("_position"),
                F.lit(0).alias("_feature"),
                "variant_key",
            )
        )
        tie = F.col("_feature").desc() if feature_first else F.col("_feature").asc()
        order = (
            Window.partitionBy("contig")
            .orderBy("_position", tie)
            .rowsBetween(Window.unboundedPreceding, Window.currentRow)
        )
        return (
            events.withColumn("_count", F.sum("_feature").over(order))
            .filter(F.col("_feature") == 0)
            .select("variant_key", "_count")
        )

    # started: f_start <= variant end; ended: f_end < variant start
    started = running("f_start", "end_position", True)
    ended = running("f_end", "start_position", False)
    return (
        started.withColumnRenamed("_count", "_started")
        .join(ended.withColumnRenamed("_count", "_ended"), "variant_key")
        .select("variant_key", (F.col("_started") - F.col("_ended")).alias(column))
        .filter(F.col(column) > 0)
    )


def variant_frames(rules):
    """(dataset, frame) of every source that has genomic coordinates."""
    return [
        (CLINVAR, clinvar_frame(rules)),
        (
            GNOMAD,
            _vcf_frame(
                "gnomad_variants",
                GNOMAD,
                rules,
                chrom="chr",
                pos="pos",
                ref="ref",
                alt="alt",
                method="vcf_alleles",
                ids=[_id_item("gnomad_variant", F.col("variant_id"), "variant_id")],
            ),
        ),
        (ALPHAMISSENSE, alphamissense_frame(rules)),
        (
            CADD,
            _vcf_frame(
                "cadd_scores",
                CADD,
                rules,
                chrom="chrom",
                pos="pos",
                ref="ref",
                alt="alt",
            ),
        ),
        (DBVAR, dbvar_frame(rules)),
        (
            LR_CODING,
            _vcf_frame(
                "long_range_benchmark_pathogenic_coding",
                LR_CODING,
                rules,
                chrom="CHROM",
                pos="POS",
                ref="REF",
                alt="ALT",
            ),
        ),
        (
            LR_NON_CODING,
            _vcf_frame(
                "long_range_benchmark_pathogenic_non_coding",
                LR_NON_CODING,
                rules,
                chrom="CHROM",
                pos="POS",
                ref="REF",
                alt="ALT",
            ),
        ),
        (
            LR_EQTL,
            _vcf_frame(
                "long_range_benchmark_causal_eqtl",
                LR_EQTL,
                rules,
                chrom="CHROM",
                pos="POS",
                ref="REF",
                alt="ALT",
            ),
        ),
        (
            CLINVAR_BENIGN,
            _vcf_frame(
                "clinvar_vs_benign_variants",
                CLINVAR_BENIGN,
                rules,
                chrom="chrom",
                pos="pos",
                ref="ref",
                alt="alt",
            ),
        ),
        (
            GNOMAD_BALANCED,
            _vcf_frame(
                "gnomad_balanced_variants",
                GNOMAD_BALANCED,
                rules,
                chrom="chrom",
                pos="pos",
                ref="ref",
                alt="alt",
            ),
        ),
        # gnomAD v3.1 mitochondrial variants and 1000 Genomes sites are GRCh38 files.
        (
            GNOMAD_MTDNA,
            _vcf_frame(
                "gnomad_mtdna_variants",
                GNOMAD_MTDNA,
                rules,
                chrom="chromosome",
                pos="position",
                ref="ref",
                alt="alt",
                method="vcf_alleles",
            ),
        ),
        (
            THOUSAND_GENOMES,
            _vcf_frame(
                "thousand_genomes_variants",
                THOUSAND_GENOMES,
                rules,
                chrom="CHROM",
                pos="POS",
                ref="REF",
                alt="ALT",
                method="vcf_alleles",
                ids=[
                    _id_item(
                        "rsid",
                        F.when(F.col("ID").rlike(r"^rs\d+$"), F.col("ID")),
                        "ID",
                    )
                ],
            ),
        ),
    ]


def _variant_id_rows(frame, dataset):
    rows = frame.select(
        F.col("variant_key").alias("entity_key"),
        F.explode("v_ids").alias("item"),
        F.col("key_method").alias("match_method"),
    )
    return rows.select(
        F.lit("variant").alias("entity_type"),
        "entity_key",
        F.col("item.id_type").alias("id_type"),
        F.col("item.id_value").alias("id_value"),
        F.lit(dataset).alias("source_dataset"),
        F.col("item.source_field").alias("source_field"),
        "match_method",
    )


def pharmgkb_variant_frame(rules, id_map):
    """PharmGKB variants keyed by an exact, unique rsID of another source, else
    by their own accession (`id_map`: unique rsID rows of the other sources)."""
    d = PHARMGKB_VARIANTS
    frame = _bronze("pharmgkb_variants").withColumn(
        "rsid_text",
        F.when(
            _clean("Variant_Name", rules, d).rlike(r"^rs\d+$"),
            _clean("Variant_Name", rules, d),
        ),
    )
    pairs = frame.select(
        F.col("Variant_ID").alias("own"),
        F.lit("rsid").alias("id_type"),
        F.col("rsid_text").alias("id_value"),
    ).filter(F.col("id_value").isNotNull())
    links = link_by_ids(pairs, id_map)
    frame = frame.join(links, frame["Variant_ID"] == links["own"], "left")
    own = F.concat(F.lit("pharmgkb_variant:"), F.col("Variant_ID"))
    return (
        frame.withColumn("variant_key", F.coalesce(F.col("entity_key"), own))
        .withColumn(
            "key_method",
            F.when(
                F.col("entity_key").isNotNull(), F.lit("rsid_via_other_source")
            ).otherwise(F.lit("own_source_id")),
        )
        .withColumn("v_source", F.lit(d))
        .withColumn(
            "v_ids",
            F.filter(
                F.array(
                    _id_item("pharmgkb_variant", F.col("Variant_ID"), "Variant_ID"),
                    _id_item("rsid", F.col("rsid_text"), "Variant_Name"),
                ),
                lambda item: item["id_value"].isNotNull(),
            ),
        )
        .filter(F.col("variant_key").isNotNull())
    )


def clinvar_other_ids(rules):
    """Crosswalk rows of the ClinVar `OtherIDs` list (`ClinGen:CA123,OMIM:...`): one
    row per listed id, the label in lower case as the id type."""
    item = F.explode(F.split(_clean("OtherIDs", rules, CLINVAR), r"\s*,\s*"))
    rows = (
        clinvar_frame(rules)
        .select("variant_key", "key_method", item.alias("item"))
        .withColumn("label", F.lower(F.regexp_extract("item", r"^([^:]+):", 1)))
        .withColumn("value", F.regexp_extract("item", r"^[^:]+:(.+)$", 1))
        .filter((F.col("label") != "") & (F.col("value") != ""))
    )
    return rows.select(
        F.lit("variant").alias("entity_type"),
        F.col("variant_key").alias("entity_key"),
        F.col("label").alias("id_type"),
        F.col("value").alias("id_value"),
        F.lit(CLINVAR).alias("source_dataset"),
        F.lit("OtherIDs").alias("source_field"),
        F.col("key_method").alias("match_method"),
    ).distinct()


def variant_crosswalk(rules):
    """Variant id rows (with the ClinVar `OtherIDs`); PharmGKB variants link through
    rsIDs that one variant of the other sources holds."""
    frames = variant_frames(rules)
    rows = _union([_variant_id_rows(frame, dataset) for dataset, frame in frames])
    rows = rows.unionByName(clinvar_other_ids(rules))
    id_map = unique_id_map(finish_crosswalk(rows), ["rsid"])
    pharmgkb = pharmgkb_variant_frame(rules, id_map)
    return rows.unionByName(_variant_id_rows(pharmgkb, PHARMGKB_VARIANTS))


def variant_id_map(xwalk):
    """The unique rsID rows of the variant crosswalk, without the PharmGKB rows
    (the map the PharmGKB link was made with)."""
    rows = xwalk.filter(
        (F.col("entity_type") == "variant")
        & (F.col("source_dataset") != PHARMGKB_VARIANTS)
    ).select(
        "entity_type",
        "entity_key",
        "id_type",
        "id_value",
        "source_dataset",
        "source_field",
        "match_method",
    )
    return unique_id_map(finish_crosswalk(rows), ["rsid"])


# COMMAND ----------

# DBTITLE 1,Entity builders: variant names and claims


def variant_names(rules, id_map):
    keyed = {"key_col": "entity_key", "rules": rules, "entity_type": "variant"}
    clinvar = clinvar_frame(rules).withColumn("entity_key", F.col("variant_key"))
    gnomad = variant_frames(rules)[1][1].withColumn("entity_key", F.col("variant_key"))
    pharmgkb = pharmgkb_variant_frame(rules, id_map).withColumn(
        "entity_key", F.col("variant_key")
    )
    return _union(
        [
            names_frame(
                clinvar, dataset=CLINVAR, specs=[("Name", "hgvs_name", "text")], **keyed
            ),
            names_frame(
                gnomad,
                dataset=GNOMAD,
                specs=[("variant_id", "gnomad_variant_id", "text")],
                **keyed,
            ),
            names_frame(
                pharmgkb,
                dataset=PHARMGKB_VARIANTS,
                specs=[
                    ("Variant_Name", "name", "text"),
                    ("Synonyms", "synonym", "comma"),
                ],
                **keyed,
            ),
        ]
    )


def pharmgkb_variant_genes(rules, id_map, gene_map):
    """(entity_key, pharmgkb_gene_key, gene_link_method): the genes of a PharmGKB
    variant by its PharmGKB gene ids (`Gene_IDs`), else by its gene symbols
    (`Gene_Symbols`, position by position). `gene_map` holds the unique
    (id_type, id_value, entity_key) rows of the genes (`pharmgkb` and `symbol`)."""
    frame = pharmgkb_variant_frame(rules, id_map).withColumn(
        "entity_key", F.col("variant_key")
    )
    ids = F.split(_clean("Gene_IDs", rules, PHARMGKB_VARIANTS), r"\s*,\s*")
    symbols = F.split(_clean("Gene_Symbols", rules, PHARMGKB_VARIANTS), r"\s*,\s*")
    pairs = frame.select(
        "entity_key",
        F.posexplode_outer(ids).alias("position", "gene_id"),
        symbols.alias("symbols"),
    )
    symbol = F.element_at("symbols", F.col("position") + 1)
    pairs = pairs.withColumn("gene_symbol", F.upper(F.trim(symbol)))
    linked = link_entities(
        pairs.filter(F.col("gene_id").isNotNull() | F.col("gene_symbol").isNotNull()),
        [(F.trim(F.col("gene_id")), "pharmgkb"), (F.col("gene_symbol"), "symbol")],
        gene_map,
        "pharmgkb_gene_key",
        "gene_link_method",
    )
    return linked.select("entity_key", "pharmgkb_gene_key", "gene_link_method")


def variant_claims(rules, id_map, transcript_genes, gene_map):
    """Claims of the variant fields. `transcript_genes` (transcript_key,
    gene_key) turns the AlphaMissense transcript into a gene claim; `gene_map`
    links the PharmGKB variant genes. ClinVar claims carry their snapshot."""
    base = {"entity_type": "variant", "key_col": "entity_key", "rules": rules}
    clinvar = (
        clinvar_frame(rules)
        .withColumn("entity_key", F.col("variant_key"))
        .withColumnRenamed("snapshot", "source_snapshot")
    )
    alpha = (
        alphamissense_frame(rules)
        .withColumn("entity_key", F.col("variant_key"))
        .withColumn("tx_key", ensembl_unversioned_expr(F.col("transcript_id")))
        .join(
            F.broadcast(transcript_genes),
            F.col("tx_key") == F.col("transcript_key"),
            "inner",
        )
        .withColumn("transcript_id", F.col("gene_key"))
    )
    dbvar = dbvar_frame(rules).withColumn("entity_key", F.col("variant_key"))
    pharmgkb = pharmgkb_variant_frame(rules, id_map).withColumn(
        "entity_key", F.col("variant_key")
    )
    return _union(
        [
            claims_frame(
                clinvar,
                fields=[
                    ("variant_type", "Type"),
                    ("origin", "OriginSimple"),
                    ("rsid", "RS_dbSNP"),
                    ("gene_key", "GeneID"),
                    ("name", "Name"),
                    ("gene_symbol", "GeneSymbol"),
                    ("origin_detail", "Origin"),
                    ("chromosome_accession", "ChromosomeAccession"),
                    ("reference_allele_text", "ReferenceAllele"),
                    ("alternate_allele_text", "AlternateAllele"),
                    ("cytogenetic_location", "Cytogenetic"),
                    ("guidelines", "Guidelines"),
                    ("tested_in_genetic_testing_registry", "TestedInGTR"),
                    ("other_ids", "OtherIDs"),
                    ("submitter_categories", "SubmitterCategories"),
                ],
                source_dataset=CLINVAR,
                snapshot_col="source_snapshot",
                **base,
            ),
            claims_frame(
                alpha,
                fields=[("gene_key", "transcript_id")],
                source_dataset=ALPHAMISSENSE,
                **base,
            ),
            claims_frame(
                dbvar,
                fields=[
                    ("variant_type", "SVTYPE"),
                    ("name", "ID"),
                    ("structural_variant_length", "SVLEN"),
                    ("quality", "QUAL"),
                    ("source_file_type", "file_type"),
                ],
                numeric_fields=("structural_variant_length", "quality"),
                source_dataset=DBVAR,
                **base,
            ),
            claims_frame(
                pharmgkb,
                fields=[
                    ("name", "Variant_Name"),
                    ("genomic_location", "Location"),
                    ("variant_annotation_count", "Variant_Annotation_count"),
                    ("clinical_annotation_count", "Clinical_Annotation_count"),
                    (
                        "level_1_2_clinical_annotation_count",
                        "Level_1_2_Clinical_Annotation_count",
                    ),
                    ("guideline_annotation_count", "Guideline_Annotation_count"),
                    ("label_annotation_count", "Label_Annotation_count"),
                ],
                numeric_fields=(
                    "variant_annotation_count",
                    "clinical_annotation_count",
                    "level_1_2_clinical_annotation_count",
                    "guideline_annotation_count",
                    "label_annotation_count",
                ),
                source_dataset=PHARMGKB_VARIANTS,
                **base,
            ),
            claims_frame(
                variant_frames(rules)[-1][1].withColumn(
                    "entity_key", F.col("variant_key")
                ),
                fields=[("quality", "QUAL"), ("filter_status", "FILTER")],
                numeric_fields=("quality",),
                source_dataset=THOUSAND_GENOMES,
                **base,
            ),
            claims_frame(
                pharmgkb_variant_genes(rules, id_map, gene_map).filter(
                    F.col("pharmgkb_gene_key").isNotNull()
                ),
                fields=[("gene_key", "pharmgkb_gene_key")],
                source_dataset=PHARMGKB_VARIANTS,
                **base,
            ),
        ]
    )


# COMMAND ----------

# DBTITLE 1,Entity builders: id maps, symbol maps, links


def entity_id_map(xwalk, entity_type):
    """(id_type, id_value, entity_key) of the unique ids of one entity type."""
    return unique_id_map(xwalk.filter(F.col("entity_type") == entity_type))


def term_map(names, entity_type, term_types, id_type):
    """(id_type, id_value, entity_key) of the upper-case terms (of `term_types`)
    that exactly one entity of the type holds."""
    terms = (
        names.filter(
            (F.col("entity_type") == entity_type) & F.col("term_type").isin(*term_types)
        )
        .select(F.upper(F.col("term")).alias("id_value"), F.col("entity_key"))
        .distinct()
    )
    return (
        terms.groupBy("id_value")
        .agg(
            F.countDistinct("entity_key").alias("holders"),
            F.min("entity_key").alias("entity_key"),
        )
        .filter(F.col("holders") == 1)
        .select(F.lit(id_type).alias("id_type"), "id_value", "entity_key")
    )


def link_entities(df, specs, id_map, key_out, method_out):
    """Add `key_out` (the entity key of the first spec that finds one) and
    `method_out` (its id type, `unlinked` or `no_id`). A spec is (value Column,
    id type text or Column); `id_map` holds unique ids only, so no row is copied."""
    out = df
    keys, methods, values = [], [], []
    for index, (value, id_type) in enumerate(specs):
        tmp_value, tmp_key = f"_le_value_{index}", f"_le_key_{index}"
        tmp_id, tmp_type = f"_le_id_{index}", f"_le_type_{index}"
        out = out.withColumn(tmp_value, value)
        if isinstance(id_type, str):
            lookup = id_map.filter(F.col("id_type") == id_type).select(
                F.col("id_value").alias(tmp_id), F.col("entity_key").alias(tmp_key)
            )
            condition = F.col(tmp_value) == F.col(tmp_id)
            label = F.lit(id_type)
        else:
            out = out.withColumn(tmp_type, id_type)
            lookup = id_map.select(
                F.col("id_value").alias(tmp_id),
                F.col("id_type").alias(f"_le_maptype_{index}"),
                F.col("entity_key").alias(tmp_key),
            )
            condition = (F.col(tmp_value) == F.col(tmp_id)) & (
                F.col(tmp_type) == F.col(f"_le_maptype_{index}")
            )
            label = F.col(tmp_type)
        out = out.join(lookup, condition, "left")
        keys.append(F.col(tmp_key))
        methods.append(F.when(F.col(tmp_key).isNotNull(), label))
        values.append(F.col(tmp_value).isNotNull())
    out = out.withColumn(key_out, F.coalesce(*keys)).withColumn(
        method_out,
        F.coalesce(
            *methods,
            F.when(_reduce(lambda a, b: a | b, values), F.lit("unlinked")),
            F.lit("no_id"),
        ),
    )
    return out.drop(*[c for c in out.columns if c.startswith("_le_")])


def split_curie(column, rules):
    """(id_type, id_value) Columns of a `LABEL:value` text; `OMIM:123` -> omim, 123."""
    text = F.col(column) if isinstance(column, str) else column
    label = F.regexp_extract(text, r"^([^:]+):", 1)
    value = F.regexp_extract(text, r"^[^:]+:(.*)$", 1)
    id_type = id_type_expr(label, rules)
    return id_type, id_value_expr(id_type, value)


def band_expr(entries, value, default):
    """Column expr: the label of the first band whose `below` is above `value`."""
    expr = F.lit(default)
    for entry in reversed(entries):
        expr = F.when(
            value < F.lit(float(entry["below"])), F.lit(entry["label"])
        ).otherwise(expr)
    return F.when(value.isNotNull(), expr)


def split_list(column, separators=r"[;|,]"):
    """Array of the trimmed, non-empty parts of a list text; null for no part.
    The array is an intermediate value: a notebook explodes it into rows."""
    text = F.col(column) if isinstance(column, str) else column
    parts = F.filter(
        F.transform(F.split(text, separators), lambda x: F.trim(x)), lambda x: x != ""
    )
    return F.when(F.size(parts) > 0, parts)


# COMMAND ----------

# DBTITLE 1,Entity builders: parser UDFs (small text parsers over Spark rows)

HGVS_NAME_FIELDS = [
    ("transcript_accession", "string"),
    ("transcript_version", "int"),
    ("gene_symbol", "string"),
    ("coding_notation", "string"),
    ("coding_region", "string"),
    ("coding_start", "int"),
    ("coding_start_offset", "int"),
    ("coding_end", "int"),
    ("coding_end_offset", "int"),
    ("coding_edit_type", "string"),
    ("coding_reference_base", "string"),
    ("coding_alternate_base", "string"),
    ("coding_inserted_bases", "string"),
    ("protein_notation", "string"),
    ("protein_reference_amino_acid", "string"),
    ("protein_position", "int"),
    ("protein_end_reference_amino_acid", "string"),
    ("protein_end_position", "int"),
    ("protein_edit_type", "string"),
    ("protein_alternate_amino_acid", "string"),
    ("protein_inserted_amino_acids", "string"),
    ("parse_status", "string"),
]
HGVS_PROTEIN_FIELDS = [
    ("protein_notation", "string"),
    ("protein_reference_amino_acid", "string"),
    ("protein_position", "int"),
    ("protein_end_reference_amino_acid", "string"),
    ("protein_end_position", "int"),
    ("protein_edit_type", "string"),
    ("protein_alternate_amino_acid", "string"),
    ("protein_inserted_amino_acids", "string"),
]
TCGA_FIELDS = [
    ("project", "string"),
    ("tissue_source_site", "string"),
    ("participant_barcode", "string"),
    ("sample_barcode", "string"),
    ("sample_type_code", "string"),
    ("vial", "string"),
    ("portion", "string"),
    ("analyte", "string"),
    ("plate", "string"),
    ("center", "string"),
    ("parse_status", "string"),
]
PARSER_KEY = {
    "coding_notation": "c_notation",
    "coding_region": "c_region",
    "coding_start_offset": "c_start_offset",
    "coding_start": "c_start",
    "coding_end_offset": "c_end_offset",
    "coding_end": "c_end",
    "coding_edit_type": "c_edit_type",
    "coding_reference_base": "c_reference",
    "coding_alternate_base": "c_alternate",
    "coding_inserted_bases": "c_inserted",
    "protein_notation": "p_notation",
    "protein_reference_amino_acid": "p_reference",
    "protein_position": "p_position",
    "protein_end_reference_amino_acid": "p_end_reference",
    "protein_end_position": "p_end_position",
    "protein_edit_type": "p_edit_type",
    "protein_alternate_amino_acid": "p_alternate",
    "protein_inserted_amino_acids": "p_inserted",
}

# The parsed conditions are an intermediate list: a notebook explodes it into rows.
CONDITION_DDL = "array<struct<position:int,ids:array<string>,name:string,accession:string,pairing_status:string>>"


def _ddl(fields):
    return "struct<" + ",".join(f"{name}:{kind}" for name, kind in fields) + ">"


def hgvs_name_udf():
    names = [name for name, _ in HGVS_NAME_FIELDS]
    return F.udf(
        lambda text: tuple(parse_hgvs_name(text)[PARSER_KEY.get(n, n)] for n in names),
        _ddl(HGVS_NAME_FIELDS),
    )


def hgvs_protein_udf():
    names = [name for name, _ in HGVS_PROTEIN_FIELDS]
    return F.udf(
        lambda text: tuple(
            parse_hgvs_protein(text)[PARSER_KEY.get(n, n)] for n in names
        ),
        _ddl(HGVS_PROTEIN_FIELDS),
    )


def tcga_barcode_udf():
    names = [name for name, _ in TCGA_FIELDS]
    return F.udf(
        lambda text: tuple(parse_tcga_barcode(text)[n] for n in names),
        _ddl(TCGA_FIELDS),
    )


def clinvar_conditions_udf():
    keys = ("position", "ids", "name", "accession", "pairing_status")

    def parse(ids, names, accessions):
        return [
            tuple(row[k] for k in keys)
            for row in parse_clinvar_conditions(ids, names, accessions)
        ]

    return F.udf(parse, CONDITION_DDL)


def parse_distinct(df, inputs, parser, output: str):
    """`df` with `output` = `parser(*inputs)`; the parser runs once per distinct
    input and the result joins back null-safe."""
    keys = [f"_input_{i}" for i in range(len(inputs))]
    parsed = (
        df.select(*[F.col(c).alias(k) for c, k in zip(inputs, keys, strict=True)])
        .distinct()
        .withColumn(output, parser(*[F.col(k) for k in keys]))
    )
    matched = _reduce(
        lambda left, right: left & right,
        [F.col(c).eqNullSafe(F.col(k)) for c, k in zip(inputs, keys, strict=True)],
    )
    return df.join(parsed, matched, "left").drop(*keys)


def faers_date(column):
    """A FAERS `yyyymmdd` text as a date; null for a partial date."""
    text = F.col(column)
    return F.when(
        text.rlike(r"^\d{8}$"),
        F.expr(f"cast(try_to_timestamp({column}, 'yyyyMMdd') as date)"),
    )


# COMMAND ----------

# DBTITLE 1,Entity builders: band helpers for the old cutoffs and weights


def from_band_expr(entries, value, default):
    """Column expr: the label of the first entry (list falling by `from`) whose
    `from` the value reaches; `default` when none."""
    expr = F.lit(default)
    for entry in reversed(entries):
        expr = F.when(value >= F.lit(entry["from"]), F.lit(entry["label"])).otherwise(
            expr
        )
    return F.when(value.isNotNull(), expr)


def points_expr(entries, value):
    """Column expr: the points of the first entry (list falling by `from`) whose
    `from` the value reaches; 0 when none."""
    expr = F.lit(0)
    for entry in reversed(entries):
        expr = F.when(value >= F.lit(entry["from"]), F.lit(entry["points"])).otherwise(
            expr
        )
    return F.coalesce(expr, F.lit(0))


def any_match_expr(entries, flags, test_names, default):
    """Column expr: the label of the first entry with any test whose flag name
    is in the array column `flags`; `test_names` turns a test into a flag name."""
    expr = F.lit(default)
    for entry in reversed(entries):
        condition = F.lit(False)
        for test in entry["any"]:
            condition = condition | F.array_contains(flags, test_names(test))
        expr = F.when(condition, F.lit(entry["label"])).otherwise(expr)
    return expr


# COMMAND ----------

# DBTITLE 1,Entity builders: source columns and row shaping (use the notebook's RULES)


def source_text(column, dataset):
    """Trimmed text of a Bronze column; null for a placeholder."""
    return clean_placeholders(column, RULES, f"{dataset}.{column}")


def source_number(column, kind, dataset):
    """A Bronze column as a number (`int`, `bigint` or `double`)."""
    return typed_number(column, kind, RULES, f"{dataset}.{column}")


def source_double(column, dataset):
    return source_number(column, "double", dataset)


def source_typed(source, kind, dataset):
    """A Bronze column as the declared Spark type (text, number or boolean)."""
    text = source_text(source, dataset)
    if kind == "boolean":
        return F.when(text.isNotNull(), F.lower(text) == "true")
    if kind in ("int", "bigint", "double"):
        return source_number(source, kind, dataset)
    return text


def text_for(dataset):
    """`text(column)` of one dataset."""
    return lambda column: source_text(column, dataset)


def number_for(dataset, default_kind="double"):
    """`number(column, kind)` of one dataset."""
    return lambda column, kind=default_kind: source_number(column, kind, dataset)


def shape_rows(df, columns, dataset, fixed=None, from_source=False, **given):
    """The frame in the declared columns of a table: `fixed` literals, `given`
    Columns, else the same-named column of `df` (when `from_source`), else null."""
    fixed = fixed or {}
    values = []
    for name, kind in columns:
        if name == "source_dataset":
            value = F.lit(dataset)
        elif name in fixed:
            value = F.lit(fixed[name])
        elif name in given:
            value = given[name]
        elif from_source and name in df.columns:
            value = F.col(name)
        else:
            value = F.lit(None)
        values.append(value.cast(kind).alias(name))
    return df.select(*values)


def rows_factory(columns, label_column=None, from_source=False):
    """`rows(df, [label,] dataset, **given)` of one table."""

    def rows(df, *args, **given):
        if label_column is None:
            (dataset,) = args
            fixed = {}
        else:
            label, dataset = args
            fixed = {label_column: label}
        return shape_rows(df, columns, dataset, fixed, from_source, **given)

    return rows


def mapped_rows_factory(columns, label_column):
    """`rows(df, label, dataset, mapping)`: `mapping` gives the source column of a target column."""

    def rows(df, label, dataset, mapping):
        values = []
        for name, kind in columns:
            if name == label_column:
                value = F.lit(label)
            elif name == "source_dataset":
                value = F.lit(dataset)
            elif name in mapping:
                value = source_typed(mapping[name], kind, dataset)
            elif name in df.columns:
                value = F.col(name)
            else:
                value = F.lit(None)
            values.append(value.cast(kind).alias(name))
        return df.select(*values)

    return rows


def pair_linker(first_map, first_name, second_map, second_name):
    """`linked(df, first_specs, second_specs)`: two entity links, each with its key
    and its link method column."""

    def linked(df, first_specs, second_specs):
        df = link_entities(
            df, first_specs, first_map, f"{first_name}_key", f"{first_name}_link_method"
        )
        return link_entities(
            df,
            second_specs,
            second_map,
            f"{second_name}_key",
            f"{second_name}_link_method",
        )

    return linked


# COMMAND ----------

# DBTITLE 1,Entity master builder (gene, transcript, protein, disease, drug)
# The key flags and the key-source rule of each entity type: the id types the
# crosswalk holds decide which kind of key the master has.


def _master_key_flags(entity_type: str) -> list:
    return {
        "gene": [
            (
                "in_ncbi",
                (F.col("source_dataset") == "ncbi_gene.gene_info")
                & (F.col("id_type") == "ncbi_gene"),
            )
        ],
        "transcript": [
            ("has_ensembl", F.col("id_type") == "ensembl_transcript"),
            ("has_refseq", F.col("id_type") == "refseq_rna"),
        ],
        "protein": [
            ("has_uniprot", F.col("id_type") == "uniprot"),
            ("has_own_ensembl", F.col("match_method") == "own_ensembl_protein"),
        ],
        "disease": [("has_medgen", F.col("id_type") == "medgen")],
        "drug": [
            ("has_chembl", F.col("id_type") == "chembl"),
            ("has_pharmgkb", F.col("id_type") == "pharmgkb"),
        ],
    }[entity_type]


def _master_key_source(entity_type: str):
    return {
        "gene": F.when(F.col("in_ncbi"), F.lit("ncbi_gene_id")).otherwise("own_id"),
        "transcript": F.when(F.col("has_ensembl"), "ensembl_transcript")
        .when(F.col("has_refseq"), "refseq_rna")
        .otherwise("own_refseq_annotation_name"),
        "protein": F.when(F.col("has_uniprot"), "uniprot_accession")
        .when(F.col("has_own_ensembl"), "own_ensembl_protein")
        .otherwise("own_refseq_protein"),
        "disease": F.when(F.col("has_medgen"), "medgen_cui").otherwise(
            "own_opentargets_id"
        ),
        "drug": F.when(F.col("has_chembl"), "chembl_id")
        .when(F.col("has_pharmgkb"), "pharmgkb_accession")
        .otherwise("own_tahoe_name"),
    }[entity_type]


def build_master(entity_type: str, claims, xwalk, coordinates=None):
    """(rows, conflicts): the master rows of one entity type, shaped for
    `conform_union(..., "entity_master")` after `add_genomics_provenance`.
    `xwalk` is the whole crosswalk; `coordinates` (GRCh38, primary) is given for
    the types that carry an interval (gene, transcript)."""
    type_xwalk = xwalk.filter(F.col("entity_type") == entity_type)
    masters, conflicts = resolve_master(
        claims, entity_type=entity_type, fields=MASTER_FIELDS[entity_type]
    )
    keys = type_xwalk.groupBy("entity_key").agg(
        *[
            F.max(F.when(condition, F.lit(True))).alias(name)
            for name, condition in _master_key_flags(entity_type)
        ]
    )
    rows = keys.join(masters, "entity_key", "left")
    if coordinates is not None:
        rows = rows.join(
            coordinates.filter(F.col("entity_type") == entity_type).select(
                "entity_key",
                F.col("contig").alias("grch38_contig"),
                F.col("start_position").alias("grch38_start_position"),
                F.col("end_position").alias("grch38_end_position"),
                F.col("strand").alias("grch38_strand"),
                F.col("source_dataset").alias("grch38_source"),
            ),
            "entity_key",
            "left",
        )
    rows = rows.withColumn("entity_type", F.lit(entity_type)).withColumn(
        "key_source", _master_key_source(entity_type)
    )
    if entity_type == "gene":
        location = F.col("map_location")
        chromosome = r"^(?:[0-9]+|X|Y|MT)"
        for name, pattern in (
            ("cytoband_arm", r"([pq])"),
            ("cytoband_band", r"[pq](\d+)"),
            ("cytoband_sub_band", r"[pq]\d+\.(\d+)"),
        ):
            rows = rows.withColumn(
                name,
                F.nullif(
                    F.regexp_extract(location, chromosome + pattern, 1), F.lit("")
                ),
            )
    rows = rows.withColumn("_srid", sha_key("entity_type", "entity_key"))
    return rows, conflicts
