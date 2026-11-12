# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # GENOMICS BRONZE SHARED CONFIG
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** November 2026
# MAGIC
# MAGIC **Purpose:** the source and dataset list of `genomics_bronze`, pulled into
# MAGIC the loader with `%run`. Definitions only.

# COMMAND ----------

# DBTITLE 1,Imports
import re

# COMMAND ----------

# DBTITLE 1,Configuration
SCHEMA = "genomics_bronze"

# source folder in data/staging -> datasets. One Bronze table per dataset, named
# <source>_<dataset> like its staged chunk files.
SOURCES = {
    "aact": [
        "browse_conditions",
        "browse_interventions",
        "conditions",
        "eligibilities",
        "interventions",
        "keywords",
        "studies",
    ],
    "acmg": ["secondary_findings_genes"],
    "alphafold": ["residue_confidence"],
    "alphamissense": ["variant_scores"],
    "biogrid": ["interactions"],
    "cadd": ["scores"],
    "chembl": ["compounds", "uniprot_mapping"],
    "clingen": ["dosage_sensitivity", "gene_validity"],
    "clinvar": ["variant_summary", "variation_allele"],
    "clinvar_vs_benign": ["variants"],
    "collectri": ["regulons"],
    "cpic": ["pairs"],
    "dbvar": ["variants"],
    "ddinter": ["interactions"],
    "depmap": ["gene_effect", "models"],
    "encode": ["ccre_registry", "enhancer_gene_links"],
    "ensembl_compara": ["orthologues", "paralogues"],
    "faers": ["demo", "drug", "indi", "outc", "reac", "rpsr", "ther"],
    "fda": ["pgx_biomarkers"],
    "gencode": ["annotation"],
    "gnomad": ["variants"],
    "gnomad_balanced": ["variants"],
    "gnomad_mtdna": ["variants"],
    "gtex": ["gene_median_tpm"],
    "gtr": ["test_condition_gene"],
    "hpo": ["genes_to_phenotype", "phenotype_annotations", "terms", "term_relations"],
    "iedb": ["epitopes", "mhc_ligands"],
    "long_range_benchmark": [
        "causal_eqtl",
        "pathogenic_coding",
        "pathogenic_non_coding",
    ],
    "maps_embeddings": ["metadata"],
    "medgen": ["concepts", "relations"],
    "ncbi_gene": ["gene2refseq", "gene_info", "mim2gene_medgen"],
    "opentargets": [
        "association_by_datasource_direct",
        "association_overall_direct",
        "disease",
        "drug_mechanism_of_action",
        "drug_molecule",
        "evidence_gwas_credible_sets",
        "target",
    ],
    "pharmgkb": ["drugs", "genes", "relationships", "variants"],
    "proteingym": [
        "clinical_reference",
        "clinical_substitutions",
        "dms_reference",
        "dms_substitutions",
    ],
    "reactome": ["pathways", "pathway_mappings", "pathway_relations"],
    "refseq": ["annotation"],
    "spectralbio": ["variants"],
    "string": ["protein_aliases", "protein_info", "protein_links"],
    "tahoe": [
        "cell_line_metadata",
        "drug_metadata",
        "gene_metadata",
        "pseudobulk_differential_expression",
        "sample_metadata",
    ],
    "tcga": ["mc3_somatic_mutations"],
    "thousand_genomes": ["variants"],
    "ucsc_conservation": ["phastcons_100way", "phylop_100way"],
    "uniprot": ["entries", "features"],
}

# Datasets with a text value that spans lines; the others are read line by line.
MULTILINE_TABLES = {"hpo_terms", "opentargets_disease", "opentargets_drug_molecule"}

# COMMAND ----------

# DBTITLE 1,Helpers


def table_name(source: str, dataset: str) -> str:
    return f"{source}_{dataset}"


def file_pattern(source: str, dataset: str) -> re.Pattern:
    """Matches `<source>_<dataset>_chunk_NNNNN.csv`."""
    return re.compile(rf"^{re.escape(table_name(source, dataset))}_chunk_\d{{5}}\.csv$")


def clean_columns(columns: list[str]) -> list[str]:
    """Replace the characters Delta rejects in a column name with `_`.
    A name that is already clean is kept as it is."""
    cleaned = [
        re.sub(r"[^0-9A-Za-z_]+", "_", name).strip("_")
        if re.search(r"[^0-9A-Za-z_]", name)
        else name
        for name in columns
    ]
    lowered = [name.lower() for name in cleaned]
    if "" in cleaned or len(set(lowered)) != len(lowered):
        raise RuntimeError(f"column names collide after cleaning: {columns}")
    return cleaned
