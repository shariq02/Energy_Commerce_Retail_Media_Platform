# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # SILVER -- TRIALS, DEPENDENCIES, STRUCTURE AND EPITOPES (GENOMICS)
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** November 2026
# MAGIC
# MAGIC **Purpose:** registered clinical trials (AACT), AlphaFold residue confidence,
# MAGIC DepMap gene effects, ENCODE-rE2G enhancer-gene links and IEDB epitopes with
# MAGIC their MHC ligand assays. Runs after `05` and before `08`.

# COMMAND ----------

# DBTITLE 1,Genomics library
# MAGIC %run ../_genomics_common

# COMMAND ----------

# DBTITLE 1,Configuration
from pyspark.sql import functions as F

SOURCE = "genomics"
COMPONENT = "silver/genomics/07_trials_dependencies_structure"
RID = run_id()
FINDINGS = "genomics"
RULES = load_genomics_rules()
text = source_text
number = source_number

xwalk = read_silver("entity_id_crosswalk")
names = read_silver("entity_name")
gene_map = entity_id_map(xwalk, "gene").unionByName(
    term_map(names, "gene", ("symbol", "official_symbol"), "gene_symbol")
)
protein_map = entity_id_map(xwalk, "protein")


def iri_id(column, dataset, kind):
    """The id at the end of an IEDB or UniProt IRI (`.../epitope/123` -> 123)."""
    return F.nullif(
        F.regexp_extract(text(column, dataset), rf"/{kind}/([^/\s]+)$", 1), F.lit("")
    )


# COMMAND ----------

# DBTITLE 1,Build -- clinical_trial
STUDIES = "aact.studies"
ELIGIBILITIES = "aact.eligibilities"
studies = read_genomics_bronze("aact_studies").select(
    text("nct_id", STUDIES).alias("nct_id"),
    *[text(src, STUDIES).alias(dst) for src, dst in TRIAL_STUDY_COLUMNS.items()],
)
eligibilities = read_genomics_bronze("aact_eligibilities").select(
    text("nct_id", ELIGIBILITIES).alias("nct_id"),
    *[
        text(src, ELIGIBILITIES).alias(dst)
        for src, dst in TRIAL_ELIGIBILITY_COLUMNS.items()
    ],
)
trial = (
    studies.filter(F.col("nct_id").isNotNull())
    .join(eligibilities, "nct_id", "left")
    .withColumn("source_dataset", F.lit(STUDIES))
    .withColumn("_srid", F.col("nct_id"))
)
trial = conform_genomics(add_genomics_provenance(trial, "_srid", RID), "clinical_trial")

# COMMAND ----------

# DBTITLE 1,Write, inspect and export -- clinical_trial
write_inspect_export(
    trial,
    "clinical_trial",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    findings=FINDINGS,
    key_cols=["nct_id"],
    extra_checks={
        "trials_without_eligibility": lambda t: t.filter(
            F.col("eligibility_criteria").isNull()
        ).count(),
        "trials_by_study_type": lambda t: str(
            {
                r["study_type"]: r["count"]
                for r in t.groupBy("study_type").count().collect()
            }
        ),
    },
)

# COMMAND ----------

# DBTITLE 1,Build -- protein_residue_score
ALPHAFOLD = "alphafold.residue_confidence"
plddt = number("plddt", "double", ALPHAFOLD)
residues = link_entities(
    read_genomics_bronze("alphafold_residue_confidence"),
    [(text("accession", ALPHAFOLD), "uniprot")],
    protein_map,
    "protein_key",
    "protein_link_method",
)
bands = RULES["plddt_bands"]
residue_score = (
    residues.select(
        "protein_key",
        text("accession", ALPHAFOLD).alias("uniprot_accession"),
        "protein_link_method",
        text("fragment", ALPHAFOLD).alias("fragment"),
        number("residue_number", "int", ALPHAFOLD).alias("residue_number"),
        text("residue", ALPHAFOLD).alias("residue"),
        F.lit("plddt").alias("score_name"),
        plddt.alias("score_value"),
        from_band_expr(bands["bands"], plddt, bands["band_default"]).alias(
            "score_band"
        ),
        F.lit(ALPHAFOLD).alias("source_dataset"),
    )
    .filter(F.col("score_value").isNotNull())
    .withColumn(
        "_srid",
        sha_key("uniprot_accession", "fragment", "residue_number", "score_name"),
    )
)
residue_score = conform_genomics(
    add_genomics_provenance(residue_score, "_srid", RID), "protein_residue_score"
)

# COMMAND ----------

# DBTITLE 1,Write, inspect and export -- protein_residue_score
write_inspect_export(
    residue_score,
    "protein_residue_score",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    findings=FINDINGS,
    key_cols=["source_record_id"],
    extra_checks={
        "proteins": lambda t: t.select("uniprot_accession").distinct().count(),
        "proteins_without_a_protein_key": lambda t: (
            t.filter(F.col("protein_key").isNull())
            .select("uniprot_accession")
            .distinct()
            .count()
        ),
        "residues_per_band": lambda t: str(
            {
                r["score_band"]: r["count"]
                for r in t.groupBy("score_band").count().collect()
            }
        ),
    },
    link_method_counts=True,
)

# COMMAND ----------

# DBTITLE 1,Build -- gene_dependency
# A DepMap gene column is "SYMBOL (NCBI id)".
DEPMAP = "depmap.gene_effect"
gene_label = text("gene", DEPMAP)
effects = read_genomics_bronze("depmap_gene_effect").select(
    text("model_id", DEPMAP).alias("cell_line_key"),
    F.trim(F.regexp_extract(gene_label, r"^(.*?)\s*\(", 1)).alias("gene_symbol"),
    F.nullif(F.regexp_extract(gene_label, r"\((\d+)\)\s*$", 1), F.lit("")).alias(
        "ncbi_gene_id"
    ),
    number("gene_effect", "double", DEPMAP).alias("gene_effect"),
)
effects = link_entities(
    effects,
    [(F.col("ncbi_gene_id"), "ncbi_gene"), (F.upper("gene_symbol"), "gene_symbol")],
    gene_map,
    "gene_key",
    "gene_link_method",
)
dependency = (
    effects.filter(
        F.col("cell_line_key").isNotNull() & F.col("gene_effect").isNotNull()
    )
    .withColumn("source_dataset", F.lit(DEPMAP))
    .withColumn("_srid", sha_key("cell_line_key", "gene_symbol", "ncbi_gene_id"))
)
dependency = conform_genomics(
    add_genomics_provenance(dependency, "_srid", RID), "gene_dependency"
)

# COMMAND ----------

# DBTITLE 1,Write, inspect and export -- gene_dependency
write_inspect_export(
    dependency,
    "gene_dependency",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    findings=FINDINGS,
    key_cols=["source_record_id"],
    extra_checks={
        "cell_lines": lambda t: t.select("cell_line_key").distinct().count(),
        "genes": lambda t: t.select("gene_symbol", "ncbi_gene_id").distinct().count(),
        "genes_without_a_gene_key": lambda t: (
            t.filter(F.col("gene_key").isNull())
            .select("gene_symbol")
            .distinct()
            .count()
        ),
    },
    link_method_counts=True,
)

# COMMAND ----------

# DBTITLE 1,Build -- regulatory_element_gene_link
# ENCODE-rE2G intervals are BED (0-based start): start + 1 gives the closed interval.
RE2G = "encode.enhancer_gene_links"
links = link_entities(
    read_genomics_bronze("encode_enhancer_gene_links"),
    [
        (ensembl_unversioned_expr(text("TargetGeneEnsemblID", RE2G)), "ensembl"),
        (F.upper(text("TargetGene", RE2G)), "gene_symbol"),
    ],
    gene_map,
    "gene_key",
    "gene_link_method",
)
self_promoter = F.upper(text("isSelfPromoter", RE2G))
element_link = (
    links.select(
        text("name", RE2G).alias("element_key"),
        chromosome_expr(text("chr", RE2G)).alias("contig"),
        (number("start", "bigint", RE2G) + 1).alias("start_position"),
        number("end", "bigint", RE2G).alias("end_position"),
        text("class", RE2G).alias("element_class"),
        "gene_key",
        "gene_link_method",
        text("TargetGene", RE2G).alias("target_gene_symbol"),
        text("TargetGeneEnsemblID", RE2G).alias("target_gene_ensembl_id"),
        number("TargetGeneTSS", "bigint", RE2G).alias(
            "target_gene_transcription_start_site"
        ),
        F.when(self_promoter == "TRUE", True)
        .when(self_promoter == "FALSE", False)
        .alias("is_self_promoter"),
        text("biosample", RE2G).alias("biosample"),
        text("CellType", RE2G).alias("cell_type"),
        number("Score", "double", RE2G).alias("score"),
        F.lit(RE2G).alias("source_dataset"),
    )
    .distinct()
    .withColumn(
        "_srid",
        sha_key(
            "element_key",
            "target_gene_symbol",
            "target_gene_ensembl_id",
            "biosample",
            "cell_type",
            "score",
        ),
    )
)
element_link = conform_genomics(
    add_genomics_provenance(element_link, "_srid", RID), "regulatory_element_gene_link"
)

# COMMAND ----------

# DBTITLE 1,Write, inspect and export -- regulatory_element_gene_link
write_inspect_export(
    element_link,
    "regulatory_element_gene_link",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    findings=FINDINGS,
    key_cols=["source_record_id"],
    extra_checks={
        "elements": lambda t: t.select("element_key").distinct().count(),
        "biosamples": lambda t: t.select("biosample").distinct().count(),
        "target_genes_without_a_gene_key": lambda t: (
            t.filter(F.col("gene_key").isNull())
            .select("target_gene_symbol")
            .distinct()
            .count()
        ),
    },
    link_method_counts=True,
)

# COMMAND ----------

# DBTITLE 1,Build -- epitope
EPITOPES = "iedb.epitopes"
epitopes = link_entities(
    read_genomics_bronze("iedb_epitopes"),
    [(iri_id("Epitope_Molecule_Parent_IRI", EPITOPES, "uniprot"), "uniprot")],
    protein_map,
    "protein_key",
    "protein_link_method",
)
epitope = (
    epitopes.select(
        iri_id("Epitope_ID_IEDB_IRI", EPITOPES, "epitope").alias("epitope_id"),
        *[text(src, EPITOPES).alias(dst) for src, dst in IEDB_EPITOPE_COLUMNS.items()],
        "protein_key",
        "protein_link_method",
        F.lit(EPITOPES).alias("source_dataset"),
    )
    .filter(F.col("epitope_id").isNotNull())
    .withColumn("_srid", F.col("epitope_id"))
)
epitope = conform_genomics(add_genomics_provenance(epitope, "_srid", RID), "epitope")

# COMMAND ----------

# DBTITLE 1,Write, inspect and export -- epitope
write_inspect_export(
    epitope,
    "epitope",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    findings=FINDINGS,
    key_cols=["epitope_id"],
    extra_checks={
        "epitopes_with_a_human_protein": lambda t: t.filter(
            F.col("protein_key").isNotNull()
        ).count(),
        "epitopes_by_source_organism": lambda t: str(
            {
                r["source_organism"]: r["count"]
                for r in t.groupBy("source_organism")
                .count()
                .orderBy(F.col("count").desc())
                .limit(10)
                .collect()
            }
        ),
    },
    link_method_counts=True,
)

# COMMAND ----------

# DBTITLE 1,Build -- epitope_mhc_assay
LIGANDS = "iedb.mhc_ligands"
ligands = link_entities(
    read_genomics_bronze("iedb_mhc_ligands"),
    [(iri_id("Epitope_Molecule_Parent_IRI", LIGANDS, "uniprot"), "uniprot")],
    protein_map,
    "protein_key",
    "protein_link_method",
)
assay = (
    ligands.select(
        iri_id("Assay_ID_IEDB_IRI", LIGANDS, "assay").alias("assay_id"),
        iri_id("Epitope_Epitope_IRI", LIGANDS, "epitope").alias("epitope_id"),
        *[text(src, LIGANDS).alias(dst) for src, dst in IEDB_ASSAY_COLUMNS.items()],
        "protein_key",
        "protein_link_method",
        F.lit(LIGANDS).alias("source_dataset"),
    )
    .filter(F.col("assay_id").isNotNull())
    .withColumn("_srid", F.col("assay_id"))
)
assay = conform_genomics(
    add_genomics_provenance(assay, "_srid", RID), "epitope_mhc_assay"
)

# COMMAND ----------

# DBTITLE 1,Write, inspect and export -- epitope_mhc_assay
write_inspect_export(
    assay,
    "epitope_mhc_assay",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    findings=FINDINGS,
    key_cols=["assay_id"],
    extra_checks={
        "mhc_alleles": lambda t: t.select("mhc_restriction_name").distinct().count(),
        "assays_by_mhc_class": lambda t: str(
            {
                r["mhc_restriction_class"]: r["count"]
                for r in t.groupBy("mhc_restriction_class").count().collect()
            }
        ),
        "assays_with_a_human_protein": lambda t: t.filter(
            F.col("protein_key").isNotNull()
        ).count(),
    },
    link_method_counts=True,
)

# COMMAND ----------

# DBTITLE 1,Check and export -- rejected values of the lenient number reads
export_rejected_values(FINDINGS, COMPONENT, read_genomics_bronze)
