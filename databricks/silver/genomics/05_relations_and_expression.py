# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # SILVER -- RELATIONS AND EXPRESSION (GENOMICS)
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** November 2026
# MAGIC
# MAGIC **Purpose:** the relations between entities as one flat table (gene-disease,
# MAGIC drug-gene, pharmacogenomic, phenotype, pathway-member, protein-interaction
# MAGIC and ClinVar variation-member records) and the median gene expression per
# MAGIC tissue.

# COMMAND ----------

# DBTITLE 1,Genomics library
# MAGIC %run ../_genomics_common

# COMMAND ----------

# DBTITLE 1,Configuration
from pyspark.sql import functions as F

SOURCE = "genomics"
COMPONENT = "silver/genomics/05_relations_and_expression"
RID = run_id()
FINDINGS = "genomics"
RULES = load_genomics_rules()
COLUMNS = GENOMICS_STRUCTURES["entity_relation"]
text = source_text
number = source_number


def relation_rows(df, kind, subkind, dataset, keep=(), **given):
    """`df` in the columns of `entity_relation`: the kind and sub-kind (a text or a
    Column), the columns of `keep` as they are, and the `given` Columns."""
    carried = {name: F.col(name) for name in keep if name not in given}
    fixed = {"relation_kind": kind}
    if isinstance(subkind, str):
        fixed["relation_subkind"] = subkind
    else:
        carried["relation_subkind"] = subkind
    return shape_rows(df, COLUMNS, dataset, fixed, **carried, **given)


# COMMAND ----------

# DBTITLE 1,Build -- entity_relation: gene-disease records
# -- Read Silver -- id maps
xwalk = read_silver("entity_id_crosswalk")
gene_map = entity_id_map(xwalk, "gene")
disease_map = entity_id_map(xwalk, "disease")
linked = pair_linker(gene_map, "gene", disease_map, "disease")
LINKS = ("gene_key", "disease_key", "gene_link_method", "disease_link_method")

# -- Transform -- OMIM gene links and ClinGen
OMIM = "ncbi_gene.mim2gene_medgen"
omim = linked(
    read_genomics_bronze("ncbi_gene_mim2gene_medgen"),
    [(text("GeneID", OMIM), "ncbi_gene")],
    [(text("MedGenCUI", OMIM), "medgen"), (text("MIM_number", OMIM), "omim")],
)
parts = [
    relation_rows(
        omim,
        "gene_disease",
        "omim_gene_link",
        OMIM,
        LINKS,
        source_gene_id=text("GeneID", OMIM),
        source_disease_id=F.coalesce(text("MedGenCUI", OMIM), text("MIM_number", OMIM)),
        gene_disease_relation_type=text("type", OMIM),
        comment=F.concat_ws(" | ", text("Source", OMIM), text("Comment", OMIM)),
    )
]
VALIDITY = "clingen.gene_validity"
validity = linked(
    read_genomics_bronze("clingen_gene_validity"),
    [(hgnc_expr(text("GENE_ID_HGNC", VALIDITY)), "hgnc")],
    [(id_value_expr(F.lit("mondo"), text("DISEASE_ID_MONDO", VALIDITY)), "mondo")],
)
parts.append(
    relation_rows(
        validity,
        "gene_disease",
        "clingen_validity",
        VALIDITY,
        LINKS,
        source_gene_id=text("GENE_ID_HGNC", VALIDITY),
        source_disease_id=text("DISEASE_ID_MONDO", VALIDITY),
        classification=text("CLASSIFICATION", VALIDITY),
        mode_of_inheritance=text("MOI", VALIDITY),
        evaluation_date=text("CLASSIFICATION_DATE", VALIDITY),
        report_url=text("ONLINE_REPORT", VALIDITY),
        study_id=text("GCEP", VALIDITY),
        source_gene_symbol=text("GENE_SYMBOL", VALIDITY),
        source_disease_name=text("DISEASE_LABEL", VALIDITY),
        standard_operating_procedure=text("SOP", VALIDITY),
    )
)
DOSAGE = "clingen.dosage_sensitivity"
dosage = read_genomics_bronze("clingen_dosage_sensitivity")
for kind, score, description, disease in (
    (
        "haploinsufficiency",
        "Haploinsufficiency_Score",
        "Haploinsufficiency_Description",
        "Haploinsufficiency_Disease_ID",
    ),
    (
        "triplosensitivity",
        "Triplosensitivity_Score",
        "Triplosensitivity_Description",
        "Triplosensitivity_Disease_ID",
    ),
):
    id_type, id_value = split_curie(text(disease, DOSAGE), RULES)
    # one row per supporting publication of this dosage kind
    prefix = score.removesuffix("_Score")
    papers = F.filter(
        F.array(
            *[text(c, DOSAGE) for c in dosage.columns if c.startswith(f"{prefix}_PMID")]
        ),
        lambda value: value.isNotNull(),
    )
    part = linked(
        dosage.filter(text(score, DOSAGE).isNotNull()).withColumn(
            "pubmed_id", F.explode_outer(F.when(F.size(papers) > 0, papers))
        ),
        [(text("Gene_ID", DOSAGE), "ncbi_gene")],
        [(id_value, id_type)],
    )
    parts.append(
        relation_rows(
            part,
            "gene_disease",
            "clingen_dosage",
            DOSAGE,
            LINKS,
            source_gene_id=text("Gene_ID", DOSAGE),
            source_disease_id=text(disease, DOSAGE),
            gene_disease_relation_type=F.lit(kind),
            classification=text(score, DOSAGE),
            comment=text(description, DOSAGE),
            evaluation_date=text("Date_Last_Evaluated", DOSAGE),
            pubmed_id=F.col("pubmed_id"),
            source_gene_symbol=text("Gene_Symbol", DOSAGE),
            cytogenetic_band=text("cytoBand", DOSAGE),
            genomic_location=text("Genomic_Location", DOSAGE),
        )
    )

# -- Transform -- Open Targets overall, by datasource and GWAS
TARGETS = [
    (
        "opentargets_association_overall_direct",
        "opentargets.association_overall_direct",
        "opentargets_overall",
    ),
    (
        "opentargets_association_by_datasource_direct",
        "opentargets.association_by_datasource_direct",
        "opentargets_datasource",
    ),
]
for table, dataset, subkind in TARGETS:
    part = linked(
        read_genomics_bronze(table),
        [(text("targetId", dataset), "ensembl")],
        [(text("diseaseId", dataset), "opentargets_disease")],
    )
    parts.append(
        relation_rows(
            part,
            "gene_disease",
            subkind,
            dataset,
            LINKS,
            source_gene_id=text("targetId", dataset),
            source_disease_id=text("diseaseId", dataset),
            gene_disease_score=number("associationScore", "double", dataset),
            evidence_count=number("evidenceCount", "bigint", dataset),
            gene_disease_relation_type=text("aggregationType", dataset),
            aggregation_value=text("aggregationValue", dataset),
            novelty_score=number("currentNovelty", "double", dataset),
        )
    )
GWAS = "opentargets.evidence_gwas_credible_sets"
gwas = linked(
    read_genomics_bronze("opentargets_evidence_gwas_credible_sets").withColumn(
        "pubmed_id",
        F.explode_outer(F.from_json(text("literature", GWAS), "array<string>")),
    ),
    [(text("targetId", GWAS), "ensembl")],
    [(text("diseaseId", GWAS), "opentargets_disease")],
)
parts.append(
    relation_rows(
        gwas,
        "gene_disease",
        "opentargets_gwas_credible_set",
        GWAS,
        LINKS,
        source_gene_id=text("targetId", GWAS),
        source_disease_id=text("diseaseId", GWAS),
        gene_disease_score=number("score", "double", GWAS),
        study_id=text("studyLocusId", GWAS),
        aggregation_value=text("datasourceId", GWAS),
        evaluation_date=text("evidenceDate", GWAS),
        pubmed_id=F.col("pubmed_id"),
        target_from_source_id=text("targetFromSourceId", GWAS),
        disease_from_source_mapped_id=text("diseaseFromSourceMappedId", GWAS),
        datatype_id=text("datatypeId", GWAS),
        resource_score=number("resourceScore", "double", GWAS),
        curation_date=text("curationDate", GWAS),
        quality_controls=text("qualityControls", GWAS),
        publication_date=text("publicationDate", GWAS),
    )
)

# -- Transform -- ClinVar conditions from Silver and HPO
variants = (
    read_silver("variant")
    .filter((F.col("assembly") == "GRCh38") & F.col("gene_key").isNotNull())
    .select("variant_key", "gene_key")
)
conditions = (
    current_snapshot(read_silver("variant_condition"), RULES)
    .filter(F.col("disease_key").isNotNull() & ~F.col("is_generic_label"))
    .select("variant_key", "disease_key")
    .distinct()
)
pathogenic = (
    current_snapshot(read_silver("variant_classification"), RULES)
    .filter(
        (F.col("classification_type") == "germline")
        & F.col("term").isin(RULES["pathogenic_terms"])
    )
    .select("variant_key")
    .distinct()
    .withColumn("is_pathogenic", F.lit(1))
)
clinvar_pairs = (
    variants.join(conditions, "variant_key")
    .join(pathogenic, "variant_key", "left")
    .groupBy("gene_key", "disease_key")
    .agg(
        F.countDistinct("variant_key").alias("evidence_count"),
        F.countDistinct(
            F.when(F.col("is_pathogenic").isNotNull(), F.col("variant_key"))
        ).alias("supporting_count"),
    )
    .withColumn("gene_link_method", F.lit("clinvar_gene_id"))
    .withColumn("disease_link_method", F.lit("clinvar_condition_ids"))
)
parts.append(
    relation_rows(
        clinvar_pairs,
        "gene_disease",
        "clinvar_variant_condition",
        "clinvar.variant_summary",
        (*LINKS, "evidence_count", "supporting_count"),
    )
)
HPO = "hpo.genes_to_phenotype"
id_type, id_value = split_curie(text("disease_id", HPO), RULES)
hpo = linked(
    read_genomics_bronze("hpo_genes_to_phenotype")
    .filter(text("disease_id", HPO).isNotNull())
    .groupBy("ncbi_gene_id", "disease_id")
    .agg(F.countDistinct("hpo_id").alias("phenotype_count")),
    [(text("ncbi_gene_id", HPO), "ncbi_gene")],
    [(id_value, id_type)],
)
parts.append(
    relation_rows(
        hpo,
        "gene_disease",
        "hpo_gene_disease",
        HPO,
        LINKS,
        source_gene_id=text("ncbi_gene_id", HPO),
        source_disease_id=text("disease_id", HPO),
        evidence_count=F.col("phenotype_count"),
    )
)
gene_disease = _union(parts)

# COMMAND ----------

# DBTITLE 1,Build -- entity_relation: phenotype annotations
# -- Read Silver -- id maps
xwalk = read_silver("entity_id_crosswalk")
gene_map = entity_id_map(xwalk, "gene")
disease_map = entity_id_map(xwalk, "disease")
PHENOTYPE_KEEP = (
    "subject_type",
    "subject_key",
    "subject_source_id",
    "subject_link_method",
    "subject_name",
    "phenotype_key",
    "is_negated",
    "frequency",
    "onset",
    "sex",
    "modifier",
    "aspect",
    "reference",
    "biocuration",
)

# -- Transform -- disease annotations
ANN = "hpo.phenotype_annotations"
id_type, id_value = split_curie(text("database_id", ANN), RULES)
annotations = link_entities(
    read_genomics_bronze("hpo_phenotype_annotations"),
    [(id_value, id_type)],
    disease_map,
    "subject_key",
    "subject_link_method",
)
disease_rows = annotations.select(
    F.lit("disease").alias("subject_type"),
    "subject_key",
    text("database_id", ANN).alias("subject_source_id"),
    "subject_link_method",
    text("disease_name", ANN).alias("subject_name"),
    curie_expr(text("hpo_id", ANN)).alias("phenotype_key"),
    (F.upper(F.trim(F.col("qualifier"))) == "NOT").alias("is_negated"),
    text("frequency", ANN).alias("frequency"),
    text("onset", ANN).alias("onset"),
    text("sex", ANN).alias("sex"),
    text("modifier", ANN).alias("modifier"),
    text("aspect", ANN).alias("aspect"),
    text("evidence", ANN).alias("phenotype_evidence"),
    text("reference", ANN).alias("reference"),
    text("biocuration", ANN).alias("biocuration"),
)

# -- Transform -- gene annotations
GENES = "hpo.genes_to_phenotype"
genes = link_entities(
    read_genomics_bronze("hpo_genes_to_phenotype"),
    [(text("ncbi_gene_id", GENES), "ncbi_gene")],
    gene_map,
    "subject_key",
    "subject_link_method",
)
none = F.lit(None).cast("string")
gene_rows = genes.select(
    F.lit("gene").alias("subject_type"),
    "subject_key",
    text("ncbi_gene_id", GENES).alias("subject_source_id"),
    "subject_link_method",
    text("gene_symbol", GENES).alias("subject_name"),
    curie_expr(text("hpo_id", GENES)).alias("phenotype_key"),
    F.lit(False).alias("is_negated"),
    text("frequency", GENES).alias("frequency"),
    text("hpo_name", GENES).alias("phenotype_name"),
    none.alias("onset"),
    none.alias("sex"),
    none.alias("modifier"),
    none.alias("aspect"),
    none.alias("phenotype_evidence"),
    none.alias("reference"),
    none.alias("biocuration"),
)
phenotype = _union(
    [
        relation_rows(
            frame.filter(F.col("phenotype_key").isNotNull()),
            "phenotype",
            F.col("subject_type"),
            dataset,
            (*PHENOTYPE_KEEP, "phenotype_evidence", "phenotype_name"),
        )
        for frame, dataset in (
            (disease_rows.withColumn("phenotype_name", none), ANN),
            (gene_rows, GENES),
        )
    ]
)

# COMMAND ----------

# DBTITLE 1,Build -- entity_relation: drug-gene records
# -- Read Silver -- id maps and name maps
xwalk = read_silver("entity_id_crosswalk")
names = read_silver("entity_name")
gene_map = (
    entity_id_map(xwalk, "gene")
    .unionByName(term_map(names, "gene", ("symbol", "official_symbol"), "gene_symbol"))
    .unionByName(
        term_map(
            names,
            "gene",
            ("synonym", "alternate_symbol", "symbol_synonym"),
            "gene_alias",
        )
    )
)
drug_map = entity_id_map(xwalk, "drug").unionByName(
    term_map(
        names,
        "drug",
        ("name", "generic_name", "trade_name", "synonym", "brand_mixture"),
        "drug_name",
    )
)
DRUG_LINKS = ("drug_key", "gene_key", "drug_link_method", "gene_link_method")


# -- Helpers -- links and one row per publication
def upper(column):
    return F.upper(F.trim(column))


def symbol_specs(value):
    return [(upper(value), "gene_symbol"), (upper(value), "gene_alias")]


drug_linked = pair_linker(drug_map, "drug", gene_map, "gene")


def publications(column):
    """One row per publication (a record without any keeps one row)."""
    return F.explode_outer(split_list(column, r"[;|,]"))


# -- Transform -- PharmGKB chemical-gene relationships
REL = "pharmgkb.relationships"
rel = read_genomics_bronze("pharmgkb_relationships")
chemical_gene = (F.col("Entity1_type") == "Chemical") & (
    F.col("Entity2_type") == "Gene"
)
gene_chemical = (F.col("Entity1_type") == "Gene") & (
    F.col("Entity2_type") == "Chemical"
)
pairs = (
    rel.filter(chemical_gene | gene_chemical)
    .withColumn(
        "chemical_id",
        F.when(chemical_gene, F.col("Entity1_id")).otherwise(F.col("Entity2_id")),
    )
    .withColumn(
        "gene_id",
        F.when(chemical_gene, F.col("Entity2_id")).otherwise(F.col("Entity1_id")),
    )
    .withColumn("pubmed_id", publications(text("PMIDs", REL)))
)
pairs = drug_linked(
    pairs, [(F.col("chemical_id"), "pharmgkb")], [(F.col("gene_id"), "pharmgkb")]
)
parts = [
    relation_rows(
        pairs,
        "drug_gene",
        "pharmgkb_relationship",
        REL,
        (*DRUG_LINKS, "pubmed_id"),
        drug_gene_relation_type=F.lit("pharmgkb_association"),
        relation_detail=text("Association", REL),
        evidence_level=text("Evidence", REL),
        source_drug_id=F.col("chemical_id"),
        source_gene_id=F.col("gene_id"),
    )
]

# -- Transform -- CPIC pairs and FDA biomarkers
CPIC = "cpic.pairs"


def truth(value):
    """`Yes`/`True` -> true, `No`/`False` -> false, else null."""
    lowered = F.lower(value)
    return F.when(lowered.isin("yes", "true"), True).when(
        lowered.isin("no", "false"), False
    )


id_type, id_value = split_curie(text("drugid", CPIC), RULES)
cpic = drug_linked(
    read_genomics_bronze("cpic_pairs").withColumn(
        "pubmed_id", publications(text("pmids", CPIC))
    ),
    [(id_value, id_type), (upper(text("drugname", CPIC)), "drug_name")],
    symbol_specs(text("genesymbol", CPIC)),
)
parts.append(
    relation_rows(
        cpic,
        "drug_gene",
        "cpic_pair",
        CPIC,
        (*DRUG_LINKS, "pubmed_id"),
        drug_gene_relation_type=F.lit("cpic_pair"),
        relation_detail=text("guidelinename", CPIC),
        evidence_level=text("cpiclevel", CPIC),
        source_drug_id=text("drugid", CPIC),
        source_gene_id=text("genesymbol", CPIC),
        interaction_id=text("pairid", CPIC),
        report_url=text("guidelineurl", CPIC),
        clinpgx_level=text("clinpgxlevel", CPIC),
        pharmacogenomic_testing=text("pgxtesting", CPIC),
        used_for_recommendation=truth(text("usedforrecommendation", CPIC)),
        is_provisional=truth(text("provisional", CPIC)),
    )
)
FDA = "fda.pgx_biomarkers"
drug_name = F.regexp_replace(text("Drug", FDA), r"\s*\(\d+\)\s*$", "")
fda = drug_linked(
    read_genomics_bronze("fda_pgx_biomarkers"),
    [(upper(drug_name), "drug_name")],
    symbol_specs(text("Biomarker", FDA)),
)
parts.append(
    relation_rows(
        fda,
        "drug_gene",
        "fda_label_biomarker",
        FDA,
        DRUG_LINKS,
        drug_gene_relation_type=F.lit("fda_label_biomarker"),
        source_drug_id=text("Drug", FDA),
        source_gene_id=text("Biomarker", FDA),
        labeling_sections=text("Labeling_Sections", FDA),
        therapeutic_area=text("Therapeutic_Area", FDA),
    )
)

# -- Transform -- Open Targets mechanisms and Tahoe targets
MOA = "opentargets.drug_mechanism_of_action"
moa = (
    read_genomics_bronze("opentargets_drug_mechanism_of_action")
    .withColumn("chembl_id", F.explode(F.from_json("chemblIds", "array<string>")))
    .withColumn("target_id", F.explode(F.from_json("targets", "array<string>")))
)
moa = drug_linked(
    moa, [(F.col("chembl_id"), "chembl")], [(F.col("target_id"), "ensembl")]
)
parts.append(
    relation_rows(
        moa,
        "drug_gene",
        "opentargets_mechanism",
        MOA,
        DRUG_LINKS,
        drug_gene_relation_type=text("actionType", MOA),
        relation_detail=text("mechanismOfAction", MOA),
        source_drug_id=F.col("chembl_id"),
        source_gene_id=F.col("target_id"),
        target_name=text("targetName", MOA),
        target_type=text("targetType", MOA),
    )
)
TAHOE = "tahoe.drug_metadata"
tahoe = (
    read_genomics_bronze("tahoe_drug_metadata")
    .withColumn("tahoe_key", F.concat(F.lit("tahoe:"), F.lower(F.trim(F.col("drug")))))
    .withColumn(
        "target_symbol", F.explode(split_list(text("targets", TAHOE), r"[;|,]"))
    )
)
tahoe = drug_linked(
    tahoe,
    [(F.col("tahoe_key"), "tahoe_drug")],
    symbol_specs(F.col("target_symbol")),
)
parts.append(
    relation_rows(
        tahoe,
        "drug_gene",
        "tahoe_target",
        TAHOE,
        DRUG_LINKS,
        drug_gene_relation_type=F.lit("tahoe_target"),
        relation_detail=text("moa_fine", TAHOE),
        source_drug_id=text("drug", TAHOE),
        source_gene_id=F.col("target_symbol"),
    )
)
drug_gene = _union(parts)

# COMMAND ----------

# DBTITLE 1,Build -- entity_relation: pharmacogenomic associations
# -- Read Silver -- id maps
xwalk = read_silver("entity_id_crosswalk")
gene_map = entity_id_map(xwalk, "gene")
drug_map = entity_id_map(xwalk, "drug")
variant_map = entity_id_map(xwalk, "variant")

# -- Transform -- relationships and keys of each end
REL = "pharmgkb.relationships"
rel = read_genomics_bronze("pharmgkb_relationships")
chemical_gene = (F.col("Entity1_type") == "Chemical") & (
    F.col("Entity2_type") == "Gene"
)
gene_chemical = (F.col("Entity1_type") == "Gene") & (
    F.col("Entity2_type") == "Chemical"
)
other = rel.filter(~(chemical_gene | gene_chemical)).withColumn(
    "pubmed_id", publications(text("PMIDs", REL))
)


def keyed(df, id_column, type_column, prefix):
    for kind, id_map, id_type in (
        ("Gene", gene_map, "pharmgkb"),
        ("Chemical", drug_map, "pharmgkb"),
        ("Variant", variant_map, "pharmgkb_variant"),
    ):
        value = F.when(F.col(type_column) == kind, F.col(id_column))
        df = link_entities(
            df,
            [(value, id_type)],
            id_map,
            f"{prefix}_{kind}",
            f"{prefix}_{kind}_method",
        ).drop(f"{prefix}_{kind}_method")
    return df.withColumn(
        f"{prefix}_key",
        F.coalesce(
            F.col(f"{prefix}_Gene"),
            F.col(f"{prefix}_Chemical"),
            F.col(f"{prefix}_Variant"),
        ),
    )


other = keyed(
    keyed(other, "Entity1_id", "Entity1_type", "subject"),
    "Entity2_id",
    "Entity2_type",
    "object",
)

# -- Transform -- columns
pharmacogenomic = relation_rows(
    other,
    "pharmacogenomic",
    "pharmgkb_relationship",
    REL,
    ("subject_key", "object_key", "pubmed_id"),
    subject_type=text("Entity1_type", REL),
    subject_id=text("Entity1_id", REL),
    subject_name=text("Entity1_name", REL),
    object_type=text("Entity2_type", REL),
    object_id=text("Entity2_id", REL),
    object_name=text("Entity2_name", REL),
    pharmacogenomic_evidence=text("Evidence", REL),
    association=text("Association", REL),
    pharmacokinetic=text("PK", REL),
    pharmacodynamic=text("PD", REL),
)

# COMMAND ----------

# DBTITLE 1,Build -- entity_relation: pathway members
# -- Read Silver -- id maps
xwalk = read_silver("entity_id_crosswalk")
gene_map = entity_id_map(xwalk, "gene")
protein_map = entity_id_map(xwalk, "protein")
transcript_map = entity_id_map(xwalk, "transcript")

# -- Transform -- member type and links
DATASET = "reactome.pathway_mappings"
member_text = text_for(DATASET)
identifier = member_text("identifier")
label = F.lower(F.coalesce(member_text("id_type"), F.lit("")))
member_type = (
    F.when(identifier.rlike(r"^ENSG\d+"), "gene")
    .when(identifier.rlike(r"^ENST\d+"), "transcript")
    .when(identifier.rlike(r"^ENSP\d+"), "protein")
    .when(label.contains("uniprot"), "protein")
    .when(label.rlike("ncbi|entrez"), "gene")
    .otherwise("other")
)
unversioned = ensembl_unversioned_expr(identifier)
mappings = read_genomics_bronze("reactome_pathway_mappings").withColumn(
    "member_type", member_type
)
mappings = link_entities(
    mappings,
    [
        (F.when(identifier.rlike(r"^ENSP\d+"), unversioned), "ensembl_protein"),
        (
            F.when(
                (F.col("member_type") == "protein") & ~identifier.rlike(r"^ENSP\d+"),
                identifier,
            ),
            "uniprot",
        ),
    ],
    protein_map,
    "protein_key",
    "protein_method",
)
mappings = link_entities(
    mappings,
    [
        (F.when(identifier.rlike(r"^ENSG\d+"), unversioned), "ensembl"),
        (
            F.when(
                (F.col("member_type") == "gene") & identifier.rlike(r"^\d+$"),
                identifier,
            ),
            "ncbi_gene",
        ),
    ],
    gene_map,
    "gene_key",
    "gene_method",
)
mappings = link_entities(
    mappings,
    [(F.when(identifier.rlike(r"^ENST\d+"), unversioned), "ensembl_transcript")],
    transcript_map,
    "transcript_key",
    "transcript_method",
)

# -- Transform -- columns
pathway = relation_rows(
    mappings.filter(member_text("pathway_id").isNotNull() & identifier.isNotNull()),
    "pathway_member",
    "reactome_pathway_mapping",
    DATASET,
    ("member_type",),
    member_key=F.coalesce("protein_key", "gene_key", "transcript_key"),
    member_id_type=member_text("id_type"),
    member_id=identifier,
    member_link_method=F.coalesce(
        F.when(F.col("protein_key").isNotNull(), F.col("protein_method")),
        F.when(F.col("gene_key").isNotNull(), F.col("gene_method")),
        F.when(F.col("transcript_key").isNotNull(), F.col("transcript_method")),
        F.lit("unlinked"),
    ),
    pathway_key=member_text("pathway_id"),
    pathway_evidence=member_text("evidence"),
    species=member_text("species"),
)

# COMMAND ----------

# DBTITLE 1,Build -- entity_relation: protein interactions
# -- Read Silver -- id maps
xwalk = read_silver("entity_id_crosswalk")
gene_map = entity_id_map(xwalk, "gene")
protein_map = entity_id_map(xwalk, "protein")

# -- Transform -- BioGRID
BIOGRID = "biogrid.interactions"
bio_text = text_for(BIOGRID)


def first_accession(column):
    return F.nullif(F.regexp_extract(bio_text(column), r"^([^|]+)", 1), F.lit(""))


biogrid = read_genomics_bronze("biogrid_interactions")
for side in ("A", "B"):
    biogrid = link_entities(
        biogrid,
        [(bio_text(f"Entrez_Gene_Interactor_{side}"), "ncbi_gene")],
        gene_map,
        f"gene_{side}",
        f"gene_{side}_method",
    )
    biogrid = link_entities(
        biogrid,
        [(first_accession(f"SWISS_PROT_Accessions_Interactor_{side}"), "uniprot")],
        protein_map,
        f"protein_{side}",
        f"protein_{side}_method",
    )
biogrid_rows = relation_rows(
    biogrid,
    "protein_interaction",
    "biogrid",
    BIOGRID,
    interaction_id=bio_text("BioGRID_Interaction_ID"),
    interactor_a_id=bio_text("BioGRID_ID_Interactor_A"),
    interactor_b_id=bio_text("BioGRID_ID_Interactor_B"),
    protein_a_key=F.col("protein_A"),
    protein_b_key=F.col("protein_B"),
    gene_a_key=F.col("gene_A"),
    gene_b_key=F.col("gene_B"),
    experimental_system=bio_text("Experimental_System"),
    experimental_system_type=bio_text("Experimental_System_Type"),
    throughput=bio_text("Throughput"),
    interaction_score=typed_number("Score", "double", RULES, f"{BIOGRID}.Score"),
    author=bio_text("Author"),
    publication_source=bio_text("Publication_Source"),
    modification=bio_text("Modification"),
    qualifications=bio_text("Qualifications"),
    tags=bio_text("Tags"),
    source_database=bio_text("Source_Database"),
    organism_a=bio_text("Organism_ID_Interactor_A"),
    organism_b=bio_text("Organism_ID_Interactor_B"),
    ontology_term_ids=bio_text("Ontology_Term_IDs"),
    ontology_term_names=bio_text("Ontology_Term_Names"),
    ontology_term_categories=bio_text("Ontology_Term_Categories"),
    ontology_term_qualifier_ids=bio_text("Ontology_Term_Qualifier_IDs"),
    ontology_term_qualifier_names=bio_text("Ontology_Term_Qualifier_Names"),
    ontology_term_types=bio_text("Ontology_Term_Types"),
    **{
        f"{name}_{side.lower()}": bio_text(f"{column}_Interactor_{side}")
        for side in ("A", "B")
        for name, column in (
            ("source_gene", "Entrez_Gene"),
            ("systematic_name", "Systematic_Name"),
            ("official_symbol", "Official_Symbol"),
            ("synonyms", "Synonyms"),
            ("swiss_prot_accessions", "SWISS_PROT_Accessions"),
            ("trembl_accessions", "TREMBL_Accessions"),
            ("refseq_accessions", "REFSEQ_Accessions"),
        )
    },
)

# -- Transform -- STRING links (one column per evidence channel)
STRING = "string.protein_links"
string_number = number_for(STRING, "int")


def ensp(column):
    return F.regexp_replace(F.trim(F.col(column)), r"^\d+\.", "")


string = read_genomics_bronze("string_protein_links")
for side, column in (("A", "protein1"), ("B", "protein2")):
    string = link_entities(
        string,
        [(ensp(column), "ensembl_protein")],
        protein_map,
        f"protein_{side}",
        f"protein_{side}_method",
    )
string_rows = relation_rows(
    string,
    "protein_interaction",
    "string",
    STRING,
    interactor_a_id=F.col("protein1"),
    interactor_b_id=F.col("protein2"),
    protein_a_key=F.col("protein_A"),
    protein_b_key=F.col("protein_B"),
    **{f"channel_{name}_score": string_number(name) for name in STRING_CHANNELS},
    combined_score=string_number("combined_score"),
    organism_a=F.regexp_extract("protein1", r"^(\d+)\.", 1),
    organism_b=F.regexp_extract("protein2", r"^(\d+)\.", 1),
)
interaction = biogrid_rows.unionByName(string_rows)

# COMMAND ----------

# DBTITLE 1,Build -- entity_relation: ClinVar variation members
# -- Read Silver -- id map
xwalk = read_silver("entity_id_crosswalk")
variant_map = entity_id_map(xwalk, "variant")

# -- Transform -- complex variations (haplotype, diplotype, ...) and their alleles
ALLELES = "clinvar.variation_allele"
allele_text = text_for(ALLELES)
SIMPLE_TYPES = ("Variant", "Haplotype, single variant")
interpreted = F.lower(allele_text("Interpreted"))
members = link_entities(
    read_genomics_bronze("clinvar_variation_allele").filter(
        ~allele_text("Type").isin(*SIMPLE_TYPES)
    ),
    [(allele_text("AlleleID"), "clinvar_allele")],
    variant_map,
    "variant_key",
    "variant_method",
)
variation_member = relation_rows(
    members,
    "variation_member",
    allele_text("Type"),
    ALLELES,
    subject_type=F.lit("clinvar_variation"),
    subject_id=allele_text("VariationID"),
    member_type=F.lit("variant"),
    member_key=F.col("variant_key"),
    member_id_type=F.lit("clinvar_allele"),
    member_id=allele_text("AlleleID"),
    member_link_method=F.col("variant_method"),
    variation_interpreted=(
        F.when(interpreted == "yes", True).when(interpreted == "no", False)
    ),
)

# -- Check -- simple pairs that no ClinVar snapshot places (counted, not written)
summary_text = text_for("clinvar.variant_summary")
summary_pairs = (
    read_genomics_bronze("clinvar_variant_summary")
    .select(
        summary_text("VariationID").alias("VariationID"),
        summary_text("AlleleID").alias("AlleleID"),
    )
    .distinct()
)
unplaced_pairs = (
    read_genomics_bronze("clinvar_variation_allele")
    .filter(allele_text("Type").isin(*SIMPLE_TYPES))
    .select(
        allele_text("VariationID").alias("VariationID"),
        allele_text("AlleleID").alias("AlleleID"),
    )
    .distinct()
    .join(summary_pairs, ["VariationID", "AlleleID"], "left_anti")
)

# COMMAND ----------

# DBTITLE 1,Build -- entity_relation: regulation, homology, interactions, ACMG, trials
# -- Read Silver -- id and name maps
xwalk = read_silver("entity_id_crosswalk")
names = read_silver("entity_name")
gene_ids = entity_id_map(xwalk, "gene").unionByName(
    term_map(names, "gene", ("symbol", "official_symbol"), "gene_symbol")
)
drug_names = entity_id_map(xwalk, "drug").unionByName(
    term_map(
        names,
        "drug",
        ("name", "generic_name", "trade_name", "synonym", "brand_mixture"),
        "drug_name",
    )
)
disease_names = entity_id_map(xwalk, "disease").unionByName(
    term_map(names, "disease", ("pt", "name", "mh", "sy", "syn"), "disease_name")
)


def null_text(column, dataset):
    """A Bronze text; the literal `NULL` of a source is null."""
    value = text(column, dataset)
    return F.when(F.upper(value) != "NULL", value)


# -- Transform -- CollecTRI TF-target edges (one row per publication)
COLLECTRI = "collectri.regulons"
regulons = read_genomics_bronze("collectri_regulons")
for side, column in (("A", "source"), ("B", "target")):
    regulons = link_entities(
        regulons,
        [(F.upper(text(column, COLLECTRI)), "gene_symbol")],
        gene_ids,
        f"gene_{side}",
        f"gene_{side}_method",
    )
regulation = relation_rows(
    regulons.withColumn("publication", publications(text("references", COLLECTRI))),
    "regulatory_interaction",
    "collectri",
    COLLECTRI,
    interactor_a_id=text("source", COLLECTRI),
    interactor_b_id=text("target", COLLECTRI),
    gene_a_key=F.col("gene_A"),
    gene_b_key=F.col("gene_B"),
    interaction_score=number("weight", "double", COLLECTRI),
    relation_detail=text("sign_decision", COLLECTRI),
    source_database=text("resources", COLLECTRI),
    pubmed_id=F.regexp_replace(F.col("publication"), r"^CollecTRI:", ""),
)

# -- Transform -- Ensembl Compara homologies (orthologues: gene B of another species)
COMPARA = "ensembl_compara.homologues"
homologues = link_entities(
    read_genomics_bronze("ensembl_compara_homologues"),
    [(ensembl_unversioned_expr(text("gene_stable_id", COMPARA)), "ensembl")],
    entity_id_map(xwalk, "gene"),
    "gene_A",
    "gene_A_method",
)
human_b = text("homology_species", COMPARA) == "homo_sapiens"
homologues = link_entities(
    homologues,
    [
        (
            F.when(
                human_b,
                ensembl_unversioned_expr(text("homology_gene_stable_id", COMPARA)),
            ),
            "ensembl",
        )
    ],
    entity_id_map(xwalk, "gene"),
    "gene_B",
    "gene_B_method",
)
high_confidence = null_text("is_high_confidence", COMPARA)
homology = relation_rows(
    homologues,
    "homology",
    text("homology_type", COMPARA),
    COMPARA,
    interaction_id=null_text("homology_id", COMPARA),
    interactor_a_id=text("gene_stable_id", COMPARA),
    interactor_b_id=text("homology_gene_stable_id", COMPARA),
    gene_a_key=F.col("gene_A"),
    gene_b_key=F.col("gene_B"),
    gene_link_method=F.col("gene_A_method"),
    organism_a=text("species", COMPARA),
    organism_b=text("homology_species", COMPARA),
    species=text("homology_species", COMPARA),
    percent_identity=number("identity", "double", COMPARA),
    homology_percent_identity=number("homology_identity", "double", COMPARA),
    gene_order_conservation_score=null_text("goc_score", COMPARA).cast("double"),
    whole_genome_alignment_coverage=null_text("wga_coverage", COMPARA).cast("double"),
    is_high_confidence=F.when(high_confidence == "1", True).when(
        high_confidence == "0", False
    ),
)

# -- Transform -- DDInter drug-drug interactions (drug A subject, drug B object)
DDINTER = "ddinter.interactions"
interactions = read_genomics_bronze("ddinter_interactions")
for side in ("A", "B"):
    interactions = link_entities(
        interactions,
        [(F.upper(text(f"Drug_{side}", DDINTER)), "drug_name")],
        drug_names,
        f"drug_{side}",
        f"drug_{side}_method",
    )
drug_interaction = relation_rows(
    interactions,
    "drug_interaction",
    "ddinter",
    DDINTER,
    subject_type=F.lit("drug"),
    subject_id=text("DDInterID_A", DDINTER),
    subject_name=text("Drug_A", DDINTER),
    subject_key=F.col("drug_A"),
    subject_link_method=F.col("drug_A_method"),
    object_type=F.lit("drug"),
    object_id=text("DDInterID_B", DDINTER),
    object_name=text("Drug_B", DDINTER),
    object_key=F.col("drug_B"),
    object_link_method=F.col("drug_B_method"),
    evidence_level=text("Level", DDINTER),
)

# -- Transform -- ACMG secondary-findings genes and their conditions
ACMG = "acmg.secondary_findings_genes"
gene_text = text("Gene_via_GTR", ACMG)
acmg = read_genomics_bronze("acmg_secondary_findings_genes").select(
    "*",
    F.upper(F.trim(F.regexp_extract(gene_text, r"^([^\s(]+)", 1))).alias("symbol"),
    F.regexp_extract(gene_text, r"MIM\s*(\d+)", 1).alias("gene_mim"),
    F.regexp_extract(text("MedGen", ACMG), r"(C\w*\d+)", 1).alias("medgen_id"),
    F.regexp_extract(
        text("Disease_name_and_MIM_number", ACMG), r"MIM\s*(\d+)", 1
    ).alias("disease_mim"),
)
acmg = link_entities(
    acmg,
    [(F.col("gene_mim"), "omim"), (F.col("symbol"), "gene_symbol")],
    gene_ids,
    "gene_key",
    "gene_link_method",
)
acmg = link_entities(
    acmg,
    [(F.col("medgen_id"), "medgen"), (F.col("disease_mim"), "omim")],
    entity_id_map(xwalk, "disease"),
    "disease_key",
    "disease_link_method",
)
acmg_rows = relation_rows(
    acmg,
    "gene_disease",
    "acmg_secondary_findings",
    ACMG,
    ("gene_key", "gene_link_method", "disease_key", "disease_link_method"),
    source_gene_id=gene_text,
    source_disease_id=text("MedGen", ACMG),
    gene_disease_relation_type=F.lit("secondary_finding_gene"),
    relation_detail=text("Disease_name_and_MIM_number", ACMG),
    comment=text("Variations_that_may_be_pathogenic", ACMG),
)

# -- Transform -- clinical trial conditions, interventions and keywords
STUDY_TERMS = "aact.study_terms"
BROWSE_TERMS = "aact.browse_terms"
INTERVENTIONS = "aact.interventions"


def trial_rows(df, subkind, dataset, object_type, name, key, method, object_id=None):
    extra = {} if object_id is None else {"object_id": object_id}
    return relation_rows(
        df,
        "clinical_trial",
        subkind,
        dataset,
        subject_type=F.lit("clinical_trial"),
        subject_id=text("nct_id", dataset),
        object_type=F.lit(object_type),
        object_name=name,
        object_key=key,
        object_link_method=method,
        **extra,
    )


terms = read_genomics_bronze("aact_study_terms")
term_name = text("name", STUDY_TERMS)
conditions = link_entities(
    terms.filter(text("term_kind", STUDY_TERMS) == "condition"),
    [(F.upper(term_name), "disease_name")],
    disease_names,
    "disease",
    "disease_method",
)
browse = read_genomics_bronze("aact_browse_terms")
mesh = text("mesh_term", BROWSE_TERMS)
browse_kind = text("browse_kind", BROWSE_TERMS)
mesh_conditions = link_entities(
    browse.filter(browse_kind == "condition"),
    [(F.upper(mesh), "disease_name")],
    disease_names,
    "disease",
    "disease_method",
)
mesh_interventions = link_entities(
    browse.filter(browse_kind == "intervention"),
    [(F.upper(mesh), "drug_name")],
    drug_names,
    "drug",
    "drug_method",
)
intervention_name = text("name", INTERVENTIONS)
listed = link_entities(
    read_genomics_bronze("aact_interventions"),
    [(F.upper(intervention_name), "drug_name")],
    drug_names,
    "drug",
    "drug_method",
)
# e.g. mesh_condition_list, mesh_intervention_ancestor
mesh_subkind = F.concat(
    F.lit("mesh_"),
    browse_kind,
    F.lit("_"),
    F.regexp_replace(text("mesh_type", BROWSE_TERMS), r"^mesh-", ""),
)
trial = _union(
    [
        trial_rows(
            conditions,
            "condition",
            STUDY_TERMS,
            "disease",
            term_name,
            F.col("disease"),
            F.col("disease_method"),
        ),
        trial_rows(
            terms.filter(text("term_kind", STUDY_TERMS) == "keyword"),
            "keyword",
            STUDY_TERMS,
            "keyword",
            term_name,
            F.lit(None).cast("string"),
            F.lit(None).cast("string"),
        ),
        trial_rows(
            mesh_conditions,
            mesh_subkind,
            BROWSE_TERMS,
            "disease",
            mesh,
            F.col("disease"),
            F.col("disease_method"),
        ),
        trial_rows(
            mesh_interventions,
            mesh_subkind,
            BROWSE_TERMS,
            "drug",
            mesh,
            F.col("drug"),
            F.col("drug_method"),
        ),
        trial_rows(
            listed,
            F.lower(text("intervention_type", INTERVENTIONS)),
            INTERVENTIONS,
            "intervention",
            intervention_name,
            F.col("drug"),
            F.col("drug_method"),
            object_id=text("id", INTERVENTIONS),
        ),
    ]
)

# COMMAND ----------

# DBTITLE 1,Build -- entity_relation: union and keys
# -- Transform -- one flat table of all kinds
KEY_COLUMNS = [
    "relation_kind",
    "relation_subkind",
    "source_dataset",
    "gene_key",
    "disease_key",
    "source_gene_id",
    "source_disease_id",
    "gene_disease_relation_type",
    "aggregation_value",
    "study_id",
    "classification",
    "evaluation_date",
    "mode_of_inheritance",
    "report_url",
    "comment",
    "drug_key",
    "source_drug_id",
    "drug_gene_relation_type",
    "relation_detail",
    "evidence_level",
    "pubmed_id",
    "subject_type",
    "subject_id",
    "subject_source_id",
    "object_type",
    "object_id",
    "object_name",
    "object_key",
    "pharmacogenomic_evidence",
    "association",
    "pharmacokinetic",
    "pharmacodynamic",
    "phenotype_key",
    "is_negated",
    "frequency",
    "onset",
    "sex",
    "modifier",
    "phenotype_evidence",
    "reference",
    "biocuration",
    "member_id_type",
    "member_id",
    "pathway_key",
    "pathway_evidence",
    "species",
    "interaction_id",
    "interactor_a_id",
    "interactor_b_id",
    "experimental_system",
    "author",
    "publication_source",
    "combined_score",
]
relation = (
    _union(
        [
            gene_disease,
            phenotype,
            drug_gene,
            pharmacogenomic,
            pathway,
            interaction,
            variation_member,
            regulation,
            homology,
            drug_interaction,
            acmg_rows,
            trial,
        ]
    )
    .distinct()
    .withColumn("_srid", sha_key(*KEY_COLUMNS))
)
relation = conform_genomics(
    add_genomics_provenance(relation, "_srid", RID), "entity_relation"
)

# COMMAND ----------

# DBTITLE 1,Write, inspect and export -- entity_relation
write_inspect_export(
    relation,
    "entity_relation",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    findings=FINDINGS,
    key_cols=["source_record_id"],
    extra_checks={
        "kinds": lambda t: str(
            {
                r["relation_kind"]: r["count"]
                for r in t.groupBy("relation_kind").count().collect()
            }
        ),
        "subkinds": lambda t: str(
            {
                f"{r['relation_kind']}.{r['relation_subkind']}": r["count"]
                for r in t.groupBy("relation_kind", "relation_subkind")
                .count()
                .collect()
            }
        ),
        "gene_disease_rows_without_a_gene_key": lambda t: t.filter(
            (F.col("relation_kind") == "gene_disease") & F.col("gene_key").isNull()
        ).count(),
        "gene_disease_rows_without_a_disease_key": lambda t: t.filter(
            (F.col("relation_kind") == "gene_disease") & F.col("disease_key").isNull()
        ).count(),
        "drug_gene_rows_without_a_drug_key": lambda t: t.filter(
            (F.col("relation_kind") == "drug_gene") & F.col("drug_key").isNull()
        ).count(),
        "drug_gene_rows_without_a_gene_key": lambda t: t.filter(
            (F.col("relation_kind") == "drug_gene") & F.col("gene_key").isNull()
        ).count(),
        "drug_name_links": lambda t: t.filter(
            F.col("drug_link_method") == "drug_name"
        ).count(),
        "pharmacogenomic_subjects_without_a_key": lambda t: t.filter(
            (F.col("relation_kind") == "pharmacogenomic")
            & F.col("subject_key").isNull()
        ).count(),
        "negated_phenotype_annotations": lambda t: t.filter(
            F.col("is_negated")
        ).count(),
        "pathway_members_without_a_key": lambda t: t.filter(
            (F.col("relation_kind") == "pathway_member") & F.col("member_key").isNull()
        ).count(),
        "interactions_without_a_protein_key": lambda t: t.filter(
            (F.col("relation_kind") == "protein_interaction")
            & (F.col("protein_a_key").isNull() | F.col("protein_b_key").isNull())
        ).count(),
        "variation_members_without_a_key": lambda t: t.filter(
            (F.col("relation_kind") == "variation_member")
            & F.col("member_key").isNull()
        ).count(),
        "variation_allele_pairs_without_a_coordinate": lambda t: unplaced_pairs.count(),
        "links_by_kind_and_method": lambda t: str(
            {
                f"{r['relation_kind']}.{r['method']}": r["count"]
                for r in t.filter(
                    F.col("relation_kind").isin(
                        "regulatory_interaction",
                        "homology",
                        "drug_interaction",
                        "clinical_trial",
                    )
                )
                .withColumn(
                    "method",
                    F.coalesce(
                        "object_link_method",
                        F.when(
                            F.col("gene_a_key").isNotNull()
                            & F.col("gene_b_key").isNotNull(),
                            F.lit("both_genes"),
                        ),
                        F.when(F.col("gene_a_key").isNotNull(), F.lit("gene_a_only")),
                        F.lit("unlinked"),
                    ),
                )
                .groupBy("relation_kind", "method")
                .count()
                .collect()
            }
        ),
        "acmg_genes_without_a_gene_key": lambda t: t.filter(
            (F.col("relation_subkind") == "acmg_secondary_findings")
            & F.col("gene_key").isNull()
        ).count(),
    },
)

# COMMAND ----------

# DBTITLE 1,Build -- gene_disease_score_year
# -- Read Silver -- id maps
xwalk = read_silver("entity_id_crosswalk")
linked = pair_linker(
    entity_id_map(xwalk, "gene"), "gene", entity_id_map(xwalk, "disease"), "disease"
)
YEAR_SCHEMA = (
    "array<struct<year:double,associationScore:double,novelty:double,"
    "yearlyEvidenceCount:double>>"
)

# -- Transform -- one row per association and year (overall and per datasource)
years = []
for table, dataset, level in (
    (
        "opentargets_association_overall_direct",
        "opentargets.association_overall_direct",
        "overall",
    ),
    (
        "opentargets_association_by_datasource_direct",
        "opentargets.association_by_datasource_direct",
        "datasource",
    ),
):
    part = linked(
        read_genomics_bronze(table).withColumn(
            "point", F.explode(F.from_json("timeseries", YEAR_SCHEMA))
        ),
        [(text("targetId", dataset), "ensembl")],
        [(text("diseaseId", dataset), "opentargets_disease")],
    )
    years.append(
        part.select(
            "gene_key",
            "disease_key",
            text("targetId", dataset).alias("source_gene_id"),
            text("diseaseId", dataset).alias("source_disease_id"),
            "gene_link_method",
            "disease_link_method",
            F.lit(level).alias("association_level"),
            (
                text("aggregationValue", dataset)
                if level == "datasource"
                else F.lit(None).cast("string")
            ).alias("datasource_id"),
            F.col("point.year").cast("int").alias("year"),
            F.col("point.associationScore").alias("association_score"),
            F.col("point.novelty").alias("novelty"),
            F.col("point.yearlyEvidenceCount").alias("yearly_evidence_count"),
            F.lit(dataset).alias("source_dataset"),
        )
    )
score_year = (
    _union(years)
    .filter(F.col("year").isNotNull())
    .distinct()
    .withColumn(
        "_srid",
        sha_key(
            "source_gene_id",
            "source_disease_id",
            "association_level",
            "datasource_id",
            "year",
        ),
    )
)
score_year = conform_genomics(
    add_genomics_provenance(score_year, "_srid", RID), "gene_disease_score_year"
)

# COMMAND ----------

# DBTITLE 1,Write, inspect and export -- gene_disease_score_year
write_inspect_export(
    score_year,
    "gene_disease_score_year",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    findings=FINDINGS,
    key_cols=["source_record_id"],
    extra_checks={
        "associations": lambda t: (
            t.select(
                "source_gene_id",
                "source_disease_id",
                "association_level",
                "datasource_id",
            )
            .distinct()
            .count()
        ),
        "years": lambda t: t.select("year").distinct().count(),
    },
    link_method_counts=True,
)

# COMMAND ----------

# DBTITLE 1,Build -- gene_expression_median
# -- Read Silver -- gene ids
xwalk = read_silver("entity_id_crosswalk")
gene_map = entity_id_map(xwalk, "gene")

# -- Transform -- wide tissue columns to rows
DATASET = "gtex.gene_median_tpm"
gtex = read_genomics_bronze("gtex_gene_median_tpm")
tissues = gtex.columns[2:]
stack = "stack({}, {})".format(
    len(tissues), ", ".join(f"'{t}', `{t}`" for t in tissues)
)
long_rows = gtex.select(
    F.col("Name").alias("gene_ensembl_id"),
    F.col("Description").alias("gene_symbol"),
    F.expr(stack).alias("tissue_key", "median_text"),
).withColumn(
    "median_transcripts_per_million",
    typed_number("median_text", "double", RULES, None),
)
long_rows = link_entities(
    long_rows,
    [(ensembl_unversioned_expr(F.col("gene_ensembl_id")), "ensembl")],
    gene_map,
    "gene_key",
    "gene_link_method",
)

# -- Transform -- columns and provenance
expression = (
    long_rows.filter(F.col("median_transcripts_per_million").isNotNull())
    .withColumn("source_dataset", F.lit(DATASET))
    .withColumn("_srid", sha_key("gene_ensembl_id", "tissue_key"))
)
expression = conform_genomics(
    add_genomics_provenance(expression, "_srid", RID), "gene_expression_median"
)

# COMMAND ----------

# DBTITLE 1,Write, inspect and export -- gene_expression_median
write_inspect_export(
    expression,
    "gene_expression_median",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    findings=FINDINGS,
    key_cols=["gene_ensembl_id", "tissue_key"],
    extra_checks={
        "tissues": lambda t: t.select("tissue_key").distinct().count(),
        "genes_without_a_gene_key": lambda t: (
            t.filter(F.col("gene_key").isNull())
            .select("gene_ensembl_id")
            .distinct()
            .count()
        ),
        "zero_values": lambda t: t.filter(
            F.col("median_transcripts_per_million") == 0
        ).count(),
    },
    link_method_counts=True,
)
