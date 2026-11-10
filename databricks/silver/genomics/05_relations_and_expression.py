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
# MAGIC drug-gene, pharmacogenomic, phenotype, pathway-member and protein-interaction
# MAGIC records) and the median gene expression per tissue.

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
    part = linked(
        dosage.filter(text(score, DOSAGE).isNotNull()),
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
        )
    )
GWAS = "opentargets.evidence_gwas_credible_sets"
gwas = linked(
    read_genomics_bronze("opentargets_evidence_gwas_credible_sets"),
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
            (*PHENOTYPE_KEEP, "phenotype_evidence"),
        )
        for frame, dataset in ((disease_rows, ANN), (gene_rows, GENES))
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
    _union([gene_disease, phenotype, drug_gene, pharmacogenomic, pathway, interaction])
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
    },
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
