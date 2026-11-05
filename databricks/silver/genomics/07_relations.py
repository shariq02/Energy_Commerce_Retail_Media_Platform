# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER -- RELATIONS (GENOMICS)
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** November 2026
# MAGIC
# MAGIC **Purpose:** gene-disease, phenotype, drug-gene and pharmacogenomic relations.

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
from pyspark.sql import functions as F

SOURCE = "genomics"
COMPONENT = "silver/genomics/07_relations"
RID = run_id()
FINDINGS = "genomics"
RULES = load_genomics_rules()

# COMMAND ----------

# DBTITLE 1,Build -- gene_disease_association
# -- Read Silver -- id maps
xwalk = read_silver("entity_id_crosswalk")
gene_map = entity_id_map(xwalk, "gene")
disease_map = entity_id_map(xwalk, "disease")
COLUMNS = GENOMICS_STRUCTURES["gene_disease_association"]

# -- Helpers -- typed columns, links and the common row shape

text = source_text

number = source_number

linked = pair_linker(gene_map, "gene", disease_map, "disease")

rows = rows_factory(COLUMNS, label_column="record_kind", from_source=True)

# -- Transform -- OMIM gene links and ClinGen
OMIM = "ncbi_gene.mim2gene_medgen"
omim = linked(
    read_genomics_bronze("ncbi_gene_mim2gene_medgen"),
    [(text("GeneID", OMIM), "ncbi_gene")],
    [(text("MedGenCUI", OMIM), "medgen"), (text("MIM_number", OMIM), "omim")],
)
parts = [
    rows(
        omim,
        "omim_gene_link",
        OMIM,
        source_gene_id=text("GeneID", OMIM),
        source_disease_id=F.coalesce(text("MedGenCUI", OMIM), text("MIM_number", OMIM)),
        relation_type=text("type", OMIM),
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
    rows(
        validity,
        "clingen_validity",
        VALIDITY,
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
        rows(
            part,
            "clingen_dosage",
            DOSAGE,
            source_gene_id=text("Gene_ID", DOSAGE),
            source_disease_id=text(disease, DOSAGE),
            relation_type=F.lit(kind),
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
for table, dataset, kind in TARGETS:
    part = linked(
        read_genomics_bronze(table),
        [(text("targetId", dataset), "ensembl")],
        [(text("diseaseId", dataset), "opentargets_disease")],
    )
    parts.append(
        rows(
            part,
            kind,
            dataset,
            source_gene_id=text("targetId", dataset),
            source_disease_id=text("diseaseId", dataset),
            score=number("associationScore", "double", dataset),
            evidence_count=number("evidenceCount", "bigint", dataset),
            relation_type=text("aggregationType", dataset),
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
    rows(
        gwas,
        "opentargets_gwas_credible_set",
        GWAS,
        source_gene_id=text("targetId", GWAS),
        source_disease_id=text("diseaseId", GWAS),
        score=number("score", "double", GWAS),
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
    read_silver("variant_condition")
    .filter(F.col("disease_key").isNotNull() & ~F.col("is_generic_label"))
    .select("variant_key", "disease_key")
)
pathogenic = (
    read_silver("variant_classification")
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
    rows(clinvar_pairs, "clinvar_variant_condition", "clinvar.variant_summary")
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
    rows(
        hpo,
        "hpo_gene_disease",
        HPO,
        source_gene_id=text("ncbi_gene_id", HPO),
        source_disease_id=text("disease_id", HPO),
        evidence_count=F.col("phenotype_count"),
    )
)

# -- Transform -- union and provenance
association = (
    _union(parts)
    .distinct()
    .withColumn(
        "_srid",
        sha_key(
            "gene_key",
            "disease_key",
            "record_kind",
            "source_dataset",
            "source_gene_id",
            "source_disease_id",
            "relation_type",
            "aggregation_value",
            "study_id",
            "classification",
            "evaluation_date",
        ),
    )
)
association = conform_genomics(
    add_genomics_provenance(association, "_srid", RID), "gene_disease_association"
)

# COMMAND ----------

# DBTITLE 1,Write, inspect and export -- gene_disease_association
write_silver(
    association,
    "gene_disease_association",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
)

written = spark.table(genomics_table("gene_disease_association"))
blocks = inspect_table(
    written,
    "gene_disease_association",
    source=FINDINGS,
    component=COMPONENT,
    rid=RID,
    key_cols=["source_record_id"],
    extra_checks={
        "kinds": str(
            {
                r["record_kind"]: r["count"]
                for r in written.groupBy("record_kind").count().collect()
            }
        ),
        "rows_without_a_gene_key": written.filter(F.col("gene_key").isNull()).count(),
        "rows_without_a_disease_key": written.filter(
            F.col("disease_key").isNull()
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
    f"{COMPONENT.split('/')[-1]}__gene_disease_association",
    "gene_disease_association",
    blocks,
)

# COMMAND ----------

# DBTITLE 1,Build -- phenotype_annotation
# -- Read Silver -- id maps
xwalk = read_silver("entity_id_crosswalk")
gene_map = entity_id_map(xwalk, "gene")
disease_map = entity_id_map(xwalk, "disease")

# -- Transform -- disease annotations
ANN = "hpo.phenotype_annotations"

text = source_text

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
    text("evidence", ANN).alias("evidence"),
    text("reference", ANN).alias("reference"),
    text("biocuration", ANN).alias("biocuration"),
    F.lit(ANN).alias("source_dataset"),
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
    none.alias("evidence"),
    none.alias("reference"),
    none.alias("biocuration"),
    F.lit(GENES).alias("source_dataset"),
)

# -- Transform -- union and provenance
annotation = (
    disease_rows.unionByName(gene_rows)
    .filter(F.col("phenotype_key").isNotNull())
    .distinct()
    .withColumn(
        "_srid",
        sha_key(
            "subject_type",
            "subject_source_id",
            "phenotype_key",
            "is_negated",
            "frequency",
            "onset",
            "sex",
            "modifier",
            "evidence",
            "reference",
            "source_dataset",
        ),
    )
)
annotation = conform_genomics(
    add_genomics_provenance(annotation, "_srid", RID), "phenotype_annotation"
)

# COMMAND ----------

# DBTITLE 1,Write, inspect and export -- phenotype_annotation
write_silver(
    annotation, "phenotype_annotation", source=SOURCE, component=COMPONENT, rid=RID
)

written = spark.table(genomics_table("phenotype_annotation"))
blocks = inspect_table(
    written,
    "phenotype_annotation",
    source=FINDINGS,
    component=COMPONENT,
    rid=RID,
    key_cols=["source_record_id"],
    extra_checks={
        "subjects": str(
            {
                r["subject_type"]: r["count"]
                for r in written.groupBy("subject_type").count().collect()
            }
        ),
        "negated_annotations": written.filter(F.col("is_negated")).count(),
        "subjects_without_a_key": written.filter(F.col("subject_key").isNull()).count(),
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
    f"{COMPONENT.split('/')[-1]}__phenotype_annotation",
    "phenotype_annotation",
    blocks,
)

# COMMAND ----------

# DBTITLE 1,Build -- drug_gene_relation
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
COLUMNS = GENOMICS_STRUCTURES["drug_gene_relation"]

# -- Helpers -- typed columns, links and the common row shape

text = source_text


def upper(column):
    return F.upper(F.trim(column))


def symbol_specs(value):
    return [(upper(value), "gene_symbol"), (upper(value), "gene_alias")]


linked = pair_linker(drug_map, "drug", gene_map, "gene")

rows = rows_factory(COLUMNS, from_source=True)

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
)
pairs = linked(
    pairs, [(F.col("chemical_id"), "pharmgkb")], [(F.col("gene_id"), "pharmgkb")]
)
parts = [
    rows(
        pairs,
        REL,
        relation_type=F.lit("pharmgkb_association"),
        relation_detail=text("Association", REL),
        evidence_level=text("Evidence", REL),
        source_drug_id=F.col("chemical_id"),
        source_gene_id=F.col("gene_id"),
        pmids=split_list(text("PMIDs", REL), r"[;|,]"),
    )
]

# -- Transform -- CPIC pairs and FDA biomarkers
CPIC = "cpic.pairs"
id_type, id_value = split_curie(text("drugid", CPIC), RULES)
cpic = linked(
    read_genomics_bronze("cpic_pairs"),
    [(id_value, id_type), (upper(text("drugname", CPIC)), "drug_name")],
    symbol_specs(text("genesymbol", CPIC)),
)
parts.append(
    rows(
        cpic,
        CPIC,
        relation_type=F.lit("cpic_pair"),
        relation_detail=text("guidelinename", CPIC),
        evidence_level=text("cpiclevel", CPIC),
        source_drug_id=text("drugid", CPIC),
        source_gene_id=text("genesymbol", CPIC),
        pmids=split_list(text("pmids", CPIC), r"[;|,]"),
    )
)
FDA = "fda.pgx_biomarkers"
drug_name = F.regexp_replace(text("Drug", FDA), r"\s*\(\d+\)\s*$", "")
fda = linked(
    read_genomics_bronze("fda_pgx_biomarkers"),
    [(upper(drug_name), "drug_name")],
    symbol_specs(text("Biomarker", FDA)),
)
parts.append(
    rows(
        fda,
        FDA,
        relation_type=F.lit("fda_label_biomarker"),
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
moa = linked(moa, [(F.col("chembl_id"), "chembl")], [(F.col("target_id"), "ensembl")])
parts.append(
    rows(
        moa,
        MOA,
        relation_type=text("actionType", MOA),
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
tahoe = linked(
    tahoe,
    [(F.col("tahoe_key"), "tahoe_drug")],
    symbol_specs(F.col("target_symbol")),
)
parts.append(
    rows(
        tahoe,
        TAHOE,
        relation_type=F.lit("tahoe_target"),
        relation_detail=text("moa_fine", TAHOE),
        source_drug_id=text("drug", TAHOE),
        source_gene_id=F.col("target_symbol"),
    )
)

# -- Transform -- union and provenance
relation = (
    _union(parts)
    .distinct()
    .withColumn(
        "_srid",
        sha_key(
            "drug_key",
            "gene_key",
            "relation_type",
            "relation_detail",
            "source_dataset",
            "source_drug_id",
            "source_gene_id",
            "evidence_level",
        ),
    )
)
relation = conform_genomics(
    add_genomics_provenance(relation, "_srid", RID), "drug_gene_relation"
)

# COMMAND ----------

# DBTITLE 1,Write, inspect and export -- drug_gene_relation
write_silver(
    relation, "drug_gene_relation", source=SOURCE, component=COMPONENT, rid=RID
)

written = spark.table(genomics_table("drug_gene_relation"))
blocks = inspect_table(
    written,
    "drug_gene_relation",
    source=FINDINGS,
    component=COMPONENT,
    rid=RID,
    key_cols=["source_record_id"],
    extra_checks={
        "relation_types": str(
            {
                r["relation_type"]: r["count"]
                for r in written.groupBy("relation_type").count().collect()
            }
        ),
        "rows_without_a_drug_key": written.filter(F.col("drug_key").isNull()).count(),
        "rows_without_a_gene_key": written.filter(F.col("gene_key").isNull()).count(),
        "name_links": written.filter(F.col("drug_link_method") == "drug_name").count(),
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
    f"{COMPONENT.split('/')[-1]}__drug_gene_relation",
    "drug_gene_relation",
    blocks,
)

# COMMAND ----------

# DBTITLE 1,Build -- pharmacogenomic_association
# -- Read Silver -- id maps
xwalk = read_silver("entity_id_crosswalk")
gene_map = entity_id_map(xwalk, "gene")
drug_map = entity_id_map(xwalk, "drug")
variant_map = entity_id_map(xwalk, "variant")

# -- Transform -- relationships and keys of each end
REL = "pharmgkb.relationships"

text = text_for(REL)

rel = read_genomics_bronze("pharmgkb_relationships")
chemical_gene = (F.col("Entity1_type") == "Chemical") & (
    F.col("Entity2_type") == "Gene"
)
gene_chemical = (F.col("Entity1_type") == "Gene") & (
    F.col("Entity2_type") == "Chemical"
)
other = rel.filter(~(chemical_gene | gene_chemical))


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

# -- Transform -- columns and provenance
association = (
    other.select(
        text("Entity1_type").alias("subject_type"),
        text("Entity1_id").alias("subject_id"),
        text("Entity1_name").alias("subject_name"),
        "subject_key",
        text("Entity2_type").alias("object_type"),
        text("Entity2_id").alias("object_id"),
        text("Entity2_name").alias("object_name"),
        "object_key",
        text("Evidence").alias("evidence"),
        text("Association").alias("association"),
        text("PK").alias("pharmacokinetic"),
        text("PD").alias("pharmacodynamic"),
        split_list(text("PMIDs"), r"[;|,]").alias("pmids"),
        F.lit(REL).alias("source_dataset"),
    )
    .distinct()
    .withColumn(
        "_srid",
        sha_key(
            "subject_type",
            "subject_id",
            "object_type",
            "object_id",
            "evidence",
            "association",
            "pharmacokinetic",
            "pharmacodynamic",
        ),
    )
)
association = conform_genomics(
    add_genomics_provenance(association, "_srid", RID), "pharmacogenomic_association"
)

# COMMAND ----------

# DBTITLE 1,Write, inspect and export -- pharmacogenomic_association
write_silver(
    association,
    "pharmacogenomic_association",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
)

written = spark.table(genomics_table("pharmacogenomic_association"))
blocks = inspect_table(
    written,
    "pharmacogenomic_association",
    source=FINDINGS,
    component=COMPONENT,
    rid=RID,
    key_cols=["source_record_id"],
    extra_checks={
        "pairs_by_type": str(
            {
                r["subject_type"]: r["count"]
                for r in written.groupBy("subject_type").count().collect()
            }
        ),
        "subjects_without_a_key": written.filter(F.col("subject_key").isNull()).count(),
        "objects_without_a_key": written.filter(F.col("object_key").isNull()).count(),
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
    f"{COMPONENT.split('/')[-1]}__pharmacogenomic_association",
    "pharmacogenomic_association",
    blocks,
)
