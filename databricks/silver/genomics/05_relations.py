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
# MAGIC **Purpose:** gene-disease, phenotype, drug-gene, pharmacogenomic, pathway and protein
# MAGIC interaction relations.
# MAGIC
# MAGIC **Run:** `TABLES` lists the tables to build (default `all`); the others are skipped.

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

# DBTITLE 1,Imports
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "genomics"
COMPONENT = "silver/genomics/05_relations"
RID = run_id()
FINDINGS = "genomics"
RULES = load_genomics_rules()
AVAILABLE = [
    "gene_disease_association",
    "phenotype_annotation",
    "drug_gene_relation",
    "pharmacogenomic_association",
    "pathway_membership",
    "protein_interaction",
]
dbutils.widgets.text("TABLES", "all")
SELECTED = {
    name.strip() for name in dbutils.widgets.get("TABLES").split(",") if name.strip()
}
UNKNOWN = SELECTED - {"all", *AVAILABLE}
if UNKNOWN:
    message = f"unknown table(s) in TABLES: {sorted(UNKNOWN)}"
    raise ValueError(message)


def selected(table):
    return "all" in SELECTED or table in SELECTED


# COMMAND ----------

# DBTITLE 1,Read Silver -- id maps
if selected("gene_disease_association"):
    xwalk = read_silver("entity_id_crosswalk")
    gene_map = entity_id_map(xwalk, "gene")
    disease_map = entity_id_map(xwalk, "disease")
    COLUMNS = GENOMICS_STRUCTURES["gene_disease_association"]

# COMMAND ----------

# DBTITLE 1,Helpers -- typed columns, links and the common row shape
if selected("gene_disease_association"):

    def text(column, dataset):
        return clean_placeholders(column, RULES, f"{dataset}.{column}")

    def number(column, kind, dataset):
        return typed_number(column, kind, RULES, f"{dataset}.{column}")

    def linked(df, gene_specs, disease_specs):
        df = link_entities(df, gene_specs, gene_map, "gene_key", "gene_link_method")
        return link_entities(
            df, disease_specs, disease_map, "disease_key", "disease_link_method"
        )

    def rows(df, kind, dataset, **given):
        """The frame in the table's columns; a column not given is null."""
        return df.select(
            *[
                (
                    F.lit(kind)
                    if name == "record_kind"
                    else F.lit(dataset)
                    if name == "source_dataset"
                    else given[name]
                    if name in given
                    else F.col(name)
                    if name in df.columns
                    else F.lit(None)
                )
                .cast(kind_)
                .alias(name)
                for name, kind_ in COLUMNS
            ]
        )

# COMMAND ----------

# DBTITLE 1,Transform -- OMIM gene links and ClinGen
if selected("gene_disease_association"):
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
            source_disease_id=F.coalesce(
                text("MedGenCUI", OMIM), text("MIM_number", OMIM)
            ),
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

# COMMAND ----------

# DBTITLE 1,Transform -- Open Targets overall, by datasource and GWAS
if selected("gene_disease_association"):
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

# COMMAND ----------

# DBTITLE 1,Transform -- ClinVar conditions from Silver and HPO
if selected("gene_disease_association"):
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

# COMMAND ----------

# DBTITLE 1,Transform -- union and provenance
if selected("gene_disease_association"):
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
        add_genomics_provenance(association, "_srid", RID), TABLE
    )

# COMMAND ----------

# DBTITLE 1,Write Silver -- gene_disease_association
if selected("gene_disease_association"):
    write_silver(
        association,
        "gene_disease_association",
        source=SOURCE,
        component=COMPONENT,
        rid=RID,
    )

# COMMAND ----------

# DBTITLE 1,Inspect -- gene_disease_association
if selected("gene_disease_association"):
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
            "rows_without_a_gene_key": written.filter(
                F.col("gene_key").isNull()
            ).count(),
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

# COMMAND ----------

# DBTITLE 1,Export findings -- gene_disease_association
if selected("gene_disease_association"):
    write_silver_findings(
        FINDINGS,
        f"{COMPONENT.split('/')[-1]}__gene_disease_association",
        "gene_disease_association",
        blocks,
    )

# COMMAND ----------

# DBTITLE 1,Read Silver -- id maps
if selected("phenotype_annotation"):
    xwalk = read_silver("entity_id_crosswalk")
    gene_map = entity_id_map(xwalk, "gene")
    disease_map = entity_id_map(xwalk, "disease")

# COMMAND ----------

# DBTITLE 1,Transform -- disease annotations
if selected("phenotype_annotation"):
    ANN = "hpo.phenotype_annotations"

    def text(column, dataset):
        return clean_placeholders(column, RULES, f"{dataset}.{column}")

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

# COMMAND ----------

# DBTITLE 1,Transform -- gene annotations
if selected("phenotype_annotation"):
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

# COMMAND ----------

# DBTITLE 1,Transform -- union and provenance
if selected("phenotype_annotation"):
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
        add_genomics_provenance(annotation, "_srid", RID), TABLE
    )

# COMMAND ----------

# DBTITLE 1,Write Silver -- phenotype_annotation
if selected("phenotype_annotation"):
    write_silver(
        annotation, "phenotype_annotation", source=SOURCE, component=COMPONENT, rid=RID
    )

# COMMAND ----------

# DBTITLE 1,Inspect -- phenotype_annotation
if selected("phenotype_annotation"):
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
            "subjects_without_a_key": written.filter(
                F.col("subject_key").isNull()
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

# COMMAND ----------

# DBTITLE 1,Export findings -- phenotype_annotation
if selected("phenotype_annotation"):
    write_silver_findings(
        FINDINGS,
        f"{COMPONENT.split('/')[-1]}__phenotype_annotation",
        "phenotype_annotation",
        blocks,
    )

# COMMAND ----------

# DBTITLE 1,Read Silver -- id maps and name maps
if selected("drug_gene_relation"):
    xwalk = read_silver("entity_id_crosswalk")
    names = read_silver("entity_name")
    gene_map = (
        entity_id_map(xwalk, "gene")
        .unionByName(
            term_map(names, "gene", ("symbol", "official_symbol"), "gene_symbol")
        )
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

# COMMAND ----------

# DBTITLE 1,Helpers -- typed columns, links and the common row shape
if selected("drug_gene_relation"):

    def text(column, dataset):
        return clean_placeholders(column, RULES, f"{dataset}.{column}")

    def upper(column):
        return F.upper(F.trim(column))

    def symbol_specs(value):
        return [(upper(value), "gene_symbol"), (upper(value), "gene_alias")]

    def linked(df, drug_specs, gene_specs):
        df = link_entities(df, drug_specs, drug_map, "drug_key", "drug_link_method")
        return link_entities(df, gene_specs, gene_map, "gene_key", "gene_link_method")

    def rows(df, dataset, **given):
        """The frame in the table's columns; a column not given is null."""
        return df.select(
            *[
                (
                    F.lit(dataset)
                    if name == "source_dataset"
                    else given[name]
                    if name in given
                    else F.col(name)
                    if name in df.columns
                    else F.lit(None)
                )
                .cast(kind)
                .alias(name)
                for name, kind in COLUMNS
            ]
        )

# COMMAND ----------

# DBTITLE 1,Transform -- PharmGKB chemical-gene relationships
if selected("drug_gene_relation"):
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

# COMMAND ----------

# DBTITLE 1,Transform -- CPIC pairs and FDA biomarkers
if selected("drug_gene_relation"):
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

# COMMAND ----------

# DBTITLE 1,Transform -- Open Targets mechanisms and Tahoe targets
if selected("drug_gene_relation"):
    MOA = "opentargets.drug_mechanism_of_action"
    moa = (
        read_genomics_bronze("opentargets_drug_mechanism_of_action")
        .withColumn("chembl_id", F.explode(F.from_json("chemblIds", "array<string>")))
        .withColumn("target_id", F.explode(F.from_json("targets", "array<string>")))
    )
    moa = linked(
        moa, [(F.col("chembl_id"), "chembl")], [(F.col("target_id"), "ensembl")]
    )
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
        .withColumn(
            "tahoe_key", F.concat(F.lit("tahoe:"), F.lower(F.trim(F.col("drug"))))
        )
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

# COMMAND ----------

# DBTITLE 1,Transform -- union and provenance
if selected("drug_gene_relation"):
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
    relation = conform_genomics(add_genomics_provenance(relation, "_srid", RID), TABLE)

# COMMAND ----------

# DBTITLE 1,Write Silver -- drug_gene_relation
if selected("drug_gene_relation"):
    write_silver(
        relation, "drug_gene_relation", source=SOURCE, component=COMPONENT, rid=RID
    )

# COMMAND ----------

# DBTITLE 1,Inspect -- drug_gene_relation
if selected("drug_gene_relation"):
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
            "rows_without_a_drug_key": written.filter(
                F.col("drug_key").isNull()
            ).count(),
            "rows_without_a_gene_key": written.filter(
                F.col("gene_key").isNull()
            ).count(),
            "name_links": written.filter(
                F.col("drug_link_method") == "drug_name"
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

# COMMAND ----------

# DBTITLE 1,Export findings -- drug_gene_relation
if selected("drug_gene_relation"):
    write_silver_findings(
        FINDINGS,
        f"{COMPONENT.split('/')[-1]}__drug_gene_relation",
        "drug_gene_relation",
        blocks,
    )

# COMMAND ----------

# DBTITLE 1,Read Silver -- id maps
if selected("pharmacogenomic_association"):
    xwalk = read_silver("entity_id_crosswalk")
    gene_map = entity_id_map(xwalk, "gene")
    drug_map = entity_id_map(xwalk, "drug")
    variant_map = entity_id_map(xwalk, "variant")

# COMMAND ----------

# DBTITLE 1,Transform -- relationships and keys of each end
if selected("pharmacogenomic_association"):
    REL = "pharmgkb.relationships"

    def text(column):
        return clean_placeholders(column, RULES, f"{REL}.{column}")

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

# COMMAND ----------

# DBTITLE 1,Transform -- columns and provenance
if selected("pharmacogenomic_association"):
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
        add_genomics_provenance(association, "_srid", RID), TABLE
    )

# COMMAND ----------

# DBTITLE 1,Write Silver -- pharmacogenomic_association
if selected("pharmacogenomic_association"):
    write_silver(
        association,
        "pharmacogenomic_association",
        source=SOURCE,
        component=COMPONENT,
        rid=RID,
    )

# COMMAND ----------

# DBTITLE 1,Inspect -- pharmacogenomic_association
if selected("pharmacogenomic_association"):
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
            "subjects_without_a_key": written.filter(
                F.col("subject_key").isNull()
            ).count(),
            "objects_without_a_key": written.filter(
                F.col("object_key").isNull()
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

# COMMAND ----------

# DBTITLE 1,Export findings -- pharmacogenomic_association
if selected("pharmacogenomic_association"):
    write_silver_findings(
        FINDINGS,
        f"{COMPONENT.split('/')[-1]}__pharmacogenomic_association",
        "pharmacogenomic_association",
        blocks,
    )

# COMMAND ----------

# DBTITLE 1,Read Silver -- id maps
if selected("pathway_membership"):
    xwalk = read_silver("entity_id_crosswalk")
    gene_map = entity_id_map(xwalk, "gene")
    protein_map = entity_id_map(xwalk, "protein")
    transcript_map = entity_id_map(xwalk, "transcript")

# COMMAND ----------

# DBTITLE 1,Transform -- member type and links
if selected("pathway_membership"):
    DATASET = "reactome.pathway_mappings"

    def text(column):
        return clean_placeholders(column, RULES, f"{DATASET}.{column}")

    identifier = text("identifier")
    label = F.lower(F.coalesce(text("id_type"), F.lit("")))
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
                    (F.col("member_type") == "protein")
                    & ~identifier.rlike(r"^ENSP\d+"),
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

# COMMAND ----------

# DBTITLE 1,Transform -- columns and provenance
if selected("pathway_membership"):
    membership = (
        mappings.select(
            "member_type",
            F.coalesce("protein_key", "gene_key", "transcript_key").alias("member_key"),
            text("id_type").alias("member_id_type"),
            identifier.alias("member_id"),
            F.coalesce(
                F.when(F.col("protein_key").isNotNull(), F.col("protein_method")),
                F.when(F.col("gene_key").isNotNull(), F.col("gene_method")),
                F.when(F.col("transcript_key").isNotNull(), F.col("transcript_method")),
                F.lit("unlinked"),
            ).alias("member_link_method"),
            text("pathway_id").alias("pathway_key"),
            text("evidence").alias("evidence"),
            text("species").alias("species"),
            F.lit(DATASET).alias("source_dataset"),
        )
        .filter(F.col("pathway_key").isNotNull() & F.col("member_id").isNotNull())
        .distinct()
        .withColumn(
            "_srid",
            sha_key(
                "member_id_type", "member_id", "pathway_key", "evidence", "species"
            ),
        )
    )
    membership = conform_genomics(
        add_genomics_provenance(membership, "_srid", RID), TABLE
    )

# COMMAND ----------

# DBTITLE 1,Write Silver -- pathway_membership
if selected("pathway_membership"):
    write_silver(
        membership, "pathway_membership", source=SOURCE, component=COMPONENT, rid=RID
    )

# COMMAND ----------

# DBTITLE 1,Inspect -- pathway_membership
if selected("pathway_membership"):
    written = spark.table(genomics_table("pathway_membership"))
    blocks = inspect_table(
        written,
        "pathway_membership",
        source=FINDINGS,
        component=COMPONENT,
        rid=RID,
        key_cols=["source_record_id"],
        extra_checks={
            "member_types": str(
                {
                    r["member_type"]: r["count"]
                    for r in written.groupBy("member_type").count().collect()
                }
            ),
            "species": written.select("species").distinct().count(),
            "members_without_a_key": written.filter(
                F.col("member_key").isNull()
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

# COMMAND ----------

# DBTITLE 1,Export findings -- pathway_membership
if selected("pathway_membership"):
    write_silver_findings(
        FINDINGS,
        f"{COMPONENT.split('/')[-1]}__pathway_membership",
        "pathway_membership",
        blocks,
    )

# COMMAND ----------

# DBTITLE 1,Read Silver -- id maps
if selected("protein_interaction"):
    xwalk = read_silver("entity_id_crosswalk")
    gene_map = entity_id_map(xwalk, "gene")
    protein_map = entity_id_map(xwalk, "protein")
    COLUMNS = GENOMICS_STRUCTURES["protein_interaction"]

# COMMAND ----------

# DBTITLE 1,Helpers -- the common row shape
if selected("protein_interaction"):

    def rows(df, dataset, **given):
        """The frame in the table's columns; a column not given is null."""
        return df.select(
            *[
                (
                    F.lit(dataset)
                    if name == "source_dataset"
                    else given[name]
                    if name in given
                    else F.lit(None)
                )
                .cast(kind)
                .alias(name)
                for name, kind in COLUMNS
            ]
        )

# COMMAND ----------

# DBTITLE 1,Transform -- BioGRID
if selected("protein_interaction"):
    BIOGRID = "biogrid.interactions"

    def text(column):
        return clean_placeholders(column, RULES, f"{BIOGRID}.{column}")

    def first_accession(column):
        return F.nullif(F.regexp_extract(text(column), r"^([^|]+)", 1), F.lit(""))

    biogrid = read_genomics_bronze("biogrid_interactions")
    for side in ("A", "B"):
        biogrid = link_entities(
            biogrid,
            [(text(f"Entrez_Gene_Interactor_{side}"), "ncbi_gene")],
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
    biogrid_rows = rows(
        biogrid,
        BIOGRID,
        interaction_source=F.lit("biogrid"),
        interaction_id=text("BioGRID_Interaction_ID"),
        interactor_a_id=text("BioGRID_ID_Interactor_A"),
        interactor_b_id=text("BioGRID_ID_Interactor_B"),
        protein_a_key=F.col("protein_A"),
        protein_b_key=F.col("protein_B"),
        gene_a_key=F.col("gene_A"),
        gene_b_key=F.col("gene_B"),
        experimental_system=text("Experimental_System"),
        experimental_system_type=text("Experimental_System_Type"),
        throughput=text("Throughput"),
        score=typed_number("Score", "double", RULES, f"{BIOGRID}.Score"),
        author=text("Author"),
        publication_source=text("Publication_Source"),
        modification=text("Modification"),
        qualifications=text("Qualifications"),
        tags=text("Tags"),
        source_database=text("Source_Database"),
        organism_a=text("Organism_ID_Interactor_A"),
        organism_b=text("Organism_ID_Interactor_B"),
        ontology_term_ids=text("Ontology_Term_IDs"),
        ontology_term_names=text("Ontology_Term_Names"),
        ontology_term_categories=text("Ontology_Term_Categories"),
    )

# COMMAND ----------

# DBTITLE 1,Transform -- STRING links
if selected("protein_interaction"):
    STRING = "string.protein_links"

    def number(column):
        return typed_number(column, "int", RULES, f"{STRING}.{column}")

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
    CHANNELS = [
        "neighborhood",
        "fusion",
        "cooccurence",
        "coexpression",
        "experimental",
        "database",
        "textmining",
    ]
    string_rows = rows(
        string,
        STRING,
        interaction_source=F.lit("string"),
        interactor_a_id=F.col("protein1"),
        interactor_b_id=F.col("protein2"),
        protein_a_key=F.col("protein_A"),
        protein_b_key=F.col("protein_B"),
        channel_scores=F.array(*[number(name) for name in CHANNELS]),
        combined_score=number("combined_score"),
        organism_a=F.regexp_extract("protein1", r"^(\d+)\.", 1),
        organism_b=F.regexp_extract("protein2", r"^(\d+)\.", 1),
    )

# COMMAND ----------

# DBTITLE 1,Transform -- union and provenance
if selected("protein_interaction"):
    interaction = (
        biogrid_rows.unionByName(string_rows)
        .distinct()
        .withColumn(
            "_srid",
            sha_key(
                "interaction_source",
                "interaction_id",
                "interactor_a_id",
                "interactor_b_id",
                "experimental_system",
                "author",
                "publication_source",
                "combined_score",
            ),
        )
    )
    interaction = conform_genomics(
        add_genomics_provenance(interaction, "_srid", RID), TABLE
    )

# COMMAND ----------

# DBTITLE 1,Write Silver -- protein_interaction
if selected("protein_interaction"):
    write_silver(
        interaction, "protein_interaction", source=SOURCE, component=COMPONENT, rid=RID
    )

# COMMAND ----------

# DBTITLE 1,Inspect -- protein_interaction
if selected("protein_interaction"):
    written = spark.table(genomics_table("protein_interaction"))
    blocks = inspect_table(
        written,
        "protein_interaction",
        source=FINDINGS,
        component=COMPONENT,
        rid=RID,
        key_cols=["source_record_id"],
        extra_checks={
            "sources": str(
                {
                    r["interaction_source"]: r["count"]
                    for r in written.groupBy("interaction_source").count().collect()
                }
            ),
            "records_without_a_protein_key": written.filter(
                F.col("protein_a_key").isNull() | F.col("protein_b_key").isNull()
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

# COMMAND ----------

# DBTITLE 1,Export findings -- protein_interaction
if selected("protein_interaction"):
    write_silver_findings(
        FINDINGS,
        f"{COMPONENT.split('/')[-1]}__protein_interaction",
        "protein_interaction",
        blocks,
    )
