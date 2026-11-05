# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER -- NETWORK AND EXPRESSION (GENOMICS)
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** November 2026
# MAGIC
# MAGIC **Purpose:** pathway membership, protein interactions and gene expression medians.
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

# DBTITLE 1,Configuration
from pyspark.sql import functions as F

SOURCE = "genomics"
COMPONENT = "silver/genomics/08_network_and_expression"
RID = run_id()
FINDINGS = "genomics"
RULES = load_genomics_rules()
AVAILABLE = ["pathway_membership", "protein_interaction", "gene_expression_median"]
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

# DBTITLE 1,Build -- pathway_membership
if selected("pathway_membership"):
    # -- Read Silver -- id maps
    xwalk = read_silver("entity_id_crosswalk")
    gene_map = entity_id_map(xwalk, "gene")
    protein_map = entity_id_map(xwalk, "protein")
    transcript_map = entity_id_map(xwalk, "transcript")

    # -- Transform -- member type and links
    DATASET = "reactome.pathway_mappings"

    text = text_for(DATASET)

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

    # -- Transform -- columns and provenance
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

# DBTITLE 1,Write, inspect and export -- pathway_membership
if selected("pathway_membership"):
    write_silver(
        membership, "pathway_membership", source=SOURCE, component=COMPONENT, rid=RID
    )

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

    write_silver_findings(
        FINDINGS,
        f"{COMPONENT.split('/')[-1]}__pathway_membership",
        "pathway_membership",
        blocks,
    )

# COMMAND ----------

# DBTITLE 1,Build -- protein_interaction
if selected("protein_interaction"):
    # -- Read Silver -- id maps
    xwalk = read_silver("entity_id_crosswalk")
    gene_map = entity_id_map(xwalk, "gene")
    protein_map = entity_id_map(xwalk, "protein")
    COLUMNS = GENOMICS_STRUCTURES["protein_interaction"]

    # -- Helpers -- the common row shape

    rows = rows_factory(COLUMNS)

    # -- Transform -- BioGRID
    BIOGRID = "biogrid.interactions"

    text = text_for(BIOGRID)

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

    # -- Transform -- STRING links
    STRING = "string.protein_links"

    number = number_for(STRING, "int")

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

    # -- Transform -- union and provenance
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

# DBTITLE 1,Write, inspect and export -- protein_interaction
if selected("protein_interaction"):
    write_silver(
        interaction, "protein_interaction", source=SOURCE, component=COMPONENT, rid=RID
    )

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

    write_silver_findings(
        FINDINGS,
        f"{COMPONENT.split('/')[-1]}__protein_interaction",
        "protein_interaction",
        blocks,
    )

# COMMAND ----------

# DBTITLE 1,Build -- gene_expression_median
if selected("gene_expression_median"):
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
        add_genomics_provenance(expression, "_srid", RID), TABLE
    )

# COMMAND ----------

# DBTITLE 1,Write, inspect and export -- gene_expression_median
if selected("gene_expression_median"):
    write_silver(
        expression,
        "gene_expression_median",
        source=SOURCE,
        component=COMPONENT,
        rid=RID,
    )

    written = spark.table(genomics_table("gene_expression_median"))
    blocks = inspect_table(
        written,
        "gene_expression_median",
        source=FINDINGS,
        component=COMPONENT,
        rid=RID,
        key_cols=["gene_ensembl_id", "tissue_key"],
        extra_checks={
            "tissues": written.select("tissue_key").distinct().count(),
            "genes_without_a_gene_key": written.filter(F.col("gene_key").isNull())
            .select("gene_ensembl_id")
            .distinct()
            .count(),
            "zero_values": written.filter(
                F.col("median_transcripts_per_million") == 0
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
        f"{COMPONENT.split('/')[-1]}__gene_expression_median",
        "gene_expression_median",
        blocks,
    )
