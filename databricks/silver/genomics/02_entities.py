# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER -- ENTITIES (GENOMICS)
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** November 2026
# MAGIC
# MAGIC **Purpose:** the entity masters (gene, transcript, protein, disease, drug, cell line,
# MAGIC test) and their links, protein features and ontology relations.
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
COMPONENT = "silver/genomics/02_entities"
RID = run_id()
FINDINGS = "genomics"
RULES = load_genomics_rules()
AVAILABLE = [
    "gene",
    "transcript",
    "protein",
    "protein_gene_link",
    "protein_feature",
    "disease",
    "drug",
    "cell_line",
    "genetic_test",
    "ontology_relation",
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

# DBTITLE 1,Constants -- gene
if selected("gene"):
    NCBI = "ncbi_gene.gene_info"
    CHROMOSOME = r"^(?:[0-9]+|X|Y|MT)"

# COMMAND ----------

# DBTITLE 1,Read Silver -- claims, crosswalk and coordinates
if selected("gene"):
    claims = read_silver("entity_claim")
    xwalk = read_silver("entity_id_crosswalk").filter(F.col("entity_type") == "gene")
    coordinates = read_silver("entity_coordinate").filter(
        (F.col("entity_type") == "gene")
        & (F.col("assembly") == "GRCh38")
        & F.col("is_primary")
    )

# COMMAND ----------

# DBTITLE 1,Transform -- resolve the fields
if selected("gene"):
    masters, conflicts = resolve_master(claims, entity_type="gene", fields=GENE_FIELDS)
    keys = xwalk.select("entity_key").distinct()
    ncbi_keys = (
        xwalk.filter(
            (F.col("source_dataset") == NCBI) & (F.col("id_type") == "ncbi_gene")
        )
        .select("entity_key")
        .distinct()
        .withColumn("in_ncbi", F.lit(True))
    )
    primary = coordinates.select(
        "entity_key",
        F.col("contig").alias("grch38_contig"),
        F.col("start_position").alias("grch38_start_position"),
        F.col("end_position").alias("grch38_end_position"),
        F.col("strand").alias("grch38_strand"),
        F.col("source_dataset").alias("grch38_source"),
    )

# COMMAND ----------

# DBTITLE 1,Transform -- master, cytoband parts and provenance
if selected("gene"):
    location = F.col("map_location")

    def part(pattern):
        return F.nullif(F.regexp_extract(location, CHROMOSOME + pattern, 1), F.lit(""))

    gene = (
        keys.join(ncbi_keys, "entity_key", "left")
        .join(masters, "entity_key", "left")
        .join(primary, "entity_key", "left")
        .withColumn("gene_key", F.col("entity_key"))
        .withColumn(
            "key_source",
            F.when(F.col("in_ncbi"), F.lit("ncbi_gene_id")).otherwise("own_id"),
        )
        .withColumn("cytoband_arm", part(r"([pq])"))
        .withColumn("cytoband_band", part(r"[pq](\d+)"))
        .withColumn("cytoband_sub_band", part(r"[pq]\d+\.(\d+)"))
        .withColumn("_srid", F.col("gene_key"))
    )
    gene = conform_genomics(add_genomics_provenance(gene, "_srid", RID), TABLE)

# COMMAND ----------

# DBTITLE 1,Write Silver -- gene
if selected("gene"):
    write_silver(gene, "gene", source=SOURCE, component=COMPONENT, rid=RID)

# COMMAND ----------

# DBTITLE 1,Inspect -- gene
if selected("gene"):
    written = spark.table(genomics_table("gene"))
    conflict_counts = {
        f"{row['field']}_conflicts": row["count"]
        for row in conflicts.groupBy("field").count().collect()
    }
    blocks = inspect_table(
        written,
        "gene",
        source=FINDINGS,
        component=COMPONENT,
        rid=RID,
        key_cols=["gene_key"],
        df_before=keys,
        extra_checks={
            "genes_with_an_own_key": written.filter(
                F.col("key_source") == "own_id"
            ).count(),
            "genes_without_a_symbol": written.filter(F.col("symbol").isNull()).count(),
            "genes_without_a_grch38_interval": written.filter(
                F.col("grch38_contig").isNull()
            ).count(),
            **conflict_counts,
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

# DBTITLE 1,Export findings -- gene
if selected("gene"):
    write_silver_findings(FINDINGS, f"{COMPONENT.split('/')[-1]}__gene", "gene", blocks)

# COMMAND ----------

# DBTITLE 1,Read Silver -- claims, crosswalk and coordinates
if selected("transcript"):
    claims = read_silver("entity_claim")
    xwalk = read_silver("entity_id_crosswalk").filter(
        F.col("entity_type") == "transcript"
    )
    coordinates = read_silver("entity_coordinate").filter(
        (F.col("entity_type") == "transcript")
        & (F.col("assembly") == "GRCh38")
        & F.col("is_primary")
    )

# COMMAND ----------

# DBTITLE 1,Transform -- resolve the fields
if selected("transcript"):
    masters, conflicts = resolve_master(
        claims, entity_type="transcript", fields=TRANSCRIPT_FIELDS
    )
    keys = xwalk.groupBy("entity_key").agg(
        F.max(F.when(F.col("id_type") == "ensembl_transcript", F.lit(True))).alias(
            "has_ensembl"
        ),
        F.max(F.when(F.col("id_type") == "refseq_rna", F.lit(True))).alias(
            "has_refseq"
        ),
    )
    tags = (
        claims.filter(
            (F.col("entity_type") == "transcript") & (F.col("field") == "tag")
        )
        .groupBy("entity_key")
        .agg(F.sort_array(F.collect_set("value_text")).alias("tags"))
    )
    primary = coordinates.select(
        "entity_key",
        F.col("contig").alias("grch38_contig"),
        F.col("start_position").alias("grch38_start_position"),
        F.col("end_position").alias("grch38_end_position"),
        F.col("strand").alias("grch38_strand"),
        F.col("source_dataset").alias("grch38_source"),
    )

# COMMAND ----------

# DBTITLE 1,Transform -- master and provenance
if selected("transcript"):
    transcript = (
        keys.join(masters, "entity_key", "left")
        .join(tags, "entity_key", "left")
        .join(primary, "entity_key", "left")
        .withColumn("transcript_key", F.col("entity_key"))
        .withColumn(
            "key_source",
            F.when(F.col("has_ensembl"), "ensembl_transcript")
            .when(F.col("has_refseq"), "refseq_rna")
            .otherwise("own_refseq_annotation_name"),
        )
        .withColumn("_srid", F.col("transcript_key"))
    )
    transcript = conform_genomics(
        add_genomics_provenance(transcript, "_srid", RID), TABLE
    )

# COMMAND ----------

# DBTITLE 1,Write Silver -- transcript
if selected("transcript"):
    write_silver(transcript, "transcript", source=SOURCE, component=COMPONENT, rid=RID)

# COMMAND ----------

# DBTITLE 1,Inspect -- transcript
if selected("transcript"):
    written = spark.table(genomics_table("transcript"))
    conflict_counts = {
        f"{row['field']}_conflicts": row["count"]
        for row in conflicts.groupBy("field").count().collect()
    }
    blocks = inspect_table(
        written,
        "transcript",
        source=FINDINGS,
        component=COMPONENT,
        rid=RID,
        key_cols=["transcript_key"],
        df_before=keys,
        extra_checks={
            "transcripts_with_an_own_key": written.filter(
                F.col("key_source") == "own_refseq_annotation_name"
            ).count(),
            "transcripts_without_a_gene": written.filter(
                F.col("gene_key").isNull()
            ).count(),
            "transcripts_without_a_grch38_interval": written.filter(
                F.col("grch38_contig").isNull()
            ).count(),
            **conflict_counts,
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

# DBTITLE 1,Export findings -- transcript
if selected("transcript"):
    write_silver_findings(
        FINDINGS, f"{COMPONENT.split('/')[-1]}__transcript", "transcript", blocks
    )

# COMMAND ----------

# DBTITLE 1,Read Silver -- claims and crosswalk
if selected("protein"):
    claims = read_silver("entity_claim")
    xwalk = read_silver("entity_id_crosswalk").filter(F.col("entity_type") == "protein")

# COMMAND ----------

# DBTITLE 1,Transform -- resolve the fields
if selected("protein"):
    masters, conflicts = resolve_master(
        claims, entity_type="protein", fields=PROTEIN_FIELDS
    )
    keys = xwalk.groupBy("entity_key").agg(
        F.max(F.when(F.col("id_type") == "uniprot", F.lit(True))).alias("has_uniprot"),
        F.max(
            F.when(F.col("match_method") == "own_ensembl_protein", F.lit(True))
        ).alias("has_own_ensembl"),
    )

# COMMAND ----------

# DBTITLE 1,Transform -- master and provenance
if selected("protein"):
    protein = (
        keys.join(masters, "entity_key", "left")
        .withColumn("protein_key", F.col("entity_key"))
        .withColumn(
            "key_source",
            F.when(F.col("has_uniprot"), "uniprot_accession")
            .when(F.col("has_own_ensembl"), "own_ensembl_protein")
            .otherwise("own_refseq_protein"),
        )
        .withColumn("_srid", F.col("protein_key"))
    )
    protein = conform_genomics(add_genomics_provenance(protein, "_srid", RID), TABLE)

# COMMAND ----------

# DBTITLE 1,Write Silver -- protein
if selected("protein"):
    write_silver(protein, "protein", source=SOURCE, component=COMPONENT, rid=RID)

# COMMAND ----------

# DBTITLE 1,Inspect -- protein
if selected("protein"):
    written = spark.table(genomics_table("protein"))
    conflict_counts = {
        f"{row['field']}_conflicts": row["count"]
        for row in conflicts.groupBy("field").count().collect()
    }
    blocks = inspect_table(
        written,
        "protein",
        source=FINDINGS,
        component=COMPONENT,
        rid=RID,
        key_cols=["protein_key"],
        df_before=keys,
        extra_checks={
            "proteins_with_an_own_key": written.filter(
                F.col("key_source") != "uniprot_accession"
            ).count(),
            "proteins_without_a_name": written.filter(F.col("name").isNull()).count(),
            **conflict_counts,
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

# DBTITLE 1,Export findings -- protein
if selected("protein"):
    write_silver_findings(
        FINDINGS, f"{COMPONENT.split('/')[-1]}__protein", "protein", blocks
    )

# COMMAND ----------

# DBTITLE 1,Read Silver -- claims and names
if selected("protein_gene_link"):
    claims = read_silver("entity_claim")
    names = read_silver("entity_name")

# COMMAND ----------

# DBTITLE 1,Transform -- exact links through transcripts
if selected("protein_gene_link"):
    tx_claims = claims.filter(F.col("entity_type") == "transcript")
    genes = tx_claims.filter(F.col("field") == "gene_key").select(
        "entity_key",
        "source_dataset",
        F.col("value_text").alias("gene_key"),
        "source_field",
    )
    proteins = tx_claims.filter(F.col("field") == "protein_key").select(
        "entity_key", "source_dataset", F.col("value_text").alias("protein_key")
    )
    exact = genes.join(proteins, ["entity_key", "source_dataset"]).select(
        "protein_key",
        "gene_key",
        F.lit("transcript_exact").alias("link_method"),
        F.lit(None).cast("int").alias("genes_sharing_symbol"),
        "source_dataset",
        "source_field",
    )

# COMMAND ----------

# DBTITLE 1,Transform -- links by gene symbol
if selected("protein_gene_link"):
    symbols = (
        names.filter(
            (F.col("entity_type") == "gene")
            & F.col("term_type").isin("symbol", "official_symbol")
        )
        .select(
            F.upper(F.col("term")).alias("symbol_key"),
            F.col("entity_key").alias("gene_key"),
        )
        .distinct()
    )
    sharing = symbols.groupBy("symbol_key").agg(
        F.countDistinct("gene_key").alias("genes_sharing_symbol")
    )
    by_symbol = (
        claims.filter(
            (F.col("entity_type") == "protein") & (F.col("field") == "gene_symbol")
        )
        .select(
            F.col("entity_key").alias("protein_key"),
            F.upper(F.trim(F.col("value_text"))).alias("symbol_key"),
            "source_dataset",
            "source_field",
        )
        .join(symbols, "symbol_key")
        .join(sharing, "symbol_key")
        .select(
            "protein_key",
            "gene_key",
            F.lit("symbol").alias("link_method"),
            "genes_sharing_symbol",
            "source_dataset",
            "source_field",
        )
    )

# COMMAND ----------

# DBTITLE 1,Transform -- union and provenance
if selected("protein_gene_link"):
    links = (
        exact.unionByName(by_symbol)
        .distinct()
        .withColumn(
            "_srid",
            sha_key(
                "protein_key",
                "gene_key",
                "link_method",
                "source_dataset",
                "source_field",
            ),
        )
    )
    links = conform_genomics(add_genomics_provenance(links, "_srid", RID), TABLE)

# COMMAND ----------

# DBTITLE 1,Write Silver -- protein_gene_link
if selected("protein_gene_link"):
    write_silver(
        links, "protein_gene_link", source=SOURCE, component=COMPONENT, rid=RID
    )

# COMMAND ----------

# DBTITLE 1,Inspect -- protein_gene_link
if selected("protein_gene_link"):
    written = spark.table(genomics_table("protein_gene_link"))
    blocks = inspect_table(
        written,
        "protein_gene_link",
        source=FINDINGS,
        component=COMPONENT,
        rid=RID,
        key_cols=["source_record_id"],
        extra_checks={
            "exact_links": written.filter(
                F.col("link_method") == "transcript_exact"
            ).count(),
            "symbol_links": written.filter(F.col("link_method") == "symbol").count(),
            "symbol_links_with_several_genes": written.filter(
                F.col("genes_sharing_symbol") > 1
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

# DBTITLE 1,Export findings -- protein_gene_link
if selected("protein_gene_link"):
    write_silver_findings(
        FINDINGS,
        f"{COMPONENT.split('/')[-1]}__protein_gene_link",
        "protein_gene_link",
        blocks,
    )

# COMMAND ----------

# DBTITLE 1,Read Silver -- id crosswalk
if selected("protein_feature"):
    xwalk = read_silver("entity_id_crosswalk").filter(F.col("entity_type") == "protein")

# COMMAND ----------

# DBTITLE 1,Transform -- parse the feature records
if selected("protein_feature"):
    features = with_entity_key(
        read_genomics_bronze("uniprot_features"),
        "accession",
        xwalk,
        "uniprot.entries",
        "uniprot",
    )
    location = F.trim(F.col("location"))
    LOCATION = r"^(\d+)(?:\.\.(\d+))?$"
    start = F.nullif(F.regexp_extract(location, LOCATION, 1), F.lit(""))
    end = F.nullif(F.regexp_extract(location, LOCATION, 2), F.lit(""))
    qualifiers = F.col("qualifiers")
    feature = (
        features.select(
            F.col("entity_key").alias("protein_key"),
            F.col("feature_type"),
            location.alias("location_raw"),
            start.cast("int").alias("start_position"),
            F.coalesce(end, start).cast("int").alias("end_position"),
            F.nullif(
                F.regexp_extract(qualifiers, r'/note="([^"]*)"', 1), F.lit("")
            ).alias("note"),
            F.array_distinct(
                F.expr("regexp_extract_all(qualifiers, '(ECO:[0-9]+)', 1)")
            ).alias("evidence_codes"),
            F.array_distinct(
                F.expr("regexp_extract_all(qualifiers, 'PubMed:([0-9]+)', 1)")
            ).alias("pubmed_ids"),
            F.array_distinct(
                F.expr("regexp_extract_all(qualifiers, 'dbSNP:(rs[0-9]+)', 1)")
            ).alias("dbsnp_ids"),
            qualifiers.alias("qualifiers"),
        )
        .distinct()
        .withColumn(
            "_srid",
            sha_key("protein_key", "feature_type", "location_raw", "qualifiers"),
        )
    )
    feature = conform_genomics(add_genomics_provenance(feature, "_srid", RID), TABLE)

# COMMAND ----------

# DBTITLE 1,Write Silver -- protein_feature
if selected("protein_feature"):
    write_silver(
        feature, "protein_feature", source=SOURCE, component=COMPONENT, rid=RID
    )

# COMMAND ----------

# DBTITLE 1,Inspect -- protein_feature
if selected("protein_feature"):
    written = spark.table(genomics_table("protein_feature"))
    blocks = inspect_table(
        written,
        "protein_feature",
        source=FINDINGS,
        component=COMPONENT,
        rid=RID,
        key_cols=["source_record_id"],
        extra_checks={
            "feature_records_in_bronze": read_genomics_bronze(
                "uniprot_features"
            ).count(),
            "features_without_positions": written.filter(
                F.col("start_position").isNull()
            ).count(),
            "feature_types": written.select("feature_type").distinct().count(),
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

# DBTITLE 1,Export findings -- protein_feature
if selected("protein_feature"):
    write_silver_findings(
        FINDINGS,
        f"{COMPONENT.split('/')[-1]}__protein_feature",
        "protein_feature",
        blocks,
    )

# COMMAND ----------

# DBTITLE 1,Read Silver -- claims and crosswalk
if selected("disease"):
    claims = read_silver("entity_claim")
    xwalk = read_silver("entity_id_crosswalk").filter(F.col("entity_type") == "disease")

# COMMAND ----------

# DBTITLE 1,Transform -- resolve the fields
if selected("disease"):
    masters, conflicts = resolve_master(
        claims, entity_type="disease", fields=DISEASE_FIELDS
    )
    keys = xwalk.groupBy("entity_key").agg(
        F.max(F.when(F.col("id_type") == "medgen", F.lit(True))).alias("has_medgen")
    )

# COMMAND ----------

# DBTITLE 1,Transform -- master and provenance
if selected("disease"):
    disease = (
        keys.join(masters, "entity_key", "left")
        .withColumn("disease_key", F.col("entity_key"))
        .withColumn(
            "key_source",
            F.when(F.col("has_medgen"), "medgen_cui").otherwise("own_opentargets_id"),
        )
        .withColumn("_srid", F.col("disease_key"))
    )
    disease = conform_genomics(add_genomics_provenance(disease, "_srid", RID), TABLE)

# COMMAND ----------

# DBTITLE 1,Write Silver -- disease
if selected("disease"):
    write_silver(disease, "disease", source=SOURCE, component=COMPONENT, rid=RID)

# COMMAND ----------

# DBTITLE 1,Inspect -- disease
if selected("disease"):
    written = spark.table(genomics_table("disease"))
    conflict_counts = {
        f"{row['field']}_conflicts": row["count"]
        for row in conflicts.groupBy("field").count().collect()
    }
    blocks = inspect_table(
        written,
        "disease",
        source=FINDINGS,
        component=COMPONENT,
        rid=RID,
        key_cols=["disease_key"],
        df_before=keys,
        extra_checks={
            "diseases_with_an_own_key": written.filter(
                F.col("key_source") == "own_opentargets_id"
            ).count(),
            "diseases_without_a_name": written.filter(F.col("name").isNull()).count(),
            **conflict_counts,
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

# DBTITLE 1,Export findings -- disease
if selected("disease"):
    write_silver_findings(
        FINDINGS, f"{COMPONENT.split('/')[-1]}__disease", "disease", blocks
    )

# COMMAND ----------

# DBTITLE 1,Read Silver -- claims and crosswalk
if selected("drug"):
    claims = read_silver("entity_claim")
    xwalk = read_silver("entity_id_crosswalk").filter(F.col("entity_type") == "drug")

# COMMAND ----------

# DBTITLE 1,Transform -- resolve the fields
if selected("drug"):
    masters, conflicts = resolve_master(claims, entity_type="drug", fields=DRUG_FIELDS)
    keys = xwalk.groupBy("entity_key").agg(
        F.max(F.when(F.col("id_type") == "chembl", F.lit(True))).alias("has_chembl"),
        F.max(F.when(F.col("id_type") == "pharmgkb", F.lit(True))).alias(
            "has_pharmgkb"
        ),
    )

# COMMAND ----------

# DBTITLE 1,Transform -- master and provenance
if selected("drug"):
    drug = (
        keys.join(masters, "entity_key", "left")
        .withColumn("drug_key", F.col("entity_key"))
        .withColumn(
            "key_source",
            F.when(F.col("has_chembl"), "chembl_id")
            .when(F.col("has_pharmgkb"), "pharmgkb_accession")
            .otherwise("own_tahoe_name"),
        )
        .withColumn("_srid", F.col("drug_key"))
    )
    drug = conform_genomics(add_genomics_provenance(drug, "_srid", RID), TABLE)

# COMMAND ----------

# DBTITLE 1,Write Silver -- drug
if selected("drug"):
    write_silver(drug, "drug", source=SOURCE, component=COMPONENT, rid=RID)

# COMMAND ----------

# DBTITLE 1,Inspect -- drug
if selected("drug"):
    written = spark.table(genomics_table("drug"))
    conflict_counts = {
        f"{row['field']}_conflicts": row["count"]
        for row in conflicts.groupBy("field").count().collect()
    }
    blocks = inspect_table(
        written,
        "drug",
        source=FINDINGS,
        component=COMPONENT,
        rid=RID,
        key_cols=["drug_key"],
        df_before=keys,
        extra_checks={
            "drugs_with_an_own_key": written.filter(
                F.col("key_source") != "chembl_id"
            ).count(),
            "drugs_without_a_name": written.filter(F.col("name").isNull()).count(),
            **conflict_counts,
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

# DBTITLE 1,Export findings -- drug
if selected("drug"):
    write_silver_findings(FINDINGS, f"{COMPONENT.split('/')[-1]}__drug", "drug", blocks)

# COMMAND ----------

# DBTITLE 1,Read Bronze -- Tahoe cell lines
if selected("cell_line"):
    DATASET = "tahoe.cell_line_metadata"
    cells = read_genomics_bronze("tahoe_cell_line_metadata")

    def clean(column):
        return clean_placeholders(column, RULES, f"{DATASET}.{column}")

    keyed = cells.filter(clean("Cell_ID_DepMap").isNotNull())

# COMMAND ----------

# DBTITLE 1,Transform -- group the drivers per cell line
if selected("cell_line"):
    driver = F.struct(
        clean("Driver_Gene_Symbol").alias("gene_symbol"),
        clean("Driver_VarZyg").alias("variant_zygosity"),
        clean("Driver_VarType").alias("variant_type"),
        clean("Driver_ProtEffect_or_CdnaEffect").alias("effect"),
        clean("Driver_Mech_InferDM").alias("mechanism"),
        clean("Driver_GeneType_DM").alias("gene_type"),
    )
    cell_line = (
        keyed.withColumn(
            "driver", F.when(clean("Driver_Gene_Symbol").isNotNull(), driver)
        )
        .groupBy(clean("Cell_ID_DepMap").alias("cell_line_key"))
        .agg(
            F.min(clean("cell_name")).alias("cell_name"),
            F.min(clean("Cell_ID_Cellosaur")).alias("cellosaur_id"),
            F.min(clean("Organ")).alias("organ"),
            F.sort_array(F.collect_set("driver")).alias("drivers"),
        )
        .withColumn("_srid", F.col("cell_line_key"))
    )
    cell_line = conform_genomics(
        add_genomics_provenance(cell_line, "_srid", RID), TABLE
    )

# COMMAND ----------

# DBTITLE 1,Write Silver -- cell_line
if selected("cell_line"):
    write_silver(cell_line, "cell_line", source=SOURCE, component=COMPONENT, rid=RID)

# COMMAND ----------

# DBTITLE 1,Inspect -- cell_line
if selected("cell_line"):
    written = spark.table(genomics_table("cell_line"))
    blocks = inspect_table(
        written,
        "cell_line",
        source=FINDINGS,
        component=COMPONENT,
        rid=RID,
        key_cols=["cell_line_key"],
        extra_checks={
            "rows_without_a_depmap_id": cells.count() - keyed.count(),
            "cell_lines_without_drivers": written.filter(
                F.size("drivers") == 0
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

# DBTITLE 1,Export findings -- cell_line
if selected("cell_line"):
    write_silver_findings(
        FINDINGS, f"{COMPONENT.split('/')[-1]}__cell_line", "cell_line", blocks
    )

# COMMAND ----------

# DBTITLE 1,Read Silver -- gene and disease ids
if selected("genetic_test"):
    xwalk = read_silver("entity_id_crosswalk")
    gene_ids = (
        xwalk.filter(
            (F.col("entity_type") == "gene") & (F.col("id_type") == "ncbi_gene")
        )
        .select(
            F.col("id_value").alias("_gene_id"), F.col("entity_key").alias("_gene_key")
        )
        .distinct()
    )
    disease_ids = (
        xwalk.filter(
            (F.col("entity_type") == "disease") & (F.col("id_type") == "medgen")
        )
        .select(
            F.col("id_value").alias("_cui"), F.col("entity_key").alias("_disease_key")
        )
        .distinct()
    )

# COMMAND ----------

# DBTITLE 1,Transform -- latest version of each test
if selected("genetic_test"):
    DATASET = "gtr.test_condition_gene"

    def clean(column):
        return clean_placeholders(column, RULES, f"{DATASET}.{column}")

    rows = (
        genetic_test()
        .withColumn(
            "version",
            F.nullif(
                F.regexp_extract("accession_version", r"\.(\d+)$", 1), F.lit("")
            ).cast("int"),
        )
        .withColumn("kind", F.lower(F.trim(F.col("object"))))
    )
    latest = rows.groupBy("test_key").agg(
        F.max("version").alias("latest_number"),
        F.countDistinct("version").alias("versions"),
    )
    current = rows.join(latest, "test_key").filter(
        F.col("version").eqNullSafe(F.col("latest_number"))
    )

# COMMAND ----------

# DBTITLE 1,Transform -- genes and conditions as arrays
if selected("genetic_test"):
    genes = (
        current.filter(F.col("kind") == "gene")
        .join(gene_ids, clean("gene_or_SNOMED_CT_ID") == F.col("_gene_id"), "left")
        .groupBy("test_key")
        .agg(
            F.sort_array(
                F.collect_set(
                    F.struct(
                        F.col("_gene_key").alias("gene_key"),
                        clean("gene_symbol").alias("gene_symbol"),
                    )
                )
            ).alias("genes")
        )
    )
    conditions = (
        current.filter(F.col("kind") == "condition")
        .join(disease_ids, clean("GTR_identifier") == F.col("_cui"), "left")
        .groupBy("test_key")
        .agg(
            F.sort_array(
                F.collect_set(
                    F.struct(
                        F.col("_disease_key").alias("disease_key"),
                        clean("object_name").alias("condition_name"),
                        clean("MIM_number").alias("mim_number"),
                        clean("gene_or_SNOMED_CT_ID").alias("snomed_ct_id"),
                    )
                )
            ).alias("conditions")
        )
    )

# COMMAND ----------

# DBTITLE 1,Transform -- test record and provenance
if selected("genetic_test"):
    GENE_ARRAY = "array<struct<gene_key:string,gene_symbol:string>>"
    CONDITION_ARRAY = (
        "array<struct<disease_key:string,condition_name:string,"
        "mim_number:string,snomed_ct_id:string>>"
    )
    tests = (
        current.groupBy("test_key")
        .agg(
            F.max("latest_number").cast("string").alias("latest_version"),
            F.max("versions").alias("versions"),
            F.min(clean("test_type")).alias("test_type"),
        )
        .join(genes, "test_key", "left")
        .join(conditions, "test_key", "left")
        .withColumn("genes", F.coalesce(F.col("genes"), F.array().cast(GENE_ARRAY)))
        .withColumn(
            "conditions",
            F.coalesce(F.col("conditions"), F.array().cast(CONDITION_ARRAY)),
        )
        .withColumn("_srid", F.col("test_key"))
    )
    tests = conform_genomics(add_genomics_provenance(tests, "_srid", RID), TABLE)

# COMMAND ----------

# DBTITLE 1,Write Silver -- genetic_test
if selected("genetic_test"):
    write_silver(tests, "genetic_test", source=SOURCE, component=COMPONENT, rid=RID)

# COMMAND ----------

# DBTITLE 1,Inspect -- genetic_test
if selected("genetic_test"):
    written = spark.table(genomics_table("genetic_test"))
    blocks = inspect_table(
        written,
        "genetic_test",
        source=FINDINGS,
        component=COMPONENT,
        rid=RID,
        key_cols=["test_key"],
        extra_checks={
            "tests_in_bronze": rows.select("test_key").distinct().count(),
            "tests_without_genes": written.filter(F.size("genes") == 0).count(),
            "tests_without_conditions": written.filter(
                F.size("conditions") == 0
            ).count(),
            "rows_with_an_unknown_object": rows.filter(
                ~F.col("kind").isin("gene", "condition")
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

# DBTITLE 1,Export findings -- genetic_test
if selected("genetic_test"):
    write_silver_findings(
        FINDINGS, f"{COMPONENT.split('/')[-1]}__genetic_test", "genetic_test", blocks
    )

# COMMAND ----------

# DBTITLE 1,Read Silver -- id crosswalk
if selected("ontology_relation"):
    xwalk = read_silver("entity_id_crosswalk")
    disease_keys = (
        xwalk.filter(
            (F.col("entity_type") == "disease")
            & (F.col("id_type") == "opentargets_disease")
        )
        .select(F.col("id_value").alias("_ot_id"), F.col("entity_key").alias("_ot_key"))
        .distinct()
    )

# COMMAND ----------

# DBTITLE 1,Transform -- Open Targets disease parents
if selected("ontology_relation"):
    terms = (
        read_genomics_bronze("opentargets_disease")
        .withColumn("curie", curie_expr("id"))
        .withColumn("ontology", F.regexp_extract("curie", r"^([A-Za-z]+):", 1))
    )
    parents = terms.select(
        "id",
        "ontology",
        F.explode(F.from_json("parents", "array<string>")).alias("parent_id"),
    )
    child_map = disease_keys.select(
        F.col("_ot_id").alias("id"), F.col("_ot_key").alias("child_mapped")
    )
    parent_map = disease_keys.select(
        F.col("_ot_id").alias("parent_id"), F.col("_ot_key").alias("parent_mapped")
    )
    opentargets = (
        parents.join(child_map, "id", "left")
        .join(parent_map, "parent_id", "left")
        .select(
            F.nullif("ontology", F.lit("")).alias("ontology"),
            F.coalesce(F.col("parent_mapped"), F.col("parent_id")).alias("parent_key"),
            F.coalesce(F.col("child_mapped"), F.col("id")).alias("child_key"),
            F.lit("parent").alias("relation"),
            F.lit("parent_child").alias("direction"),
            F.lit("opentargets.disease").alias("source_dataset"),
            F.lit("parents").alias("source_field"),
        )
    )

# COMMAND ----------

# DBTITLE 1,Transform -- HPO and Reactome
if selected("ontology_relation"):
    hpo = read_genomics_bronze("hpo_term_relations").select(
        F.lit("HP").alias("ontology"),
        F.when(F.col("pred") == "is_a", curie_expr("obj"))
        .otherwise(curie_expr("sub"))
        .alias("parent_key"),
        F.when(F.col("pred") == "is_a", curie_expr("sub"))
        .otherwise(curie_expr("obj"))
        .alias("child_key"),
        F.col("pred").alias("relation"),
        F.when(F.col("pred") == "is_a", "parent_child")
        .otherwise("source_order")
        .alias("direction"),
        F.lit("hpo.term_relations").alias("source_dataset"),
        F.lit("sub,pred,obj").alias("source_field"),
    )
    reactome = read_genomics_bronze("reactome_pathway_relations").select(
        F.lit("reactome").alias("ontology"),
        F.col("parent_pathway_id").alias("parent_key"),
        F.col("child_pathway_id").alias("child_key"),
        F.lit("parent").alias("relation"),
        F.lit("parent_child").alias("direction"),
        F.lit("reactome.pathway_relations").alias("source_dataset"),
        F.lit("parent_pathway_id,child_pathway_id").alias("source_field"),
    )

# COMMAND ----------

# DBTITLE 1,Transform -- MedGen relations (direction not stated)
if selected("ontology_relation"):
    medgen = (
        read_genomics_bronze("medgen_relations")
        .filter(F.col("SUPPRESS") == "N")
        .select(
            F.col("SAB").alias("ontology"),
            F.col("CUI1").alias("parent_key"),
            F.col("CUI2").alias("child_key"),
            F.coalesce(F.nullif(F.col("RELA"), F.lit("")), F.col("REL")).alias(
                "relation"
            ),
            F.lit("source_order").alias("direction"),
            F.lit("medgen.relations").alias("source_dataset"),
            F.lit("CUI1,CUI2,REL,RELA").alias("source_field"),
        )
    )

# COMMAND ----------

# DBTITLE 1,Transform -- union and provenance
if selected("ontology_relation"):
    relations = (
        opentargets.unionByName(hpo)
        .unionByName(reactome)
        .unionByName(medgen)
        .filter(F.col("parent_key").isNotNull() & F.col("child_key").isNotNull())
        .distinct()
        .withColumn(
            "_srid",
            sha_key(
                "ontology",
                "parent_key",
                "child_key",
                "relation",
                "direction",
                "source_dataset",
                "source_field",
            ),
        )
    )
    relations = conform_genomics(
        add_genomics_provenance(relations, "_srid", RID), TABLE
    )

# COMMAND ----------

# DBTITLE 1,Write Silver -- ontology_relation
if selected("ontology_relation"):
    write_silver(
        relations, "ontology_relation", source=SOURCE, component=COMPONENT, rid=RID
    )

# COMMAND ----------

# DBTITLE 1,Inspect -- ontology_relation
if selected("ontology_relation"):
    written = spark.table(genomics_table("ontology_relation"))
    blocks = inspect_table(
        written,
        "ontology_relation",
        source=FINDINGS,
        component=COMPONENT,
        rid=RID,
        key_cols=["source_record_id"],
        extra_checks={
            "source_order_relations": written.filter(
                F.col("direction") == "source_order"
            ).count(),
            "parent_child_relations": written.filter(
                F.col("direction") == "parent_child"
            ).count(),
            "self_relations": written.filter(
                F.col("parent_key") == F.col("child_key")
            ).count(),
            "ontologies": written.select("ontology").distinct().count(),
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

# DBTITLE 1,Export findings -- ontology_relation
if selected("ontology_relation"):
    write_silver_findings(
        FINDINGS,
        f"{COMPONENT.split('/')[-1]}__ontology_relation",
        "ontology_relation",
        blocks,
    )
