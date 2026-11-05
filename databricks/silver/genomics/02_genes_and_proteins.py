# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER -- GENES AND PROTEINS (GENOMICS)
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** November 2026
# MAGIC
# MAGIC **Purpose:** the gene, transcript and protein masters, their links and the protein
# MAGIC features.

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
COMPONENT = "silver/genomics/02_genes_and_proteins"
RID = run_id()
FINDINGS = "genomics"
RULES = load_genomics_rules()

# COMMAND ----------

# DBTITLE 1,Build -- gene
# -- Constants -- gene
NCBI = "ncbi_gene.gene_info"
CHROMOSOME = r"^(?:[0-9]+|X|Y|MT)"

# -- Read Silver -- claims, crosswalk and coordinates
claims = read_silver("entity_claim")
xwalk = read_silver("entity_id_crosswalk").filter(F.col("entity_type") == "gene")
coordinates = read_silver("entity_coordinate").filter(
    (F.col("entity_type") == "gene")
    & (F.col("assembly") == "GRCh38")
    & F.col("is_primary")
)

# -- Transform -- resolve the fields
masters, conflicts = resolve_master(claims, entity_type="gene", fields=GENE_FIELDS)
keys = xwalk.select("entity_key").distinct()
ncbi_keys = (
    xwalk.filter((F.col("source_dataset") == NCBI) & (F.col("id_type") == "ncbi_gene"))
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

# -- Transform -- master, cytoband parts and provenance
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
gene = conform_genomics(add_genomics_provenance(gene, "_srid", RID), "gene")

# COMMAND ----------

# DBTITLE 1,Write, inspect and export -- gene
write_silver(gene, "gene", source=SOURCE, component=COMPONENT, rid=RID)

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

write_silver_findings(FINDINGS, f"{COMPONENT.split('/')[-1]}__gene", "gene", blocks)

# COMMAND ----------

# DBTITLE 1,Build -- transcript
# -- Read Silver -- claims, crosswalk and coordinates
claims = read_silver("entity_claim")
xwalk = read_silver("entity_id_crosswalk").filter(F.col("entity_type") == "transcript")
coordinates = read_silver("entity_coordinate").filter(
    (F.col("entity_type") == "transcript")
    & (F.col("assembly") == "GRCh38")
    & F.col("is_primary")
)

# -- Transform -- resolve the fields
masters, conflicts = resolve_master(
    claims, entity_type="transcript", fields=TRANSCRIPT_FIELDS
)
keys = xwalk.groupBy("entity_key").agg(
    F.max(F.when(F.col("id_type") == "ensembl_transcript", F.lit(True))).alias(
        "has_ensembl"
    ),
    F.max(F.when(F.col("id_type") == "refseq_rna", F.lit(True))).alias("has_refseq"),
)
tags = (
    claims.filter((F.col("entity_type") == "transcript") & (F.col("field") == "tag"))
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

# -- Transform -- master and provenance
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
    add_genomics_provenance(transcript, "_srid", RID), "transcript"
)

# COMMAND ----------

# DBTITLE 1,Write, inspect and export -- transcript
write_silver(transcript, "transcript", source=SOURCE, component=COMPONENT, rid=RID)

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

write_silver_findings(
    FINDINGS, f"{COMPONENT.split('/')[-1]}__transcript", "transcript", blocks
)

# COMMAND ----------

# DBTITLE 1,Build -- protein
# -- Read Silver -- claims and crosswalk
claims = read_silver("entity_claim")
xwalk = read_silver("entity_id_crosswalk").filter(F.col("entity_type") == "protein")

# -- Transform -- resolve the fields
masters, conflicts = resolve_master(
    claims, entity_type="protein", fields=PROTEIN_FIELDS
)
keys = xwalk.groupBy("entity_key").agg(
    F.max(F.when(F.col("id_type") == "uniprot", F.lit(True))).alias("has_uniprot"),
    F.max(F.when(F.col("match_method") == "own_ensembl_protein", F.lit(True))).alias(
        "has_own_ensembl"
    ),
)

# -- Transform -- master and provenance
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
protein = conform_genomics(add_genomics_provenance(protein, "_srid", RID), "protein")

# COMMAND ----------

# DBTITLE 1,Write, inspect and export -- protein
write_silver(protein, "protein", source=SOURCE, component=COMPONENT, rid=RID)

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

write_silver_findings(
    FINDINGS, f"{COMPONENT.split('/')[-1]}__protein", "protein", blocks
)

# COMMAND ----------

# DBTITLE 1,Build -- protein_gene_link
# -- Read Silver -- claims and names
claims = read_silver("entity_claim")
names = read_silver("entity_name")

# -- Transform -- exact links through transcripts
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

# -- Transform -- links by gene symbol
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

# -- Transform -- union and provenance
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
links = conform_genomics(
    add_genomics_provenance(links, "_srid", RID), "protein_gene_link"
)

# COMMAND ----------

# DBTITLE 1,Write, inspect and export -- protein_gene_link
write_silver(links, "protein_gene_link", source=SOURCE, component=COMPONENT, rid=RID)

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

write_silver_findings(
    FINDINGS,
    f"{COMPONENT.split('/')[-1]}__protein_gene_link",
    "protein_gene_link",
    blocks,
)

# COMMAND ----------

# DBTITLE 1,Build -- protein_feature
# -- Read Silver -- id crosswalk
xwalk = read_silver("entity_id_crosswalk").filter(F.col("entity_type") == "protein")

# -- Transform -- parse the feature records
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
        F.nullif(F.regexp_extract(qualifiers, r'/note="([^"]*)"', 1), F.lit("")).alias(
            "note"
        ),
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
feature = conform_genomics(
    add_genomics_provenance(feature, "_srid", RID), "protein_feature"
)

# COMMAND ----------

# DBTITLE 1,Write, inspect and export -- protein_feature
write_silver(feature, "protein_feature", source=SOURCE, component=COMPONENT, rid=RID)

written = spark.table(genomics_table("protein_feature"))
blocks = inspect_table(
    written,
    "protein_feature",
    source=FINDINGS,
    component=COMPONENT,
    rid=RID,
    key_cols=["source_record_id"],
    extra_checks={
        "feature_records_in_bronze": read_genomics_bronze("uniprot_features").count(),
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

write_silver_findings(
    FINDINGS,
    f"{COMPONENT.split('/')[-1]}__protein_feature",
    "protein_feature",
    blocks,
)
