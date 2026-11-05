# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER -- PROTEIN GENE LINK (GENOMICS)
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** November 2026
# MAGIC
# MAGIC **Purpose:** which gene encodes which protein: exact through transcripts, and by
# MAGIC gene symbol (ambiguous symbols are kept with their gene count).

# COMMAND ----------

# DBTITLE 1,Shared library
# MAGIC %run ../../_silver_common

# COMMAND ----------

# DBTITLE 1,Inspection library
# MAGIC %run ../../_silver_inspect

# COMMAND ----------

# DBTITLE 1,Genomics library
# MAGIC %run ../../_genomics_common

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "genomics"
COMPONENT = "silver/genomics/reference/08_protein_gene_link"
RID = run_id()
FINDINGS = "genomics"
TABLE = "protein_gene_link"
RULES = load_genomics_rules()

# COMMAND ----------

# DBTITLE 1,Read Silver -- claims and names
claims = read_silver("entity_claim")
names = read_silver("entity_name")

# COMMAND ----------

# DBTITLE 1,Transform -- exact links through transcripts
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
links = (
    exact.unionByName(by_symbol)
    .distinct()
    .withColumn(
        "_srid",
        sha_key(
            "protein_key", "gene_key", "link_method", "source_dataset", "source_field"
        ),
    )
)
links = conform_genomics(add_genomics_provenance(links, "_srid", RID), TABLE)

# COMMAND ----------

# DBTITLE 1,Write Silver -- protein_gene_link
write_silver(links, TABLE, source=SOURCE, component=COMPONENT, rid=RID)

# COMMAND ----------

# DBTITLE 1,Inspect -- protein_gene_link
written = spark.table(genomics_table(TABLE))
blocks = inspect_table(
    written,
    TABLE,
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
    },
)

# COMMAND ----------

# DBTITLE 1,Export findings -- protein_gene_link
write_silver_findings(FINDINGS, f"{COMPONENT.split('/')[-1]}__{TABLE}", TABLE, blocks)
