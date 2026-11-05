# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER -- TRANSCRIPT (GENOMICS)
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** November 2026
# MAGIC
# MAGIC **Purpose:** one record per transcript; each field is the best claim by the
# MAGIC precedence rules, with its source and method.

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
COMPONENT = "silver/genomics/gene_protein/06_transcript"
RID = run_id()
FINDINGS = "genomics"
TABLE = "transcript"
RULES = load_genomics_rules()

# COMMAND ----------

# DBTITLE 1,Read Silver -- claims, crosswalk and coordinates
claims = read_silver("entity_claim")
xwalk = read_silver("entity_id_crosswalk").filter(F.col("entity_type") == "transcript")
coordinates = read_silver("entity_coordinate").filter(
    (F.col("entity_type") == "transcript")
    & (F.col("assembly") == "GRCh38")
    & F.col("is_primary")
)

# COMMAND ----------

# DBTITLE 1,Transform -- resolve the fields
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

# COMMAND ----------

# DBTITLE 1,Transform -- master and provenance
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
transcript = conform_genomics(add_genomics_provenance(transcript, "_srid", RID), TABLE)

# COMMAND ----------

# DBTITLE 1,Write Silver -- transcript
write_silver(transcript, TABLE, source=SOURCE, component=COMPONENT, rid=RID)

# COMMAND ----------

# DBTITLE 1,Inspect -- transcript
written = spark.table(genomics_table(TABLE))
conflict_counts = {
    f"{row['field']}_conflicts": row["count"]
    for row in conflicts.groupBy("field").count().collect()
}
blocks = inspect_table(
    written,
    TABLE,
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
    },
)

# COMMAND ----------

# DBTITLE 1,Export findings -- transcript
write_silver_findings(FINDINGS, f"{COMPONENT.split('/')[-1]}__{TABLE}", TABLE, blocks)
