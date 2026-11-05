# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER -- GENE (GENOMICS)
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** November 2026
# MAGIC
# MAGIC **Purpose:** one record per gene; each field is the best claim by the
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
COMPONENT = "silver/genomics/gene_protein/05_gene"
RID = run_id()
FINDINGS = "genomics"
TABLE = "gene"
NCBI = "ncbi_gene.gene_info"
CHROMOSOME = r"^(?:[0-9]+|X|Y|MT)"

# COMMAND ----------

# DBTITLE 1,Read Silver -- claims, crosswalk and coordinates
claims = read_silver("entity_claim")
xwalk = read_silver("entity_id_crosswalk").filter(F.col("entity_type") == "gene")
coordinates = read_silver("entity_coordinate").filter(
    (F.col("entity_type") == "gene")
    & (F.col("assembly") == "GRCh38")
    & F.col("is_primary")
)

# COMMAND ----------

# DBTITLE 1,Transform -- resolve the fields
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

# COMMAND ----------

# DBTITLE 1,Transform -- master, cytoband parts and provenance
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
write_silver(gene, TABLE, source=SOURCE, component=COMPONENT, rid=RID)

# COMMAND ----------

# DBTITLE 1,Inspect -- gene
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
    },
)

# COMMAND ----------

# DBTITLE 1,Export findings -- gene
write_silver_findings(FINDINGS, f"{COMPONENT.split('/')[-1]}__{TABLE}", TABLE, blocks)
