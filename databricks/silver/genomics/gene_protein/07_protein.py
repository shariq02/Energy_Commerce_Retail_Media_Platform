# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER -- PROTEIN (GENOMICS)
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** November 2026
# MAGIC
# MAGIC **Purpose:** one record per protein; each field is the best claim by the
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
COMPONENT = "silver/genomics/gene_protein/07_protein"
RID = run_id()
FINDINGS = "genomics"
TABLE = "protein"
RULES = load_genomics_rules()

# COMMAND ----------

# DBTITLE 1,Read Silver -- claims and crosswalk
claims = read_silver("entity_claim")
xwalk = read_silver("entity_id_crosswalk").filter(F.col("entity_type") == "protein")

# COMMAND ----------

# DBTITLE 1,Transform -- resolve the fields
masters, conflicts = resolve_master(
    claims, entity_type="protein", fields=PROTEIN_FIELDS
)
keys = xwalk.groupBy("entity_key").agg(
    F.max(F.when(F.col("id_type") == "uniprot", F.lit(True))).alias("has_uniprot"),
    F.max(F.when(F.col("match_method") == "own_ensembl_protein", F.lit(True))).alias(
        "has_own_ensembl"
    ),
)

# COMMAND ----------

# DBTITLE 1,Transform -- master and provenance
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
write_silver(protein, TABLE, source=SOURCE, component=COMPONENT, rid=RID)

# COMMAND ----------

# DBTITLE 1,Inspect -- protein
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
    key_cols=["protein_key"],
    df_before=keys,
    extra_checks={
        "proteins_with_an_own_key": written.filter(
            F.col("key_source") != "uniprot_accession"
        ).count(),
        "proteins_without_a_name": written.filter(F.col("name").isNull()).count(),
        **conflict_counts,
    },
)

# COMMAND ----------

# DBTITLE 1,Export findings -- protein
write_silver_findings(FINDINGS, f"{COMPONENT.split('/')[-1]}__{TABLE}", TABLE, blocks)
