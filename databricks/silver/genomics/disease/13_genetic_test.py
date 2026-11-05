# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER -- GENETIC TEST (GENOMICS)
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** November 2026
# MAGIC
# MAGIC **Purpose:** one record per GTR test (accession without version): the latest
# MAGIC version's genes and conditions as arrays.

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

# DBTITLE 1,Entity builders
# MAGIC %run ../../_genomics_entities

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "genomics"
COMPONENT = "silver/genomics/disease/13_genetic_test"
RID = run_id()
FINDINGS = "genomics"
TABLE = "genetic_test"
RULES = load_genomics_rules()

# COMMAND ----------

# DBTITLE 1,Read Silver -- gene and disease ids
xwalk = read_silver("entity_id_crosswalk")
gene_ids = (
    xwalk.filter((F.col("entity_type") == "gene") & (F.col("id_type") == "ncbi_gene"))
    .select(F.col("id_value").alias("_gene_id"), F.col("entity_key").alias("_gene_key"))
    .distinct()
)
disease_ids = (
    xwalk.filter((F.col("entity_type") == "disease") & (F.col("id_type") == "medgen"))
    .select(F.col("id_value").alias("_cui"), F.col("entity_key").alias("_disease_key"))
    .distinct()
)

# COMMAND ----------

# DBTITLE 1,Transform -- latest version of each test
DATASET = "gtr.test_condition_gene"


def clean(column):
    return clean_placeholders(column, RULES, f"{DATASET}.{column}")


rows = (
    genetic_test()
    .withColumn(
        "version",
        F.nullif(F.regexp_extract("accession_version", r"\.(\d+)$", 1), F.lit("")).cast(
            "int"
        ),
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
        "conditions", F.coalesce(F.col("conditions"), F.array().cast(CONDITION_ARRAY))
    )
    .withColumn("_srid", F.col("test_key"))
)
tests = conform_genomics(add_genomics_provenance(tests, "_srid", RID), TABLE)

# COMMAND ----------

# DBTITLE 1,Write Silver -- genetic_test
write_silver(tests, TABLE, source=SOURCE, component=COMPONENT, rid=RID)

# COMMAND ----------

# DBTITLE 1,Inspect -- genetic_test
written = spark.table(genomics_table(TABLE))
blocks = inspect_table(
    written,
    TABLE,
    source=FINDINGS,
    component=COMPONENT,
    rid=RID,
    key_cols=["test_key"],
    extra_checks={
        "tests_in_bronze": rows.select("test_key").distinct().count(),
        "tests_without_genes": written.filter(F.size("genes") == 0).count(),
        "tests_without_conditions": written.filter(F.size("conditions") == 0).count(),
        "rows_with_an_unknown_object": rows.filter(
            ~F.col("kind").isin("gene", "condition")
        ).count(),
    },
)

# COMMAND ----------

# DBTITLE 1,Export findings -- genetic_test
write_silver_findings(FINDINGS, f"{COMPONENT.split('/')[-1]}__{TABLE}", TABLE, blocks)
