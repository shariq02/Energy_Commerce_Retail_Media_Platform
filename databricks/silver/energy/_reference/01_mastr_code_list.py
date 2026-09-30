# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER -- MASTR CODE LIST
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** every MaStR code list (katalog categories and values, unit
# MAGIC types, location types, market functions, market roles) in one structure
# MAGIC keyed by (catalog_kind, code_id). Run before any MaStR notebook: the
# MAGIC katalog decode reads it.

# COMMAND ----------

# DBTITLE 1,Shared library
# MAGIC %run ../../_silver_common

# COMMAND ----------

# DBTITLE 1,Inspection library
# MAGIC %run ../../_silver_inspect

# COMMAND ----------

# DBTITLE 1,Semantic structures library
# MAGIC %run ../../_semantic_common

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "mastr"
COMPONENT = "silver/energy/_reference/01_mastr_code_list"
RID = run_id()
FINDINGS = "mastr"
TABLE = "mastr_code_list"

# COMMAND ----------

# DBTITLE 1,Read Bronze -- mastr_katalogkategorien
categories_bronze = read_bronze("mastr_katalogkategorien")

# COMMAND ----------

# DBTITLE 1,Read Bronze -- mastr_katalogwerte
values_bronze = read_bronze("mastr_katalogwerte")

# COMMAND ----------

# DBTITLE 1,Read Bronze -- mastr_code_lookup (four small lookups, catalog_kind)
lookup_bronze = read_bronze("mastr_code_lookup")

# COMMAND ----------

# DBTITLE 1,Transform -- one long code list
codes = (
    categories_bronze.select(
        F.lit("katalogkategorien").alias("catalog_kind"),
        strip_float_suffix("Id").alias("code_id"),
        F.lit(None).cast("string").alias("parent_id"),
        F.col("Name").alias("label"),
        F.lit("mastr_katalogkategorien").alias("source_dataset"),
    )
    .unionByName(
        values_bronze.select(
            F.lit("katalogwerte").alias("catalog_kind"),
            strip_float_suffix("Id").alias("code_id"),
            strip_float_suffix("KatalogKategorieId").alias("parent_id"),
            F.col("Wert").alias("label"),
            F.lit("mastr_katalogwerte").alias("source_dataset"),
        )
    )
    .unionByName(
        lookup_bronze.select(
            "catalog_kind",
            strip_float_suffix("Id").alias("code_id"),
            F.lit(None).cast("string").alias("parent_id"),
            F.col("Wert").alias("label"),
            F.lit("mastr_code_lookup").alias("source_dataset"),
        )
    )
    .dropDuplicates()
    .withColumn("_srid", sha_key("catalog_kind", "code_id"))
)
codes = add_semantic_provenance(codes, SOURCE, None, RID, "_srid")

# COMMAND ----------

# DBTITLE 1,Write Silver -- mastr_code_list
write_semantic(
    conform(codes, SEMANTIC_STRUCTURES[TABLE]),
    TABLE,
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Inspect -- mastr_code_list
written = spark.table(semantic_table(TABLE))
findings_blocks = inspect_table(
    written,
    TABLE,
    source=FINDINGS,
    component=COMPONENT,
    rid=RID,
    key_cols=["catalog_kind", "code_id"],
    extra_checks=structure_extra_checks(written),
)

# COMMAND ----------

# DBTITLE 1,Export findings -- mastr_code_list
write_silver_findings(
    FINDINGS, f"{COMPONENT.split('/')[-1]}__{TABLE}", TABLE, findings_blocks
)
