# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER -- ENTITY CLAIM (GENOMICS)
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** November 2026
# MAGIC
# MAGIC **Purpose:** every source value of every genomics entity, one row per entity,
# MAGIC field and source column; the masters pick from these.

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
COMPONENT = "silver/genomics/reference/03_entity_claim"
RID = run_id()
FINDINGS = "genomics"
TABLE = "entity_claim"
RULES = load_genomics_rules()

# COMMAND ----------

# DBTITLE 1,Read Silver -- crosswalk and names
xwalk = read_silver("entity_id_crosswalk")
names = read_silver("entity_name")

# COMMAND ----------

# DBTITLE 1,Transform -- claims of every entity type
claims = _union(
    [
        gene_claims(RULES, xwalk),
        transcript_claims(RULES, xwalk, names),
        protein_claims(RULES, xwalk),
        disease_claims(RULES, xwalk),
        drug_claims(RULES, xwalk),
        small_claims(RULES),
    ]
)

# COMMAND ----------

# DBTITLE 1,Transform -- keys and provenance
claims = claims.distinct().withColumn(
    "_srid",
    sha_key(
        "entity_type",
        "entity_key",
        "field",
        "source_dataset",
        "source_field",
        "value_text",
    ),
)
claims = conform_genomics(add_genomics_provenance(claims, "_srid", RID), TABLE)

# COMMAND ----------

# DBTITLE 1,Write Silver -- entity_claim
write_silver(claims, TABLE, source=SOURCE, component=COMPONENT, rid=RID)

# COMMAND ----------

# DBTITLE 1,Inspect -- entity_claim
written = spark.table(genomics_table(TABLE))
blocks = inspect_table(
    written,
    TABLE,
    source=FINDINGS,
    component=COMPONENT,
    rid=RID,
    key_cols=["source_record_id"],
    extra_checks={
        "entity_types": written.select("entity_type").distinct().count(),
        "claims_without_rank": written.filter(F.col("source_rank").isNull()).count(),
        "llm_generated_claims": written.filter(
            F.col("source_type") == "llm_generated"
        ).count(),
    },
)

# COMMAND ----------

# DBTITLE 1,Export findings -- entity_claim
write_silver_findings(FINDINGS, f"{COMPONENT.split('/')[-1]}__{TABLE}", TABLE, blocks)
