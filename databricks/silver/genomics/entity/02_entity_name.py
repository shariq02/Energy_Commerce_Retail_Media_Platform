# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER -- ENTITY NAME (GENOMICS)
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** November 2026
# MAGIC
# MAGIC **Purpose:** every name, synonym and designation of every genomics entity,
# MAGIC one row per entity, term, term type and source column.

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
COMPONENT = "silver/genomics/entity/02_entity_name"
RID = run_id()
FINDINGS = "genomics"
TABLE = "entity_name"
RULES = load_genomics_rules()

# COMMAND ----------

# DBTITLE 1,Read Silver -- id crosswalk
xwalk = read_silver("entity_id_crosswalk")

# COMMAND ----------

# DBTITLE 1,Transform -- names, sharing count and provenance
names = finish_names(names_all(RULES, xwalk)).withColumn(
    "_srid",
    sha_key(
        "entity_type",
        "entity_key",
        "term",
        "term_type",
        "source_dataset",
        "source_field",
    ),
)
names = conform_genomics(add_genomics_provenance(names, "_srid", RID), TABLE)

# COMMAND ----------

# DBTITLE 1,Write Silver -- entity_name
write_silver(names, TABLE, source=SOURCE, component=COMPONENT, rid=RID)

# COMMAND ----------

# DBTITLE 1,Inspect -- entity_name
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
        "terms_held_by_several_entities": written.filter(
            F.col("entities_sharing_term") > 1
        )
        .select("entity_type", F.upper("term"))
        .distinct()
        .count(),
    },
)

# COMMAND ----------

# DBTITLE 1,Export findings -- entity_name
write_silver_findings(FINDINGS, f"{COMPONENT.split('/')[-1]}__{TABLE}", TABLE, blocks)
