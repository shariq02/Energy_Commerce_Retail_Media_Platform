# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER -- ENTITY ID CROSSWALK (GENOMICS)
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** November 2026
# MAGIC
# MAGIC **Purpose:** every id of every genomics entity, one row per entity, id type,
# MAGIC id value and source column.

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
COMPONENT = "silver/genomics/reference/01_entity_id_crosswalk"
RID = run_id()
FINDINGS = "genomics"
TABLE = "entity_id_crosswalk"
RULES = load_genomics_rules()

# COMMAND ----------

# DBTITLE 1,Transform -- ids of every entity type
crosswalk = finish_crosswalk(crosswalk_all(RULES))

# COMMAND ----------

# DBTITLE 1,Transform -- keys and provenance
crosswalk = crosswalk.withColumn(
    "_srid",
    sha_key(
        "entity_type",
        "entity_key",
        "id_type",
        "id_value",
        "source_dataset",
        "source_field",
    ),
)
crosswalk = conform_genomics(add_genomics_provenance(crosswalk, "_srid", RID), TABLE)

# COMMAND ----------

# DBTITLE 1,Write Silver -- entity_id_crosswalk
write_silver(crosswalk, TABLE, source=SOURCE, component=COMPONENT, rid=RID)

# COMMAND ----------

# DBTITLE 1,Inspect -- entity_id_crosswalk
written = spark.table(genomics_table(TABLE))
blocks = inspect_table(
    written,
    TABLE,
    source=FINDINGS,
    component=COMPONENT,
    rid=RID,
    key_cols=[
        "entity_type",
        "entity_key",
        "id_type",
        "id_value",
        "source_dataset",
        "source_field",
    ],
    extra_checks={
        "entity_types": written.select("entity_type").distinct().count(),
        "entities_with_an_own_key": written.filter(
            F.col("match_method").startswith("own_")
        )
        .select("entity_type", "entity_key")
        .distinct()
        .count(),
        "ids_held_by_several_entities": written.filter(F.col("entities_sharing_id") > 1)
        .select("entity_type", "id_type", "id_value")
        .distinct()
        .count(),
    },
)

# COMMAND ----------

# DBTITLE 1,Export findings -- entity_id_crosswalk
write_silver_findings(FINDINGS, f"{COMPONENT.split('/')[-1]}__{TABLE}", TABLE, blocks)
