# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER -- DISEASE (GENOMICS)
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** November 2026
# MAGIC
# MAGIC **Purpose:** one record per disease concept (MedGen CUI, else the own ontology id);
# MAGIC each field is the best claim, with its source and method.

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
COMPONENT = "silver/genomics/disease/10_disease"
RID = run_id()
FINDINGS = "genomics"
TABLE = "disease"
RULES = load_genomics_rules()

# COMMAND ----------

# DBTITLE 1,Read Silver -- claims and crosswalk
claims = read_silver("entity_claim")
xwalk = read_silver("entity_id_crosswalk").filter(F.col("entity_type") == "disease")

# COMMAND ----------

# DBTITLE 1,Transform -- resolve the fields
masters, conflicts = resolve_master(
    claims, entity_type="disease", fields=DISEASE_FIELDS
)
keys = xwalk.groupBy("entity_key").agg(
    F.max(F.when(F.col("id_type") == "medgen", F.lit(True))).alias("has_medgen")
)

# COMMAND ----------

# DBTITLE 1,Transform -- master and provenance
disease = (
    keys.join(masters, "entity_key", "left")
    .withColumn("disease_key", F.col("entity_key"))
    .withColumn(
        "key_source",
        F.when(F.col("has_medgen"), "medgen_cui").otherwise("own_opentargets_id"),
    )
    .withColumn("_srid", F.col("disease_key"))
)
disease = conform_genomics(add_genomics_provenance(disease, "_srid", RID), TABLE)

# COMMAND ----------

# DBTITLE 1,Write Silver -- disease
write_silver(disease, TABLE, source=SOURCE, component=COMPONENT, rid=RID)

# COMMAND ----------

# DBTITLE 1,Inspect -- disease
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
    key_cols=["disease_key"],
    df_before=keys,
    extra_checks={
        "diseases_with_an_own_key": written.filter(
            F.col("key_source") == "own_opentargets_id"
        ).count(),
        "diseases_without_a_name": written.filter(F.col("name").isNull()).count(),
        **conflict_counts,
    },
)

# COMMAND ----------

# DBTITLE 1,Export findings -- disease
write_silver_findings(FINDINGS, f"{COMPONENT.split('/')[-1]}__{TABLE}", TABLE, blocks)
