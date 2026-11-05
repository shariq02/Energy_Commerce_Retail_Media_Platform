# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER -- DRUG (GENOMICS)
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** November 2026
# MAGIC
# MAGIC **Purpose:** one record per drug (ChEMBL id, else PharmGKB accession, else the
# MAGIC Tahoe name); each field is the best claim, with source and method.

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
COMPONENT = "silver/genomics/reference/11_drug"
RID = run_id()
FINDINGS = "genomics"
TABLE = "drug"
RULES = load_genomics_rules()

# COMMAND ----------

# DBTITLE 1,Read Silver -- claims and crosswalk
claims = read_silver("entity_claim")
xwalk = read_silver("entity_id_crosswalk").filter(F.col("entity_type") == "drug")

# COMMAND ----------

# DBTITLE 1,Transform -- resolve the fields
masters, conflicts = resolve_master(claims, entity_type="drug", fields=DRUG_FIELDS)
keys = xwalk.groupBy("entity_key").agg(
    F.max(F.when(F.col("id_type") == "chembl", F.lit(True))).alias("has_chembl"),
    F.max(F.when(F.col("id_type") == "pharmgkb", F.lit(True))).alias("has_pharmgkb"),
)

# COMMAND ----------

# DBTITLE 1,Transform -- master and provenance
drug = (
    keys.join(masters, "entity_key", "left")
    .withColumn("drug_key", F.col("entity_key"))
    .withColumn(
        "key_source",
        F.when(F.col("has_chembl"), "chembl_id")
        .when(F.col("has_pharmgkb"), "pharmgkb_accession")
        .otherwise("own_tahoe_name"),
    )
    .withColumn("_srid", F.col("drug_key"))
)
drug = conform_genomics(add_genomics_provenance(drug, "_srid", RID), TABLE)

# COMMAND ----------

# DBTITLE 1,Write Silver -- drug
write_silver(drug, TABLE, source=SOURCE, component=COMPONENT, rid=RID)

# COMMAND ----------

# DBTITLE 1,Inspect -- drug
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
    key_cols=["drug_key"],
    df_before=keys,
    extra_checks={
        "drugs_with_an_own_key": written.filter(
            F.col("key_source") != "chembl_id"
        ).count(),
        "drugs_without_a_name": written.filter(F.col("name").isNull()).count(),
        **conflict_counts,
    },
)

# COMMAND ----------

# DBTITLE 1,Export findings -- drug
write_silver_findings(FINDINGS, f"{COMPONENT.split('/')[-1]}__{TABLE}", TABLE, blocks)
