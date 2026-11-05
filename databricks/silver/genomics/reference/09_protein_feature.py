# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER -- PROTEIN FEATURE (GENOMICS)
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** November 2026
# MAGIC
# MAGIC **Purpose:** one row per UniProt feature record: type, positions, note, evidence
# MAGIC codes, PubMed ids and dbSNP ids; the raw qualifiers stay.

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
COMPONENT = "silver/genomics/reference/09_protein_feature"
RID = run_id()
FINDINGS = "genomics"
TABLE = "protein_feature"
RULES = load_genomics_rules()

# COMMAND ----------

# DBTITLE 1,Read Silver -- id crosswalk
xwalk = read_silver("entity_id_crosswalk").filter(F.col("entity_type") == "protein")

# COMMAND ----------

# DBTITLE 1,Transform -- parse the feature records
features = with_entity_key(
    read_genomics_bronze("uniprot_features"),
    "accession",
    xwalk,
    "uniprot.entries",
    "uniprot",
)
location = F.trim(F.col("location"))
LOCATION = r"^(\d+)(?:\.\.(\d+))?$"
start = F.nullif(F.regexp_extract(location, LOCATION, 1), F.lit(""))
end = F.nullif(F.regexp_extract(location, LOCATION, 2), F.lit(""))
qualifiers = F.col("qualifiers")
feature = (
    features.select(
        F.col("entity_key").alias("protein_key"),
        F.col("feature_type"),
        location.alias("location_raw"),
        start.cast("int").alias("start_position"),
        F.coalesce(end, start).cast("int").alias("end_position"),
        F.nullif(F.regexp_extract(qualifiers, r'/note="([^"]*)"', 1), F.lit("")).alias(
            "note"
        ),
        F.array_distinct(
            F.expr("regexp_extract_all(qualifiers, '(ECO:[0-9]+)', 1)")
        ).alias("evidence_codes"),
        F.array_distinct(
            F.expr("regexp_extract_all(qualifiers, 'PubMed:([0-9]+)', 1)")
        ).alias("pubmed_ids"),
        F.array_distinct(
            F.expr("regexp_extract_all(qualifiers, 'dbSNP:(rs[0-9]+)', 1)")
        ).alias("dbsnp_ids"),
        qualifiers.alias("qualifiers"),
    )
    .distinct()
    .withColumn(
        "_srid",
        sha_key("protein_key", "feature_type", "location_raw", "qualifiers"),
    )
)
feature = conform_genomics(add_genomics_provenance(feature, "_srid", RID), TABLE)

# COMMAND ----------

# DBTITLE 1,Write Silver -- protein_feature
write_silver(feature, TABLE, source=SOURCE, component=COMPONENT, rid=RID)

# COMMAND ----------

# DBTITLE 1,Inspect -- protein_feature
written = spark.table(genomics_table(TABLE))
blocks = inspect_table(
    written,
    TABLE,
    source=FINDINGS,
    component=COMPONENT,
    rid=RID,
    key_cols=["source_record_id"],
    extra_checks={
        "feature_records_in_bronze": read_genomics_bronze("uniprot_features").count(),
        "features_without_positions": written.filter(
            F.col("start_position").isNull()
        ).count(),
        "feature_types": written.select("feature_type").distinct().count(),
    },
)

# COMMAND ----------

# DBTITLE 1,Export findings -- protein_feature
write_silver_findings(FINDINGS, f"{COMPONENT.split('/')[-1]}__{TABLE}", TABLE, blocks)
