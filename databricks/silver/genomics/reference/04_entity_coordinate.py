# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER -- ENTITY COORDINATE (GENOMICS)
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** November 2026
# MAGIC
# MAGIC **Purpose:** every genomic interval of every genomics entity, 1-based and closed,
# MAGIC one row per source and assembly.

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
from pyspark.sql import Window
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "genomics"
COMPONENT = "silver/genomics/reference/04_entity_coordinate"
RID = run_id()
FINDINGS = "genomics"
TABLE = "entity_coordinate"
RULES = load_genomics_rules()

# COMMAND ----------

# DBTITLE 1,Read Silver -- id crosswalk
xwalk = read_silver("entity_id_crosswalk")

# COMMAND ----------

# DBTITLE 1,Transform -- intervals, length, primary interval and provenance
# one primary interval per entity and assembly, by the coordinate source order
primary = Window.partitionBy("entity_type", "entity_key", "assembly").orderBy(
    dataset_rank_expr(RULES).asc_nulls_last(),
    F.col("source_dataset"),
    F.col("contig"),
    F.col("start_position"),
)
coordinates = (
    coordinate_all(RULES, xwalk)
    .distinct()
    .withColumn("interval_length", F.col("end_position") - F.col("start_position") + 1)
    .withColumn("is_primary", F.row_number().over(primary) == 1)
    .withColumn(
        "_srid",
        sha_key(
            "entity_type",
            "entity_key",
            "feature_level",
            "source_dataset",
            "assembly",
            "contig",
            "start_position",
            "end_position",
            "strand",
        ),
    )
)
coordinates = conform_genomics(
    add_genomics_provenance(coordinates, "_srid", RID), TABLE
)

# COMMAND ----------

# DBTITLE 1,Write Silver -- entity_coordinate
write_silver(coordinates, TABLE, source=SOURCE, component=COMPONENT, rid=RID)

# COMMAND ----------

# DBTITLE 1,Inspect -- entity_coordinate
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
        "intervals_with_start_after_end": written.filter(
            F.col("start_position") > F.col("end_position")
        ).count(),
        "genes_without_a_grch38_interval": xwalk.filter(F.col("entity_type") == "gene")
        .select("entity_key")
        .distinct()
        .join(
            written.filter(
                (F.col("entity_type") == "gene") & (F.col("assembly") == "GRCh38")
            )
            .select("entity_key")
            .distinct(),
            "entity_key",
            "left_anti",
        )
        .count(),
    },
)

# COMMAND ----------

# DBTITLE 1,Export findings -- entity_coordinate
write_silver_findings(FINDINGS, f"{COMPONENT.split('/')[-1]}__{TABLE}", TABLE, blocks)
