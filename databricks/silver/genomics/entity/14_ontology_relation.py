# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER -- ONTOLOGY RELATION (GENOMICS)
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** November 2026
# MAGIC
# MAGIC **Purpose:** parent-child links of the disease, phenotype and pathway ontologies;
# MAGIC `direction` says whether the source states which end is the parent.

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
COMPONENT = "silver/genomics/entity/14_ontology_relation"
RID = run_id()
FINDINGS = "genomics"
TABLE = "ontology_relation"
RULES = load_genomics_rules()

# COMMAND ----------

# DBTITLE 1,Read Silver -- id crosswalk
xwalk = read_silver("entity_id_crosswalk")
disease_keys = (
    xwalk.filter(
        (F.col("entity_type") == "disease")
        & (F.col("id_type") == "opentargets_disease")
    )
    .select(F.col("id_value").alias("_ot_id"), F.col("entity_key").alias("_ot_key"))
    .distinct()
)

# COMMAND ----------

# DBTITLE 1,Transform -- Open Targets disease parents
terms = (
    read_genomics_bronze("opentargets_disease")
    .withColumn("curie", curie_expr("id"))
    .withColumn("ontology", F.regexp_extract("curie", r"^([A-Za-z]+):", 1))
)
parents = terms.select(
    "id",
    "ontology",
    F.explode(F.from_json("parents", "array<string>")).alias("parent_id"),
)
child_map = disease_keys.select(
    F.col("_ot_id").alias("id"), F.col("_ot_key").alias("child_mapped")
)
parent_map = disease_keys.select(
    F.col("_ot_id").alias("parent_id"), F.col("_ot_key").alias("parent_mapped")
)
opentargets = (
    parents.join(child_map, "id", "left")
    .join(parent_map, "parent_id", "left")
    .select(
        F.nullif("ontology", F.lit("")).alias("ontology"),
        F.coalesce(F.col("parent_mapped"), F.col("parent_id")).alias("parent_key"),
        F.coalesce(F.col("child_mapped"), F.col("id")).alias("child_key"),
        F.lit("parent").alias("relation"),
        F.lit("parent_child").alias("direction"),
        F.lit("opentargets.disease").alias("source_dataset"),
        F.lit("parents").alias("source_field"),
    )
)

# COMMAND ----------

# DBTITLE 1,Transform -- HPO and Reactome
hpo = read_genomics_bronze("hpo_term_relations").select(
    F.lit("HP").alias("ontology"),
    F.when(F.col("pred") == "is_a", curie_expr("obj"))
    .otherwise(curie_expr("sub"))
    .alias("parent_key"),
    F.when(F.col("pred") == "is_a", curie_expr("sub"))
    .otherwise(curie_expr("obj"))
    .alias("child_key"),
    F.col("pred").alias("relation"),
    F.when(F.col("pred") == "is_a", "parent_child")
    .otherwise("source_order")
    .alias("direction"),
    F.lit("hpo.term_relations").alias("source_dataset"),
    F.lit("sub,pred,obj").alias("source_field"),
)
reactome = read_genomics_bronze("reactome_pathway_relations").select(
    F.lit("reactome").alias("ontology"),
    F.col("parent_pathway_id").alias("parent_key"),
    F.col("child_pathway_id").alias("child_key"),
    F.lit("parent").alias("relation"),
    F.lit("parent_child").alias("direction"),
    F.lit("reactome.pathway_relations").alias("source_dataset"),
    F.lit("parent_pathway_id,child_pathway_id").alias("source_field"),
)

# COMMAND ----------

# DBTITLE 1,Transform -- MedGen relations (direction not stated)
medgen = (
    read_genomics_bronze("medgen_relations")
    .filter(F.col("SUPPRESS") == "N")
    .select(
        F.col("SAB").alias("ontology"),
        F.col("CUI1").alias("parent_key"),
        F.col("CUI2").alias("child_key"),
        F.coalesce(F.nullif(F.col("RELA"), F.lit("")), F.col("REL")).alias("relation"),
        F.lit("source_order").alias("direction"),
        F.lit("medgen.relations").alias("source_dataset"),
        F.lit("CUI1,CUI2,REL,RELA").alias("source_field"),
    )
)

# COMMAND ----------

# DBTITLE 1,Transform -- union and provenance
relations = (
    opentargets.unionByName(hpo)
    .unionByName(reactome)
    .unionByName(medgen)
    .filter(F.col("parent_key").isNotNull() & F.col("child_key").isNotNull())
    .distinct()
    .withColumn(
        "_srid",
        sha_key(
            "ontology",
            "parent_key",
            "child_key",
            "relation",
            "direction",
            "source_dataset",
            "source_field",
        ),
    )
)
relations = conform_genomics(add_genomics_provenance(relations, "_srid", RID), TABLE)

# COMMAND ----------

# DBTITLE 1,Write Silver -- ontology_relation
write_silver(relations, TABLE, source=SOURCE, component=COMPONENT, rid=RID)

# COMMAND ----------

# DBTITLE 1,Inspect -- ontology_relation
written = spark.table(genomics_table(TABLE))
blocks = inspect_table(
    written,
    TABLE,
    source=FINDINGS,
    component=COMPONENT,
    rid=RID,
    key_cols=["source_record_id"],
    extra_checks={
        "source_order_relations": written.filter(
            F.col("direction") == "source_order"
        ).count(),
        "parent_child_relations": written.filter(
            F.col("direction") == "parent_child"
        ).count(),
        "self_relations": written.filter(
            F.col("parent_key") == F.col("child_key")
        ).count(),
        "ontologies": written.select("ontology").distinct().count(),
    },
)

# COMMAND ----------

# DBTITLE 1,Export findings -- ontology_relation
write_silver_findings(FINDINGS, f"{COMPONENT.split('/')[-1]}__{TABLE}", TABLE, blocks)
