# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER -- ENTITY DERIVED ATTRIBUTE (GENOMICS)
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** November 2026
# MAGIC
# MAGIC **Purpose:** every computed attribute (flags, labels, scores) with its rule, matched
# MAGIC text and as-of date; the rules are in the versioned rules file.

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
COMPONENT = "silver/genomics/entity/15_entity_derived_attribute"
RID = run_id()
FINDINGS = "genomics"
TABLE = "entity_derived_attribute"
RULES = load_genomics_rules()

# COMMAND ----------

# DBTITLE 1,Read Silver -- masters and claims
gene = read_silver("gene")
protein = read_silver("protein")
disease = read_silver("disease")
claims = read_silver("entity_claim")
VERSION = genomics_rule_version(RULES)
AS_OF = F.to_date(F.lit(RID.split("-")[1][:8]), "yyyyMMdd")
COMMON = {"rules": RULES, "version": VERSION, "as_of": AS_OF}

# COMMAND ----------


# DBTITLE 1,Transform -- gene texts
def claim_text(field):
    return (
        claims.filter((F.col("entity_type") == "gene") & (F.col("field") == field))
        .groupBy(F.col("entity_key").alias("gene_key"))
        .agg(F.concat_ws(" ", F.sort_array(F.collect_set("value_text"))).alias(field))
    )


texts = (
    gene.select(
        "gene_key",
        "full_name",
        "description",
        "map_location",
        "cytoband_arm",
        "cytoband_band",
    )
    .join(claim_text("function_descriptions"), "gene_key", "left")
    .join(claim_text("subcellular_locations"), "gene_key", "left")
    .withColumn(
        "function_text",
        F.concat_ws(" ", "full_name", "description", "function_descriptions"),
    )
    .withColumn(
        "location_text", F.concat_ws(" ", "subcellular_locations", "description")
    )
    .withColumn(
        "process_text", F.concat_ws(" ", "description", "function_descriptions")
    )
    .withColumn("description_text", F.coalesce(F.col("description"), F.lit("")))
)

# COMMAND ----------

# DBTITLE 1,Transform -- keyword flags of genes
function_flags = keyword_rows(
    texts,
    entity_type="gene",
    key="gene_key",
    text="function_text",
    group="gene_function",
    text_source="gene.full_name+description+function_descriptions",
    **COMMON,
)
location_flags = keyword_rows(
    texts,
    entity_type="gene",
    key="gene_key",
    text="location_text",
    group="gene_location",
    text_source="subcellular_locations+gene.description",
    **COMMON,
)
process_flags = keyword_rows(
    texts,
    entity_type="gene",
    key="gene_key",
    text="process_text",
    group="gene_process",
    text_source="gene.description+function_descriptions",
    **COMMON,
)
description_flags = keyword_rows(
    texts,
    entity_type="gene",
    key="gene_key",
    text="description_text",
    group="gene_description",
    text_source="gene.description",
    **COMMON,
)
description_values = captured_rows(
    texts,
    entity_type="gene",
    key="gene_key",
    text="description_text",
    group="gene_description_value",
    text_source="gene.description",
    numbers=("member_number", "type_number"),
    **COMMON,
)

# COMMAND ----------

# DBTITLE 1,Transform -- cytoband flags, primary function and druggability
limits = RULES["cytoband_limits"]
band = F.col("cytoband_band").cast("int")
cytoband = texts.filter(
    F.col("cytoband_arm").isNotNull() & F.col("cytoband_band").isNotNull()
)
cytoband_rows = _union(
    [
        derived_rows(
            cytoband.filter(condition),
            entity_type="gene",
            key="gene_key",
            attribute=name,
            rule_name="cytoband_limits",
            method="rule",
            value_boolean=F.lit(True),
            matched=F.col("map_location"),
            text_source="gene.map_location",
            **{k: v for k, v in COMMON.items() if k != "rules"},
        )
        for name, condition in (
            ("is_telomeric", band >= limits["telomeric_from"]),
            ("is_centromeric", band <= limits["centromeric_to"]),
            (
                "is_pericentromeric",
                (band >= limits["pericentromeric_from"])
                & (band <= limits["pericentromeric_to"]),
            ),
        )
    ]
)
flags = (
    function_flags.unionByName(location_flags)
    .groupBy(F.col("entity_key").alias("gene_key"))
    .agg(F.collect_set("attribute").alias("flags"))
)
scored = (
    gene.select("gene_key")
    .join(flags, "gene_key", "left")
    .withColumn("flags", F.coalesce(F.col("flags"), F.array().cast("array<string>")))
    .withColumn(
        "primary_function",
        first_match_expr(
            RULES["primary_function"],
            F.col("flags"),
            "label",
            RULES["primary_function_default"],
        ),
    )
    .withColumn(
        "druggability_score",
        first_match_expr(
            RULES["druggability"],
            F.col("flags"),
            "score",
            float(RULES["druggability_default"]),
        ),
    )
)
rule_fields = {k: v for k, v in COMMON.items() if k != "rules"}
primary_rows = derived_rows(
    scored,
    entity_type="gene",
    key="gene_key",
    attribute="primary_function",
    value_text=F.col("primary_function"),
    rule_name="primary_function",
    method="rule",
    text_source="function flags",
    **rule_fields,
)
druggability_rows = derived_rows(
    scored,
    entity_type="gene",
    key="gene_key",
    attribute="druggability_score",
    value_number=F.col("druggability_score"),
    rule_name="druggability",
    method="rule",
    text_source="function and location flags",
    **rule_fields,
)

# COMMAND ----------

# DBTITLE 1,Transform -- protein name flags and disease categories
protein_flags = keyword_rows(
    protein.filter(F.col("name").isNotNull()),
    entity_type="protein",
    key="protein_key",
    text="name",
    group="protein_name",
    text_source="protein.name",
    **COMMON,
)
disease_flags = keyword_rows(
    disease.filter(F.col("name").isNotNull()),
    entity_type="disease",
    key="disease_key",
    text="name",
    group="disease_category",
    text_source="disease.name",
    **COMMON,
)

# COMMAND ----------

# DBTITLE 1,Transform -- union and provenance
derived = (
    function_flags.unionByName(location_flags)
    .unionByName(process_flags)
    .unionByName(description_flags)
    .unionByName(description_values)
    .unionByName(cytoband_rows)
    .unionByName(primary_rows)
    .unionByName(druggability_rows)
    .unionByName(protein_flags)
    .unionByName(disease_flags)
    .distinct()
    .withColumn(
        "_srid",
        sha_key(
            "entity_type",
            "entity_key",
            "attribute",
            "rule_name",
            "value_text",
            "value_number",
        ),
    )
)
derived = conform_genomics(add_genomics_provenance(derived, "_srid", RID), TABLE)

# COMMAND ----------

# DBTITLE 1,Write Silver -- entity_derived_attribute
write_silver(derived, TABLE, source=SOURCE, component=COMPONENT, rid=RID)

# COMMAND ----------

# DBTITLE 1,Inspect -- entity_derived_attribute
written = spark.table(genomics_table(TABLE))
blocks = inspect_table(
    written,
    TABLE,
    source=FINDINGS,
    component=COMPONENT,
    rid=RID,
    key_cols=["source_record_id"],
    extra_checks={
        "genes_without_a_function_flag": written.filter(
            F.col("rule_name") == "primary_function"
        )
        .filter(F.col("value_text") == RULES["primary_function_default"])
        .count(),
        "attributes": written.select("rule_name", "attribute").distinct().count(),
    },
)

# COMMAND ----------

# DBTITLE 1,Export findings -- entity_derived_attribute
write_silver_findings(FINDINGS, f"{COMPONENT.split('/')[-1]}__{TABLE}", TABLE, blocks)
