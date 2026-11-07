# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # SILVER -- VARIANTS (GENOMICS)
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** November 2026
# MAGIC
# MAGIC **Purpose:** variants with their notation, conditions and classifications.

# COMMAND ----------

# DBTITLE 1,Shared library
# MAGIC %run ../_silver_common

# COMMAND ----------

# DBTITLE 1,Inspection library
# MAGIC %run ../_silver_inspect

# COMMAND ----------

# DBTITLE 1,Genomics library
# MAGIC %run ../_genomics_common

# COMMAND ----------

# DBTITLE 1,Entity helpers
# MAGIC %run ../_genomics_entities

# COMMAND ----------

# DBTITLE 1,Configuration
from pyspark.sql import functions as F

SOURCE = "genomics"
COMPONENT = "silver/genomics/04_variants"
RID = run_id()
FINDINGS = "genomics"
RULES = load_genomics_rules()

# COMMAND ----------

# DBTITLE 1,Build -- variant
# -- Read Silver -- claims and crosswalk
claims = read_silver("entity_claim")
xwalk = read_silver("entity_id_crosswalk")
id_map = variant_id_map(xwalk)

# -- Transform -- coordinate rows of every source
CORE = [
    "variant_key",
    "key_method",
    "v_assembly",
    "v_contig",
    "v_position",
    "v_reference",
    "v_alternate",
    "v_end",
    "v_source",
]
none = F.lit(None).cast("string")
pharmgkb = pharmgkb_variant_frame(RULES, id_map).select(
    "variant_key",
    "key_method",
    F.lit("unspecified").alias("v_assembly"),
    none.alias("v_contig"),
    none.alias("v_position"),
    none.alias("v_reference"),
    none.alias("v_alternate"),
    none.alias("v_end"),
    "v_source",
)
core = _union([frame.select(*CORE) for _, frame in variant_frames(RULES)] + [pharmgkb])

# -- Transform -- one record per variant


def number(column):
    return F.when(F.col(column).rlike(r"^\d+$"), F.col(column).cast("bigint"))


records = (
    core.groupBy("variant_key")
    .agg(
        F.max(F.when(F.col("key_method") == "vcf_alleles", F.lit(1))).alias(
            "has_exact"
        ),
        F.max(F.when(F.col("v_assembly") != "unspecified", F.col("v_assembly"))).alias(
            "known_assembly"
        ),
        F.max("v_contig").alias("contig"),
        F.max(number("v_position")).alias("position"),
        F.max("v_reference").alias("reference_allele"),
        F.max("v_alternate").alias("alternate_allele"),
        F.max(number("v_end")).alias("end_position"),
        F.sort_array(F.collect_set("v_source")).alias("source_datasets"),
    )
    .withColumn("assembly", F.coalesce(F.col("known_assembly"), F.lit("unspecified")))
)

# -- Transform -- resolve the fields, master and provenance
masters, conflicts = resolve_master(
    claims, entity_type="variant", fields=VARIANT_FIELDS
)
masters = masters.withColumnRenamed("entity_key", "variant_key")
variant = (
    records.join(masters, "variant_key", "left")
    .withColumn(
        "key_source",
        F.when(
            F.col("variant_key").rlike(r"^(clinvar_allele|dbvar|pharmgkb_variant):"),
            "own_source_id",
        )
        .when(F.col("has_exact").isNotNull(), "vcf_alleles")
        .otherwise("vcf_alleles_assumed_grch38"),
    )
    .withColumn("_srid", F.col("variant_key"))
)
variant = conform_genomics(add_genomics_provenance(variant, "_srid", RID), "variant")

# COMMAND ----------

# DBTITLE 1,Write, inspect and export -- variant
write_silver(variant, "variant", source=SOURCE, component=COMPONENT, rid=RID)

written = spark.table(genomics_table("variant"))
conflict_counts = {
    f"{row['field']}_conflicts": row["count"]
    for row in conflicts.groupBy("field").count().collect()
}
blocks = inspect_table(
    written,
    "variant",
    source=FINDINGS,
    component=COMPONENT,
    rid=RID,
    key_cols=["variant_key"],
    df_before=records,
    extra_checks={
        "variants_with_an_own_key": written.filter(
            F.col("key_source") == "own_source_id"
        ).count(),
        "variants_without_a_position": written.filter(
            F.col("position").isNull()
        ).count(),
        "variants_without_a_gene": written.filter(F.col("gene_key").isNull()).count(),
        "variants_by_assembly": str(
            {
                r["assembly"]: r["count"]
                for r in written.groupBy("assembly").count().collect()
            }
        ),
        **conflict_counts,
        **{
            f"{column}_counts": str(
                {
                    r[column]: r["count"]
                    for r in written.groupBy(column).count().collect()
                }
            )
            for column in written.columns
            if column.endswith("_link_method")
        },
    },
)

write_silver_findings(
    FINDINGS, f"{COMPONENT.split('/')[-1]}__variant", "variant", blocks
)

# COMMAND ----------

# DBTITLE 1,Build -- variant_hgvs
# -- Read Silver -- transcript ids
xwalk = read_silver("entity_id_crosswalk")
transcripts = entity_id_map(xwalk, "transcript")

# -- Transform -- ClinVar names
frames = dict(variant_frames(RULES))
name_column = clean_placeholders("Name", RULES, "clinvar.variant_summary.Name")
parsed = (
    frames[CLINVAR]
    .select("variant_key", name_column.alias("name"), "snapshot")
    .filter(F.col("name").isNotNull())
    .withColumn("parsed", hgvs_name_udf()(F.col("name")))
)
clinvar = parsed.select(
    "variant_key",
    *[F.col(f"parsed.{n}").alias(n) for n, _ in HGVS_NAME_FIELDS],
    F.lit(CLINVAR).alias("source_dataset"),
    F.lit("Name").alias("source_field"),
    F.col("snapshot").alias("source_snapshot"),
)

# -- Transform -- AlphaMissense protein changes
alpha_columns = ["variant_key", "transcript_id", "protein_variant"]
alpha = frames[ALPHAMISSENSE].select(*alpha_columns)
change = F.regexp_extract("protein_variant", r"^([A-Z*])(\d+)([A-Z*])$", 0)
alphamissense = alpha.select(
    "variant_key",
    ensembl_unversioned_expr(F.col("transcript_id")).alias("transcript_accession"),
    F.nullif(F.regexp_extract("transcript_id", r"\.(\d+)$", 1), F.lit(""))
    .cast("int")
    .alias("transcript_version"),
    F.lit(None).cast("string").alias("gene_symbol"),
    *[F.lit(None).cast(kind).alias(n) for n, kind in HGVS_NAME_FIELDS[3:13]],
    F.col("protein_variant").alias("protein_notation"),
    F.regexp_extract("protein_variant", r"^([A-Z*])(\d+)([A-Z*])$", 1).alias(
        "protein_reference_amino_acid"
    ),
    F.nullif(
        F.regexp_extract("protein_variant", r"^([A-Z*])(\d+)([A-Z*])$", 2),
        F.lit(""),
    )
    .cast("int")
    .alias("protein_position"),
    F.lit(None).cast("string").alias("protein_end_reference_amino_acid"),
    F.lit(None).cast("int").alias("protein_end_position"),
    F.when(change != "", F.lit("missense")).alias("protein_edit_type"),
    F.regexp_extract("protein_variant", r"^([A-Z*])(\d+)([A-Z*])$", 3).alias(
        "protein_alternate_amino_acid"
    ),
    F.lit(None).cast("string").alias("protein_inserted_amino_acids"),
    F.when(change != "", F.lit("ok"))
    .otherwise(F.lit("unparsed"))
    .alias("parse_status"),
    F.lit(ALPHAMISSENSE).alias("source_dataset"),
    F.lit("protein_variant").alias("source_field"),
    F.lit(None).cast("string").alias("source_snapshot"),
)

# -- Transform -- transcript links and provenance
hgvs = clinvar.unionByName(alphamissense)
hgvs = link_entities(
    hgvs,
    [
        (F.col("transcript_accession"), "refseq_rna"),
        (F.col("transcript_accession"), "ensembl_transcript"),
    ],
    transcripts,
    "transcript_key",
    "transcript_link_method",
).drop("transcript_link_method")
hgvs = hgvs.distinct().withColumn(
    "_srid",
    sha_key(
        "variant_key",
        "transcript_accession",
        "transcript_version",
        "coding_notation",
        "protein_notation",
        "source_dataset",
        "source_snapshot",
    ),
)
hgvs = conform_genomics(add_genomics_provenance(hgvs, "_srid", RID), "variant_hgvs")

# COMMAND ----------

# DBTITLE 1,Write, inspect and export -- variant_hgvs
write_silver(hgvs, "variant_hgvs", source=SOURCE, component=COMPONENT, rid=RID)

written = spark.table(genomics_table("variant_hgvs"))
blocks = inspect_table(
    written,
    "variant_hgvs",
    source=FINDINGS,
    component=COMPONENT,
    rid=RID,
    key_cols=["source_record_id"],
    extra_checks={
        "clinvar_unparsed": written.filter(
            (F.col("source_dataset") == CLINVAR) & (F.col("parse_status") == "unparsed")
        ).count(),
        "clinvar_partial": written.filter(
            (F.col("source_dataset") == CLINVAR) & (F.col("parse_status") == "partial")
        ).count(),
        "rows_without_a_transcript": written.filter(
            F.col("transcript_key").isNull()
        ).count(),
        "edit_types": str(
            {
                r["protein_edit_type"]: r["count"]
                for r in written.groupBy("protein_edit_type").count().collect()
            }
        ),
        **{
            f"{column}_counts": str(
                {
                    r[column]: r["count"]
                    for r in written.groupBy(column).count().collect()
                }
            )
            for column in written.columns
            if column.endswith("_link_method")
        },
    },
)

write_silver_findings(
    FINDINGS, f"{COMPONENT.split('/')[-1]}__variant_hgvs", "variant_hgvs", blocks
)

# COMMAND ----------

# DBTITLE 1,Build -- variant_condition
# -- Read Silver -- disease ids
xwalk = read_silver("entity_id_crosswalk")
disease_map = entity_id_map(xwalk, "disease")

# -- Transform -- pair ids, names and accessions
DATASET = "clinvar.variant_summary"
frames = dict(variant_frames(RULES))

text = text_for(DATASET)

parsed = frames[CLINVAR].select(
    "variant_key",
    F.col("snapshot").alias("source_snapshot"),
    F.explode(
        clinvar_conditions_udf()(
            text("PhenotypeIDS"), text("PhenotypeList"), text("RCVaccession")
        )
    ).alias("c"),
)
conditions = parsed.select(
    "variant_key",
    "source_snapshot",
    F.col("c.position").alias("condition_position"),
    F.col("c.name").alias("condition_name"),
    F.col("c.accession").alias("clinvar_condition_accession"),
    F.col("c.ids").alias("condition_ids"),
    F.col("c.pairing_status").alias("pairing_status"),
).distinct()

# -- Transform -- disease links through exact ids
ids = conditions.select(
    "variant_key",
    "source_snapshot",
    "condition_position",
    F.explode("condition_ids").alias("raw_id"),
)
id_type, id_value = split_curie("raw_id", RULES)
linked = (
    ids.withColumn("id_type", id_type)
    .withColumn("id_value", id_value)
    .join(disease_map, ["id_type", "id_value"], "inner")
    .groupBy("variant_key", "source_snapshot", "condition_position")
    .agg(
        F.countDistinct("entity_key").alias("candidates"),
        F.min("entity_key").alias("disease_key"),
    )
    .filter(F.col("candidates") == 1)
    .select("variant_key", "source_snapshot", "condition_position", "disease_key")
)

# -- Transform -- label flag and provenance
condition = (
    conditions.join(
        linked, ["variant_key", "source_snapshot", "condition_position"], "left"
    )
    .withColumn(
        "disease_link_method",
        F.when(F.col("disease_key").isNotNull(), "ids")
        .when(F.size("condition_ids") == 0, "no_id")
        .otherwise("unlinked"),
    )
    .withColumn(
        "is_generic_label",
        F.lower(F.trim(F.col("condition_name"))).isin(
            RULES["generic_condition_labels"]
        ),
    )
    .withColumn("source_dataset", F.lit(CLINVAR))
    .withColumn(
        "_srid",
        sha_key(
            "variant_key",
            "source_snapshot",
            "condition_position",
            "clinvar_condition_accession",
            "condition_name",
        ),
    )
)
condition = conform_genomics(
    add_genomics_provenance(condition, "_srid", RID), "variant_condition"
)

# COMMAND ----------

# DBTITLE 1,Write, inspect and export -- variant_condition
write_silver(
    condition, "variant_condition", source=SOURCE, component=COMPONENT, rid=RID
)

written = spark.table(genomics_table("variant_condition"))
blocks = inspect_table(
    written,
    "variant_condition",
    source=FINDINGS,
    component=COMPONENT,
    rid=RID,
    key_cols=["source_record_id"],
    extra_checks={
        "pairing_mismatches": written.filter(
            F.col("pairing_status") == "count_mismatch"
        ).count(),
        "conditions_without_a_disease_key": written.filter(
            F.col("disease_key").isNull()
        ).count(),
        "generic_labels": written.filter(F.col("is_generic_label")).count(),
        **{
            f"{column}_counts": str(
                {
                    r[column]: r["count"]
                    for r in written.groupBy(column).count().collect()
                }
            )
            for column in written.columns
            if column.endswith("_link_method")
        },
    },
)

write_silver_findings(
    FINDINGS,
    f"{COMPONENT.split('/')[-1]}__variant_condition",
    "variant_condition",
    blocks,
)

# COMMAND ----------

# DBTITLE 1,Build -- variant_classification
# -- Transform -- the three classification types
DATASET = "clinvar.variant_summary"
frames = dict(variant_frames(RULES))
clinvar = frames[CLINVAR]
stars = F.create_map(
    *[F.lit(x) for pair in RULES["clinvar_review_stars"].items() for x in pair]
)

text = text_for(DATASET)


def date_of(column):
    return F.expr(f"cast(try_to_timestamp({column}, 'MMM dd, yyyy') as date)")


TYPES = [
    (
        "germline",
        "ClinicalSignificance",
        "ReviewStatus",
        "LastEvaluated",
        "SCVsForAggregateGermlineClassification",
    ),
    (
        "somatic_clinical_impact",
        "SomaticClinicalImpact",
        "ReviewStatusClinicalImpact",
        "SomaticClinicalImpactLastEvaluated",
        "SCVsForAggregateSomaticClinicalImpact",
    ),
    (
        "oncogenicity",
        "Oncogenicity",
        "ReviewStatusOncogenicity",
        "OncogenicityLastEvaluated",
        "SCVsForAggregateOncogenicityClassification",
    ),
]
parts = []
for kind, term, review, evaluated, scv in TYPES:
    germline = kind == "germline"
    parts.append(
        clinvar.select(
            "variant_key",
            F.lit(kind).alias("classification_type"),
            text(term).alias("term"),
            text(review).alias("review_status"),
            F.try_element_at(stars, F.lower(text(review))).alias("review_stars"),
            date_of(evaluated).alias("last_evaluated"),
            (
                typed_number(
                    "NumberSubmitters", "int", RULES, f"{DATASET}.NumberSubmitters"
                )
                if germline
                else F.lit(None).cast("int")
            ).alias("number_submitters"),
            (text("ClinSigSimple") if germline else F.lit(None).cast("string")).alias(
                "clinical_significance_simple"
            ),
            split_list(text(scv), r"[|,;]").alias("clinvar_submission_accessions"),
            F.lit(CLINVAR).alias("source_dataset"),
            F.col("snapshot").alias("source_snapshot"),
        ).filter(F.col("term").isNotNull())
    )
classification = _union(parts)

# -- Transform -- keys and provenance
classification = classification.distinct().withColumn(
    "_srid",
    sha_key(
        "variant_key",
        "source_snapshot",
        "classification_type",
        "term",
        "review_status",
        "last_evaluated",
        "clinvar_submission_accessions",
    ),
)
classification = conform_genomics(
    add_genomics_provenance(classification, "_srid", RID), "variant_classification"
)

# COMMAND ----------

# DBTITLE 1,Write, inspect and export -- variant_classification
write_silver(
    classification,
    "variant_classification",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
)

written = spark.table(genomics_table("variant_classification"))
blocks = inspect_table(
    written,
    "variant_classification",
    source=FINDINGS,
    component=COMPONENT,
    rid=RID,
    key_cols=["source_record_id"],
    extra_checks={
        "types": str(
            {
                r["classification_type"]: r["count"]
                for r in written.groupBy("classification_type").count().collect()
            }
        ),
        "review_statuses_without_stars": written.filter(
            F.col("review_status").isNotNull() & F.col("review_stars").isNull()
        ).count(),
        "rows_without_a_date": written.filter(F.col("last_evaluated").isNull()).count(),
        **{
            f"{column}_counts": str(
                {
                    r[column]: r["count"]
                    for r in written.groupBy(column).count().collect()
                }
            )
            for column in written.columns
            if column.endswith("_link_method")
        },
    },
)

write_silver_findings(
    FINDINGS,
    f"{COMPONENT.split('/')[-1]}__variant_classification",
    "variant_classification",
    blocks,
)
