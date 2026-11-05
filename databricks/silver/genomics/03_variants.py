# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
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
# MAGIC **Purpose:** variants with their notation, conditions, classifications, scores,
# MAGIC frequencies and feature overlaps.
# MAGIC
# MAGIC **Run:** `TABLES` lists the tables to build (default `all`); the others are skipped.

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

# DBTITLE 1,Imports
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "genomics"
COMPONENT = "silver/genomics/03_variants"
RID = run_id()
FINDINGS = "genomics"
RULES = load_genomics_rules()
AVAILABLE = [
    "variant",
    "variant_hgvs",
    "variant_condition",
    "variant_classification",
    "variant_score",
    "position_score",
    "variant_population_frequency",
    "variant_feature_overlap",
]
dbutils.widgets.text("TABLES", "all")
SELECTED = {
    name.strip() for name in dbutils.widgets.get("TABLES").split(",") if name.strip()
}
UNKNOWN = SELECTED - {"all", *AVAILABLE}
if UNKNOWN:
    message = f"unknown table(s) in TABLES: {sorted(UNKNOWN)}"
    raise ValueError(message)


def selected(table):
    return "all" in SELECTED or table in SELECTED


# COMMAND ----------

# DBTITLE 1,Read Silver -- claims and crosswalk
if selected("variant"):
    claims = read_silver("entity_claim")
    xwalk = read_silver("entity_id_crosswalk")
    id_map = variant_id_map(xwalk)

# COMMAND ----------

# DBTITLE 1,Transform -- coordinate rows of every source
if selected("variant"):
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
    core = _union(
        [frame.select(*CORE) for _, frame in variant_frames(RULES)] + [pharmgkb]
    )

# COMMAND ----------

# DBTITLE 1,Transform -- one record per variant
if selected("variant"):

    def number(column):
        return F.when(F.col(column).rlike(r"^\d+$"), F.col(column).cast("bigint"))

    records = (
        core.groupBy("variant_key")
        .agg(
            F.max(F.when(F.col("key_method") == "vcf_alleles", F.lit(1))).alias(
                "has_exact"
            ),
            F.max(
                F.when(F.col("v_assembly") != "unspecified", F.col("v_assembly"))
            ).alias("known_assembly"),
            F.max("v_contig").alias("contig"),
            F.max(number("v_position")).alias("position"),
            F.max("v_reference").alias("reference_allele"),
            F.max("v_alternate").alias("alternate_allele"),
            F.max(number("v_end")).alias("end_position"),
            F.sort_array(F.collect_set("v_source")).alias("source_datasets"),
        )
        .withColumn(
            "assembly", F.coalesce(F.col("known_assembly"), F.lit("unspecified"))
        )
    )

# COMMAND ----------

# DBTITLE 1,Transform -- resolve the fields, master and provenance
if selected("variant"):
    masters, conflicts = resolve_master(
        claims, entity_type="variant", fields=VARIANT_FIELDS
    )
    masters = masters.withColumnRenamed("entity_key", "variant_key")
    variant = (
        records.join(masters, "variant_key", "left")
        .withColumn(
            "key_source",
            F.when(
                F.col("variant_key").rlike(
                    r"^(clinvar_allele|dbvar|pharmgkb_variant):"
                ),
                "own_source_id",
            )
            .when(F.col("has_exact").isNotNull(), "vcf_alleles")
            .otherwise("vcf_alleles_assumed_grch38"),
        )
        .withColumn("_srid", F.col("variant_key"))
    )
    variant = conform_genomics(add_genomics_provenance(variant, "_srid", RID), TABLE)

# COMMAND ----------

# DBTITLE 1,Write Silver -- variant
if selected("variant"):
    write_silver(variant, "variant", source=SOURCE, component=COMPONENT, rid=RID)

# COMMAND ----------

# DBTITLE 1,Inspect -- variant
if selected("variant"):
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
            "variants_without_a_gene": written.filter(
                F.col("gene_key").isNull()
            ).count(),
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

# COMMAND ----------

# DBTITLE 1,Export findings -- variant
if selected("variant"):
    write_silver_findings(
        FINDINGS, f"{COMPONENT.split('/')[-1]}__variant", "variant", blocks
    )

# COMMAND ----------

# DBTITLE 1,Read Silver -- transcript ids
if selected("variant_hgvs"):
    xwalk = read_silver("entity_id_crosswalk")
    transcripts = entity_id_map(xwalk, "transcript")

# COMMAND ----------

# DBTITLE 1,Transform -- ClinVar names
if selected("variant_hgvs"):
    frames = dict(variant_frames(RULES))
    name_column = clean_placeholders("Name", RULES, "clinvar.variant_summary.Name")
    parsed = (
        frames[CLINVAR]
        .select("variant_key", name_column.alias("name"))
        .filter(F.col("name").isNotNull())
        .withColumn("parsed", hgvs_name_udf()(F.col("name")))
    )
    clinvar = parsed.select(
        "variant_key",
        *[F.col(f"parsed.{n}").alias(n) for n, _ in HGVS_NAME_FIELDS],
        F.lit(CLINVAR).alias("source_dataset"),
        F.lit("Name").alias("source_field"),
    )

# COMMAND ----------

# DBTITLE 1,Transform -- AlphaMissense protein changes
if selected("variant_hgvs"):
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
    )

# COMMAND ----------

# DBTITLE 1,Transform -- transcript links and provenance
if selected("variant_hgvs"):
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
        ),
    )
    hgvs = conform_genomics(add_genomics_provenance(hgvs, "_srid", RID), TABLE)

# COMMAND ----------

# DBTITLE 1,Write Silver -- variant_hgvs
if selected("variant_hgvs"):
    write_silver(hgvs, "variant_hgvs", source=SOURCE, component=COMPONENT, rid=RID)

# COMMAND ----------

# DBTITLE 1,Inspect -- variant_hgvs
if selected("variant_hgvs"):
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
                (F.col("source_dataset") == CLINVAR)
                & (F.col("parse_status") == "unparsed")
            ).count(),
            "clinvar_partial": written.filter(
                (F.col("source_dataset") == CLINVAR)
                & (F.col("parse_status") == "partial")
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

# COMMAND ----------

# DBTITLE 1,Export findings -- variant_hgvs
if selected("variant_hgvs"):
    write_silver_findings(
        FINDINGS, f"{COMPONENT.split('/')[-1]}__variant_hgvs", "variant_hgvs", blocks
    )

# COMMAND ----------

# DBTITLE 1,Read Silver -- disease ids
if selected("variant_condition"):
    xwalk = read_silver("entity_id_crosswalk")
    disease_map = entity_id_map(xwalk, "disease")

# COMMAND ----------

# DBTITLE 1,Transform -- pair ids, names and accessions
if selected("variant_condition"):
    DATASET = "clinvar.variant_summary"
    frames = dict(variant_frames(RULES))

    def text(column):
        return clean_placeholders(column, RULES, f"{DATASET}.{column}")

    parsed = frames[CLINVAR].select(
        "variant_key",
        F.explode(
            clinvar_conditions_udf()(
                text("PhenotypeIDS"), text("PhenotypeList"), text("RCVaccession")
            )
        ).alias("c"),
    )
    conditions = parsed.select(
        "variant_key",
        F.col("c.position").alias("condition_position"),
        F.col("c.name").alias("condition_name"),
        F.col("c.accession").alias("clinvar_condition_accession"),
        F.col("c.ids").alias("condition_ids"),
        F.col("c.pairing_status").alias("pairing_status"),
    ).distinct()

# COMMAND ----------

# DBTITLE 1,Transform -- disease links through exact ids
if selected("variant_condition"):
    ids = conditions.select(
        "variant_key", "condition_position", F.explode("condition_ids").alias("raw_id")
    )
    id_type, id_value = split_curie("raw_id", RULES)
    linked = (
        ids.withColumn("id_type", id_type)
        .withColumn("id_value", id_value)
        .join(disease_map, ["id_type", "id_value"], "inner")
        .groupBy("variant_key", "condition_position")
        .agg(
            F.countDistinct("entity_key").alias("candidates"),
            F.min("entity_key").alias("disease_key"),
        )
        .filter(F.col("candidates") == 1)
        .select("variant_key", "condition_position", "disease_key")
    )

# COMMAND ----------

# DBTITLE 1,Transform -- label flag and provenance
if selected("variant_condition"):
    condition = (
        conditions.join(linked, ["variant_key", "condition_position"], "left")
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
                "condition_position",
                "clinvar_condition_accession",
                "condition_name",
            ),
        )
    )
    condition = conform_genomics(
        add_genomics_provenance(condition, "_srid", RID), TABLE
    )

# COMMAND ----------

# DBTITLE 1,Write Silver -- variant_condition
if selected("variant_condition"):
    write_silver(
        condition, "variant_condition", source=SOURCE, component=COMPONENT, rid=RID
    )

# COMMAND ----------

# DBTITLE 1,Inspect -- variant_condition
if selected("variant_condition"):
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

# COMMAND ----------

# DBTITLE 1,Export findings -- variant_condition
if selected("variant_condition"):
    write_silver_findings(
        FINDINGS,
        f"{COMPONENT.split('/')[-1]}__variant_condition",
        "variant_condition",
        blocks,
    )

# COMMAND ----------

# DBTITLE 1,Transform -- the three classification types
if selected("variant_classification"):
    DATASET = "clinvar.variant_summary"
    frames = dict(variant_frames(RULES))
    clinvar = frames[CLINVAR]
    stars = F.create_map(
        *[F.lit(x) for pair in RULES["clinvar_review_stars"].items() for x in pair]
    )

    def text(column):
        return clean_placeholders(column, RULES, f"{DATASET}.{column}")

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
                (
                    text("ClinSigSimple") if germline else F.lit(None).cast("string")
                ).alias("clinical_significance_simple"),
                split_list(text(scv), r"[|,;]").alias("clinvar_submission_accessions"),
                F.lit(CLINVAR).alias("source_dataset"),
            ).filter(F.col("term").isNotNull())
        )
    classification = _union(parts)

# COMMAND ----------

# DBTITLE 1,Transform -- keys and provenance
if selected("variant_classification"):
    classification = classification.distinct().withColumn(
        "_srid", sha_key("variant_key", "classification_type", "term", "review_status")
    )
    classification = conform_genomics(
        add_genomics_provenance(classification, "_srid", RID), TABLE
    )

# COMMAND ----------

# DBTITLE 1,Write Silver -- variant_classification
if selected("variant_classification"):
    write_silver(
        classification,
        "variant_classification",
        source=SOURCE,
        component=COMPONENT,
        rid=RID,
    )

# COMMAND ----------

# DBTITLE 1,Inspect -- variant_classification
if selected("variant_classification"):
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
            "rows_without_a_date": written.filter(
                F.col("last_evaluated").isNull()
            ).count(),
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

# COMMAND ----------

# DBTITLE 1,Export findings -- variant_classification
if selected("variant_classification"):
    write_silver_findings(
        FINDINGS,
        f"{COMPONENT.split('/')[-1]}__variant_classification",
        "variant_classification",
        blocks,
    )

# COMMAND ----------

# DBTITLE 1,Transform -- scores of every source
if selected("variant_score"):
    frames = dict(variant_frames(RULES))
    none = F.lit(None).cast("string")

    def number(column, dataset):
        return typed_number(column, "double", RULES, f"{dataset}.{column}")

    alpha = frames[ALPHAMISSENSE].select(
        "variant_key",
        F.lit("alphamissense_pathogenicity").alias("score_name"),
        number("am_pathogenicity", ALPHAMISSENSE).alias("score_value"),
        clean_placeholders("am_class", RULES, f"{ALPHAMISSENSE}.am_class").alias(
            "score_label"
        ),
        clean_placeholders(
            "transcript_id", RULES, f"{ALPHAMISSENSE}.transcript_id"
        ).alias("context"),
        F.lit(ALPHAMISSENSE).alias("source_dataset"),
        F.lit("am_pathogenicity").alias("source_field"),
    )
    cadd = frames[CADD].select(
        "variant_key",
        F.lit("cadd_phred").alias("score_name"),
        number("phred", CADD).alias("score_value"),
        none.alias("score_label"),
        none.alias("context"),
        F.lit(CADD).alias("source_dataset"),
        F.lit("phred").alias("source_field"),
    )
    gpn = [
        frames[CLINVAR_BENIGN].select(
            "variant_key",
            F.lit(column.lower()).alias("score_name"),
            number(column, CLINVAR_BENIGN).alias("score_value"),
            none.alias("score_label"),
            none.alias("context"),
            F.lit(CLINVAR_BENIGN).alias("source_dataset"),
            F.lit(column).alias("source_field"),
        )
        for column in ("GPN_Star_V100", "GPN_Star_P243", "GPN_Star_M447")
    ]
    scores = _union([alpha, cadd, *gpn]).filter(F.col("score_value").isNotNull())

# COMMAND ----------

# DBTITLE 1,Transform -- keys and provenance
if selected("variant_score"):
    scores = scores.distinct().withColumn(
        "_srid",
        sha_key(
            "variant_key", "score_name", "source_dataset", "context", "score_value"
        ),
    )
    scores = conform_genomics(add_genomics_provenance(scores, "_srid", RID), TABLE)

# COMMAND ----------

# DBTITLE 1,Write Silver -- variant_score
if selected("variant_score"):
    write_silver(scores, "variant_score", source=SOURCE, component=COMPONENT, rid=RID)

# COMMAND ----------

# DBTITLE 1,Inspect -- variant_score
if selected("variant_score"):
    written = spark.table(genomics_table("variant_score"))
    blocks = inspect_table(
        written,
        "variant_score",
        source=FINDINGS,
        component=COMPONENT,
        rid=RID,
        key_cols=["source_record_id"],
        extra_checks={
            "scores": str(
                {
                    r["score_name"]: r["count"]
                    for r in written.groupBy("score_name").count().collect()
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

# COMMAND ----------

# DBTITLE 1,Export findings -- variant_score
if selected("variant_score"):
    write_silver_findings(
        FINDINGS, f"{COMPONENT.split('/')[-1]}__variant_score", "variant_score", blocks
    )

# COMMAND ----------

# DBTITLE 1,Transform -- both conservation scores
if selected("position_score"):
    SOURCES = [
        (
            "ucsc_conservation_phylop_100way",
            "ucsc_conservation.phylop_100way",
            "phylop_100way",
        ),
        (
            "ucsc_conservation_phastcons_100way",
            "ucsc_conservation.phastcons_100way",
            "phastcons_100way",
        ),
    ]
    parts = []
    for table, dataset, name in SOURCES:
        parts.append(
            read_genomics_bronze(table).select(
                F.lit("unspecified").alias("assembly"),
                chromosome_expr(
                    clean_placeholders("chromosome", RULES, f"{dataset}.chromosome")
                ).alias("contig"),
                typed_number("position", "bigint", RULES, f"{dataset}.position").alias(
                    "position"
                ),
                F.lit(name).alias("score_name"),
                typed_number("score", "double", RULES, f"{dataset}.score").alias(
                    "score_value"
                ),
                F.lit(dataset).alias("source_dataset"),
            )
        )
    position = _union(parts).filter(
        F.col("contig").isNotNull()
        & F.col("position").isNotNull()
        & F.col("score_value").isNotNull()
    )

# COMMAND ----------

# DBTITLE 1,Transform -- keys and provenance
if selected("position_score"):
    position = position.distinct().withColumn(
        "_srid", sha_key("assembly", "contig", "position", "score_name")
    )
    position = conform_genomics(add_genomics_provenance(position, "_srid", RID), TABLE)

# COMMAND ----------

# DBTITLE 1,Write Silver -- position_score
if selected("position_score"):
    write_silver(
        position, "position_score", source=SOURCE, component=COMPONENT, rid=RID
    )

# COMMAND ----------

# DBTITLE 1,Inspect -- position_score
if selected("position_score"):
    written = spark.table(genomics_table("position_score"))
    blocks = inspect_table(
        written,
        "position_score",
        source=FINDINGS,
        component=COMPONENT,
        rid=RID,
        key_cols=["source_record_id"],
        extra_checks={
            "scores": str(
                {
                    r["score_name"]: r["count"]
                    for r in written.groupBy("score_name").count().collect()
                }
            ),
            "contigs": written.select("contig").distinct().count(),
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

# COMMAND ----------

# DBTITLE 1,Export findings -- position_score
if selected("position_score"):
    write_silver_findings(
        FINDINGS,
        f"{COMPONENT.split('/')[-1]}__position_score",
        "position_score",
        blocks,
    )

# COMMAND ----------

# DBTITLE 1,Transform -- long rows per population
if selected("variant_population_frequency"):
    frames = dict(variant_frames(RULES))
    gnomad = frames[GNOMAD]
    none_long = F.lit(None).cast("bigint")

    def number(column, kind="double"):
        return typed_number(column, kind, RULES, f"{GNOMAD}.{column}")

    status = clean_placeholders("gnomad_filters", RULES, f"{GNOMAD}.gnomad_filters")
    POPULATIONS = [
        "afr",
        "ami",
        "amr",
        "asj",
        "eas",
        "fin",
        "mid",
        "nfe",
        "sas",
        "remaining",
    ]

    def rows(
        population,
        measure,
        frequency,
        count=none_long,
        total=none_long,
        homozygotes=none_long,
    ):
        return gnomad.select(
            "variant_key",
            population.alias("population"),
            F.lit(measure).alias("measure"),
            frequency.alias("allele_frequency"),
            count.alias("allele_count"),
            total.alias("allele_number"),
            homozygotes.alias("homozygote_count"),
            status.alias("filter_status"),
            F.lit(GNOMAD).alias("source_dataset"),
        )

    group = clean_placeholders("gnomad_grpmax", RULES, f"{GNOMAD}.gnomad_grpmax")
    parts = [
        rows(
            F.lit("all"),
            "allele_frequency",
            number("gnomad_af"),
            number("gnomad_ac", "bigint"),
            number("gnomad_an", "bigint"),
            number("gnomad_nhomalt", "bigint"),
        )
    ]
    parts += [
        rows(F.lit(pop), "allele_frequency", number(f"gnomad_af_{pop}"))
        for pop in POPULATIONS
    ]
    parts += [
        rows(group, "highest_group_allele_frequency", number("gnomad_af_grpmax")),
        rows(group, "filtering_allele_frequency_95", number("gnomad_faf95_max")),
        rows(
            F.lit("max_ancestry"),
            "highest_ancestry_allele_frequency",
            number("gnomad_af_max_ancestry"),
        ),
    ]
    frequency = _union(parts).filter(
        F.col("allele_frequency").isNotNull() & F.col("population").isNotNull()
    )

# COMMAND ----------

# DBTITLE 1,Transform -- keys and provenance
if selected("variant_population_frequency"):
    frequency = frequency.distinct().withColumn(
        "_srid", sha_key("variant_key", "population", "measure", "source_dataset")
    )
    frequency = conform_genomics(
        add_genomics_provenance(frequency, "_srid", RID), TABLE
    )

# COMMAND ----------

# DBTITLE 1,Write Silver -- variant_population_frequency
if selected("variant_population_frequency"):
    write_silver(
        frequency,
        "variant_population_frequency",
        source=SOURCE,
        component=COMPONENT,
        rid=RID,
    )

# COMMAND ----------

# DBTITLE 1,Inspect -- variant_population_frequency
if selected("variant_population_frequency"):
    written = spark.table(genomics_table("variant_population_frequency"))
    blocks = inspect_table(
        written,
        "variant_population_frequency",
        source=FINDINGS,
        component=COMPONENT,
        rid=RID,
        key_cols=["source_record_id"],
        extra_checks={
            "measures": str(
                {
                    r["measure"]: r["count"]
                    for r in written.groupBy("measure").count().collect()
                }
            ),
            "populations": written.select("population").distinct().count(),
            "frequencies_above_one": written.filter(
                F.col("allele_frequency") > 1
            ).count(),
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

# COMMAND ----------

# DBTITLE 1,Export findings -- variant_population_frequency
if selected("variant_population_frequency"):
    write_silver_findings(
        FINDINGS,
        f"{COMPONENT.split('/')[-1]}__variant_population_frequency",
        "variant_population_frequency",
        blocks,
    )

# COMMAND ----------

# DBTITLE 1,Read Silver -- variants, intervals, protein positions
if selected("variant_feature_overlap"):
    BIN = 100000
    variants = (
        read_silver("variant")
        .filter(
            (F.col("assembly") == "GRCh38")
            & F.col("position").isNotNull()
            & F.col("contig").isNotNull()
        )
        .select(
            "variant_key",
            "contig",
            F.col("position").alias("start_position"),
            (
                F.col("position")
                + F.greatest(F.length("reference_allele"), F.lit(1))
                - 1
            ).alias("end_position"),
        )
        .withColumn("bin", F.floor(F.col("start_position") / BIN))
    )
    coordinates = read_silver("entity_coordinate").filter(
        (F.col("assembly") == "GRCh38") & F.col("is_primary")
    )
    gene_types = read_silver("gene").select(
        "gene_key", F.col("gene_type").alias("feature_type")
    )
    transcript_types = read_silver("transcript").select(
        "transcript_key", F.col("transcript_type").alias("feature_type")
    )
    element_types = (
        read_silver("entity_claim")
        .filter(
            (F.col("entity_type") == "regulatory_element")
            & (F.col("field") == "ccre_class")
        )
        .select(
            F.col("entity_key").alias("element_key"),
            F.col("value_text").alias("feature_type"),
        )
        .distinct()
    )

# COMMAND ----------

# DBTITLE 1,Transform -- interval overlaps (binned range join)
if selected("variant_feature_overlap"):

    def features_of(entity_type, types, key_column):
        frame = coordinates.filter(F.col("entity_type") == entity_type).select(
            F.col("entity_key").alias("feature_key"),
            F.col("contig").alias("f_contig"),
            F.col("start_position").alias("f_start"),
            F.col("end_position").alias("f_end"),
        )
        if types is None:
            return frame.withColumn("feature_type", F.lit(None).cast("string"))
        return frame.join(
            types, frame["feature_key"] == types[key_column], "left"
        ).drop(key_column)

    def overlaps(features, kind, source):
        spread = features.withColumn(
            "bin",
            F.explode(
                F.sequence(
                    F.floor(F.col("f_start") / BIN), F.floor(F.col("f_end") / BIN)
                )
            ),
        )
        joined = variants.join(
            spread,
            (variants["contig"] == spread["f_contig"])
            & (variants["bin"] == spread["bin"])
            & (variants["start_position"] <= spread["f_end"])
            & (variants["end_position"] >= spread["f_start"]),
        )
        return joined.select(
            "variant_key",
            F.lit(kind).alias("feature_kind"),
            "feature_key",
            "feature_type",
            F.lit("interval").alias("overlap_method"),
            F.lit(source).alias("source_dataset"),
        ).distinct()

    gene_overlap = overlaps(
        features_of("gene", gene_types, "gene_key"), "gene", "entity_coordinate"
    )
    transcript_overlap = overlaps(
        features_of("transcript", transcript_types, "transcript_key"),
        "transcript",
        "entity_coordinate",
    )
    element_overlap = overlaps(
        features_of("regulatory_element", element_types, "element_key"),
        "regulatory_element",
        "entity_coordinate",
    )
    parts = [gene_overlap, transcript_overlap, element_overlap]

# COMMAND ----------

# DBTITLE 1,Transform -- exon, CDS and UTR rows (when Bronze holds them)
if selected("variant_feature_overlap"):
    GENCODE = "gencode.annotation"
    structure = read_genomics_bronze("gencode_annotation").filter(
        F.col("feature").isin("exon", "CDS", "UTR")
    )
    structure_features = structure.select(
        ensembl_unversioned_expr(F.col("transcript_id")).alias("feature_key"),
        chromosome_expr("seqname").alias("f_contig"),
        typed_number("start", "bigint", RULES, f"{GENCODE}.start").alias("f_start"),
        typed_number("end", "bigint", RULES, f"{GENCODE}.end").alias("f_end"),
        F.col("feature").alias("feature_type"),
    ).filter(F.col("f_start").isNotNull() & F.col("f_end").isNotNull())
    for kind in ("exon", "CDS", "UTR"):
        parts.append(
            overlaps(
                structure_features.filter(F.col("feature_type") == kind),
                kind.lower(),
                GENCODE,
            )
        )

# COMMAND ----------

# DBTITLE 1,Transform -- protein features by protein position
if selected("variant_feature_overlap"):
    hgvs = (
        read_silver("variant_hgvs")
        .filter(
            F.col("protein_position").isNotNull() & F.col("transcript_key").isNotNull()
        )
        .select("variant_key", "transcript_key", "protein_position")
    )
    proteins = (
        read_silver("transcript")
        .filter(F.col("protein_key").isNotNull())
        .select("transcript_key", "protein_key")
    )
    protein_features = read_silver("protein_feature").select(
        "protein_key",
        F.col("source_record_id").alias("feature_key"),
        "feature_type",
        "start_position",
        "end_position",
    )
    protein_overlap = (
        hgvs.join(proteins, "transcript_key")
        .join(protein_features, "protein_key")
        .filter(
            (F.col("start_position") <= F.col("protein_position"))
            & (F.col("end_position") >= F.col("protein_position"))
        )
        .select(
            "variant_key",
            F.lit("protein_feature").alias("feature_kind"),
            "feature_key",
            "feature_type",
            F.lit("protein_position").alias("overlap_method"),
            F.lit("protein_feature").alias("source_dataset"),
        )
        .distinct()
    )
    parts.append(protein_overlap)

# COMMAND ----------

# DBTITLE 1,Transform -- union and provenance
if selected("variant_feature_overlap"):
    overlap = _union(parts).withColumn(
        "_srid", sha_key("variant_key", "feature_kind", "feature_key", "overlap_method")
    )
    overlap = conform_genomics(add_genomics_provenance(overlap, "_srid", RID), TABLE)

# COMMAND ----------

# DBTITLE 1,Write Silver -- variant_feature_overlap
if selected("variant_feature_overlap"):
    write_silver(
        overlap, "variant_feature_overlap", source=SOURCE, component=COMPONENT, rid=RID
    )

# COMMAND ----------

# DBTITLE 1,Inspect -- variant_feature_overlap
if selected("variant_feature_overlap"):
    written = spark.table(genomics_table("variant_feature_overlap"))
    blocks = inspect_table(
        written,
        "variant_feature_overlap",
        source=FINDINGS,
        component=COMPONENT,
        rid=RID,
        key_cols=["source_record_id"],
        extra_checks={
            "kinds": str(
                {
                    r["feature_kind"]: r["count"]
                    for r in written.groupBy("feature_kind").count().collect()
                }
            ),
            "exon_rows_in_bronze": structure.count(),
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

# COMMAND ----------

# DBTITLE 1,Export findings -- variant_feature_overlap
if selected("variant_feature_overlap"):
    write_silver_findings(
        FINDINGS,
        f"{COMPONENT.split('/')[-1]}__variant_feature_overlap",
        "variant_feature_overlap",
        blocks,
    )
